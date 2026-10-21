// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package opensearchexporter

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestIsRetryableError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"flush deadline", fmt.Errorf("flush: %w", context.DeadlineExceeded), true},
		{"canceled", context.Canceled, true},
		{"url timeout", &url.Error{Op: "Post", URL: "https://os:9200/_bulk", Err: timeoutErr{}}, true},
		{"http 429", fmt.Errorf("flush: %w", &opensearch.StructError{Status: http.StatusTooManyRequests}), true},
		{"http 503", &opensearch.StructError{Status: http.StatusServiceUnavailable}, true},
		{"http 502 string body", &opensearch.StringError{Status: http.StatusBadGateway, Err: "<html>bad gateway</html>"}, true},
		{"proxy html body", fmt.Errorf("flush: %w: %w", opensearch.ErrJSONUnmarshalBody, errors.New("invalid character '<'")), true},
		{"http 400", &opensearch.StructError{Status: http.StatusBadRequest}, false},
		{"http 413", &opensearch.StringError{Status: http.StatusRequestEntityTooLarge}, false},
		{"already permanent", consumererror.NewPermanent(context.DeadlineExceeded), false},
		{"encoding error", errors.New("json: unsupported value"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isRetryableError(tt.err))
		})
	}
}

func testLogs(n int) plog.Logs {
	ld := plog.NewLogs()
	sl := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	for i := range n {
		sl.LogRecords().AppendEmpty().Body().SetStr(fmt.Sprintf("record %d", i))
	}
	return ld
}

// bulkItems answers an NDJSON _bulk body with one response item per action line.
func bulkItems(t *testing.T, r *http.Request, item func(i int, index string) map[string]any) []byte {
	t.Helper()
	var items []map[string]any
	sc := bufio.NewScanner(r.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	i := 0
	for sc.Scan() {
		var action map[string]map[string]any
		require.NoError(t, json.Unmarshal(sc.Bytes(), &action))
		var op string
		var meta map[string]any
		for k, v := range action {
			op, meta = k, v
		}
		require.True(t, sc.Scan(), "missing document line")
		index, _ := meta["_index"].(string)
		items = append(items, map[string]any{op: item(i, index)})
		i++
	}
	errorsFlag := false
	for _, it := range items {
		for _, v := range it {
			if s, _ := v.(map[string]any)["status"].(int); s > 201 {
				errorsFlag = true
			}
		}
	}
	out, err := json.Marshal(map[string]any{"took": 1, "errors": errorsFlag, "items": items})
	require.NoError(t, err)
	return out
}

func pushThroughIndexer(ctx context.Context, t *testing.T, srv *httptest.Server, ld plog.Logs, onErrorIndex string) error {
	t.Helper()
	client, err := newOpenSearchClient(srv.URL, srv.Client(), zap.NewNop())
	require.NoError(t, err)
	cfg := &Config{LogsIndex: "syslog", BulkAction: "create"}
	lbi := newLogBulkIndexer("create", &encodeModel{}, "", &ErrorClassConfig{}, onErrorIndex, nil, zap.NewNop(), 1, 0)
	require.NoError(t, lbi.start(client))
	lbi.submit(ctx, ld, newIndexResolver("ss4o_logs", "", ""), cfg, time.Now())
	lbi.close(ctx)
	if ferr := lbi.flushOnErrorIndex(ctx, client); ferr != nil {
		lbi.appendPermanentError(ferr)
	}
	return lbi.joinedError()
}

func requireRetryAll(t *testing.T, err error, want int) {
	t.Helper()
	require.Error(t, err)
	assert.False(t, consumererror.IsPermanent(err), "must not be permanent: %v", err)
	var logsErr consumererror.Logs
	require.ErrorAs(t, err, &logsErr)
	assert.Equal(t, want, logsErr.Data().LogRecordCount(), "retry payload must carry every failed record")
}

// A bulk request that runs into the exporter timeout must be retried, not dropped as permanent.
// Before the fix this returned "Permanent error: flush: context deadline exceeded", which made
// exporterhelper skip retry_on_failure and pushed the failover connector to its next level.
func TestLogBulkIndexerTimeoutIsRetryable(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	defer close(release)

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	err := pushThroughIndexer(ctx, t, srv, testLogs(3), "syslog-deadletter")
	requireRetryAll(t, err, 3)
	assert.Equal(t, 1, strings.Count(err.Error(), "context deadline exceeded"), "one line per distinct error, not per record: %v", err)
}

// A whole-request 429 or 5xx (coordinator overloaded, proxy error) is retryable.
func TestLogBulkIndexerHTTPStatusIsRetryable(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusServiceUnavailable, http.StatusGatewayTimeout} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				_, _ = fmt.Fprintf(w, `{"error":{"type":"es_rejected_execution_exception","reason":"busy"},"status":%d}`, status)
			}))
			defer srv.Close()
			requireRetryAll(t, pushThroughIndexer(t.Context(), t, srv, testLogs(3), "syslog-deadletter"), 3)
		})
	}
}

// Mapping errors go to the dead-letter index; transient item errors are retried, and all of them
// are retried, not only the first one (exporterhelper only takes the first consumererror.Logs).
func TestLogBulkIndexerMixedItemErrors(t *testing.T) {
	var deadletterDocs atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(bulkItems(t, r, func(i int, index string) map[string]any {
			if strings.HasSuffix(index, "deadletter") {
				deadletterDocs.Add(1)
				return map[string]any{"_index": index, "status": 201}
			}
			switch i {
			case 0:
				return map[string]any{"_index": index, "status": 400, "error": map[string]any{"type": "mapper_parsing_exception", "reason": "failed to parse field"}}
			case 1, 2:
				return map[string]any{"_index": index, "status": 429, "error": map[string]any{"type": "es_rejected_execution_exception", "reason": "queue full"}}
			default:
				return map[string]any{"_index": index, "status": 201}
			}
		}))
	}))
	defer srv.Close()

	err := pushThroughIndexer(t.Context(), t, srv, testLogs(4), "syslog-deadletter")
	requireRetryAll(t, err, 2)
	assert.Equal(t, int32(1), deadletterDocs.Load(), "mapping error must be written to the dead-letter index")
}

// Mapping errors alone are not an export failure when a dead-letter index is configured.
func TestLogBulkIndexerMappingErrorGoesToDeadletter(t *testing.T) {
	var deadletterDocs atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(bulkItems(t, r, func(_ int, index string) map[string]any {
			if strings.HasSuffix(index, "deadletter") {
				deadletterDocs.Add(1)
				return map[string]any{"_index": index, "status": 201}
			}
			return map[string]any{"_index": index, "status": 400, "error": map[string]any{"type": "mapper_parsing_exception", "reason": "failed to parse field"}}
		}))
	}))
	defer srv.Close()

	require.NoError(t, pushThroughIndexer(t.Context(), t, srv, testLogs(2), "syslog-deadletter"))
	assert.Equal(t, int32(2), deadletterDocs.Load())
}

// Without a dead-letter index a mapping error stays permanent, so it is not replayed forever.
func TestLogBulkIndexerMappingErrorWithoutDeadletterIsPermanent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(bulkItems(t, r, func(_ int, index string) map[string]any {
			return map[string]any{"_index": index, "status": 400, "error": map[string]any{"type": "mapper_parsing_exception", "reason": "failed to parse field"}}
		}))
	}))
	defer srv.Close()

	err := pushThroughIndexer(t.Context(), t, srv, testLogs(2), "")
	require.Error(t, err)
	assert.True(t, consumererror.IsPermanent(err))
}

// A batch above the bulk indexer's 5MB flush threshold is flushed by its worker while records
// are still being added. If the exporter timeout fires during that flush, Close returns before
// the worker finishes: the whole batch must be retried, and the worker's late item callbacks
// must not modify the records handed back for retry (the retry sender re-encodes them).
func TestLogBulkIndexerTimeoutDuringWorkerFlush(t *testing.T) {
	release := make(chan struct{})
	responded := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(bulkItems(t, r, func(_ int, index string) map[string]any {
			return map[string]any{"_index": index, "status": 429, "error": map[string]any{"type": "es_rejected_execution_exception", "reason": "queue full"}}
		}))
		close(responded)
	}))
	defer srv.Close()

	ld := plog.NewLogs()
	sl := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	big := strings.Repeat("x", 100_000)
	for range 80 {
		sl.LogRecords().AppendEmpty().Body().SetStr(big)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	err := pushThroughIndexer(ctx, t, srv, ld, "syslog-deadletter")
	requireRetryAll(t, err, 80)

	close(release)
	<-responded
	logsErr, _ := errors.AsType[consumererror.Logs](err)
	records := logsErr.Data().ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	assert.Never(t, func() bool {
		for i := range records.Len() {
			if _, ok := records.At(i).Attributes().Get("opensearch.error.status"); ok {
				return true
			}
		}
		return false
	}, 500*time.Millisecond, 10*time.Millisecond, "late worker callbacks modified the retry payload")
}

// Records handed back for retry are re-encoded on the next attempt, so they must be the records
// as received: error attributes stamped on them end up in the document and a strict mapping on
// attributes rejects every retried record. The input batch must not be modified either.
func TestLogBulkIndexerRetryPayloadIsUnmodified(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(bulkItems(t, r, func(i int, index string) map[string]any {
			if strings.HasSuffix(index, "deadletter") {
				return map[string]any{"_index": index, "status": 201}
			}
			switch i {
			case 0:
				return map[string]any{"_index": index, "status": 400, "error": map[string]any{"type": "mapper_parsing_exception", "reason": "failed to parse field"}}
			case 1, 2:
				return map[string]any{"_index": index, "status": 429, "error": map[string]any{"type": "es_rejected_execution_exception", "reason": "queue full"}}
			default:
				return map[string]any{"_index": index, "status": 201}
			}
		}))
	}))
	defer srv.Close()

	ld := testLogs(4)
	var m plog.JSONMarshaler
	before, err := m.MarshalLogs(ld)
	require.NoError(t, err)

	err = pushThroughIndexer(t.Context(), t, srv, ld, "syslog-deadletter")
	requireRetryAll(t, err, 2)

	after, merr := m.MarshalLogs(ld)
	require.NoError(t, merr)
	assert.JSONEq(t, string(before), string(after), "input logs were modified")

	logsErr, _ := errors.AsType[consumererror.Logs](err)
	records := logsErr.Data().ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	for i := range records.Len() {
		assert.Zero(t, records.At(i).Attributes().Len(), "retried record %d carries attributes: %v", i, records.At(i).Attributes().AsRaw())
	}
}
