// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package opensearchexporter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Timeout=0 preserves the legacy single-ctx behavior: a cancelled parent propagates to
// Add and the flush exits via "enqueue" failures. This tests the opt-out through the
// real flush path, not an isolated context helper.
func TestFlushOnErrorIndexLegacyModeHonorsParentCancel(t *testing.T) {
	const docCount = 4

	var deadletterSeen bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "_bulk") && readContains(t, r, "deadletter") {
			deadletterSeen = true
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(bulkItems(t, r, func(_ int, index string) map[string]any {
			if !strings.HasSuffix(index, "deadletter") {
				return map[string]any{"_index": index, "status": 400, "error": map[string]any{"type": "mapper_parsing_exception", "reason": "bad field"}}
			}
			return map[string]any{"_index": index, "status": 503, "error": map[string]any{"type": "es_rejected_execution_exception", "reason": "queue full"}}
		}))
	}))
	defer srv.Close()

	m, reader := newTestMetrics(t)
	client, err := newOpenSearchClient(srv.URL, srv.Client(), zap.NewNop())
	require.NoError(t, err)
	cfg := &Config{LogsIndex: "syslog", BulkAction: "create"}
	lbi := newLogBulkIndexer("create", &encodeModel{}, "", &ErrorClassConfig{}, "syslog-deadletter", m, zap.NewNop(), 0)
	require.NoError(t, lbi.start(client))

	parent, cancel := context.WithCancel(t.Context())
	lbi.submit(parent, testLogs(docCount), newIndexResolver("ss4o_logs", "", ""), cfg, time.Now())
	lbi.close(parent)
	// Cancel before the dead-letter flush. In legacy mode this must propagate to Add
	// and bucket every doc under "enqueue" — the dead-letter server endpoint must
	// never be reached.
	cancel()
	if ferr := lbi.flushOnErrorIndex(parent, client, 0); ferr != nil {
		lbi.appendPermanentError(ferr)
	}

	// Server must never see a successful dead-letter request: in legacy mode the
	// cancelled parent flows through Add straight into opensearchutil, so no doc can
	// complete an HTTP round-trip. This is the opt-out guarantee.
	assert.False(t, deadletterSeen, "legacy mode (timeout=0) must honor parent cancel: server must not see a successful dead-letter request")

	// Legacy mode's known trade-off: Close inherits the cancelled ctx and may return
	// before wg.Wait() completes, so a small number of docs can be dropped without a
	// recorded fate. We assert at least some failures were recorded (opt-out isn't a
	// total black hole) and none were silently misattributed to item_indexing (which
	// would imply the server responded).
	failures := collectInt64Sum(t, reader, "otelcol_opensearch_exporter_on_error_flush_failures")
	var total int64
	for reason, v := range failures {
		assert.NotContains(t, reason, "item_indexing", "cancelled parent must not produce item_indexing results, got %v", failures)
		total += v
	}
	assert.Positive(t, total, "legacy mode must record at least some failures, got %v", failures)
	assert.LessOrEqual(t, total, int64(docCount), "failures must not exceed submitted doc count, got %v", failures)
}

// Positive timeout detaches the Add ctx from an expired parent deadline: dead-letter
// writes still reach the server and land on real item fates.
func TestFlushOnErrorIndexPositiveTimeoutDetachesFromExpiredParent(t *testing.T) {
	const docCount = 4

	var deadletterSeen int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "_bulk") && readContains(t, r, "deadletter") {
			deadletterSeen++
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(bulkItems(t, r, func(_ int, index string) map[string]any {
			if !strings.HasSuffix(index, "deadletter") {
				return map[string]any{"_index": index, "status": 400, "error": map[string]any{"type": "mapper_parsing_exception", "reason": "bad field"}}
			}
			return map[string]any{"_index": index, "status": 503, "error": map[string]any{"type": "es_rejected_execution_exception", "reason": "queue full"}}
		}))
	}))
	defer srv.Close()

	m, reader := newTestMetrics(t)
	client, err := newOpenSearchClient(srv.URL, srv.Client(), zap.NewNop())
	require.NoError(t, err)
	cfg := &Config{LogsIndex: "syslog", BulkAction: "create"}
	lbi := newLogBulkIndexer("create", &encodeModel{}, "", &ErrorClassConfig{}, "syslog-deadletter", m, zap.NewNop(), 0)
	require.NoError(t, lbi.start(client))

	// Populate the dead-letter buffer with a live parent, then flush with an
	// already-expired parent to prove the detach kicks in.
	live, liveCancel := context.WithCancel(t.Context())
	lbi.submit(live, testLogs(docCount), newIndexResolver("ss4o_logs", "", ""), cfg, time.Now())
	lbi.close(live)
	liveCancel()

	expired, expiredCancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer expiredCancel()
	require.Error(t, expired.Err(), "sanity: expired ctx must be past deadline")

	if ferr := lbi.flushOnErrorIndex(expired, client, 2*time.Second); ferr != nil {
		lbi.appendPermanentError(ferr)
	}

	assert.Positive(t, deadletterSeen, "positive timeout must detach Add from expired parent: server must see dead-letter requests")
	failures := collectInt64Sum(t, reader, "otelcol_opensearch_exporter_on_error_flush_failures")
	var total int64
	for reason, v := range failures {
		assert.NotContains(t, reason, "enqueue", "positive timeout must not bucket docs under enqueue when parent is expired, got %v", failures)
		total += v
	}
	assert.Equal(t, int64(docCount), total, "every doc must land on a real item fate, got failures=%v", failures)
}
