// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package opensearchexporter

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Every dead-letter doc must get a recorded fate even if ctx is cancelled mid-flush.
func TestFlushOnErrorIndexAccountsForEveryDocUnderCtxCancel(t *testing.T) {
	const docCount = 8

	// Hold the dead-letter request so we can cancel ctx mid-flight.
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseServer := func() { releaseOnce.Do(func() { close(release) }) }
	// Register cleanup before anything can fail so srv.Close() never blocks on a
	// handler waiting on release.
	t.Cleanup(releaseServer)
	var released atomic.Bool
	var deadletterSeen atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		isDeadletter := strings.Contains(r.URL.Path, "_bulk") && readContains(t, r, "deadletter")
		if isDeadletter {
			deadletterSeen.Add(1)
			<-release
			released.Store(true)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(bulkItems(t, r, func(_ int, index string) map[string]any {
			if !strings.HasSuffix(index, "deadletter") {
				// Main write: everything is a permanent 400 so it is routed to dead-letter.
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

	parent, cancelParent := context.WithCancel(t.Context())
	defer cancelParent()
	lbi.submit(parent, testLogs(docCount), newIndexResolver("ss4o_logs", "", ""), cfg, time.Now())
	lbi.close(parent)

	// Start the dead-letter flush with its own ctx and cancel it while the handler is
	// blocked. The flush must still account for every doc before returning.
	flushCtx, cancelFlush := context.WithCancel(parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if ferr := lbi.flushOnErrorIndex(flushCtx, client, 5*time.Second); ferr != nil {
			lbi.appendPermanentError(ferr)
		}
	}()

	// Wait until the server is actually serving the dead-letter batch, then cancel.
	require.Eventually(t, func() bool { return deadletterSeen.Load() >= 1 }, 2*time.Second, 5*time.Millisecond, "deadletter request did not reach server")
	cancelFlush()
	// Let the server respond so workers finish their flush and fire OnFailure callbacks.
	releaseServer()
	// Flush must return — Close uses a detached context so wg.Wait() completes once the
	// handler responds.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("flushOnErrorIndex did not return after workers drained")
	}
	require.True(t, released.Load(), "server handler was never released")

	failures := collectInt64Sum(t, reader, "otelcol_opensearch_exporter_on_error_flush_failures")
	var total int64
	for _, v := range failures {
		total += v
	}
	assert.Equal(t, int64(docCount), total, "every dead-letter doc must have a recorded final fate: got %v", failures)
}

// readContains buffers the request body (restoring it) and reports whether it contains s.
func readContains(t *testing.T, r *http.Request, s string) bool {
	t.Helper()
	b, err := io.ReadAll(r.Body)
	require.NoError(t, err)
	r.Body = io.NopCloser(bytes.NewReader(b))
	return strings.Contains(string(b), s)
}

// Mixed-outcome: half the dead-letter docs succeed, half fail. Proves no doc is
// dropped on either leg.
func TestFlushOnErrorIndexMixedSuccessAndFailure(t *testing.T) {
	const docCount = 8

	// Server-side toggle routes dead-letter docs to success/failure independent of
	// worker ordering.
	var deadletterSuccess, deadletterFailure atomic.Int32
	var toggle atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(bulkItems(t, r, func(_ int, index string) map[string]any {
			if !strings.HasSuffix(index, "deadletter") {
				// Primary write: permanent 400, route to dead-letter.
				return map[string]any{"_index": index, "status": 400, "error": map[string]any{"type": "mapper_parsing_exception", "reason": "bad field"}}
			}
			if toggle.Load() {
				toggle.Store(false)
				deadletterFailure.Add(1)
				return map[string]any{"_index": index, "status": 503, "error": map[string]any{"type": "es_rejected_execution_exception", "reason": "queue full"}}
			}
			toggle.Store(true)
			deadletterSuccess.Add(1)
			return map[string]any{"_index": index, "status": 201}
		}))
	}))
	defer srv.Close()

	m, reader := newTestMetrics(t)
	client, err := newOpenSearchClient(srv.URL, srv.Client(), zap.NewNop())
	require.NoError(t, err)
	cfg := &Config{LogsIndex: "syslog", BulkAction: "create"}
	lbi := newLogBulkIndexer("create", &encodeModel{}, "", &ErrorClassConfig{}, "syslog-deadletter", m, zap.NewNop(), 0)
	require.NoError(t, lbi.start(client))

	ctx := t.Context()
	lbi.submit(ctx, testLogs(docCount), newIndexResolver("ss4o_logs", "", ""), cfg, time.Now())
	lbi.close(ctx)
	// timeout=0 exercises the legacy single-ctx path end-to-end.
	if ferr := lbi.flushOnErrorIndex(ctx, client, 0); ferr != nil {
		lbi.appendPermanentError(ferr)
	}

	metricFailures := collectInt64Sum(t, reader, "otelcol_opensearch_exporter_on_error_flush_failures")
	var metricFailureTotal int64
	for _, v := range metricFailures {
		metricFailureTotal += v
	}
	serverSuccess := int64(deadletterSuccess.Load())
	serverFailure := int64(deadletterFailure.Load())

	assert.Equal(t, int64(docCount), serverSuccess+serverFailure, "server must observe one request per doc (success=%d failure=%d)", serverSuccess, serverFailure)
	assert.Equal(t, serverFailure, metricFailureTotal, "metric failures must match server-side failures")
	assert.Equal(t, int64(docCount), serverSuccess+metricFailureTotal, "submitted must equal success + failure")
	// Guard against degenerate all-success or all-failure routing.
	assert.Positive(t, serverSuccess, "mixed test must exercise at least one success")
	assert.Positive(t, serverFailure, "mixed test must exercise at least one failure")
}

// Regression: a cancelled parent must not block Add. Every doc must land on a real
// fate, not the "enqueue" bucket.
func TestFlushOnErrorIndexEnqueuesEveryDocWhenParentCtxAlreadyCancelled(t *testing.T) {
	const docCount = 8

	var deadletterSeen atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		isDeadletter := strings.Contains(r.URL.Path, "_bulk") && readContains(t, r, "deadletter")
		if isDeadletter {
			deadletterSeen.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(bulkItems(t, r, func(_ int, index string) map[string]any {
			if !strings.HasSuffix(index, "deadletter") {
				// Primary write: permanent 400 so everything is routed to dead-letter.
				return map[string]any{"_index": index, "status": 400, "error": map[string]any{"type": "mapper_parsing_exception", "reason": "bad field"}}
			}
			// Dead-letter: permanent 503 so every doc reaches an item_indexing fate.
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

	// Populate the dead-letter buffer via the primary push path, using a live ctx.
	primaryCtx, primaryCancel := context.WithCancel(t.Context())
	lbi.submit(primaryCtx, testLogs(docCount), newIndexResolver("ss4o_logs", "", ""), cfg, time.Now())
	lbi.close(primaryCtx)

	// Cancel the parent *before* the dead-letter flush. If Add still honored this ctx,
	// every enqueue would fail and the server would see zero dead-letter requests.
	primaryCancel()

	if ferr := lbi.flushOnErrorIndex(primaryCtx, client, 5*time.Second); ferr != nil {
		lbi.appendPermanentError(ferr)
	}

	// Every dead-letter doc must have reached the server despite the cancelled parent.
	assert.GreaterOrEqual(t, deadletterSeen.Load(), int32(1), "cancelled parent must not block dead-letter requests from reaching server")

	failures := collectInt64Sum(t, reader, "otelcol_opensearch_exporter_on_error_flush_failures")
	var total int64
	for reason, v := range failures {
		// "enqueue" only fires when Add short-circuits on ctx cancel; the fix prevents it.
		assert.NotContains(t, reason, "enqueue", "no doc may be bucketed under enqueue when parent ctx is cancelled: got %v", failures)
		total += v
	}
	assert.Equal(t, int64(docCount), total, "every doc must land on a real item fate, got failures=%v", failures)
}

// Regression for the Close-drain split. flush_bytes=1 + a short Add timeout exhausts
// Add's budget before Close starts; Close's independent budget must drain the in-flight
// worker. Observing an "enqueue" failure proves Add timed out, then every submitted
// doc must land on a final fate.
func TestFlushOnErrorIndexCloseDrainsAfterAddTimeout(t *testing.T) {
	const docCount = 32

	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseServer := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseServer)

	var deadletterSeen atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		isDeadletter := strings.Contains(r.URL.Path, "_bulk") && readContains(t, r, "deadletter")
		if isDeadletter {
			deadletterSeen.Add(1)
			<-release
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
	// flush_bytes=1 forces an HTTP flush per doc, producing "enqueue" failures when
	// Add's budget expires.
	lbi := newLogBulkIndexer("create", &encodeModel{}, "", &ErrorClassConfig{}, "syslog-deadletter", m, zap.NewNop(), 1)
	require.NoError(t, lbi.start(client))

	parent, cancelParent := context.WithCancel(t.Context())
	defer cancelParent()
	lbi.submit(parent, testLogs(docCount), newIndexResolver("ss4o_logs", "", ""), cfg, time.Now())
	lbi.close(parent)

	done := make(chan struct{})
	go func() {
		defer close(done)
		if ferr := lbi.flushOnErrorIndex(parent, client, 100*time.Millisecond); ferr != nil {
			lbi.appendPermanentError(ferr)
		}
	}()

	// Confirm the server received a dead-letter request (otherwise Add never applied
	// backpressure).
	require.Eventually(t, func() bool { return deadletterSeen.Load() >= 1 }, 2*time.Second, 5*time.Millisecond, "deadletter request did not reach server")

	// Wait for an "enqueue" failure to prove Add timed out before Close.
	require.Eventually(t, func() bool {
		for reason, v := range collectInt64Sum(t, reader, "otelcol_opensearch_exporter_on_error_flush_failures") {
			if strings.Contains(reason, "enqueue") && v > 0 {
				return true
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond, "Add did not time out: no enqueue failures recorded")

	// Release the blocked server so the in-flight worker can finish via Close's drain.
	releaseServer()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("flushOnErrorIndex did not return after Close drain budget")
	}

	failures := collectInt64Sum(t, reader, "otelcol_opensearch_exporter_on_error_flush_failures")
	var total int64
	for _, v := range failures {
		total += v
	}
	assert.Equal(t, int64(docCount), total, "Close drain must finalize every doc even when Add ctx expired, got %v", failures)
}

// Regression: endpoint never responds, flush_bytes=1 queues N serial requests. Flush
// must still return within timeout+grace (not timeout × N) and every doc must be
// recorded.
func TestFlushOnErrorIndexReturnsWhenEndpointBlocksForever(t *testing.T) {
	const docCount = 16
	const requestTimeout = 150 * time.Millisecond

	never := make(chan struct{})

	var deadletterSeen atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		isDeadletter := strings.Contains(r.URL.Path, "_bulk") && readContains(t, r, "deadletter")
		if isDeadletter {
			deadletterSeen.Add(1)
			// Block until request ctx is cancelled (transport timeout or shared
			// drain deadline) or deferred teardown runs.
			select {
			case <-r.Context().Done():
			case <-never:
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(bulkItems(t, r, func(_ int, index string) map[string]any {
			return map[string]any{"_index": index, "status": 400, "error": map[string]any{"type": "mapper_parsing_exception", "reason": "bad field"}}
		}))
	}))
	defer func() {
		close(never)
		srv.Close()
	}()

	// Mirrors logExporter.Start when OnErrorFlush.Timeout > 0.
	baseClient := srv.Client()
	transport := &perRequestTimeoutTransport{base: baseClient.Transport, timeout: requestTimeout}
	onErrorHTTPClient := &http.Client{Transport: transport}
	onErrorClient, err := newOpenSearchClient(srv.URL, onErrorHTTPClient, zap.NewNop())
	require.NoError(t, err)

	m, reader := newTestMetrics(t)
	cfg := &Config{LogsIndex: "syslog", BulkAction: "create"}
	primaryClient, err := newOpenSearchClient(srv.URL, baseClient, zap.NewNop())
	require.NoError(t, err)

	// flush_bytes=1 queues N serial requests; shared deadline bounds total time.
	lbi := newLogBulkIndexer("create", &encodeModel{}, "", &ErrorClassConfig{}, "syslog-deadletter", m, zap.NewNop(), 1)
	lbi.onErrorDrainDeadlineSetter = transport.setSharedDeadline
	require.NoError(t, lbi.start(primaryClient))

	ctx := t.Context()
	lbi.submit(ctx, testLogs(docCount), newIndexResolver("ss4o_logs", "", ""), cfg, time.Now())
	lbi.close(ctx)

	done := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(done)
		if ferr := lbi.flushOnErrorIndex(ctx, onErrorClient, requestTimeout); ferr != nil {
			lbi.appendPermanentError(ferr)
		}
	}()

	// Total time must not scale with queue depth.
	maxWait := requestTimeout + onErrorCloseDrainGrace + 2*time.Second
	select {
	case <-done:
	case <-time.After(maxWait):
		t.Fatalf("flushOnErrorIndex did not return within %v despite shared drain deadline", maxWait)
	}
	elapsed := time.Since(start)

	assert.Less(t, elapsed, requestTimeout+onErrorCloseDrainGrace+time.Second,
		"flush time scaled with queue depth, shared drain deadline is not bounding requests")

	assert.Positive(t, deadletterSeen.Load(), "server must have received at least one dead-letter request")

	// Every doc recorded: completed via item_transport, remaining via drain_timeout.
	failures := collectInt64Sum(t, reader, "otelcol_opensearch_exporter_on_error_flush_failures")
	var total, drainTimeout, itemTransport int64
	for reason, v := range failures {
		total += v
		switch {
		case strings.Contains(reason, "drain_timeout"):
			drainTimeout += v
		case strings.Contains(reason, "item_transport"):
			itemTransport += v
		}
	}
	assert.Equal(t, int64(docCount), total, "submitted == recorded invariant must hold under endpoint hang, got %v", failures)
	// At least one leg must be exercised; the split between them depends on scheduling.
	assert.Positive(t, drainTimeout+itemTransport, "items must be bucketed under drain_timeout and/or item_transport, got %v", failures)
}
