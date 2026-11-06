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

// Every dead-letter doc must get a recorded fate (success or failure metric) even if the
// caller's ctx is cancelled mid-flush. Enforces the invariant
// on_error_docs_submitted == sum(on_error_flush_failures) + successes.
func TestFlushOnErrorIndexAccountsForEveryDocUnderCtxCancel(t *testing.T) {
	const docCount = 8

	// Hold the dead-letter request so we can cancel ctx while workers are in flight, then
	// release and let each doc get its 503 OnFailure callback.
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseServer := func() { releaseOnce.Do(func() { close(release) }) }
	// Register the release cleanup before anything can fail so srv.Close() never blocks
	// on a handler that is still waiting on `release`, even if a later require.Eventually
	// times out before the server ever reached the blocking read.
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

// Mixed-outcome variant: half the dead-letter docs succeed (201), half fail (503). This
// exercises the full invariant submitted == success + failure (both legs), not just the
// all-fail path covered above. Successes do not emit a metric so we count them server-side
// and assert server_success + metric_failure == docCount, which proves no doc is dropped
// without a fate on either leg.
func TestFlushOnErrorIndexMixedSuccessAndFailure(t *testing.T) {
	const docCount = 8

	// Route dead-letter docs to success/failure in roughly equal shares without depending
	// on the order in which the opensearchutil worker pool delivers items. A server-side
	// toggle (not the per-request index i) flips on every dead-letter doc, so with any
	// number of workers or batches we still get ~half success and ~half failure. The
	// test asserts only the invariants (total, metric == server failure), which are
	// order-independent; the toggle just ensures both legs are exercised.
	var deadletterSuccess, deadletterFailure atomic.Int32
	var toggle atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(bulkItems(t, r, func(_ int, index string) map[string]any {
			if !strings.HasSuffix(index, "deadletter") {
				// Primary write: everything is a permanent 400 so it is routed to dead-letter.
				return map[string]any{"_index": index, "status": 400, "error": map[string]any{"type": "mapper_parsing_exception", "reason": "bad field"}}
			}
			// Dead-letter: alternate per doc via a server-side toggle so routing is
			// independent of per-request item index or worker pool scheduling.
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

	// Every dead-letter doc reached the server exactly once.
	assert.Equal(t, int64(docCount), serverSuccess+serverFailure, "server must observe one request per doc (success=%d failure=%d)", serverSuccess, serverFailure)
	// Metric failure count matches the server-side failure count: no phantom failures, no missed failures.
	assert.Equal(t, serverFailure, metricFailureTotal, "metric failures must match server-side failures")
	// Full invariant: every doc accounted for as success-or-failure, none silently dropped.
	assert.Equal(t, int64(docCount), serverSuccess+metricFailureTotal, "submitted must equal success + failure")
	// Both legs actually exercised: guards against the toggle degenerating under future
	// routing changes and silently turning this into an all-success or all-failure test.
	assert.Positive(t, serverSuccess, "mixed test must exercise at least one success")
	assert.Positive(t, serverFailure, "mixed test must exercise at least one failure")
}

// Regression: when the parent push ctx is already cancelled before flushOnErrorIndex
// runs, Add must still enqueue every doc (because the internal workerCtx is derived via
// context.WithoutCancel) and every doc must land on a real fate — not the "enqueue"
// bucket. Previously Add used the caller ctx directly, so a cancelled parent would
// short-circuit enqueue and bucket every doc under reason="enqueue" without ever hitting
// the server.
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
		// No doc may be bucketed under "enqueue": that reason only fires when Add
		// short-circuits on ctx cancel, which this fix prevents.
		assert.NotContains(t, reason, "enqueue", "no doc may be bucketed under enqueue when parent ctx is cancelled: got %v", failures)
		total += v
	}
	assert.Equal(t, int64(docCount), total, "every doc must land on a real item fate, got failures=%v", failures)
}

// Regression for the Close-drain-budget fix. Forces Add backpressure (large doc count
// and small flush_bytes so each Add triggers an HTTP flush the server blocks) with a
// short Add timeout, so Add exhausts its budget *before* Close begins. Close then runs
// with its own onErrorCloseDrainBudget and must drain the in-flight worker that is
// still waiting on the blocked server. Without the split (Add and Close sharing one
// expired ctx), Close would return before wg.Wait() completes and the in-flight doc
// would be dropped without a final fate.
//
// Signal that Add timed out by observing the "enqueue" failure reason: Add only buckets
// docs there when its ctx expires mid-loop. Then release the server and assert every
// submitted doc landed on a final fate.
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
	// flush_bytes=1 forces the bulk indexer to flush after every single enqueued doc,
	// so Add is immediately queued behind the blocked HTTP request. Combined with many
	// docs and a short Add timeout, this reliably produces "enqueue" failures during
	// Add before Close starts.
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

	// Confirm the server actually received at least one dead-letter request (otherwise
	// Add never applied backpressure and the test does not exercise the regression).
	require.Eventually(t, func() bool { return deadletterSeen.Load() >= 1 }, 2*time.Second, 5*time.Millisecond, "deadletter request did not reach server")

	// Wait for Add to have timed out: the "enqueue" metric is only recorded when Add
	// returns ctx.DeadlineExceeded mid-loop. Polling this proves Add's budget expired
	// *before* Close begins its own drain window, which is the regression we are guarding.
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

// Regression for transport-level bounding of dead-letter writes: when the server never
// responds (handler blocks forever) and the dead-letter opensearchapi.Client is wrapped
// with a per-request timeout transport, flushOnErrorIndex must still return promptly
// instead of blocking indefinitely on opensearchutil's wg.Wait(). This guards the
// advertised shutdown bound against a hung endpoint.
func TestFlushOnErrorIndexReturnsWhenEndpointBlocksForever(t *testing.T) {
	const docCount = 4
	const requestTimeout = 150 * time.Millisecond

	never := make(chan struct{})
	t.Cleanup(func() { close(never) }) // release server at test teardown so httptest.Server.Close doesn't hang

	var deadletterSeen atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		isDeadletter := strings.Contains(r.URL.Path, "_bulk") && readContains(t, r, "deadletter")
		if isDeadletter {
			deadletterSeen.Add(1)
			// Block until the request context is cancelled (transport-level timeout)
			// or the test cleanup runs — whichever happens first.
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
	defer srv.Close()

	// Build the dead-letter client with the per-request timeout transport, mirroring
	// what logExporter.Start does in production when OnErrorFlush.Timeout > 0.
	baseClient := srv.Client()
	onErrorHTTPClient := &http.Client{
		Transport: &perRequestTimeoutTransport{base: baseClient.Transport, timeout: requestTimeout},
	}
	onErrorClient, err := newOpenSearchClient(srv.URL, onErrorHTTPClient, zap.NewNop())
	require.NoError(t, err)

	m, reader := newTestMetrics(t)
	cfg := &Config{LogsIndex: "syslog", BulkAction: "create"}
	// Use the base client (non-wrapped) for primary writes so the primary batch can
	// finish normally and route docs to dead-letter.
	primaryClient, err := newOpenSearchClient(srv.URL, baseClient, zap.NewNop())
	require.NoError(t, err)

	lbi := newLogBulkIndexer("create", &encodeModel{}, "", &ErrorClassConfig{}, "syslog-deadletter", m, zap.NewNop(), 0)
	require.NoError(t, lbi.start(primaryClient))

	ctx := t.Context()
	lbi.submit(ctx, testLogs(docCount), newIndexResolver("ss4o_logs", "", ""), cfg, time.Now())
	lbi.close(ctx)

	// Flush must return within a bounded window even though the server never responds.
	// Budget: requestTimeout for the request + onErrorCloseDrainGrace for drain + a
	// generous safety margin for scheduling.
	done := make(chan struct{})
	go func() {
		defer close(done)
		if ferr := lbi.flushOnErrorIndex(ctx, onErrorClient, requestTimeout); ferr != nil {
			lbi.appendPermanentError(ferr)
		}
	}()

	maxWait := requestTimeout + onErrorCloseDrainGrace + 2*time.Second
	select {
	case <-done:
	case <-time.After(maxWait):
		t.Fatalf("flushOnErrorIndex did not return within %v despite transport-level timeout", maxWait)
	}

	assert.Positive(t, deadletterSeen.Load(), "server must have received at least one dead-letter request")

	// Workers must have observed the transport-level cancellation and fired OnFailure
	// callbacks: without the per-request timeout wrap, opensearchutil workers would
	// still be blocked and no item_transport failures would be recorded even though
	// Close returned. Proving the metric fired confirms the transport bound unblocks
	// workers (not just Close's closeCtx abandoning them).
	failures := collectInt64Sum(t, reader, "otelcol_opensearch_exporter_on_error_flush_failures")
	var itemTransport int64
	for reason, v := range failures {
		if strings.Contains(reason, "item_transport") {
			itemTransport += v
		}
	}
	assert.Positive(t, itemTransport, "transport-level timeout must unblock workers so item_transport failures are recorded, got %v", failures)
}
