// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package opensearchexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/opensearchexporter"

import (
	"bytes"
	"context"
	"errors"
	"time"

	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/opensearch-project/opensearch-go/v4/opensearchutil"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type traceBulkIndexer struct {
	bulkAction          string
	pipeline            string
	model               mappingModel
	errs                []error
	bulkIndexer         opensearchutil.BulkIndexer
	errorClassification *ErrorClassificationConfig
}

func newTraceBulkIndexer(bulkAction string, model mappingModel, pipeline string, errorClassification *ErrorClassificationConfig) *traceBulkIndexer {
	return &traceBulkIndexer{
		bulkAction:          bulkAction,
		pipeline:            pipeline,
		model:               model,
		errs:                nil,
		bulkIndexer:         nil,
		errorClassification: errorClassification,
	}
}

func (tbi *traceBulkIndexer) joinedError() error {
	return errors.Join(tbi.errs...)
}

func (tbi *traceBulkIndexer) start(client *opensearchapi.Client) error {
	var startErr error
	tbi.bulkIndexer, startErr = newOpenSearchBulkIndexer(client, tbi.onIndexerError, tbi.pipeline)
	return startErr
}

func (tbi *traceBulkIndexer) close(ctx context.Context) {
	closeErr := tbi.bulkIndexer.Close(ctx)
	if closeErr != nil {
		tbi.errs = append(tbi.errs, closeErr)
	}
}

func (tbi *traceBulkIndexer) onIndexerError(_ context.Context, indexerErr error) {
	if indexerErr != nil {
		tbi.appendPermanentError(consumererror.NewPermanent(indexerErr))
	}
}

func (tbi *traceBulkIndexer) appendPermanentError(e error) {
	tbi.errs = append(tbi.errs, consumererror.NewPermanent(e))
}

func (tbi *traceBulkIndexer) appendRetryTraceError(err error, trace ptrace.Traces) {
	tbi.errs = append(tbi.errs, consumererror.NewTraces(err, trace))
}

func (tbi *traceBulkIndexer) appendPermanentTraceError(err error, trace ptrace.Traces) {
	tbi.errs = append(tbi.errs, consumererror.NewTraces(consumererror.NewPermanent(err), trace))
}

func (tbi *traceBulkIndexer) submit(ctx context.Context, td ptrace.Traces, ir *indexResolver, cfg *Config, timestamp time.Time) {
	keys := ir.extractPlaceholderKeys(cfg.TracesIndex)
	timeSuffix := ir.calculateTimeSuffix(cfg.TracesIndexTimeFormat, timestamp)
	resourceSpans := td.ResourceSpans()

	for i := 0; i < resourceSpans.Len(); i++ {
		il := resourceSpans.At(i)
		resource := il.Resource()
		resourceAttrs := ir.collectResourceAttributes(resource, keys)
		scopeSpans := il.ScopeSpans()

		for j := 0; j < scopeSpans.Len(); j++ {
			scopeSpan := scopeSpans.At(j)
			scopeAttrs := ir.collectScopeAttributes(scopeSpan.Scope(), keys)
			spans := scopeSpans.At(j).Spans()

			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				indexName := ir.resolveIndexName(cfg.TracesIndex, cfg.TracesIndexFallback, span.Attributes(), keys, scopeAttrs, resourceAttrs, timeSuffix)
				tbi.processItem(ctx, indexName, resource, il.SchemaUrl(), scopeSpan.Scope(), scopeSpan.SchemaUrl(), span)
			}
		}
	}
}

func (tbi *traceBulkIndexer) processItem(ctx context.Context, indexName string, resource pcommon.Resource, resourceSchemaURL string, scope pcommon.InstrumentationScope, scopeSchemaURL string, span ptrace.Span) {
	payload, err := tbi.model.encodeTrace(resource, scope, scopeSchemaURL, span)
	if err != nil {
		tbi.appendPermanentError(err)
	} else {
		ItemFailureHandler := func(_ context.Context, _ opensearchutil.BulkIndexerItem, resp opensearchapi.BulkRespItem, itemErr error) {
			// Setup error handler. The handler handles the per item response status based on the
			// selective ACKing in the bulk response.
			tbi.processItemFailure(resp, itemErr, makeTrace(resource, resourceSchemaURL, scope, scopeSchemaURL, span))
		}
		bi := tbi.newBulkIndexerItem(payload, indexName)
		bi.OnFailure = ItemFailureHandler
		err = tbi.bulkIndexer.Add(ctx, bi)
		if err != nil {
			tbi.appendRetryTraceError(err, makeTrace(resource, resourceSchemaURL, scope, scopeSchemaURL, span))
		}
	}
}

func makeTrace(resource pcommon.Resource, resourceSchemaURL string, scope pcommon.InstrumentationScope, scopeSchemaURL string, span ptrace.Span) ptrace.Traces {
	traces := ptrace.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	resource.CopyTo(rs.Resource())
	rs.SetSchemaUrl(resourceSchemaURL)
	ss := rs.ScopeSpans().AppendEmpty()

	ss.SetSchemaUrl(scopeSchemaURL)
	scope.CopyTo(ss.Scope())
	s := ss.Spans().AppendEmpty()

	span.CopyTo(s)

	return traces
}

func (tbi *traceBulkIndexer) processItemFailure(resp opensearchapi.BulkRespItem, itemErr error, traces ptrace.Traces) {
	// Stamp error attributes before handling
	if resp.Error != nil && traces.ResourceSpans().Len() > 0 &&
		traces.ResourceSpans().At(0).ScopeSpans().Len() > 0 &&
		traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().Len() > 0 {
		span := traces.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
		if resp.Error.Type != "" {
			span.Attributes().PutStr("opensearch.error.type", resp.Error.Type)
		}
		if resp.Error.Reason != "" {
			span.Attributes().PutStr("opensearch.error.reason", resp.Error.Reason)
		}
		if resp.Status != 0 {
			span.Attributes().PutInt("opensearch.error.status", int64(resp.Status))
			span.Attributes().PutStr("opensearch.error.classification", classify(resp.Status, resp.Error.Type, tbi.errorClassification))
		}
	}

	switch {
	case shouldRetryEvent(resp.Status):
		// Recoverable OpenSearch error
		tbi.appendRetryTraceError(responseAsError(resp), traces)
	case resp.Status != 0 && itemErr == nil:
		// Non-recoverable OpenSearch error while indexing document — carry record for DLQ routing
		tbi.appendPermanentTraceError(responseAsError(resp), traces)
	default:
		// Encoding error. We didn't even attempt to send the event
		tbi.appendPermanentError(itemErr)
	}
}

func attributesToMapString(attributes pcommon.Map) map[string]string {
	m := make(map[string]string, attributes.Len())
	for k, v := range attributes.All() {
		m[k] = v.AsString()
	}
	return m
}

func (tbi *traceBulkIndexer) newBulkIndexerItem(document []byte, indexName string) opensearchutil.BulkIndexerItem {
	body := bytes.NewReader(document)
	item := opensearchutil.BulkIndexerItem{Action: tbi.bulkAction, Index: indexName, Body: body}
	return item
}

func newOpenSearchBulkIndexer(client *opensearchapi.Client, onIndexerError func(context.Context, error), pipeline string) (opensearchutil.BulkIndexer, error) {
	return opensearchutil.NewBulkIndexer(opensearchutil.BulkIndexerConfig{
		NumWorkers: 1,
		Client:     client,
		OnError:    onIndexerError,
		Pipeline:   pipeline,
	})
}
