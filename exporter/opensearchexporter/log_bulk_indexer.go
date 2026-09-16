// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package opensearchexporter // import "github.com/cloudoperators/opentelemetry-collector-contrib/exporter/opensearchexporter"

import (
	"bytes"
	"context"
	"errors"
	"net"
	"time"

	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/opensearch-project/opensearch-go/v4/opensearchutil"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

type logBulkIndexer struct {
	bulkAction          string
	pipeline            string
	model               mappingModel
	errs                []error
	bulkIndexer         opensearchutil.BulkIndexer
	errorClassification *ErrorClassConfig
	onErrorIndex        string
	onErrorDocs         [][]byte
	metrics             *exporterMetrics
	logger              *zap.Logger
}

func newLogBulkIndexer(bulkAction string, model mappingModel, pipeline string, errorClassification *ErrorClassConfig, onErrorIndex string, metrics *exporterMetrics, logger *zap.Logger) *logBulkIndexer {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &logBulkIndexer{
		bulkAction:          bulkAction,
		pipeline:            pipeline,
		model:               model,
		errs:                nil,
		bulkIndexer:         nil,
		errorClassification: errorClassification,
		onErrorIndex:        onErrorIndex,
		metrics:             metrics,
		logger:              logger,
	}
}

func (lbi *logBulkIndexer) log() *zap.Logger {
	if lbi.logger == nil {
		return zap.NewNop()
	}
	return lbi.logger
}

func (lbi *logBulkIndexer) start(client *opensearchapi.Client) error {
	var startErr error
	lbi.bulkIndexer, startErr = newLogOpenSearchBulkIndexer(client, lbi.onIndexerError, lbi.pipeline)
	return startErr
}

func (lbi *logBulkIndexer) joinedError() error {
	return errors.Join(lbi.errs...)
}

func (lbi *logBulkIndexer) close(ctx context.Context) {
	closeErr := lbi.bulkIndexer.Close(ctx)
	if closeErr != nil {
		lbi.log().Debug("main bulk indexer close returned error", zap.Error(closeErr))
		lbi.errs = append(lbi.errs, closeErr)
	}
}

func (lbi *logBulkIndexer) onIndexerError(_ context.Context, indexerErr error) {
	if indexerErr != nil {
		lbi.log().Debug("opensearch bulk indexer transport error", zap.Error(indexerErr))
		lbi.appendPermanentError(consumererror.NewPermanent(indexerErr))
	}
}

func (lbi *logBulkIndexer) appendPermanentError(e error) {
	lbi.errs = append(lbi.errs, consumererror.NewPermanent(e))
}

func (lbi *logBulkIndexer) appendRetryLogError(err error, log plog.Logs) {
	lbi.errs = append(lbi.errs, consumererror.NewLogs(err, log))
}

func (lbi *logBulkIndexer) submit(ctx context.Context, ld plog.Logs, ir *indexResolver, cfg *Config, timestamp time.Time) {
	lbi.log().Debug("opensearch log bulk indexer submit", zap.Int("resource_log_count", ld.ResourceLogs().Len()), zap.String("logs_index", cfg.LogsIndex), zap.String("on_error_index", lbi.onErrorIndex))
	keys := ir.extractPlaceholderKeys(cfg.LogsIndex)
	timeSuffix := ir.calculateTimeSuffix(cfg.LogsIndexTimeFormat, timestamp)
	resourceLogs := ld.ResourceLogs()

	for i := 0; i < resourceLogs.Len(); i++ {
		il := resourceLogs.At(i)
		resource := il.Resource()
		resourceAttrs := ir.collectResourceAttributes(resource, keys)
		scopeLogs := il.ScopeLogs()

		for j := 0; j < scopeLogs.Len(); j++ {
			scopeSpan := scopeLogs.At(j)
			scopeAttrs := ir.collectScopeAttributes(scopeSpan.Scope(), keys)
			logs := scopeLogs.At(j).LogRecords()

			for k := 0; k < logs.Len(); k++ {
				log := logs.At(k)
				indexName := ir.resolveIndexName(cfg.LogsIndex, cfg.LogsIndexFallback, log.Attributes(), keys, scopeAttrs, resourceAttrs, timeSuffix)
				lbi.log().Debug("opensearch dispatching log item", zap.String("index", indexName))
				lbi.processItem(ctx, indexName, resource, il.SchemaUrl(), scopeSpan.Scope(), scopeSpan.SchemaUrl(), log)
			}
		}
	}
}

func (lbi *logBulkIndexer) processItem(ctx context.Context, indexName string, resource pcommon.Resource, resourceSchemaURL string, scope pcommon.InstrumentationScope, scopeSchemaURL string, logRecord plog.LogRecord) {
	payload, err := lbi.model.encodeLog(resource, scope, scopeSchemaURL, logRecord)
	if err != nil {
		lbi.log().Debug("failed to encode log record", zap.Error(err), zap.String("index", indexName))
		lbi.appendPermanentError(err)
	} else {
		ItemFailureHandler := func(itemCtx context.Context, _ opensearchutil.BulkIndexerItem, resp opensearchapi.BulkRespItem, itemErr error) {
			lbi.processItemFailure(itemCtx, resp, itemErr, logRecord, payload, resource, resourceSchemaURL, scope, scopeSchemaURL)
		}
		bi := lbi.newBulkIndexerItem(payload, indexName)
		bi.OnFailure = ItemFailureHandler
		err = lbi.bulkIndexer.Add(ctx, bi)
		if err != nil {
			lbi.log().Debug("failed to enqueue log item to bulk indexer, will retry", zap.Error(err), zap.String("index", indexName))
			lbi.appendRetryLogError(err, makeLog(resource, resourceSchemaURL, scope, scopeSchemaURL, logRecord))
		}
	}
}

func makeLog(resource pcommon.Resource, resourceSchemaURL string, scope pcommon.InstrumentationScope, scopeSchemaURL string, log plog.LogRecord) plog.Logs {
	logs := plog.NewLogs()
	rs := logs.ResourceLogs().AppendEmpty()
	resource.CopyTo(rs.Resource())
	rs.SetSchemaUrl(resourceSchemaURL)
	ss := rs.ScopeLogs().AppendEmpty()

	ss.SetSchemaUrl(scopeSchemaURL)
	scope.CopyTo(ss.Scope())
	s := ss.LogRecords().AppendEmpty()

	log.CopyTo(s)

	return logs
}

func (lbi *logBulkIndexer) processItemFailure(ctx context.Context, resp opensearchapi.BulkRespItem, itemErr error, originalLogRecord plog.LogRecord, originalPayload []byte, resource pcommon.Resource, resourceSchemaURL string, scope pcommon.InstrumentationScope, scopeSchemaURL string) {
	lbi.log().Debug("opensearch item failure callback fired", zap.Int("status", resp.Status), zap.String("index", resp.Index))
	logs, class := lbi.formatItemError(resp, originalLogRecord, resource, resourceSchemaURL, scope, scopeSchemaURL)

	errType := "unknown"
	errReason := "unknown"
	if resp.Error != nil {
		if resp.Error.Type != "" {
			errType = resp.Error.Type
		}
		if resp.Error.Reason != "" {
			errReason = trimReasonPreview(resp.Error.Reason)
		}
	}

	switch {
	case class == "transient":
		// Retryable per HTTP status or user/built-in class override.
		lbi.log().Debug("opensearch item transient error, will retry", zap.Int("status", resp.Status), zap.String("error_type", errType), zap.String("error_reason", errReason), zap.String("index", resp.Index))
		if lbi.metrics != nil {
			lbi.metrics.recordTransientError(ctx, errType, resp.Index)
		}
		lbi.appendRetryLogError(responseAsError(resp), logs)

	case resp.Status != 0 && itemErr == nil:
		// Permanent indexing error — route to on error index if configured, otherwise return to pipeline
		if lbi.onErrorIndex != "" {
			lbi.log().Debug("opensearch item permanent error, routing to on-error index", zap.Int("status", resp.Status), zap.String("error_type", errType), zap.String("error_reason", errReason), zap.String("error_class", class), zap.String("main_index", resp.Index), zap.String("on_error_index", lbi.onErrorIndex))
			if lbi.metrics != nil {
				lbi.metrics.recordOnErrorDoc(ctx, errType, class, resp.Status, resp.Index)
			}
			lbi.submitToOnError(ctx, resp, originalPayload)
		} else {
			lbi.log().Debug("opensearch item permanent error, no on-error index configured, dropping", zap.Int("status", resp.Status), zap.String("error_type", errType), zap.String("error_reason", errReason), zap.String("error_class", class), zap.String("index", resp.Index))
			if lbi.metrics != nil {
				lbi.metrics.recordPermanentError(ctx, errType, class, resp.Status, resp.Index)
			}
			lbi.appendPermanentError(responseAsError(resp))
		}

	default:
		// Transport/network errors or unexpected issues from bulk indexer
		var netErr net.Error
		if errors.As(itemErr, &netErr) {
			// Network error (connection refused, timeout, etc.) — retry
			lbi.log().Debug("opensearch item network error, will retry", zap.Error(itemErr), zap.String("index", resp.Index))
			if lbi.metrics != nil {
				lbi.metrics.recordTransientError(ctx, "network_error", resp.Index)
			}
			lbi.appendRetryLogError(itemErr, logs)
		} else {
			// Other unexpected error — permanent
			lbi.log().Debug("opensearch item unexpected error, marking permanent", zap.Error(itemErr), zap.Int("status", resp.Status), zap.String("index", resp.Index))
			if lbi.metrics != nil {
				lbi.metrics.recordPermanentError(ctx, "unknown", "permanent", 0, resp.Index)
			}
			lbi.appendPermanentError(itemErr)
		}
	}
}

func (lbi *logBulkIndexer) newBulkIndexerItem(document []byte, indexName string) opensearchutil.BulkIndexerItem {
	body := bytes.NewReader(document)
	item := opensearchutil.BulkIndexerItem{Action: lbi.bulkAction, Index: indexName, Body: body}
	return item
}

func newLogOpenSearchBulkIndexer(client *opensearchapi.Client, onIndexerError func(context.Context, error), pipeline string) (opensearchutil.BulkIndexer, error) {
	return opensearchutil.NewBulkIndexer(opensearchutil.BulkIndexerConfig{
		NumWorkers: 1,
		Client:     client,
		OnError:    onIndexerError,
		Pipeline:   pipeline,
	})
}
