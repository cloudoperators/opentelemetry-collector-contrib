// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package opensearchexporter // import "github.com/cloudoperators/opentelemetry-collector-contrib/exporter/opensearchexporter"

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	errorClassification *ErrorClassificationConfig
	logger              *zap.Logger
	dlqIndex            string
	dlqDocs             [][]byte
}

func newLogBulkIndexer(bulkAction string, model mappingModel, pipeline string, errorClassification *ErrorClassificationConfig, logger *zap.Logger, dlqIndex string) *logBulkIndexer {
	return &logBulkIndexer{
		bulkAction:          bulkAction,
		pipeline:            pipeline,
		model:               model,
		errs:                nil,
		bulkIndexer:         nil,
		errorClassification: errorClassification,
		logger:              logger,
		dlqIndex:            dlqIndex,
	}
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
		lbi.errs = append(lbi.errs, closeErr)
	}
}

func (lbi *logBulkIndexer) onIndexerError(_ context.Context, indexerErr error) {
	if indexerErr != nil {
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
				lbi.processItem(ctx, indexName, resource, il.SchemaUrl(), scopeSpan.Scope(), scopeSpan.SchemaUrl(), log)
			}
		}
	}
}

func (lbi *logBulkIndexer) processItem(ctx context.Context, indexName string, resource pcommon.Resource, resourceSchemaURL string, scope pcommon.InstrumentationScope, scopeSchemaURL string, logRecord plog.LogRecord) {
	payload, err := lbi.model.encodeLog(resource, scope, scopeSchemaURL, logRecord)
	if err != nil {
		lbi.appendPermanentError(err)
	} else {
		ItemFailureHandler := func(itemCtx context.Context, _ opensearchutil.BulkIndexerItem, resp opensearchapi.BulkRespItem, itemErr error) {
			lbi.processItemFailure(itemCtx, resp, itemErr, logRecord, payload, resource, resourceSchemaURL, scope, scopeSchemaURL)
		}
		bi := lbi.newBulkIndexerItem(payload, indexName)
		bi.OnFailure = ItemFailureHandler
		err = lbi.bulkIndexer.Add(ctx, bi)
		if err != nil {
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

func (lbi *logBulkIndexer) processItemFailure(_ context.Context, resp opensearchapi.BulkRespItem, itemErr error, originalLogRecord plog.LogRecord, _ []byte, resource pcommon.Resource, resourceSchemaURL string, scope pcommon.InstrumentationScope, scopeSchemaURL string) {
	// Stamp error attributes on ORIGINAL record (mutate in place so downstream consumers can act on them).
	// resp.Error may be nil when OpenSearch reports only a status (e.g. transport-level failures surfaced
	// via itemErr), so we default type/reason to "unknown" and always stamp status + classification when a
	// status is present.
	errType := "unknown"
	errReason := "unknown"
	if resp.Error != nil {
		if resp.Error.Type != "" {
			errType = resp.Error.Type
		}
		if resp.Error.Reason != "" {
			errReason = resp.Error.Reason
		}
	}
	if resp.Status != 0 || resp.Error != nil {
		originalLogRecord.Attributes().PutStr("opensearch.error.type", errType)
		originalLogRecord.Attributes().PutStr("opensearch.error.reason", errReason)
		if resp.Status != 0 {
			originalLogRecord.Attributes().PutInt("opensearch.error.status", int64(resp.Status))
		}
		originalLogRecord.Attributes().PutStr("opensearch.error.classification", classifyError(resp.Status, errType, lbi.errorClassification))
	}

	// Build copy AFTER stamping original so copy also has attrs
	logs := makeLog(resource, resourceSchemaURL, scope, scopeSchemaURL, originalLogRecord)

	switch {
	case shouldRetryEvent(resp.Status):
		lbi.appendRetryLogError(responseAsError(resp), logs)

	case resp.Status != 0 && itemErr == nil:
		// Permanent indexing error — route to DLQ index if configured, otherwise return to pipeline
		if lbi.dlqIndex != "" {
			lbi.submitToDLQ(ctx, resp, originalPayload)
		} else {
			lbi.appendPermanentError(responseAsError(resp))
		}

	default:
		lbi.appendPermanentError(itemErr)
	}
}

func (lbi *logBulkIndexer) submitToDLQ(_ context.Context, resp opensearchapi.BulkRespItem, originalPayload []byte) {
	envelope := map[string]any{
		"error": map[string]any{
			"type":           resp.Error.Type,
			"reason":         resp.Error.Reason,
			"status":         resp.Status,
			"classification": classify(resp.Status, resp.Error.Type, lbi.errorClassification),
		},
		"original": json.RawMessage(originalPayload),
	}
	doc, err := json.Marshal(envelope)
	if err != nil {
		if lbi.logger != nil {
			lbi.logger.Error("failed to encode DLQ envelope", zap.Error(err))
		}
		return
	}
	lbi.dlqDocs = append(lbi.dlqDocs, doc)
}

func (lbi *logBulkIndexer) flushDLQ(ctx context.Context, client *opensearchapi.Client) error {
	if len(lbi.dlqDocs) == 0 {
		return nil
	}
	dlqIndexer, err := newLogOpenSearchBulkIndexer(client, func(_ context.Context, indexerErr error) {
		if lbi.logger != nil {
			lbi.logger.Error("DLQ bulk indexer error", zap.Error(indexerErr))
		}
	}, lbi.pipeline)
	if err != nil {
		return err
	}
	for _, doc := range lbi.dlqDocs {
		doc := doc
		item := opensearchutil.BulkIndexerItem{
			Action: "index",
			Index:  lbi.dlqIndex,
			Body:   bytes.NewReader(doc),
			OnFailure: func(_ context.Context, _ opensearchutil.BulkIndexerItem, r opensearchapi.BulkRespItem, e error) {
				if lbi.logger != nil {
					lbi.logger.Error("failed to write to DLQ index",
						zap.Int("status", r.Status),
						zap.Any("error", r.Error),
						zap.NamedError("err", e),
					)
				}
			},
		}
		if addErr := dlqIndexer.Add(ctx, item); addErr != nil {
			if lbi.logger != nil {
				lbi.logger.Error("failed to add item to DLQ bulk indexer", zap.Error(addErr))
			}
		}
	}
	return dlqIndexer.Close(ctx)
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
