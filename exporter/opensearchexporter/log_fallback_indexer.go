// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package opensearchexporter // import "github.com/cloudoperators/opentelemetry-collector-contrib/exporter/opensearchexporter"

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/opensearch-project/opensearch-go/v4/opensearchutil"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

func (lbi *logBulkIndexer) formatItemError(resp opensearchapi.BulkRespItem, originalLogRecord plog.LogRecord, resource pcommon.Resource, resourceSchemaURL string, scope pcommon.InstrumentationScope, scopeSchemaURL string) (plog.Logs, string) {
	// Stamp error attributes on ORIGINAL record (mutate in place so downstream consumers can act on them).
	// resp.Error may be nil when OpenSearch reports only a status (e.g. transport-level failures surfaced
	// via itemErr), so we default type/reason to "unknown" and always stamp status + class when a
	// status is present.
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
	if resp.Status != 0 || resp.Error != nil {
		originalLogRecord.Attributes().PutStr("opensearch.error.type", errType)
		originalLogRecord.Attributes().PutStr("opensearch.error.reason", errReason)
		if resp.Status != 0 {
			originalLogRecord.Attributes().PutInt("opensearch.error.status", int64(resp.Status))
		}
		originalLogRecord.Attributes().PutStr("opensearch.error.class", classifyError(resp.Status, errType, lbi.errorClassification))
	}

	return makeLog(resource, resourceSchemaURL, scope, scopeSchemaURL, originalLogRecord), classifyError(resp.Status, errType, lbi.errorClassification)
}

// luceneMaxFieldBytes is a safe chunk size that stays under the 32766-byte Lucene keyword limit
// even when the payload contains multi-byte UTF-8 sequences.
const luceneMaxFieldBytes = 32000

// originalLogChunks splits the payload into strings that each fit within the Lucene field limit,
// keyed as original_log, original_log_1, original_log_2, … for the on-error index envelope.
func originalLogChunks(payload []byte) map[string]string {
	chunks := map[string]string{}
	baseKey := "original_log"
	for i, start := 0, 0; start < len(payload); i, start = i+1, start+luceneMaxFieldBytes {
		end := start + luceneMaxFieldBytes
		if end > len(payload) {
			end = len(payload)
		}
		key := baseKey
		if i > 0 {
			key = baseKey + "_" + strconv.Itoa(i)
		}
		chunks[key] = string(payload[start:end])
	}
	return chunks
}

// trimReasonPreview strips the "Preview of field's value: '...'" suffix that OpenSearch appends
// to mapper_parsing_exception reasons, which can contain the full field value.
func trimReasonPreview(reason string) string {
	const previewMarker = ". Preview of field's value:"
	if idx := strings.Index(reason, previewMarker); idx != -1 {
		return reason[:idx]
	}
	return reason
}

func (lbi *logBulkIndexer) submitToOnError(_ context.Context, resp opensearchapi.BulkRespItem, originalPayload []byte) {
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
	errorFields := map[string]any{
		"type":   errType,
		"reason": errReason,
		"status": resp.Status,
		"class":  classifyError(resp.Status, errType, lbi.errorClassification),
	}
	for k, v := range originalLogChunks(originalPayload) {
		errorFields[k] = v
	}
	envelope := map[string]any{
		"@timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		"error":      errorFields,
	}
	doc, err := json.Marshal(envelope)
	if err != nil {
		lbi.log().Debug("failed to marshal on-error envelope, dropping",
			zap.Error(err),
			zap.String("on_error_index", lbi.onErrorIndex),
		)
		lbi.appendPermanentError(err)
		return
	}
	lbi.onErrorDocs = append(lbi.onErrorDocs, doc)
}

func (lbi *logBulkIndexer) flushOnErrorIndex(ctx context.Context, client *opensearchapi.Client) error {
	if len(lbi.onErrorDocs) == 0 {
		return nil
	}
	lbi.log().Debug("flushing documents to on-error index",
		zap.Int("doc_count", len(lbi.onErrorDocs)),
		zap.String("on_error_index", lbi.onErrorIndex),
		zap.String("bulk_action", lbi.bulkAction),
	)
	recordFlushFailure := func() {
		if lbi.metrics != nil {
			lbi.metrics.recordOnErrorFlushFailure(ctx, lbi.onErrorIndex)
		}
	}
	onErrorIndexer, err := newLogOpenSearchBulkIndexer(client, lbi.onIndexerError, lbi.pipeline)
	if err != nil {
		lbi.log().Debug("failed to create on-error bulk indexer",
			zap.Error(err),
			zap.String("on_error_index", lbi.onErrorIndex),
		)
		return err
	}
	for _, doc := range lbi.onErrorDocs {
		doc := doc
		item := opensearchutil.BulkIndexerItem{
			Action: lbi.bulkAction,
			Index:  lbi.onErrorIndex,
			Body:   bytes.NewReader(doc),
		}
		item.OnFailure = func(_ context.Context, _ opensearchutil.BulkIndexerItem, resp opensearchapi.BulkRespItem, itemErr error) {
			recordFlushFailure()
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
			if itemErr != nil {
				lbi.log().Debug("failed to write document to on-error index (transport error), dropping",
					zap.Error(itemErr),
					zap.String("on_error_index", lbi.onErrorIndex),
					zap.ByteString("document", doc),
				)
				lbi.appendPermanentError(itemErr)
				return
			}
			lbi.log().Debug("failed to write document to on-error index (indexing error), dropping",
				zap.Int("status", resp.Status),
				zap.String("error_type", errType),
				zap.String("error_reason", errReason),
				zap.String("on_error_index", lbi.onErrorIndex),
				zap.ByteString("document", doc),
			)
			lbi.appendPermanentError(responseAsError(resp))
		}
		if addErr := onErrorIndexer.Add(ctx, item); addErr != nil {
			lbi.log().Debug("failed to enqueue document into on-error bulk indexer",
				zap.Error(addErr),
				zap.String("on_error_index", lbi.onErrorIndex),
			)
			lbi.appendPermanentError(addErr)
		}
	}
	if closeErr := onErrorIndexer.Close(ctx); closeErr != nil {
		lbi.log().Debug("on-error bulk indexer close returned error",
			zap.Error(closeErr),
			zap.String("on_error_index", lbi.onErrorIndex),
		)
		return closeErr
	}
	return nil
}
