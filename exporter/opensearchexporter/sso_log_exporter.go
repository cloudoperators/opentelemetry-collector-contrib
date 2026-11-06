// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package opensearchexporter // import "github.com/cloudoperators/opentelemetry-collector-contrib/exporter/opensearchexporter"

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/cloudoperators/opentelemetry-collector-contrib/exporter/opensearchexporter/internal/pool"
)

type logExporter struct {
	client        *opensearchapi.Client
	onErrorClient *opensearchapi.Client
	Index         string
	bulkAction    string
	model         mappingModel
	httpSettings  confighttp.ClientConfig
	telemetry     component.TelemetrySettings
	config        *Config
	indexResolver *indexResolver
	metrics       *exporterMetrics
}

func newLogExporter(cfg *Config, set exporter.Settings) (*logExporter, error) {
	var model mappingModel
	if cfg.MappingsSettings.Mode == MappingBodyMap.String() {
		model = &bodyMapMappingModel{
			bufferPool: pool.NewBufferPool(),
		}
	} else {
		model = &encodeModel{
			dedup:             cfg.MappingsSettings.Dedup,
			dedot:             cfg.MappingsSettings.Dedot,
			sso:               cfg.MappingsSettings.Mode == MappingSS4O.String(),
			otelV1:            cfg.MappingsSettings.Mode == MappingOTelV1.String(),
			flattenAttributes: cfg.MappingsSettings.Mode == MappingFlattenAttributes.String(),
			timestampField:    cfg.MappingsSettings.TimestampField,
			unixTime:          cfg.MappingsSettings.UnixTimestamp,
			dataset:           cfg.Dataset,
			namespace:         cfg.Namespace,
		}
	}

	metrics, err := newExporterMetrics(set.TelemetrySettings)
	if err != nil {
		return nil, err
	}
	defaultPrefix := "ss4o_logs"
	dataset := cfg.Dataset
	namespace := cfg.Namespace
	if cfg.MappingsSettings.Mode == MappingOTelV1.String() {
		defaultPrefix = "otel-v1-logs"
		dataset = ""
		namespace = ""
	}

	return &logExporter{
		telemetry:     set.TelemetrySettings,
		bulkAction:    cfg.BulkAction,
		httpSettings:  cfg.ClientConfig,
		model:         model,
		config:        cfg,
		indexResolver: newIndexResolver(defaultPrefix, dataset, namespace),
		metrics:       metrics,
	}, nil
}

func (l *logExporter) Start(ctx context.Context, host component.Host) error {
	httpClient, err := l.httpSettings.ToClient(ctx, host.GetExtensions(), l.telemetry)
	if err != nil {
		return err
	}

	client, err := newOpenSearchClient(l.httpSettings.Endpoint, httpClient, l.telemetry.Logger)
	if err != nil {
		return err
	}

	l.client = client

	// Dead-letter writes use a dedicated client whose transport caps every HTTP
	// round-trip at OnErrorFlush.Timeout. This guarantees flush and shutdown bounds
	// even if opensearchutil's worker pool ignores the ctx passed to Close, since the
	// transport layer will unblock stuck requests on its own.
	if l.config.OnErrorFlush.Timeout > 0 {
		onErrorHTTPClient := &http.Client{
			Transport: &perRequestTimeoutTransport{
				base:    httpClient.Transport,
				timeout: l.config.OnErrorFlush.Timeout,
			},
		}
		onErrorClient, oerr := newOpenSearchClient(l.httpSettings.Endpoint, onErrorHTTPClient, l.telemetry.Logger)
		if oerr != nil {
			return oerr
		}
		l.onErrorClient = onErrorClient
	} else {
		l.onErrorClient = client
	}

	if l.config.MappingsSettings.ManageIndexTemplate {
		tm := newTemplateManager(client, l.telemetry.Logger)
		tm.ensureTemplates(ctx)
	}

	return nil
}

func (l *logExporter) pushLogData(ctx context.Context, ld plog.Logs) error {
	indexer := newLogBulkIndexer(l.bulkAction, l.model, l.config.Pipeline, &l.config.ErrorClass, l.config.LogsIndexOnError, l.metrics, l.telemetry.Logger, l.config.FlushBytes)
	startErr := indexer.start(l.client)
	if startErr != nil {
		return startErr
	}

	// Use timestamp for index resolution
	logTimestamp := time.Now() // Replace with actual log timestamp extraction
	indexer.submit(ctx, ld, l.indexResolver, l.config, logTimestamp)
	indexer.close(ctx)

	// Dead-letter flush context handling is owned by flushOnErrorIndex itself based on
	// OnErrorFlush.Timeout: zero preserves the legacy single-ctx behavior; positive
	// detaches the flush from the push ctx's cancellation with separate Add/Close budgets.
	// l.onErrorClient's transport enforces a per-request timeout at the HTTP layer so
	// opensearchutil workers cannot block Close indefinitely on a stuck request.
	if err := indexer.flushOnErrorIndex(ctx, l.onErrorClient, l.config.OnErrorFlush.Timeout); err != nil {
		indexer.appendPermanentError(err)
	}
	return indexer.joinedError()
}

// perRequestTimeoutTransport wraps a base RoundTripper and enforces a hard timeout on
// every outbound request by attaching a context with the configured deadline. Used by
// the dead-letter path so opensearchutil's worker pool — which does not propagate our
// higher-level flush contexts into its HTTP calls — cannot block indefinitely on a
// stuck server, guaranteeing the advertised flush and shutdown bounds hold.
type perRequestTimeoutTransport struct {
	base    http.RoundTripper
	timeout time.Duration
}

func (t *perRequestTimeoutTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(req.Context(), t.timeout)
	// cancel is intentionally not deferred: the response body may still be read after
	// RoundTrip returns, so we rely on the net/http machinery to call cancel when the
	// body is closed. Standard pattern for context-wrapping RoundTrippers.
	req = req.WithContext(ctx)
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		cancel()
		return nil, err
	}
	resp.Body = &cancelOnCloseBody{ReadCloser: resp.Body, cancel: cancel}
	return resp, nil
}

type cancelOnCloseBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *cancelOnCloseBody) Close() error {
	err := b.ReadCloser.Close()
	b.cancel()
	return err
}
