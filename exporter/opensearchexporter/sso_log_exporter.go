// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package opensearchexporter // import "github.com/cloudoperators/opentelemetry-collector-contrib/exporter/opensearchexporter"

import (
	"context"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/cloudoperators/opentelemetry-collector-contrib/exporter/opensearchexporter/internal/pool"
)

type logExporter struct {
	client               *opensearchapi.Client
	onErrorBaseTransport http.RoundTripper
	Index                string
	bulkAction           string
	model                mappingModel
	httpSettings         confighttp.ClientConfig
	telemetry            component.TelemetrySettings
	config               *Config
	indexResolver        *indexResolver
	metrics              *exporterMetrics
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

	// Dead-letter writes get a per-flush transport+client wrapping the shared base
	// RoundTripper. Building the wrapper per flush keeps each concurrent flush's drain
	// deadline isolated from overlapping flushes.
	if l.config.OnErrorFlush.Timeout > 0 {
		l.onErrorBaseTransport = httpClient.Transport
	}

	if l.config.MappingsSettings.ManageIndexTemplate {
		tm := newTemplateManager(client, l.telemetry.Logger)
		tm.ensureTemplates(ctx)
	}

	return nil
}

func (l *logExporter) pushLogData(ctx context.Context, ld plog.Logs) error {
	indexer := newLogBulkIndexer(l.bulkAction, l.model, l.config.Pipeline, &l.config.ErrorClass, l.config.LogsIndexOnError, l.metrics, l.telemetry.Logger, l.config.FlushBytes)

	onErrorClient := l.client
	if l.onErrorBaseTransport != nil {
		transport := &perRequestTimeoutTransport{
			base:    l.onErrorBaseTransport,
			timeout: l.config.OnErrorFlush.Timeout,
		}
		c, err := newOpenSearchClient(l.httpSettings.Endpoint, &http.Client{Transport: transport}, l.telemetry.Logger)
		if err != nil {
			return err
		}
		onErrorClient = c
		indexer.onErrorDrainDeadlineSetter = transport.setSharedDeadline
	}

	startErr := indexer.start(l.client)
	if startErr != nil {
		return startErr
	}

	// Use timestamp for index resolution
	logTimestamp := time.Now() // Replace with actual log timestamp extraction
	indexer.submit(ctx, ld, l.indexResolver, l.config, logTimestamp)
	indexer.close(ctx)

	// Timeout=0 preserves legacy single-ctx behavior; timeout>0 is handled inside
	// flushOnErrorIndex via a detached ctx and the transport's per-request bound.
	if err := indexer.flushOnErrorIndex(ctx, onErrorClient, l.config.OnErrorFlush.Timeout); err != nil {
		indexer.appendPermanentError(err)
	}
	return indexer.joinedError()
}

// perRequestTimeoutTransport bounds every outbound request two ways:
//   - per-request timeout caps a single round-trip,
//   - optional shared deadline set before a drain makes queued requests fail fast
//     once the overall budget is spent (prevents N serial requests under flush_bytes=1
//     from blowing the shutdown bound).
type perRequestTimeoutTransport struct {
	base    http.RoundTripper
	timeout time.Duration

	mu             sync.RWMutex
	sharedDeadline time.Time // zero means no shared deadline
}

// setSharedDeadline installs an absolute deadline every subsequent RoundTrip must
// honor alongside its per-request timeout. Zero clears it.
func (t *perRequestTimeoutTransport) setSharedDeadline(d time.Time) {
	t.mu.Lock()
	t.sharedDeadline = d
	t.mu.Unlock()
}

func (t *perRequestTimeoutTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.mu.RLock()
	shared := t.sharedDeadline
	t.mu.RUnlock()

	// Fail fast once the shared drain deadline has passed.
	if !shared.IsZero() && !time.Now().Before(shared) {
		return nil, context.DeadlineExceeded
	}

	effective := time.Now().Add(t.timeout)
	if !shared.IsZero() && shared.Before(effective) {
		effective = shared
	}
	ctx, cancel := context.WithDeadline(req.Context(), effective)
	// cancel is called by cancelOnCloseBody when the response body closes; standard
	// pattern for context-wrapping RoundTrippers.
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
