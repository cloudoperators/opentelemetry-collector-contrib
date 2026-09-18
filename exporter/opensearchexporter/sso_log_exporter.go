// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package opensearchexporter // import "github.com/cloudoperators/opentelemetry-collector-contrib/exporter/opensearchexporter"

import (
	"context"
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

	if l.config.MappingsSettings.ManageIndexTemplate {
		tm := newTemplateManager(client, l.telemetry.Logger)
		tm.ensureTemplates(ctx)
	}

	return nil
}

func (l *logExporter) pushLogData(ctx context.Context, ld plog.Logs) error {
	indexer := newLogBulkIndexer(l.bulkAction, l.model, l.config.Pipeline, &l.config.ErrorClass, l.config.LogsIndexOnError, l.metrics, l.telemetry.Logger)
	startErr := indexer.start(l.client)
	if startErr != nil {
		return startErr
	}

	// Use timestamp for index resolution
	logTimestamp := time.Now() // Replace with actual log timestamp extraction
	indexer.submit(ctx, ld, l.indexResolver, l.config, logTimestamp)
	indexer.close(ctx)

	// Flush OnError errors are captured in indexer.errs and returned via joinedError
	if err := indexer.flushOnErrorIndex(ctx, l.client); err != nil {
		indexer.appendPermanentError(err)
	}
	return indexer.joinedError()
}
