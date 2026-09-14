// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package opensearchexporter // import "github.com/cloudoperators/opentelemetry-collector-contrib/exporter/opensearchexporter"

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cloudoperators/opentelemetry-collector-contrib/exporter/opensearchexporter/internal/metadata"
)

// exporterMetrics holds all instrumentation counters for the opensearch exporter.
type exporterMetrics struct {
	onErrorDocsTotal         metric.Int64Counter
	onErrorFlushFailureTotal metric.Int64Counter
	permanentErrorsTotal     metric.Int64Counter
	transientErrorsTotal     metric.Int64Counter
}

func newExporterMetrics(mp metric.MeterProvider) (*exporterMetrics, error) {
	meter := mp.Meter(metadata.ScopeName)

	onErrorDocsTotal, err := meter.Int64Counter(
		"otelcol_opensearch_exporter_on_error_docs",
		metric.WithDescription("Number of documents routed to the on-error index due to permanent indexing failures."),
		metric.WithUnit("{document}"),
	)
	if err != nil {
		return nil, err
	}

	onErrorFlushFailureTotal, err := meter.Int64Counter(
		"otelcol_opensearch_exporter_on_error_flush_failures",
		metric.WithDescription("Number of failures when flushing documents to the on-error index."),
		metric.WithUnit("{failure}"),
	)
	if err != nil {
		return nil, err
	}

	permanentErrorsTotal, err := meter.Int64Counter(
		"otelcol_opensearch_exporter_permanent_errors",
		metric.WithDescription("Number of documents that encountered a permanent indexing error."),
		metric.WithUnit("{document}"),
	)
	if err != nil {
		return nil, err
	}

	transientErrorsTotal, err := meter.Int64Counter(
		"otelcol_opensearch_exporter_transient_errors",
		metric.WithDescription("Number of documents that encountered a transient (retriable) indexing error."),
		metric.WithUnit("{document}"),
	)
	if err != nil {
		return nil, err
	}

	return &exporterMetrics{
		onErrorDocsTotal:         onErrorDocsTotal,
		onErrorFlushFailureTotal: onErrorFlushFailureTotal,
		permanentErrorsTotal:     permanentErrorsTotal,
		transientErrorsTotal:     transientErrorsTotal,
	}, nil
}

func (m *exporterMetrics) recordOnErrorDoc(ctx context.Context, errorType, errorClass string, status int) {
	m.onErrorDocsTotal.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("error_type", errorType),
			attribute.String("error_class", errorClass),
			attribute.Int("status", status),
		),
	)
}

func (m *exporterMetrics) recordOnErrorFlushFailure(ctx context.Context) {
	m.onErrorFlushFailureTotal.Add(ctx, 1)
}

func (m *exporterMetrics) recordPermanentError(ctx context.Context, errorType, errorClass string, status int) {
	m.permanentErrorsTotal.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("error_type", errorType),
			attribute.String("error_class", errorClass),
			attribute.Int("status", status),
		),
	)
}

func (m *exporterMetrics) recordTransientError(ctx context.Context, errorType string) {
	m.transientErrorsTotal.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("error_type", errorType),
		),
	)
}
