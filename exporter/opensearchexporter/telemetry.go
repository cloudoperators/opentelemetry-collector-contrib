// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package opensearchexporter // import "github.com/cloudoperators/opentelemetry-collector-contrib/exporter/opensearchexporter"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cloudoperators/opentelemetry-collector-contrib/exporter/opensearchexporter/internal/metadata"
)

type exporterMetrics struct {
	*metadata.TelemetryBuilder
}

func newExporterMetrics(settings component.TelemetrySettings) (*exporterMetrics, error) {
	tb, err := metadata.NewTelemetryBuilder(settings)
	if err != nil {
		return nil, err
	}
	return &exporterMetrics{TelemetryBuilder: tb}, nil
}

func (m *exporterMetrics) recordOnErrorDoc(ctx context.Context, errorType, errorClass string, status int, index string) {
	m.OpensearchExporterOnErrorDocs.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("error_type", errorType),
			attribute.String("error_class", errorClass),
			attribute.Int("status", status),
			attribute.String("index", index),
		),
	)
}

func (m *exporterMetrics) recordOnErrorFlushFailure(ctx context.Context, index string) {
	m.OpensearchExporterOnErrorFlushFailures.Add(ctx, 1, metric.WithAttributes(attribute.String("index", index)))
}

func (m *exporterMetrics) recordPermanentError(ctx context.Context, errorType, errorClass string, status int, index string) {
	m.OpensearchExporterPermanentErrors.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("error_type", errorType),
			attribute.String("error_class", errorClass),
			attribute.Int("status", status),
			attribute.String("index", index),
		),
	)
}

func (m *exporterMetrics) recordTransientError(ctx context.Context, errorType string, index string) {
	m.OpensearchExporterTransientErrors.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("error_type", errorType),
			attribute.String("index", index),
		),
	)
}
