// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package opensearchexporter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/collector/pdata/plog"
)

// newTestMetrics returns an exporterMetrics backed by an in-memory reader
// so tests can assert recorded values without a real collector pipeline.
func newTestMetrics(t *testing.T) (*exporterMetrics, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	m, err := newExporterMetrics(provider)
	require.NoError(t, err)
	return m, reader
}

func collectInt64Sum(t *testing.T, reader *sdkmetric.ManualReader, metricName string) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != metricName {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				key := ""
				for _, attr := range dp.Attributes.ToSlice() {
					if key != "" {
						key += ","
					}
					key += string(attr.Key) + "=" + attr.Value.Emit()
				}
				out[key] += dp.Value
			}
		}
	}
	return out
}

func TestRecordOnErrorDoc(t *testing.T) {
	m, reader := newTestMetrics(t)
	ctx := context.Background()

	m.recordOnErrorDoc(ctx, "mapper_parsing_exception", "permanent", 400)
	m.recordOnErrorDoc(ctx, "mapper_parsing_exception", "permanent", 400)
	m.recordOnErrorDoc(ctx, "illegal_argument_exception", "permanent", 400)

	sums := collectInt64Sum(t, reader, "otelcol_opensearch_exporter_on_error_docs")
	assert.Equal(t, int64(2), sums["error_class=permanent,error_type=mapper_parsing_exception,status=400"])
	assert.Equal(t, int64(1), sums["error_class=permanent,error_type=illegal_argument_exception,status=400"])
}

func TestRecordOnErrorFlushFailure(t *testing.T) {
	m, reader := newTestMetrics(t)
	ctx := context.Background()

	m.recordOnErrorFlushFailure(ctx)
	m.recordOnErrorFlushFailure(ctx)

	sums := collectInt64Sum(t, reader, "otelcol_opensearch_exporter_on_error_flush_failures")
	total := int64(0)
	for _, v := range sums {
		total += v
	}
	assert.Equal(t, int64(2), total)
}

func TestRecordPermanentError(t *testing.T) {
	m, reader := newTestMetrics(t)
	ctx := context.Background()

	m.recordPermanentError(ctx, "version_conflict_engine_exception", "permanent", 409)

	sums := collectInt64Sum(t, reader, "otelcol_opensearch_exporter_permanent_errors")
	assert.Equal(t, int64(1), sums["error_class=permanent,error_type=version_conflict_engine_exception,status=409"])
}

func TestRecordTransientError(t *testing.T) {
	m, reader := newTestMetrics(t)
	ctx := context.Background()

	m.recordTransientError(ctx, "es_rejected_execution_exception")
	m.recordTransientError(ctx, "es_rejected_execution_exception")

	sums := collectInt64Sum(t, reader, "otelcol_opensearch_exporter_transient_errors")
	assert.Equal(t, int64(2), sums["error_type=es_rejected_execution_exception"])
}

func TestLogBulkIndexerRecordsMetricsOnItemFailure(t *testing.T) {
	m, reader := newTestMetrics(t)
	ctx := context.Background()

	tests := []struct {
		name           string
		status         int
		errType        string
		onErrorIndex   string
		wantOnError    int64
		wantPermanent  int64
		wantTransient  int64
	}{
		{
			name:          "transient error increments transient counter",
			status:        503,
			errType:       "es_rejected_execution_exception",
			wantTransient: 1,
		},
		{
			name:          "permanent error without on-error index increments permanent counter",
			status:        400,
			errType:       "mapper_parsing_exception",
			wantPermanent: 1,
		},
		{
			name:         "permanent error with on-error index increments on-error-docs counter",
			status:       400,
			errType:      "mapper_parsing_exception",
			onErrorIndex: "errors-index",
			wantOnError:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			localMetrics, localReader := newTestMetrics(t)
			lbi := &logBulkIndexer{
				errorClassification: nil,
				onErrorIndex:        tt.onErrorIndex,
				metrics:             localMetrics,
			}
			resp := bulkRespItemWithError(t, tt.status, tt.errType, "some reason")

			logs := plog.NewLogs()
			rs := logs.ResourceLogs().AppendEmpty()
			ss := rs.ScopeLogs().AppendEmpty()
			logRecord := ss.LogRecords().AppendEmpty()

			lbi.processItemFailure(ctx, resp, nil, logRecord, []byte(`{}`), rs.Resource(), rs.SchemaUrl(), ss.Scope(), ss.SchemaUrl())

			onError := collectInt64Sum(t, localReader, "otelcol_opensearch_exporter_on_error_docs")
			permanent := collectInt64Sum(t, localReader, "otelcol_opensearch_exporter_permanent_errors")
			transient := collectInt64Sum(t, localReader, "otelcol_opensearch_exporter_transient_errors")

			totalOnError := int64(0)
			for _, v := range onError {
				totalOnError += v
			}
			totalPermanent := int64(0)
			for _, v := range permanent {
				totalPermanent += v
			}
			totalTransient := int64(0)
			for _, v := range transient {
				totalTransient += v
			}

			assert.Equal(t, tt.wantOnError, totalOnError, "on_error_docs_total mismatch")
			assert.Equal(t, tt.wantPermanent, totalPermanent, "permanent_errors_total mismatch")
			assert.Equal(t, tt.wantTransient, totalTransient, "transient_errors_total mismatch")
		})
	}

	// suppress unused variable
	_ = reader
	_ = m
}
