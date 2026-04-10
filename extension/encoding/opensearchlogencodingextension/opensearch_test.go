// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package opensearchlogencodingextension

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestMarshalLogs_BasicSingleRecord(t *testing.T) {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "test-svc")

	sl := rl.ScopeLogs().AppendEmpty()
	sl.SetSchemaUrl("https://opentelemetry.io/schemas/1.21.0")
	scope := sl.Scope()
	scope.SetName("test-logger")
	scope.SetVersion("1.0.0")

	rec := sl.LogRecords().AppendEmpty()
	rec.SetTimestamp(pcommon.NewTimestampFromTime(time.Date(2024, 1, 15, 10, 30, 45, 123456789, time.UTC)))
	rec.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Date(2024, 1, 15, 10, 30, 46, 0, time.UTC)))
	rec.SetSeverityText("INFO")
	rec.SetSeverityNumber(plog.SeverityNumberInfo)
	rec.Body().SetStr("test log message")
	rec.Attributes().PutStr("user.id", "12345")
	rec.SetTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	rec.SetSpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8})

	e := &opensearchLogExtension{}
	result, err := e.MarshalLogs(logs)
	require.NoError(t, err)
	require.NotEmpty(t, result)

	var doc map[string]any
	require.NoError(t, json.Unmarshal(result, &doc))

	assert.Equal(t, "2024-01-15T10:30:45.123456789Z", doc["@timestamp"])
	assert.Equal(t, "test log message", doc["body"])
	assert.Equal(t, "2024-01-15T10:30:46Z", doc["observedTimestamp"])
	assert.Equal(t, "0102030405060708090a0b0c0d0e0f10", doc["traceId"])
	assert.Equal(t, "0102030405060708", doc["spanId"])

	severity := doc["severity"].(map[string]any)
	assert.Equal(t, "INFO", severity["text"])
	assert.Equal(t, float64(9), severity["number"])

	attrs := doc["attributes"].(map[string]any)
	assert.Equal(t, "12345", attrs["user.id"])

	resource := doc["resource"].(map[string]any)
	assert.Equal(t, "test-svc", resource["service.name"])

	assert.Equal(t, "https://opentelemetry.io/schemas/1.21.0", doc["schemaUrl"])

	is := doc["instrumentationScope"].(map[string]any)
	assert.Equal(t, "test-logger", is["name"])
	assert.Equal(t, "1.0.0", is["version"])
	assert.Equal(t, "https://opentelemetry.io/schemas/1.21.0", is["schemaUrl"])
}
