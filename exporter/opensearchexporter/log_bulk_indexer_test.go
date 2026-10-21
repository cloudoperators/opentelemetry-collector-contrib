// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package opensearchexporter

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

// bulkRespItemWithError builds a BulkRespItem with the given status and error fields.
// The Error field on BulkRespItem is an anonymous struct, so we round-trip through JSON
// to populate it without duplicating the type definition.
func bulkRespItemWithError(t *testing.T, status int, errType, errReason string) opensearchapi.BulkRespItem {
	t.Helper()
	payload := map[string]any{"status": status}
	if errType != "" || errReason != "" {
		payload["error"] = map[string]any{"type": errType, "reason": errReason}
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var item opensearchapi.BulkRespItem
	if err := json.Unmarshal(raw, &item); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return item
}

func TestJoinedError(t *testing.T) {
	tests := []struct {
		name     string
		errs     []error
		hasError bool
	}{
		{"no errors", nil, false},
		{"single error", []error{errors.New("test")}, true},
		{"multiple errors", []error{errors.New("err1"), errors.New("err2")}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lbi := &logBulkIndexer{errs: tt.errs}
			err := lbi.joinedError()
			if (err != nil) != tt.hasError {
				t.Errorf("joinedError() = %v, expected error: %v", err, tt.hasError)
			}
		})
	}
}

func TestProcessItemFailure(t *testing.T) {
	tests := []struct {
		name         string
		status       int
		initialErrs  int
		expectedErrs int
	}{
		{"retry status", 500, 0, 1},
		{"permanent status", 400, 0, 1},
		{"no status", 0, 0, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lbi := &logBulkIndexer{errs: make([]error, tt.initialErrs), logger: zap.NewNop()}
			resp := opensearchapi.BulkRespItem{Status: tt.status}
			logs := plog.NewLogs()
			rs := logs.ResourceLogs().AppendEmpty()
			ss := rs.ScopeLogs().AppendEmpty()
			logRecord := ss.LogRecords().AppendEmpty()
			lbi.processItemFailure(t.Context(), resp, nil, logRecord, nil, rs.Resource(), rs.SchemaUrl(), ss.Scope(), ss.SchemaUrl())
			if got := len(lbi.errs) + len(lbi.retryErrs); got != tt.expectedErrs {
				t.Errorf("expected %d errors, got %d", tt.expectedErrs, got)
			}
		})
	}
}

// onErrorFields returns the "error" object of the single on-error document lbi collected.
func onErrorFields(t *testing.T, lbi *logBulkIndexer) map[string]any {
	t.Helper()
	if len(lbi.onErrorDocs) != 1 {
		t.Fatalf("expected 1 on-error document, got %d", len(lbi.onErrorDocs))
	}
	var doc map[string]any
	if err := json.Unmarshal(lbi.onErrorDocs[0], &doc); err != nil {
		t.Fatalf("unmarshal on-error document: %v", err)
	}
	fields, ok := doc["error"].(map[string]any)
	if !ok {
		t.Fatalf("on-error document has no error object: %s", lbi.onErrorDocs[0])
	}
	return fields
}

func TestProcessItemFailureRecordsErrorDetails(t *testing.T) {
	tests := []struct {
		name         string
		status       int
		errType      string
		errReason    string
		expectType   string
		expectReason string
		expectClass  string
	}{
		{
			name:         "permanent mapping error",
			status:       400,
			errType:      "mapper_parsing_exception",
			errReason:    "failed to parse field",
			expectType:   "mapper_parsing_exception",
			expectReason: "failed to parse field",
			expectClass:  "permanent",
		},
		{
			name:        "retryable 503 classified transient",
			status:      503,
			errType:     "es_rejected_execution_exception",
			errReason:   "queue capacity exceeded",
			expectClass: "transient",
		},
		{
			name:        "status without resp.Error",
			status:      500,
			expectClass: "transient",
		},
		{
			name:         "4xx without resp.Error",
			status:       404,
			expectType:   "unknown",
			expectReason: "unknown",
			expectClass:  "permanent",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lbi := &logBulkIndexer{onErrorIndex: "deadletter", logger: zap.NewNop()}
			resp := bulkRespItemWithError(t, tt.status, tt.errType, tt.errReason)
			logs := plog.NewLogs()
			rs := logs.ResourceLogs().AppendEmpty()
			ss := rs.ScopeLogs().AppendEmpty()
			logRecord := ss.LogRecords().AppendEmpty()
			logRecord.Body().SetStr("payload")

			lbi.processItemFailure(t.Context(), resp, nil, logRecord, []byte(`{"body":"payload"}`), rs.Resource(), rs.SchemaUrl(), ss.Scope(), ss.SchemaUrl())

			if n := logRecord.Attributes().Len(); n != 0 {
				t.Errorf("input record was modified: %v", logRecord.Attributes().AsRaw())
			}
			if tt.expectClass == "transient" {
				if len(lbi.onErrorDocs) != 0 {
					t.Errorf("transient error must not be written to the on-error index")
				}
				if got := lbi.retryLogs.LogRecordCount(); got != 1 {
					t.Fatalf("expected 1 record for retry, got %d", got)
				}
				retried := lbi.retryLogs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
				if n := retried.Attributes().Len(); n != 0 {
					t.Errorf("retried record carries attributes: %v", retried.Attributes().AsRaw())
				}
				return
			}
			fields := onErrorFields(t, lbi)
			if fields["type"] != tt.expectType {
				t.Errorf("expected error.type=%q, got %v", tt.expectType, fields["type"])
			}
			if fields["reason"] != tt.expectReason {
				t.Errorf("expected error.reason=%q, got %v", tt.expectReason, fields["reason"])
			}
			if fields["status"] != float64(tt.status) {
				t.Errorf("expected error.status=%d, got %v", tt.status, fields["status"])
			}
			if fields["class"] != tt.expectClass {
				t.Errorf("expected error.class=%q, got %v", tt.expectClass, fields["class"])
			}
			if fields["original_log"] != `{"body":"payload"}` {
				t.Errorf("expected original_log to hold the payload, got %v", fields["original_log"])
			}
		})
	}
}

func TestProcessItemFailureUsesUserClassification(t *testing.T) {
	cfg := &ErrorClassConfig{
		Permanent: []string{"custom_permanent_error"},
		Transient: []string{"custom_transient_error"},
	}
	lbi := &logBulkIndexer{errorClassification: cfg, onErrorIndex: "deadletter", logger: zap.NewNop()}
	// 500 status normally transient, but user override marks it permanent
	resp := bulkRespItemWithError(t, 500, "custom_permanent_error", "")
	logs := plog.NewLogs()
	rs := logs.ResourceLogs().AppendEmpty()
	ss := rs.ScopeLogs().AppendEmpty()
	logRecord := ss.LogRecords().AppendEmpty()

	lbi.processItemFailure(t.Context(), resp, nil, logRecord, nil, rs.Resource(), rs.SchemaUrl(), ss.Scope(), ss.SchemaUrl())

	if got := onErrorFields(t, lbi)["class"]; got != "permanent" {
		t.Errorf("expected class=permanent from user override, got %v", got)
	}
	if len(lbi.retryErrs) != 0 {
		t.Errorf("permanent error must not be retried, got %v", lbi.retryErrs)
	}
}

func TestNewLogBulkIndexerWithPipeline(t *testing.T) {
	tests := []struct {
		name     string
		pipeline string
	}{
		{"empty pipeline", ""},
		{"with pipeline", "my-pipeline"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lbi := newLogBulkIndexer("create", nil, tt.pipeline, nil, "", nil, zap.NewNop(), 1, 0)
			if lbi.pipeline != tt.pipeline {
				t.Errorf("expected pipeline %q, got %q", tt.pipeline, lbi.pipeline)
			}
			if lbi.bulkAction != "create" {
				t.Errorf("expected bulkAction 'create', got %s", lbi.bulkAction)
			}
		})
	}
}

func TestNewBulkIndexerItem(t *testing.T) {
	lbi := &logBulkIndexer{bulkAction: "index"}
	payload := []byte(`{"test": "data"}`)
	indexName := "test-index"
	item := lbi.newBulkIndexerItem(payload, indexName)

	if item.Action != "index" {
		t.Errorf("expected action 'index', got %s", item.Action)
	}
	if item.Index != indexName {
		t.Errorf("expected index %s, got %s", indexName, item.Index)
	}
	if item.Body == nil {
		t.Error("expected body to be set")
	}
}

func TestMakeLog(t *testing.T) {
	resource := pcommon.NewResource()
	resource.Attributes().PutStr("service.name", "test-service")
	scope := pcommon.NewInstrumentationScope()
	scope.SetName("test-scope")
	logRecord := plog.NewLogRecord()
	logRecord.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	logs := makeLog(resource, "resource-schema", scope, "scope-schema", logRecord)

	if logs.ResourceLogs().Len() != 1 {
		t.Error("expected 1 resource log")
	}
	rl := logs.ResourceLogs().At(0)
	if rl.SchemaUrl() != "resource-schema" {
		t.Errorf("expected schema 'resource-schema', got %s", rl.SchemaUrl())
	}
	if rl.ScopeLogs().Len() != 1 {
		t.Error("expected 1 scope log")
	}
	sl := rl.ScopeLogs().At(0)
	if sl.SchemaUrl() != "scope-schema" {
		t.Errorf("expected schema 'scope-schema', got %s", sl.SchemaUrl())
	}
	if sl.LogRecords().Len() != 1 {
		t.Error("expected 1 log record")
	}
}
