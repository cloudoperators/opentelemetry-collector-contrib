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
			lbi := &logBulkIndexer{errs: make([]error, tt.initialErrs)}
			resp := opensearchapi.BulkRespItem{Status: tt.status}
			logs := plog.NewLogs()
			rs := logs.ResourceLogs().AppendEmpty()
			ss := rs.ScopeLogs().AppendEmpty()
			logRecord := ss.LogRecords().AppendEmpty()
			lbi.processItemFailure(t.Context(), resp, nil, logRecord, nil, rs.Resource(), rs.SchemaUrl(), ss.Scope(), ss.SchemaUrl())
			if len(lbi.errs) != tt.expectedErrs {
				t.Errorf("expected %d errors, got %d", tt.expectedErrs, len(lbi.errs))
			}
		})
	}
}

func TestProcessItemFailureStampsAttributes(t *testing.T) {
	tests := []struct {
		name                   string
		status                 int
		errType                string
		errReason              string
		expectType             string
		expectReason           string
		expectStatus           int64
		expectClassification   string
		expectAttrsSet         bool
	}{
		{
			name:                 "permanent mapping error stamps all attrs",
			status:               400,
			errType:              "mapper_parsing_exception",
			errReason:            "failed to parse field",
			expectType:           "mapper_parsing_exception",
			expectReason:         "failed to parse field",
			expectStatus:         400,
			expectClassification: "permanent",
			expectAttrsSet:       true,
		},
		{
			name:                 "retryable 503 classified transient",
			status:               503,
			errType:              "es_rejected_execution_exception",
			errReason:            "queue capacity exceeded",
			expectType:           "es_rejected_execution_exception",
			expectReason:         "queue capacity exceeded",
			expectStatus:         503,
			expectClassification: "transient",
			expectAttrsSet:       true,
		},
		{
			name:                 "status without resp.Error stamps unknown fields",
			status:               500,
			expectType:           "unknown",
			expectReason:         "unknown",
			expectStatus:         500,
			expectClassification: "transient",
			expectAttrsSet:       true,
		},
		{
			name:           "no status and no resp.Error stamps nothing",
			status:         0,
			expectAttrsSet: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lbi := &logBulkIndexer{}
			resp := bulkRespItemWithError(t, tt.status, tt.errType, tt.errReason)
			logs := plog.NewLogs()
			rs := logs.ResourceLogs().AppendEmpty()
			ss := rs.ScopeLogs().AppendEmpty()
			logRecord := ss.LogRecords().AppendEmpty()

			lbi.processItemFailure(t.Context(), resp, nil, logRecord, nil, rs.Resource(), rs.SchemaUrl(), ss.Scope(), ss.SchemaUrl())

			attrs := logRecord.Attributes()
			if !tt.expectAttrsSet {
				if attrs.Len() != 0 {
					t.Errorf("expected no attributes, got %d", attrs.Len())
				}
				return
			}

			if v, ok := attrs.Get("opensearch.error.type"); !ok || v.AsString() != tt.expectType {
				t.Errorf("expected opensearch.error.type=%q, got ok=%v value=%q", tt.expectType, ok, v.AsString())
			}
			if v, ok := attrs.Get("opensearch.error.reason"); !ok || v.AsString() != tt.expectReason {
				t.Errorf("expected opensearch.error.reason=%q, got ok=%v value=%q", tt.expectReason, ok, v.AsString())
			}
			if v, ok := attrs.Get("opensearch.error.status"); !ok || v.Int() != tt.expectStatus {
				t.Errorf("expected opensearch.error.status=%d, got ok=%v value=%d", tt.expectStatus, ok, v.Int())
			}
			if v, ok := attrs.Get("opensearch.error.classification"); !ok || v.AsString() != tt.expectClassification {
				t.Errorf("expected opensearch.error.classification=%q, got ok=%v value=%q", tt.expectClassification, ok, v.AsString())
			}
		})
	}
}

func TestProcessItemFailureUsesUserClassification(t *testing.T) {
	cfg := &ErrorClassificationConfig{
		Permanent: []string{"custom_permanent_error"},
		Transient: []string{"custom_transient_error"},
	}
	lbi := &logBulkIndexer{errorClassification: cfg}
	// 500 status normally transient, but user override marks it permanent
	resp := bulkRespItemWithError(t, 500, "custom_permanent_error", "")
	logs := plog.NewLogs()
	rs := logs.ResourceLogs().AppendEmpty()
	ss := rs.ScopeLogs().AppendEmpty()
	logRecord := ss.LogRecords().AppendEmpty()

	lbi.processItemFailure(t.Context(), resp, nil, logRecord, nil, rs.Resource(), rs.SchemaUrl(), ss.Scope(), ss.SchemaUrl())

	v, ok := logRecord.Attributes().Get("opensearch.error.classification")
	if !ok || v.AsString() != "permanent" {
		t.Errorf("expected classification=permanent from user override, got ok=%v value=%q", ok, v.AsString())
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
			lbi := newLogBulkIndexer("create", nil, tt.pipeline, nil, "")
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
