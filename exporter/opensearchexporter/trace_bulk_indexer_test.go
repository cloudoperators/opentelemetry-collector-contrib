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
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestTraceJoinedError(t *testing.T) {
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
			tbi := &traceBulkIndexer{errs: tt.errs}
			err := tbi.joinedError()
			if (err != nil) != tt.hasError {
				t.Errorf("joinedError() = %v, expected error: %v", err, tt.hasError)
			}
		})
	}
}

func TestTraceProcessItemFailure(t *testing.T) {
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
			tbi := &traceBulkIndexer{errs: make([]error, tt.initialErrs)}
			resp := opensearchapi.BulkRespItem{Status: tt.status}
			traces := ptrace.NewTraces()
			rs := traces.ResourceSpans().AppendEmpty()
			ss := rs.ScopeSpans().AppendEmpty()
			span := ss.Spans().AppendEmpty()
			tbi.processItemFailure(resp, nil, span, rs.Resource(), rs.SchemaUrl(), ss.Scope(), ss.SchemaUrl())
			if len(tbi.errs) != tt.expectedErrs {
				t.Errorf("expected %d errors, got %d", tt.expectedErrs, len(tbi.errs))
			}
		})
	}
}

func TestNewTraceBulkIndexerWithPipeline(t *testing.T) {
	tests := []struct {
		name     string
		pipeline string
	}{
		{"empty pipeline", ""},
		{"with pipeline", "my-pipeline"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tbi := newTraceBulkIndexer("create", nil, tt.pipeline, nil, nil)
			if tbi.pipeline != tt.pipeline {
				t.Errorf("expected pipeline %q, got %q", tt.pipeline, tbi.pipeline)
			}
			if tbi.bulkAction != "create" {
				t.Errorf("expected bulkAction 'create', got %s", tbi.bulkAction)
			}
		})
	}
}

func TestTraceNewBulkIndexerItem(t *testing.T) {
	tbi := &traceBulkIndexer{bulkAction: "create"}
	payload := []byte(`{"test": "data"}`)
	indexName := "test-index"
	item := tbi.newBulkIndexerItem(payload, indexName)

	if item.Action != "create" {
		t.Errorf("expected action 'create', got %s", item.Action)
	}
	if item.Index != indexName {
		t.Errorf("expected index %s, got %s", indexName, item.Index)
	}
	if item.Body == nil {
		t.Error("expected body to be set")
	}
}

func TestMakeTrace(t *testing.T) {
	resource := pcommon.NewResource()
	resource.Attributes().PutStr("service.name", "test-service")
	scope := pcommon.NewInstrumentationScope()
	scope.SetName("test-scope")
	span := ptrace.NewSpan()
	span.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))

	traces := makeTrace(resource, "resource-schema", scope, "scope-schema", span)

	if traces.ResourceSpans().Len() != 1 {
		t.Error("expected 1 resource span")
	}
	rs := traces.ResourceSpans().At(0)
	if rs.SchemaUrl() != "resource-schema" {
		t.Errorf("expected schema 'resource-schema', got %s", rs.SchemaUrl())
	}
	if rs.ScopeSpans().Len() != 1 {
		t.Error("expected 1 scope span")
	}
	ss := rs.ScopeSpans().At(0)
	if ss.SchemaUrl() != "scope-schema" {
		t.Errorf("expected schema 'scope-schema', got %s", ss.SchemaUrl())
	}
	if ss.Spans().Len() != 1 {
		t.Error("expected 1 span")
	}
}

func TestResponseAsError(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    string
	}{
		{
			name:    "type and reason set",
			payload: `{"error":{"type":"mapper_parsing_exception","reason":"bad field"}}`,
			want:    "mapper_parsing_exception: bad field",
		},
		{
			name:    "only type set",
			payload: `{"error":{"type":"illegal_argument_exception"}}`,
			want:    "illegal_argument_exception",
		},
		{
			name:    "only reason set",
			payload: `{"error":{"reason":"something went wrong"}}`,
			want:    "something went wrong",
		},
		{
			name:    "empty error object",
			payload: `{"error":{}}`,
			want:    "unknown error",
		},
		{
			name:    "no error field",
			payload: `{}`,
			want:    "unknown error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var item opensearchapi.BulkRespItem
			if err := json.Unmarshal([]byte(tt.payload), &item); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			got := responseAsError(item).Error()
			if got != tt.want {
				t.Errorf("responseAsError() = %q, want %q", got, tt.want)
			}
		})
	}
}
