// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package opensearchexporter

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter/exportertest"

	"github.com/cloudoperators/opentelemetry-collector-contrib/exporter/opensearchexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
)

func TestOpenSearchTraceExporter(t *testing.T) {
	type requestHandler struct {
		ValidateReceivedDocuments func(*testing.T, int, []map[string]any)
		ResponseJSONPath          string
	}

	checkAndRespond := func(responsePath string) requestHandler {
		pass := func(t *testing.T, _ int, docs []map[string]any) {
			for _, doc := range docs {
				require.NotEmpty(t, doc)
			}
		}
		return requestHandler{pass, responsePath}
	}
	tests := []struct {
		Label                  string
		TracePath              string
		RequestHandlers        []requestHandler
		ValidateExporterReturn func(error)
	}{
		{
			"Round trip",
			"testdata/traces-sample-a.yaml",
			[]requestHandler{
				checkAndRespond("testdata/opensearch-response-no-error.json"),
			},
			func(err error) {
				require.NoError(t, err)
			},
		},
		{
			"Permanent error",
			"testdata/traces-sample-a.yaml",
			[]requestHandler{
				checkAndRespond("testdata/opensearch-response-permanent-error.json"),
			},
			func(err error) {
				require.True(t, consumererror.IsPermanent(err))
			},
		},
		{
			"Retryable error",
			"testdata/traces-sample-a.yaml",
			[]requestHandler{
				checkAndRespond("testdata/opensearch-response-retryable-error.json"),
				checkAndRespond("testdata/opensearch-response-retryable-succeeded.json"),
			},
			func(err error) {
				require.NoError(t, err)
			},
		},

		{
			"Retryable error, succeeds on second try",
			"testdata/traces-sample-a.yaml",
			[]requestHandler{
				checkAndRespond("testdata/opensearch-response-retryable-error.json"),
				checkAndRespond("testdata/opensearch-response-retryable-error-2-attempt.json"),
				checkAndRespond("testdata/opensearch-response-retryable-succeeded.json"),
			},
			func(err error) {
				require.NoError(t, err)
			},
		},
	}

	getReceivedDocuments := func(body io.ReadCloser) []map[string]any {
		var rtn []map[string]any
		var err error
		decoder := json.NewDecoder(body)
		for decoder.More() {
			var jsonData any
			err = decoder.Decode(&jsonData)
			require.NoError(t, err)
			require.NotNil(t, jsonData)

			strMap := jsonData.(map[string]any)
			if actionData, isBulkAction := strMap["create"]; isBulkAction {
				validateBulkAction(t, "ss4o_traces-default-namespace", actionData.(map[string]any))
			} else {
				rtn = append(rtn, strMap)
			}
		}
		return rtn
	}

	for _, tc := range tests {
		// Create HTTP listener
		requestCount := 0
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var err error
			docs := getReceivedDocuments(r.Body)
			assert.LessOrEqualf(t, requestCount, len(tc.RequestHandlers), "Test case generated more requests than it has response for.")
			tc.RequestHandlers[requestCount].ValidateReceivedDocuments(t, requestCount, docs)

			w.WriteHeader(http.StatusOK)
			response, _ := os.ReadFile(tc.RequestHandlers[requestCount].ResponseJSONPath)
			_, err = w.Write(response)
			assert.NoError(t, err)

			requestCount++
		}))

		cfg := withDefaultConfig(func(config *Config) {
			config.Endpoint = ts.URL
			config.TimeoutSettings.Timeout = 0
		})

		// Create exporter
		f := NewFactory()
		exporter, err := f.CreateTraces(t.Context(), exportertest.NewNopSettings(metadata.Type), cfg)
		require.NoError(t, err)

		// Initialize the exporter
		err = exporter.Start(t.Context(), componenttest.NewNopHost())
		require.NoError(t, err)

		// Load sample data
		traces, err := golden.ReadTraces(tc.TracePath)
		require.NoError(t, err)

		// Send it
		err = exporter.ConsumeTraces(t.Context(), traces)
		tc.ValidateExporterReturn(err)
		err = exporter.Shutdown(t.Context())
		require.NoError(t, err)
		ts.Close()
	}
}

func TestOpenSearchLogExporter(t *testing.T) {
	type requestHandler struct {
		ValidateReceivedDocuments func(*testing.T, int, []map[string]any)
		ResponseJSONPath          string
	}

	checkAndRespond := func(responsePath string) requestHandler {
		pass := func(t *testing.T, _ int, docs []map[string]any) {
			for _, doc := range docs {
				require.NotEmpty(t, doc)
			}
		}
		return requestHandler{pass, responsePath}
	}
	tests := []struct {
		Label                  string
		LogPath                string
		RequestHandlers        []requestHandler
		ValidateExporterReturn func(error)
	}{
		{
			"Round trip",
			"testdata/logs-sample-a.yaml",
			[]requestHandler{
				checkAndRespond("testdata/opensearch-response-no-error.json"),
			},
			func(err error) {
				require.NoError(t, err)
			},
		},
		{
			"Permanent error",
			"testdata/logs-sample-a.yaml",
			[]requestHandler{
				checkAndRespond("testdata/opensearch-response-permanent-error.json"),
			},
			func(err error) {
				require.True(t, consumererror.IsPermanent(err))
			},
		},
		{
			"Mapping error — failed record stamped, succeeded record not returned",
			"testdata/logs-two-records.yaml",
			[]requestHandler{
				{
					ValidateReceivedDocuments: func(t *testing.T, _ int, docs []map[string]any) {
						require.Len(t, docs, 2)
					},
					ResponseJSONPath: "testdata/opensearch-response-mapping-error.json",
				},
			},
			func(err error) {
				// Must be permanent (mapper_parsing_exception)
				require.True(t, consumererror.IsPermanent(err))

				// Must carry only the failed subset via consumererror.Logs
				var logsErr consumererror.Logs
				require.ErrorAs(t, err, &logsErr)
				failedLogs := logsErr.Data()
				require.Equal(t, 1, failedLogs.LogRecordCount(), "only the failed record should be returned")

				// Failed record must carry error attributes
				lr := failedLogs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
				errType, ok := lr.Attributes().Get("opensearch.error.type")
				require.True(t, ok)
				require.Equal(t, "mapper_parsing_exception", errType.AsString())

				classification, ok := lr.Attributes().Get("opensearch.error.classification")
				require.True(t, ok)
				require.Equal(t, "permanent", classification.AsString())
			},
		},
		{
			"Retryable error",
			"testdata/logs-sample-a.yaml",
			[]requestHandler{
				checkAndRespond("testdata/opensearch-response-retryable-error.json"),
				checkAndRespond("testdata/opensearch-response-retryable-succeeded.json"),
			},
			func(err error) {
				require.NoError(t, err)
			},
		},

		{
			"Retryable error, succeeds on second try",
			"testdata/logs-sample-a.yaml",
			[]requestHandler{
				checkAndRespond("testdata/opensearch-response-retryable-error.json"),
				checkAndRespond("testdata/opensearch-response-retryable-error-2-attempt.json"),
				checkAndRespond("testdata/opensearch-response-retryable-succeeded.json"),
			},
			func(err error) {
				require.NoError(t, err)
			},
		},
	}

	getReceivedDocuments := func(body io.ReadCloser) []map[string]any {
		var rtn []map[string]any
		var err error
		decoder := json.NewDecoder(body)
		for decoder.More() {
			var jsonData any
			err = decoder.Decode(&jsonData)
			require.NoError(t, err)
			require.NotNil(t, jsonData)

			strMap := jsonData.(map[string]any)
			if actionData, isBulkAction := strMap["create"]; isBulkAction {
				validateBulkAction(t, "ss4o_logs-default-namespace", actionData.(map[string]any))
			} else {
				rtn = append(rtn, strMap)
			}
		}
		return rtn
	}

	for _, tc := range tests {
		// Create HTTP listener
		requestCount := 0
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var err error
			docs := getReceivedDocuments(r.Body)
			assert.LessOrEqualf(t, requestCount, len(tc.RequestHandlers), "Test case generated more requests than it has response for.")
			tc.RequestHandlers[requestCount].ValidateReceivedDocuments(t, requestCount, docs)

			w.WriteHeader(http.StatusOK)
			response, _ := os.ReadFile(tc.RequestHandlers[requestCount].ResponseJSONPath)
			_, err = w.Write(response)
			assert.NoError(t, err)

			requestCount++
		}))

		cfg := withDefaultConfig(func(config *Config) {
			config.Endpoint = ts.URL
			config.TimeoutSettings.Timeout = 0
		})

		// Create exporter
		f := NewFactory()
		exporter, err := f.CreateLogs(t.Context(), exportertest.NewNopSettings(metadata.Type), cfg)
		require.NoError(t, err)

		// Initialize the exporter
		err = exporter.Start(t.Context(), componenttest.NewNopHost())
		require.NoError(t, err)

		// Load sample data
		logs, err := golden.ReadLogs(tc.LogPath)
		require.NoError(t, err)

		// Send it
		err = exporter.ConsumeLogs(t.Context(), logs)
		tc.ValidateExporterReturn(err)
		err = exporter.Shutdown(t.Context())
		require.NoError(t, err)
		ts.Close()
	}
}

// validateBulkAction ensures the JSON object is to the correct index.
func validateBulkAction(t *testing.T, expectedIndex string, strMap map[string]any) {
	val, exists := strMap["_index"]
	require.True(t, exists)
	require.Equal(t, expectedIndex, val)
}

func TestOpenSearchLogExporterDLQIndex(t *testing.T) {
	requestCount := 0
	var dlqDocs []map[string]any

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		decoder := json.NewDecoder(r.Body)
		var docs []map[string]any
		for decoder.More() {
			var line any
			require.NoError(t, decoder.Decode(&line))
			obj, ok := line.(map[string]any)
			if !ok {
				continue
			}
			_, isCreate := obj["create"]
			_, isIndex := obj["index"]
			if !isCreate && !isIndex {
				docs = append(docs, obj)
			}
		}

		w.WriteHeader(http.StatusOK)
		switch requestCount {
		case 0:
			// Primary bulk: 2 docs, second fails with mapper_parsing_exception
			require.Len(t, docs, 2)
			response, _ := os.ReadFile("testdata/opensearch-response-mapping-error.json")
			_, _ = w.Write(response)
		case 1:
			// DLQ bulk: 1 envelope doc for the failed record
			require.Len(t, docs, 1)
			dlqDocs = docs
			response, _ := os.ReadFile("testdata/opensearch-response-dlq-success.json")
			_, _ = w.Write(response)
		default:
			t.Errorf("unexpected request %d", requestCount)
		}
		requestCount++
	}))
	defer ts.Close()

	cfg := withDefaultConfig(func(config *Config) {
		config.Endpoint = ts.URL
		config.TimeoutSettings.Timeout = 0
		config.LogsDLQIndex = "logs-dlq"
		config.ErrorClassification = ErrorClassificationConfig{
			Permanent: []string{"mapper_parsing_exception"},
		}
	})

	f := NewFactory()
	exporter, err := f.CreateLogs(t.Context(), exportertest.NewNopSettings(metadata.Type), cfg)
	require.NoError(t, err)
	require.NoError(t, exporter.Start(t.Context(), componenttest.NewNopHost()))

	logs, err := golden.ReadLogs("testdata/logs-two-records.yaml")
	require.NoError(t, err)

	err = exporter.ConsumeLogs(t.Context(), logs)
	require.NoError(t, err, "permanent errors routed to DLQ should not be returned to pipeline")

	require.NoError(t, exporter.Shutdown(t.Context()))

	require.Equal(t, 2, requestCount, "expected primary bulk + DLQ bulk requests")
	require.Len(t, dlqDocs, 1, "one failed record should land in DLQ")

	dlqDoc := dlqDocs[0]
	errBlock, ok := dlqDoc["error"].(map[string]any)
	require.True(t, ok, "DLQ doc must have error block")
	require.Equal(t, "mapper_parsing_exception", errBlock["type"])
	require.Equal(t, "permanent", errBlock["classification"])

	_, hasOriginal := dlqDoc["original"]
	require.True(t, hasOriginal, "DLQ doc must have original field")
}
