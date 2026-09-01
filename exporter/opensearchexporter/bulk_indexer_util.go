// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package opensearchexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/opensearchexporter"

import (
	"errors"

	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

func shouldRetryEvent(status int) bool {
	retryOnStatus := []int{500, 502, 503, 504, 429}
	for _, s := range retryOnStatus {
		if s == status {
			return true
		}
	}
	return false
}

func responseAsError(item opensearchapi.BulkRespItem) error {
	if item.Error.Type == "" {
		return errors.New("unknown error")
	}
	return errors.New(item.Error.Type + ": " + item.Error.Reason)
}

func classify(status int, errorType string) string {
	// Permanent errors by type
	permanentTypes := []string{
		"mapper_parsing_exception",
		"document_parsing_exception",
		"strict_dynamic_mapping_exception",
		"illegal_argument_exception",
		"document_missing_exception",
		"version_conflict_engine_exception",
		"resource_already_exists_exception",
	}
	for _, t := range permanentTypes {
		if t == errorType {
			return "permanent"
		}
	}

	// Transient errors by type
	transientTypes := []string{
		"es_rejected_execution_exception",
		"unavailable_shards_exception",
		"cluster_block_exception",
		"timeout_exception",
		"circuit_breaking_exception",
	}
	for _, t := range transientTypes {
		if t == errorType {
			return "transient"
		}
	}

	// Fallback to status code
	if shouldRetryEvent(status) {
		return "transient"
	}
	return "permanent"
}
