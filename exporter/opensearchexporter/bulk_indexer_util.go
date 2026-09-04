// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package opensearchexporter // import "github.com/cloudoperators/opentelemetry-collector-contrib/exporter/opensearchexporter"

func classifyError(status int, errorType string, cfg *ErrorClassificationConfig) string {
	// Check user-supplied overrides first
	if cfg != nil {
		for _, t := range cfg.Permanent {
			if t == errorType {
				return "permanent"
			}
		}
		for _, t := range cfg.Transient {
			if t == errorType {
				return "transient"
			}
		}
	}

	// Built-in default set of permanent exceptions (user overrides above take precedence).
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

	// If no configured list of transient errors are provided, we define a opinionated default set of exceptions.
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
