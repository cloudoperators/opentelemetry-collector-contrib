// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package opensearchexporter // import "github.com/cloudoperators/opentelemetry-collector-contrib/exporter/opensearchexporter"

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"

	"github.com/opensearch-project/opensearch-go/v4"
	"go.opentelemetry.io/collector/consumer/consumererror"
)

func classifyError(status int, errorType string, cfg *ErrorClassConfig) string {
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

	// Built-in default set of transient exceptions (user overrides above take precedence).
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

// isRetryableError reports whether a bulk-request level error (no per-item status) is worth
// retrying: timeouts, network errors, and HTTP 429/5xx responses. Mapping and other 4xx
// errors stay permanent so they go to the on-error index instead of being replayed forever.
// Encoding failures never leave the process and stay permanent too.
//
// The net.Error check is deliberately broad and also matches durable misconfigurations
// (untrusted certificate, unresolvable host). Those are retried until max_elapsed_time,
// which only costs retries; misclassifying a transient failure as permanent loses data.
// context.Canceled is included so a batch cut short by shutdown is not dropped as permanent.
func isRetryableError(err error) bool {
	if err == nil || consumererror.IsPermanent(err) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	if _, ok := errors.AsType[net.Error](err); ok {
		return true
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}
	if structErr, ok := errors.AsType[*opensearch.StructError](err); ok {
		return shouldRetryEvent(structErr.Status)
	}
	if stringErr, ok := errors.AsType[*opensearch.StringError](err); ok {
		return shouldRetryEvent(stringErr.Status)
	}
	// Non-JSON or empty error bodies come from proxies/load balancers (502/503/504 pages).
	return errors.Is(err, opensearch.ErrUnexpectedEmptyBody) ||
		errors.Is(err, opensearch.ErrJSONUnmarshalBody) ||
		errors.Is(err, opensearch.ErrReadBody)
}
