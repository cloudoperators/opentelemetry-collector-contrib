// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package opensearchlogencodingextension // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding/opensearchlogencodingextension"

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding"
)

var _ encoding.LogsMarshalerExtension = (*opensearchLogExtension)(nil)

type opensearchLogExtension struct{}

func (*opensearchLogExtension) MarshalLogs(_ plog.Logs) ([]byte, error) {
	return nil, nil
}

func (*opensearchLogExtension) Start(context.Context, component.Host) error {
	return nil
}

func (*opensearchLogExtension) Shutdown(context.Context) error {
	return nil
}
