// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package opensearchlogencodingextension // import "github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding/opensearchlogencodingextension"

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding"
)

var _ encoding.LogsMarshalerExtension = (*opensearchLogExtension)(nil)

type opensearchLogExtension struct{}

// MarshalLogs encodes logs as NDJSON following the SS4O schema.
// Each log record becomes one JSON line. Returns nil for empty input.
func (e *opensearchLogExtension) MarshalLogs(ld plog.Logs) ([]byte, error) {
	var total int
	for i := range ld.ResourceLogs().Len() {
		for j := range ld.ResourceLogs().At(i).ScopeLogs().Len() {
			total += ld.ResourceLogs().At(i).ScopeLogs().At(j).LogRecords().Len()
		}
	}
	if total == 0 {
		return nil, nil
	}

	var out bytes.Buffer
	first := true
	for i := range ld.ResourceLogs().Len() {
		rl := ld.ResourceLogs().At(i)
		for j := range rl.ScopeLogs().Len() {
			sl := rl.ScopeLogs().At(j)
			for k := range sl.LogRecords().Len() {
				if !first {
					out.WriteByte('\n')
				}
				first = false
				encodeRecord(&out, sl.LogRecords().At(k), rl.Resource(), sl.Scope(), sl.SchemaUrl())
			}
		}
	}
	return out.Bytes(), nil
}

func encodeRecord(b *bytes.Buffer, r plog.LogRecord, res pcommon.Resource, scope pcommon.InstrumentationScope, schemaURL string) {
	fmt.Fprintf(b, `{"@timestamp":%s,"body":%s`, fmtTimestamp(r.Timestamp()), jsonQuote(r.Body().AsString()))
	if r.ObservedTimestamp() != 0 {
		fmt.Fprintf(b, `,"observedTimestamp":%s`, fmtTimestamp(r.ObservedTimestamp()))
	}
	if !r.TraceID().IsEmpty() {
		tid := r.TraceID()
		fmt.Fprintf(b, `,"traceId":"%s"`, hex.EncodeToString(tid[:]))
	}
	if !r.SpanID().IsEmpty() {
		sid := r.SpanID()
		fmt.Fprintf(b, `,"spanId":"%s"`, hex.EncodeToString(sid[:]))
	}
	fmt.Fprintf(b, `,"severity":{"text":%s,"number":%d},"attributes":`, jsonQuote(r.SeverityText()), r.SeverityNumber())
	writeMap(b, r.Attributes(), false)
	b.WriteString(`,"resource":`)
	writeMap(b, res.Attributes(), true)
	if schemaURL != "" {
		fmt.Fprintf(b, `,"schemaUrl":%s`, jsonQuote(schemaURL))
	}
	fmt.Fprintf(b, `,"instrumentationScope":{"name":%s,"version":%s`, jsonQuote(scope.Name()), jsonQuote(scope.Version()))
	if schemaURL != "" {
		fmt.Fprintf(b, `,"schemaUrl":%s`, jsonQuote(schemaURL))
	}
	if scope.Attributes().Len() > 0 {
		b.WriteString(`,"attributes":`)
		writeMap(b, scope.Attributes(), false)
	}
	b.WriteString("}}")
}

func fmtTimestamp(ts pcommon.Timestamp) string {
	if ts == 0 {
		return "null"
	}
	return jsonQuote(ts.AsTime().Format(time.RFC3339Nano))
}

func writeMap(b *bytes.Buffer, m pcommon.Map, strVal bool) {
	b.WriteByte('{')
	first := true
	m.Range(func(k string, v pcommon.Value) bool {
		if !first {
			b.WriteByte(',')
		}
		first = false
		b.WriteString(jsonQuote(k))
		b.WriteByte(':')
		if strVal {
			b.WriteString(jsonQuote(v.AsString()))
		} else {
			writeVal(b, v)
		}
		return true
	})
	b.WriteByte('}')
}

func writeVal(b *bytes.Buffer, v pcommon.Value) {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		b.WriteString(jsonQuote(v.Str()))
	case pcommon.ValueTypeBool:
		b.WriteString(strconv.FormatBool(v.Bool()))
	case pcommon.ValueTypeInt:
		b.WriteString(strconv.FormatInt(v.Int(), 10))
	case pcommon.ValueTypeDouble:
		b.WriteString(strconv.FormatFloat(v.Double(), 'g', -1, 64))
	case pcommon.ValueTypeMap:
		writeMap(b, v.Map(), false)
	case pcommon.ValueTypeSlice:
		b.WriteByte('[')
		for i := range v.Slice().Len() {
			if i > 0 {
				b.WriteByte(',')
			}
			writeVal(b, v.Slice().At(i))
		}
		b.WriteByte(']')
	case pcommon.ValueTypeBytes:
		fmt.Fprintf(b, `"%s"`, hex.EncodeToString(v.Bytes().AsRaw()))
	default:
		b.WriteString("null")
	}
}

// jsonQuote returns s as a JSON-escaped quoted string.
func jsonQuote(s string) string {
	b, _ := json.Marshal(s) //nolint:errcheck // json.Marshal on a string cannot fail
	return string(b)
}

func (*opensearchLogExtension) Start(context.Context, component.Host) error {
	return nil
}

func (*opensearchLogExtension) Shutdown(context.Context) error {
	return nil
}
