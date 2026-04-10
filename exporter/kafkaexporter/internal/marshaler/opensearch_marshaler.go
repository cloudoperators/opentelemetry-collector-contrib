package marshaler

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

// OpenSearchLogsMarshaler marshals logs to OpenSearch SS4O JSON.
type OpenSearchLogsMarshaler struct{}

func (m *OpenSearchLogsMarshaler) MarshalLogs(logs plog.Logs) ([]Message, error) {
	var total int
	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		for j := 0; j < logs.ResourceLogs().At(i).ScopeLogs().Len(); j++ {
			total += logs.ResourceLogs().At(i).ScopeLogs().At(j).LogRecords().Len()
		}
	}
	if total == 0 {
		return nil, nil
	}
	msgs, buf := make([]Message, 0, total), new(bytes.Buffer)
	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		rl := logs.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				rec := sl.LogRecords().At(k)
				buf.Reset()
				m.encode(buf, rec, rl.Resource(), sl.Scope(), sl.SchemaUrl())
				val := make([]byte, buf.Len())
				copy(val, buf.Bytes())
				var key []byte
				if !rec.TraceID().IsEmpty() {
					t := rec.TraceID()
					key = t[:]
				}
				msgs = append(msgs, Message{Key: key, Value: val})
			}
		}
	}
	return msgs, nil
}

func (m *OpenSearchLogsMarshaler) encode(b *bytes.Buffer, r plog.LogRecord, res pcommon.Resource, scope pcommon.InstrumentationScope, schema string) {
	fmt.Fprintf(b, `{"@timestamp":%s,"body":%s`, m.fmtTS(r.Timestamp()), jsonQuote(r.Body().AsString()))
	if r.ObservedTimestamp() != 0 {
		fmt.Fprintf(b, `,"observedTimestamp":%s`, m.fmtTS(r.ObservedTimestamp()))
	}
	if !r.TraceID().IsEmpty() {
		t := r.TraceID()
		fmt.Fprintf(b, `,"traceId":"%s"`, hex.EncodeToString(t[:]))
	}
	if !r.SpanID().IsEmpty() {
		s := r.SpanID()
		fmt.Fprintf(b, `,"spanId":"%s"`, hex.EncodeToString(s[:]))
	}
	fmt.Fprintf(b, `,"severity":{"text":%s,"number":%d},"attributes":`, jsonQuote(r.SeverityText()), r.SeverityNumber())
	writeMap(b, r.Attributes(), false)
	b.WriteString(`,"resource":`)
	writeMap(b, res.Attributes(), true)
	if schema != "" {
		fmt.Fprintf(b, `,"schemaUrl":%s`, jsonQuote(schema))
	}
	fmt.Fprintf(b, `,"instrumentationScope":{"name":%s,"version":%s`, jsonQuote(scope.Name()), jsonQuote(scope.Version()))
	if schema != "" {
		fmt.Fprintf(b, `,"schemaUrl":%s`, jsonQuote(schema))
	}
	if scope.Attributes().Len() > 0 {
		b.WriteString(`,"attributes":`)
		writeMap(b, scope.Attributes(), false)
	}
	b.WriteString("}}")
}

func (m *OpenSearchLogsMarshaler) fmtTS(ts pcommon.Timestamp) string {
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
		for i := 0; i < v.Slice().Len(); i++ {
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

func (m *OpenSearchLogsMarshaler) Encoding() string { return "opensearch_json" }

// jsonQuote returns s as a JSON-escaped quoted string.
func jsonQuote(s string) string {
	b, _ := json.Marshal(s) //nolint:errcheck // json.Marshal on a string cannot fail
	return string(b)
}
