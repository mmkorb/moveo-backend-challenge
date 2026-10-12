package kafka

import (
	"context"
	"testing"
	"uuid"

	"github.com/google/go-cmp/cmp"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/platform/tracectx"
)

func TestRecordHeaders(t *testing.T) {
	t.Parallel()
	id := uuid.MustParse("0199d3a4-7c2e-7b1a-9f3e-2a4b6c8d0e1f")
	base := []kgo.RecordHeader{
		{Key: HeaderEventID, Value: []byte(id.String())},
		{Key: HeaderSchemaVersion, Value: []byte(SchemaVersion)},
	}
	tc := tracectx.Context{TraceID: "4bf92f3577b34da6a3ce929d0e0e4736", SpanID: "b7ad6b7169203331", Flags: 1}
	withState := tc
	withState.State = "vendor=opaque,other=1"

	tests := []struct {
		name string
		ctx  context.Context
		want []kgo.RecordHeader
	}{
		{"should add only the base headers without a trace context", context.Background(), base},
		{
			"should add traceparent with this hop's span",
			tracectx.NewContext(context.Background(), tc),
			append(append([]kgo.RecordHeader{}, base...),
				kgo.RecordHeader{Key: "traceparent", Value: []byte("00-4bf92f3577b34da6a3ce929d0e0e4736-b7ad6b7169203331-01")}),
		},
		{
			"should pass tracestate through unchanged",
			tracectx.NewContext(context.Background(), withState),
			append(append([]kgo.RecordHeader{}, base...),
				kgo.RecordHeader{Key: "traceparent", Value: []byte("00-4bf92f3577b34da6a3ce929d0e0e4736-b7ad6b7169203331-01")},
				kgo.RecordHeader{Key: "tracestate", Value: []byte("vendor=opaque,other=1")}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if diff := cmp.Diff(tt.want, recordHeaders(tt.ctx, id)); diff != "" {
				t.Errorf("headers mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
