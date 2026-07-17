package lifeproto_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/daemonkit/wire/lifeproto"
	"github.com/yasyf/daemonkit/wire/wiretest"
)

// TestProtocolVersion pins the wire version the Swift peer also pins.
func TestProtocolVersion(t *testing.T) {
	if lifeproto.Version != 1 {
		t.Fatalf("lifeproto.Version = %d, want 1 (Swift pins lifeProtocolVersion = 1)", lifeproto.Version)
	}
}

// TestGoldenBytes freezes the exact on-wire encoding of every op's request and
// response. Any drift here breaks compatibility with already-deployed peers.
func TestGoldenBytes(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{"health request", lifeproto.NewHealthRequest(), `{"v":1,"op":"health"}`},
		{
			"health response",
			lifeproto.NewHealthResponse("1.2.3", 4242, "healthy", false, false, []string{"handoff"}),
			`{"v":1,"op":"health","version":"1.2.3","pid":4242,"state":"healthy","draining":false,"busy":false,"features":["handoff"]}`,
		},
		{
			"health response draining busy degraded",
			lifeproto.NewHealthResponse("9999.42.0-dev", 7, "degraded", true, true, []string{"handoff", "drain"}),
			`{"v":1,"op":"health","version":"9999.42.0-dev","pid":7,"state":"degraded","draining":true,"busy":true,"features":["handoff","drain"]}`,
		},
		{"shutdown request", lifeproto.NewShutdownRequest(), `{"v":1,"op":"shutdown"}`},
		{"shutdown response", lifeproto.NewShutdownResponse(true), `{"v":1,"op":"shutdown","ok":true}`},
		{"hello request", lifeproto.NewHelloRequest(), `{"v":1,"op":"hello"}`},
		{"hello response", lifeproto.NewHelloResponse([]string{"handoff", "drain"}), `{"v":1,"op":"hello","features":["handoff","drain"]}`},
		{"hello response empty features", lifeproto.NewHelloResponse(nil), `{"v":1,"op":"hello","features":[]}`},
		{"handoff request", lifeproto.NewHandoffRequest(), `{"v":1,"op":"handoff"}`},
		{"handoff response", lifeproto.NewHandoffResponse(true), `{"v":1,"op":"handoff","ok":true}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := json.Marshal(tt.value)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if string(got) != tt.want {
				t.Errorf("golden mismatch\n got: %s\nwant: %s", got, tt.want)
			}
		})
	}
}

// TestFrameRoundTrip proves a request written over Framing round-trips through
// ReadEnvelope's op dispatch back into the concrete response type.
func TestFrameRoundTrip(t *testing.T) {
	client, server := wiretest.Pair(t)
	cf := wire.NewFraming(client)
	sf := wire.NewFraming(server)

	if err := lifeproto.Write(cf, lifeproto.NewHealthRequest()); err != nil {
		t.Fatalf("write request: %v", err)
	}
	env, raw, err := lifeproto.ReadEnvelope(sf)
	if err != nil {
		t.Fatalf("read envelope: %v", err)
	}
	if env.Op != lifeproto.OpHealth {
		t.Fatalf("op = %q, want %q", env.Op, lifeproto.OpHealth)
	}
	var req lifeproto.HealthRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		t.Fatalf("unmarshal request: %v", err)
	}
	if req.V != lifeproto.Version || req.Op != lifeproto.OpHealth {
		t.Errorf("decoded request = %+v, want v=%d op=%q", req, lifeproto.Version, lifeproto.OpHealth)
	}

	want := lifeproto.NewHealthResponse("2.0.0", 99, "healthy", false, false, []string{"handoff"})
	if err := lifeproto.Write(sf, want); err != nil {
		t.Fatalf("write response: %v", err)
	}
	var resp lifeproto.HealthResponse
	if err := cf.ReadJSON(&resp); err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.Version != "2.0.0" || resp.PID != 99 || resp.State != "healthy" || len(resp.Features) != 1 || resp.Features[0] != "handoff" {
		t.Errorf("decoded response = %+v, want the written snapshot", resp)
	}
}

// TestReadEnvelopeRejectsVersion proves a foreign protocol version fails closed
// with ErrProtocolVersion rather than silently decoding.
func TestReadEnvelopeRejectsVersion(t *testing.T) {
	client, server := wiretest.Pair(t)
	cf := wire.NewFraming(client)
	sf := wire.NewFraming(server)

	if err := cf.WriteFrame([]byte(`{"v":2,"op":"health"}`)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, _, err := lifeproto.ReadEnvelope(sf); !errors.Is(err, lifeproto.ErrProtocolVersion) {
		t.Fatalf("ReadEnvelope err = %v, want ErrProtocolVersion", err)
	}
}
