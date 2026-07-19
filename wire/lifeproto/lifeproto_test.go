package lifeproto_test

import (
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/yasyf/daemonkit/wire/lifeproto"
)

type goldenFile struct {
	Version int          `json:"version"`
	Cases   []goldenCase `json:"cases"`
}

type goldenCase struct {
	Name   string          `json:"name"`
	Op     string          `json:"op"`
	Kind   string          `json:"kind"`
	Fields json.RawMessage `json:"fields"`
	Bytes  string          `json:"bytes"`
}

type healthFields struct {
	Build    string `json:"build"`
	Protocol int    `json:"protocol"`
	PID      int    `json:"pid"`
	State    string `json:"state"`
	Draining bool   `json:"draining"`
	Busy     bool   `json:"busy"`
}

func TestExactV2Golden(t *testing.T) {
	data, err := os.ReadFile("testdata/golden.json")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	var golden goldenFile
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatalf("Unmarshal golden: %v", err)
	}
	if golden.Version != lifeproto.Version || lifeproto.Version != 2 {
		t.Fatalf("golden=%d generated=%d", golden.Version, lifeproto.Version)
	}
	for _, test := range golden.Cases {
		t.Run(test.Name, func(t *testing.T) {
			message := buildGolden(t, test)
			got, err := lifeproto.Encode(message)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if string(got) != test.Bytes {
				t.Fatalf("Encode = %s, want %s", got, test.Bytes)
			}
			envelope, err := lifeproto.DecodeEnvelope([]byte(test.Bytes))
			if err != nil {
				t.Fatalf("DecodeEnvelope: %v", err)
			}
			if envelope.Op != test.Op {
				t.Fatalf("op = %q, want %q", envelope.Op, test.Op)
			}
		})
	}
}

func TestDecodeEnvelopeRejectsOldProtocol(t *testing.T) {
	_, err := lifeproto.DecodeEnvelope([]byte(`{"v":1,"op":"health"}`))
	if !errors.Is(err, lifeproto.ErrProtocolVersion) {
		t.Fatalf("DecodeEnvelope error = %v", err)
	}
}

func buildGolden(t *testing.T, test goldenCase) any {
	t.Helper()
	switch test.Op + "/" + test.Kind {
	case "health/request":
		return lifeproto.NewHealthRequest()
	case "health/response":
		var fields healthFields
		if err := json.Unmarshal(test.Fields, &fields); err != nil {
			t.Fatalf("health fields: %v", err)
		}
		return lifeproto.NewHealthResponse(fields.Build, fields.Protocol, fields.PID, fields.State, fields.Draining, fields.Busy)
	case "shutdown/request":
		return lifeproto.NewShutdownRequest()
	case "shutdown/response":
		return lifeproto.NewShutdownResponse(true)
	case "handoff/request":
		return lifeproto.NewHandoffRequest()
	case "handoff/response":
		return lifeproto.NewHandoffResponse(true)
	default:
		t.Fatalf("unknown golden case %s/%s", test.Op, test.Kind)
		return nil
	}
}
