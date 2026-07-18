package lifeproto_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/daemonkit/wire/lifeproto"
)

// goldenFields is the union of every op-specific field a golden case may carry.
type goldenFields struct {
	Version  string   `json:"version"`
	PID      int      `json:"pid"`
	State    string   `json:"state"`
	Draining bool     `json:"draining"`
	Busy     bool     `json:"busy"`
	Features []string `json:"features"`
	OK       bool     `json:"ok"`
}

type goldenCase struct {
	Name   string       `json:"name"`
	Op     string       `json:"op"`
	Kind   string       `json:"kind"`
	Fields goldenFields `json:"fields"`
	Bytes  string       `json:"bytes"`
}

type goldenFile struct {
	Version int          `json:"version"`
	Cases   []goldenCase `json:"cases"`
}

func loadGolden(t *testing.T) goldenFile {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "golden.json"))
	if err != nil {
		t.Fatalf("read golden fixture: %v", err)
	}
	var g goldenFile
	if err := json.Unmarshal(b, &g); err != nil {
		t.Fatalf("decode golden fixture: %v", err)
	}
	return g
}

// build reconstructs a case's message from its fields through the public
// constructors — the encode path a real peer takes.
func build(t *testing.T, c goldenCase) any {
	t.Helper()
	f := c.Fields
	switch c.Op + "/" + c.Kind {
	case "health/request":
		return lifeproto.NewHealthRequest()
	case "health/response":
		return lifeproto.NewHealthResponse(f.Version, f.PID, f.State, f.Draining, f.Busy, f.Features)
	case "shutdown/request":
		return lifeproto.NewShutdownRequest()
	case "shutdown/response":
		return lifeproto.NewShutdownResponse(f.OK)
	case "hello/request":
		return lifeproto.NewHelloRequest()
	case "hello/response":
		return lifeproto.NewHelloResponse(f.Features)
	case "handoff/request":
		return lifeproto.NewHandoffRequest()
	case "handoff/response":
		return lifeproto.NewHandoffResponse(f.OK)
	default:
		t.Fatalf("unknown golden case %s/%s", c.Op, c.Kind)
		return nil
	}
}

// newMessage returns a fresh pointer to a case's concrete type, for the decode
// direction.
func newMessage(t *testing.T, c goldenCase) any {
	t.Helper()
	switch c.Op + "/" + c.Kind {
	case "health/request":
		return &lifeproto.HealthRequest{}
	case "health/response":
		return &lifeproto.HealthResponse{}
	case "shutdown/request":
		return &lifeproto.ShutdownRequest{}
	case "shutdown/response":
		return &lifeproto.ShutdownResponse{}
	case "hello/request":
		return &lifeproto.HelloRequest{}
	case "hello/response":
		return &lifeproto.HelloResponse{}
	case "handoff/request":
		return &lifeproto.HandoffRequest{}
	case "handoff/response":
		return &lifeproto.HandoffResponse{}
	default:
		t.Fatalf("unknown golden case %s/%s", c.Op, c.Kind)
		return nil
	}
}

// TestCrossGolden loads the shared cross-language fixture and asserts both wire
// directions for every case: constructing from fields encodes to the frozen
// bytes, and decoding those bytes re-encodes to the same bytes. The Swift suite
// (Tests/DaemonKitTests/LifecycleWireTests.swift) loads the same file and makes
// the same assertions, so the two bindings can never drift.
func TestCrossGolden(t *testing.T) {
	g := loadGolden(t)
	if g.Version != lifeproto.Version {
		t.Fatalf("golden version %d != lifeproto.Version %d", g.Version, lifeproto.Version)
	}
	for _, c := range g.Cases {
		t.Run(c.Name, func(t *testing.T) {
			got, err := json.Marshal(build(t, c))
			if err != nil {
				t.Fatalf("marshal built value: %v", err)
			}
			if string(got) != c.Bytes {
				t.Errorf("encode mismatch\n got: %s\nwant: %s", got, c.Bytes)
			}

			msg := newMessage(t, c)
			if err := json.Unmarshal([]byte(c.Bytes), msg); err != nil {
				t.Fatalf("unmarshal golden bytes: %v", err)
			}
			reGot, err := json.Marshal(msg)
			if err != nil {
				t.Fatalf("re-marshal decoded value: %v", err)
			}
			if string(reGot) != c.Bytes {
				t.Errorf("decode/re-encode mismatch\n got: %s\nwant: %s", reGot, c.Bytes)
			}
		})
	}
}
