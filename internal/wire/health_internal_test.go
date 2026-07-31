package wire

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"runtime"
	"testing"
)

func TestDecodeHealthReportToleratesAddedFields(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    HealthReport
		wantErr bool
	}{
		{
			name:    "this era",
			payload: `{"phase":"runtime_ready","protocol":2,"generation":7,"pid":4242,"build":"b1"}`,
			want:    HealthReport{Phase: PhaseReady, Protocol: 2, Generation: 7, PID: 4242, Build: "b1"},
		},
		{
			name:    "a later era's added field",
			payload: `{"phase":"runtime_ready","protocol":2,"generation":7,"pid":4242,"build":"b1","uptime_ms":900}`,
			want:    HealthReport{Phase: PhaseReady, Protocol: 2, Generation: 7, PID: 4242, Build: "b1"},
		},
		{
			name:    "detail rides base64",
			payload: `{"phase":"runtime_draining","pid":1,"detail":"aGk="}`,
			want:    HealthReport{Phase: PhaseDraining, PID: 1, Detail: []byte("hi")},
		},
		{
			name:    "malformed payload",
			payload: `{"phase":`,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report, err := decodeHealthReport([]byte(tt.payload))
			if tt.wantErr {
				if err == nil {
					t.Fatalf("decodeHealthReport() = %+v, want an error", report)
				}
				return
			}
			if err != nil {
				t.Fatalf("decodeHealthReport() = %v", err)
			}
			if report.Phase != tt.want.Phase || report.Protocol != tt.want.Protocol ||
				report.Generation != tt.want.Generation || report.PID != tt.want.PID ||
				report.Build != tt.want.Build || !bytes.Equal(report.Detail, tt.want.Detail) {
				t.Fatalf("decodeHealthReport() = %+v, want %+v", report, tt.want)
			}
		})
	}
}

// TestInFlightCountsFromAdmissionNotFromDispatch pins the count against the
// scheduler: GOMAXPROCS(1) parks a newly created goroutine in the P's runnext
// slot instead of preempting its creator, so receiveRequest's return is
// observed before execute has run a single instruction.
func TestInFlightCountsFromAdmissionNotFromDispatch(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	server, err := NewServer(stubRuntime{phase: PhaseReady}, Config{Schemas: Schemas{"test.v1"}})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	conn, peer := net.Pipe()
	defer func() { _, _ = conn.Close(), peer.Close() }()
	for round := range 32 {
		sessCtx, cancel := context.WithCancel(context.Background())
		sess := &session{
			server:       server,
			conn:         conn,
			ctx:          sessCtx,
			cancel:       cancel,
			lane:         LaneBusiness,
			schema:       "test.v1",
			outbound:     make(chan sessionOutbound),
			requestsDone: make(chan struct{}),
			writerDone:   make(chan struct{}),
			disconnected: make(chan struct{}),
			done:         make(chan struct{}),
			active:       make(map[uint64]*requestState),
			seen:         make(map[uint64]struct{}),
		}
		if err := sess.receiveRequest(sessCtx, Frame{Kind: FrameRequest, ID: 1, Op: "echo.v1", Flags: FlagEnd}, nil); err != nil {
			cancel()
			t.Fatalf("round %d: receiveRequest() = %v", round, err)
		}
		admitted := server.InFlight()
		cancel()
		sess.requestWG.Wait()
		if admitted != 1 {
			t.Fatalf("round %d: InFlight() = %d on receiveRequest's return, want 1", round, admitted)
		}
		if settled := server.InFlight(); settled != 0 {
			t.Fatalf("round %d: InFlight() = %d after settlement, want 0", round, settled)
		}
	}
}

func TestHealthReportEnvelopeStaysStrict(t *testing.T) {
	payload, err := json.Marshal(helloAck{Protocol: ProtocolVersion, Schema: "s", Phase: PhaseReady})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	extended := append(payload[:len(payload)-1], []byte(`,"added":1}`)...)
	var ack helloAck
	if err := decodeStrict(extended, &ack); err == nil {
		t.Fatal("decodeStrict accepted an unknown envelope field")
	}
}
