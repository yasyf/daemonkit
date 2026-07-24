package wire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/trust"
)

func TestClientCloseFencesWritesAfterPendingAcknowledgement(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})
	requestRead := make(chan struct{})
	releaseResponse := make(chan struct{})
	terminalAckRead := make(chan struct{})
	goAwayRead := make(chan struct{})
	releaseGoAway := make(chan struct{})
	serverDone := make(chan error, 1)
	session := make([]byte, sessionGenerationBytes)
	session[0] = 1
	go func() {
		codec := NewCodec(serverConn)
		hello, err := codec.ReadFrame()
		if err != nil {
			serverDone <- fmt.Errorf("read hello: %w", err)
			return
		}
		if hello.Kind != FrameHello {
			serverDone <- fmt.Errorf("hello kind = %d", hello.Kind)
			return
		}
		payload, err := json.Marshal(handshakeAck{
			Protocol: ProtocolVersion, WireBuild: "client-close-v1", Session: session,
		})
		if err != nil {
			serverDone <- err
			return
		}
		if err := codec.WriteFrame(Frame{Kind: FrameHelloAck, Flags: FlagEnd, Payload: payload}); err != nil {
			serverDone <- fmt.Errorf("write hello acknowledgement: %w", err)
			return
		}
		window, err := codec.ReadFrame()
		if err != nil {
			serverDone <- fmt.Errorf("read event window: %w", err)
			return
		}
		if window.Kind != FrameWindow || window.ID != 0 {
			serverDone <- fmt.Errorf("event window = %#v", window)
			return
		}
		request, err := codec.ReadFrame()
		if err != nil {
			serverDone <- fmt.Errorf("read request: %w", err)
			return
		}
		if request.Kind != FrameRequest {
			serverDone <- fmt.Errorf("request = %#v", request)
			return
		}
		close(requestRead)
		<-releaseResponse
		response, err := json.Marshal(Response{Ack: true, Payload: []byte(`"settled"`)})
		if err != nil {
			serverDone <- err
			return
		}
		if err := codec.WriteFrame(Frame{
			Kind: FrameResponse, Flags: FlagEnd, ID: request.ID, Payload: response,
		}); err != nil {
			serverDone <- fmt.Errorf("write response: %w", err)
			return
		}
		ack, err := codec.ReadFrame()
		if err != nil {
			serverDone <- fmt.Errorf("read terminal acknowledgement: %w", err)
			return
		}
		if ack.Kind != FrameAck || ack.ID != request.ID {
			serverDone <- fmt.Errorf("terminal acknowledgement = %#v", ack)
			return
		}
		close(terminalAckRead)
		goAway, err := codec.ReadFrame()
		if err != nil {
			serverDone <- fmt.Errorf("read go-away: %w", err)
			return
		}
		if goAway.Kind != FrameGoAway {
			serverDone <- fmt.Errorf("go-away = %#v", goAway)
			return
		}
		close(goAwayRead)
		<-releaseGoAway
		serverDone <- codec.WriteFrame(Frame{Kind: FrameGoAway, Flags: FlagEnd})
	}()

	client, err := NewClient(t.Context(), ClientConfig{
		Dial: func(context.Context) (net.Conn, error) {
			return clientConn, nil
		},
		WireBuild: "client-close-v1", Role: trust.UnprotectedRole,
		WriteTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	type callResult struct {
		result Result
		err    error
	}
	called := make(chan callResult, 1)
	go func() {
		result, callErr := client.Call(context.Background(), "test.close", "", nil)
		called <- callResult{result: result, err: callErr}
	}()
	select {
	case <-requestRead:
	case <-time.After(time.Second):
		t.Fatal("peer did not receive request")
	}
	closed := make(chan error, 1)
	go func() { closed <- client.Close() }()
	deadline := time.Now().Add(time.Second)
	for !client.closing.Load() && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if !client.closing.Load() {
		t.Fatal("client did not begin close")
	}
	result, err := client.Call(t.Context(), "test.after-close", "", nil)
	if !errors.Is(err, ErrClientClosing) || result.Outcome != PreSendFailure {
		t.Fatalf("Call after Close began = %#v, %v", result, err)
	}
	close(releaseResponse)
	select {
	case <-terminalAckRead:
	case err := <-serverDone:
		t.Fatalf("peer ended before terminal acknowledgement: %v", err)
	case <-time.After(time.Second):
		t.Fatal("peer did not receive terminal acknowledgement")
	}
	settled := <-called
	if settled.err != nil || settled.result.Outcome != Delivered {
		t.Fatalf("pending Call = %#v, %v", settled.result, settled.err)
	}
	select {
	case <-goAwayRead:
	case <-time.After(time.Second):
		t.Fatal("peer did not receive go-away")
	}
	for _, frame := range []Frame{
		{Kind: FrameCancel, Flags: FlagEnd, ID: 1},
		{Kind: FrameWindow, ID: 1, Sequence: 1},
		{Kind: FrameAck, Flags: FlagEnd, ID: 1, Payload: session},
	} {
		state, err := client.sendFrame(t.Context(), frame)
		if state != frameNotSent || !errors.Is(err, ErrClientClosing) {
			t.Fatalf("send after go-away = %d, %v", state, err)
		}
	}
	close(releaseGoAway)
	if err := <-closed; err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := <-serverDone; err != nil {
		t.Fatalf("peer: %v", err)
	}
}
