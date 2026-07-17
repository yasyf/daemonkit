package wire_test

import (
	"bytes"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/daemonkit/wire/wiretest"
)

func TestFramingRoundTrip(t *testing.T) {
	client, server := wiretest.Pair(t)
	cf := wire.NewFraming(client)
	sf := wire.NewFraming(server)

	frames := [][]byte{
		[]byte(`{"op":"health"}`),
		[]byte(`{}`),
		[]byte(`{"a":1,"b":"two","c":[1,2,3]}`),
	}
	for _, want := range frames {
		if err := cf.WriteFrame(want); err != nil {
			t.Fatalf("WriteFrame(%s): %v", want, err)
		}
	}
	for _, want := range frames {
		got, err := sf.ReadFrame()
		if err != nil {
			t.Fatalf("ReadFrame: %v", err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("ReadFrame = %s, want %s", got, want)
		}
	}
}

func TestFramingJSONRoundTrip(t *testing.T) {
	client, server := wiretest.Pair(t)
	cf := wire.NewFraming(client)
	sf := wire.NewFraming(server)

	type msg struct {
		Op string `json:"op"`
		N  int    `json:"n"`
	}
	want := msg{Op: "ping", N: 7}
	if err := cf.WriteJSON(want); err != nil {
		t.Fatalf("WriteJSON: %v", err)
	}
	var got msg
	if err := sf.ReadJSON(&got); err != nil {
		t.Fatalf("ReadJSON: %v", err)
	}
	if got != want {
		t.Errorf("ReadJSON = %+v, want %+v", got, want)
	}
}

func TestFramingMaxLine(t *testing.T) {
	tests := []struct {
		name    string
		maxLine int
		payload int
		wantErr error
	}{
		{"under cap", 16, 8, nil},
		{"at cap", 16, 16, nil},
		{"one over cap", 16, 17, wire.ErrFrameTooLarge},
		{"far over cap", 16, 4096, wire.ErrFrameTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, server := wiretest.Pair(t)
			cf := wire.NewFraming(client)
			sf := wire.NewFraming(server)
			sf.MaxLine = tt.maxLine

			payload := bytes.Repeat([]byte("x"), tt.payload)
			if err := cf.WriteFrame(payload); err != nil {
				t.Fatalf("WriteFrame: %v", err)
			}
			got, err := sf.ReadFrame()
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ReadFrame err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr == nil && len(got) != tt.payload {
				t.Errorf("ReadFrame len = %d, want %d", len(got), tt.payload)
			}
		})
	}
}

func TestFramingReadDeadline(t *testing.T) {
	_, server := wiretest.Pair(t)
	sf := wire.NewFraming(server)
	sf.ReadTimeout = 50 * time.Millisecond

	start := time.Now()
	_, err := sf.ReadFrame()
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("ReadFrame past deadline = %v, want os.ErrDeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Errorf("ReadFrame returned after %s, want the deadline to elapse (~50ms)", elapsed)
	}
}

func TestWriteFrameRejectsEmbeddedLF(t *testing.T) {
	client, _ := wiretest.Pair(t)
	cf := wire.NewFraming(client)
	if err := cf.WriteFrame([]byte("a\nb")); !errors.Is(err, wire.ErrFrameContainsLF) {
		t.Fatalf("WriteFrame with embedded LF = %v, want ErrFrameContainsLF", err)
	}
}
