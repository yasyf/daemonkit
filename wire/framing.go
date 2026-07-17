// Package wire is daemonkit's transport core for unix-socket control planes:
// LF-delimited JSON framing over a net.Conn, a one-shot peer-credential read,
// and a per-op deadline ladder. It is envelope-agnostic — consumer request and
// response structs never live here; callers hand the codec raw bytes or a value
// to (un)marshal.
package wire

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"
)

// DefaultMaxLine caps a single frame at 1 MiB when Framing.MaxLine is unset.
const DefaultMaxLine = 1 << 20

// ErrFrameTooLarge means a read frame exceeded MaxLine before its LF. The caller
// closes the connection; the codec never silently truncates.
var ErrFrameTooLarge = errors.New("wire: frame exceeds MaxLine")

// ErrFrameContainsLF means a raw frame handed to WriteFrame carries an embedded
// LF, which would split the stream into two frames.
var ErrFrameContainsLF = errors.New("wire: frame contains an embedded LF")

// Framing is the LF-delimited JSON codec over one connection: one JSON object
// per line, each read bounded by MaxLine, each op bounded by the deadlines. It
// owns a persistent buffered reader, so it is not safe for concurrent use on a
// single connection. The caller owns conn and closes it.
type Framing struct {
	// MaxLine caps a read frame's bytes (excluding the LF); zero means DefaultMaxLine.
	MaxLine int
	// ReadTimeout, when positive, sets a per-frame read deadline before each ReadFrame.
	ReadTimeout time.Duration
	// WriteTimeout, when positive, sets a per-frame write deadline before each WriteFrame.
	WriteTimeout time.Duration

	conn net.Conn
	r    *bufio.Reader
}

// NewFraming wraps conn with the default 1 MiB frame cap and no deadlines.
func NewFraming(conn net.Conn) *Framing {
	return &Framing{MaxLine: DefaultMaxLine, conn: conn, r: bufio.NewReader(conn)}
}

// ReadFrame reads one LF-terminated frame and returns it without the LF. It
// returns ErrFrameTooLarge once the accumulated bytes pass MaxLine, and the read
// error (io.EOF, a deadline) otherwise.
func (f *Framing) ReadFrame() ([]byte, error) {
	if f.ReadTimeout > 0 {
		if err := f.conn.SetReadDeadline(time.Now().Add(f.ReadTimeout)); err != nil {
			return nil, fmt.Errorf("wire: set read deadline: %w", err)
		}
	}
	limit := f.MaxLine
	if limit <= 0 {
		limit = DefaultMaxLine
	}
	var frame []byte
	for {
		chunk, err := f.r.ReadSlice('\n')
		switch {
		case err == nil:
			frame = append(frame, chunk[:len(chunk)-1]...)
			if len(frame) > limit {
				return nil, ErrFrameTooLarge
			}
			return frame, nil
		case errors.Is(err, bufio.ErrBufferFull):
			frame = append(frame, chunk...)
			if len(frame) > limit {
				return nil, ErrFrameTooLarge
			}
		default:
			return nil, err
		}
	}
}

// WriteFrame writes b followed by an LF as one write. b must not contain an LF.
func (f *Framing) WriteFrame(b []byte) error {
	if bytes.IndexByte(b, '\n') >= 0 {
		return ErrFrameContainsLF
	}
	if f.WriteTimeout > 0 {
		if err := f.conn.SetWriteDeadline(time.Now().Add(f.WriteTimeout)); err != nil {
			return fmt.Errorf("wire: set write deadline: %w", err)
		}
	}
	frame := make([]byte, len(b)+1)
	n := copy(frame, b)
	frame[n] = '\n'
	if _, err := f.conn.Write(frame); err != nil {
		return fmt.Errorf("wire: write frame: %w", err)
	}
	return nil
}

// ReadJSON reads one frame and unmarshals it into v.
func (f *Framing) ReadJSON(v any) error {
	b, err := f.ReadFrame()
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, v); err != nil {
		return fmt.Errorf("wire: decode frame: %w", err)
	}
	return nil
}

// WriteJSON marshals v to a compact JSON frame and writes it.
func (f *Framing) WriteJSON(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("wire: encode frame: %w", err)
	}
	return f.WriteFrame(b)
}
