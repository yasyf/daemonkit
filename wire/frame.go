// Package wire is daemonkit's persistent multiplexed unix-socket transport.
package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"time"
)

const (
	// ProtocolVersion is the exact transport version accepted by every peer.
	ProtocolVersion uint16 = 1
	// DefaultMaxFrame caps one length-prefixed frame at 4 MiB.
	DefaultMaxFrame = 4 << 20
	frameHeaderSize = 32
)

var (
	// ErrFrameTooLarge means a declared frame exceeds the configured bound.
	ErrFrameTooLarge = errors.New("wire: frame exceeds maximum")
	// ErrFrameTruncated means a peer closed in the middle of a frame.
	ErrFrameTruncated = errors.New("wire: truncated frame")
	// ErrProtocolVersion means a frame carries a protocol other than ProtocolVersion.
	ErrProtocolVersion = errors.New("wire: unsupported protocol version")
	// ErrInvalidFrame means a frame violates the v1 structural contract.
	ErrInvalidFrame = errors.New("wire: invalid frame")
	// ErrQueueFull means a bounded session queue cannot accept more work.
	ErrQueueFull = errors.New("wire: queue at capacity")
	// ErrFlowControl means a peer sent more stream data than the granted window.
	ErrFlowControl = errors.New("wire: peer exceeded granted stream window")
)

var frameMagic = [4]byte{'D', 'K', 'S', '1'}

// FrameKind identifies one v1 session message.
type FrameKind uint8

const (
	// FrameHello begins the session handshake.
	FrameHello FrameKind = iota + 1
	// FrameHelloAck accepts the session handshake.
	FrameHelloAck
	// FrameRequest starts one request.
	FrameRequest
	// FrameResponse completes one request.
	FrameResponse
	// FrameCancel cancels one request.
	FrameCancel
	// FrameEvent pushes one session event.
	FrameEvent
	// FrameStream carries one ordered stream chunk.
	FrameStream
	// FrameGoAway requests or acknowledges settled session closure.
	FrameGoAway
	// FrameWindow grants one stream additional bounded-delivery credits.
	FrameWindow
	// FrameAck confirms that a terminal response reached the client.
	FrameAck
	// FrameLifecycle carries one daemon-owned lifecycle snapshot.
	FrameLifecycle
)

// FrameFlags modifies a frame without changing its kind.
type FrameFlags uint8

const (
	// FlagEnd marks the final payload in a request or response stream.
	FlagEnd FrameFlags = 1 << iota
)

// Frame is the transport's fixed-header message. Payload stays opaque to wire.
type Frame struct {
	Kind              FrameKind
	Flags             FrameFlags
	ID                uint64
	Sequence          uint32
	DeadlineUnixMilli int64
	Op                Op
	Tenant            string
	Payload           []byte
}

// Codec reads and writes exact-v1 length-prefixed frames over one connection.
// Reads and writes are independently serialized and safe from any goroutine.
type Codec struct {
	MaxFrame     int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	conn      net.Conn
	rights    frameRightsCodec
	rightsErr error
	readMu    sync.Mutex
	writeMu   sync.Mutex
}

// NewCodec wraps conn with the default frame cap and no deadlines.
func NewCodec(conn net.Conn) *Codec {
	rights, err := newFrameRightsCodec(conn)
	return &Codec{MaxFrame: DefaultMaxFrame, conn: conn, rights: rights, rightsErr: err}
}

// SetDeadline installs one absolute deadline for both directions. It disables
// the rolling per-frame timeouts so a caller can bound the whole handshake.
func (c *Codec) SetDeadline(deadline time.Time) error {
	c.ReadTimeout = 0
	c.WriteTimeout = 0
	if err := c.conn.SetDeadline(deadline); err != nil {
		return fmt.Errorf("wire: set deadline: %w", err)
	}
	return nil
}

// ClearDeadline removes a previously installed absolute deadline.
func (c *Codec) ClearDeadline() error {
	return c.SetDeadline(time.Time{})
}

// ReadFrame reads one complete frame and rejects foreign versions before payload use.
func (c *Codec) ReadFrame() (frame Frame, err error) {
	frame, sidecar, err := c.readFrameWithSidecar()
	if sidecar != nil {
		closeErr := sidecar.close()
		if err == nil {
			err = fmt.Errorf("%w: descriptor is not valid for this reader", errInvalidFrameSidecar)
		}
		err = errors.Join(err, closeErr)
	}
	return frame, err
}

func (c *Codec) readFrameWithSidecar() (frame Frame, sidecar frameSidecar, err error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	if c.rightsErr != nil {
		return Frame{}, nil, c.rightsErr
	}
	if c.ReadTimeout > 0 {
		if err := c.conn.SetReadDeadline(time.Now().Add(c.ReadTimeout)); err != nil {
			return Frame{}, nil, fmt.Errorf("wire: set read deadline: %w", err)
		}
		defer func() {
			clearErr := clearReadDeadline(c.conn)
			if err == nil && isCompletedFrameClose(clearErr) {
				clearErr = nil
			}
			err = errors.Join(err, clearErr)
		}()
	}
	if c.rights != nil {
		return c.rights.readFrame(c.MaxFrame)
	}
	var prefix [4]byte
	if _, err := io.ReadFull(c.conn, prefix[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return Frame{}, nil, ErrFrameTruncated
		}
		return Frame{}, nil, err
	}
	n := int(binary.BigEndian.Uint32(prefix[:]))
	limit := c.MaxFrame
	if limit <= 0 {
		limit = DefaultMaxFrame
	}
	if n > limit {
		return Frame{}, nil, fmt.Errorf("%w: %d > %d", ErrFrameTooLarge, n, limit)
	}
	if n < frameHeaderSize {
		return Frame{}, nil, fmt.Errorf("%w: body length %d", ErrInvalidFrame, n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(c.conn, body); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return Frame{}, nil, ErrFrameTruncated
		}
		return Frame{}, nil, err
	}
	frame, err = decodeFrame(body)
	return frame, nil, err
}

// WriteFrame writes one complete frame under the configured bound.
func (c *Codec) WriteFrame(frame Frame) error {
	_, _, err := c.writeFrame(frame)
	return err
}

// writeFrame reports whether any and all length-framed packet bytes reached
// the connection writer. A partial packet cannot dispatch at the peer but its
// delivery remains unknown to the caller.
func (c *Codec) writeFrame(frame Frame) (started, complete bool, err error) {
	body, err := encodeFrame(frame)
	if err != nil {
		return false, false, err
	}
	limit := c.MaxFrame
	if limit <= 0 {
		limit = DefaultMaxFrame
	}
	if len(body) > limit {
		return false, false, fmt.Errorf("%w: %d > %d", ErrFrameTooLarge, len(body), limit)
	}
	bodyLength, err := uint32Length("body", len(body))
	if err != nil {
		return false, false, err
	}
	packet := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(packet[:4], bodyLength)
	copy(packet[4:], body)
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.WriteTimeout > 0 {
		if err := c.conn.SetWriteDeadline(time.Now().Add(c.WriteTimeout)); err != nil {
			return false, false, fmt.Errorf("wire: set write deadline: %w", err)
		}
		defer func() {
			clearErr := clearWriteDeadline(c.conn)
			if err == nil && complete && isCompletedFrameClose(clearErr) {
				clearErr = nil
			}
			err = errors.Join(err, clearErr)
		}()
	}
	written, err := writeFull(c.conn, packet)
	started = written != 0
	complete = written == len(packet)
	if err != nil {
		return started, complete, fmt.Errorf("wire: write frame: %w", err)
	}
	return true, true, nil
}

func isCompletedFrameClose(err error) bool {
	return errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe)
}

func clearReadDeadline(conn net.Conn) error {
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		return fmt.Errorf("wire: clear read deadline: %w", err)
	}
	return nil
}

func clearWriteDeadline(conn net.Conn) error {
	if err := conn.SetWriteDeadline(time.Time{}); err != nil {
		return fmt.Errorf("wire: clear write deadline: %w", err)
	}
	return nil
}

func encodeFrame(frame Frame) ([]byte, error) {
	if err := validateFrame(frame); err != nil {
		return nil, err
	}
	opLength, err := uint16Length("operation", len(frame.Op))
	if err != nil {
		return nil, err
	}
	tenantLength, err := uint16Length("tenant", len(frame.Tenant))
	if err != nil {
		return nil, err
	}
	deadline, err := uint64Deadline(frame.DeadlineUnixMilli)
	if err != nil {
		return nil, err
	}
	body := make([]byte, frameHeaderSize+len(frame.Op)+len(frame.Tenant)+len(frame.Payload))
	copy(body[:4], frameMagic[:])
	binary.BigEndian.PutUint16(body[4:6], ProtocolVersion)
	body[6] = byte(frame.Kind)
	body[7] = byte(frame.Flags)
	binary.BigEndian.PutUint64(body[8:16], frame.ID)
	binary.BigEndian.PutUint32(body[16:20], frame.Sequence)
	binary.BigEndian.PutUint64(body[20:28], deadline)
	binary.BigEndian.PutUint16(body[28:30], opLength)
	binary.BigEndian.PutUint16(body[30:32], tenantLength)
	off := frameHeaderSize
	off += copy(body[off:], frame.Op)
	off += copy(body[off:], frame.Tenant)
	copy(body[off:], frame.Payload)
	return body, nil
}

func decodeFrame(body []byte) (Frame, error) {
	if len(body) < frameHeaderSize {
		return Frame{}, fmt.Errorf("%w: body length %d", ErrInvalidFrame, len(body))
	}
	if string(body[:4]) != string(frameMagic[:]) {
		return Frame{}, fmt.Errorf("%w: magic", ErrInvalidFrame)
	}
	version := binary.BigEndian.Uint16(body[4:6])
	if version != ProtocolVersion {
		return Frame{}, fmt.Errorf("%w: got %d, want %d", ErrProtocolVersion, version, ProtocolVersion)
	}
	kind := FrameKind(body[6])
	if !kind.valid() {
		return Frame{}, fmt.Errorf("%w: kind %d", ErrInvalidFrame, kind)
	}
	flags := FrameFlags(body[7])
	if flags&^FlagEnd != 0 {
		return Frame{}, fmt.Errorf("%w: flags %d", ErrInvalidFrame, flags)
	}
	opLen := int(binary.BigEndian.Uint16(body[28:30]))
	tenantLen := int(binary.BigEndian.Uint16(body[30:32]))
	if frameHeaderSize+opLen+tenantLen > len(body) {
		return Frame{}, fmt.Errorf("%w: routing lengths", ErrInvalidFrame)
	}
	off := frameHeaderSize
	op := Op(string(body[off : off+opLen]))
	off += opLen
	tenant := string(body[off : off+tenantLen])
	off += tenantLen
	payload := append([]byte(nil), body[off:]...)
	deadline, err := int64Deadline(binary.BigEndian.Uint64(body[20:28]))
	if err != nil {
		return Frame{}, err
	}
	frame := Frame{
		Kind:              kind,
		Flags:             flags,
		ID:                binary.BigEndian.Uint64(body[8:16]),
		Sequence:          binary.BigEndian.Uint32(body[16:20]),
		DeadlineUnixMilli: deadline,
		Op:                op,
		Tenant:            tenant,
		Payload:           payload,
	}
	if err := validateFrame(frame); err != nil {
		return Frame{}, err
	}
	return frame, nil
}

func (k FrameKind) valid() bool { return k >= FrameHello && k <= FrameLifecycle }

func uint32Length(field string, value int) (uint32, error) {
	if value < 0 || uint64(value) > math.MaxUint32 {
		return 0, fmt.Errorf("%w: %s length %d", ErrInvalidFrame, field, value)
	}
	return uint32(value), nil
}

func uint16Length(field string, value int) (uint16, error) {
	if value < 0 || uint64(value) > math.MaxUint16 {
		return 0, fmt.Errorf("%w: %s length %d", ErrInvalidFrame, field, value)
	}
	return uint16(value), nil
}

func uint64Length(field string, value int) (uint64, error) {
	if value < 0 {
		return 0, fmt.Errorf("%w: %s length %d", ErrInvalidFrame, field, value)
	}
	return uint64(value), nil
}

func uint64Deadline(value int64) (uint64, error) {
	if value < 0 {
		return 0, fmt.Errorf("%w: negative deadline %d", ErrInvalidFrame, value)
	}
	return uint64(value), nil
}

func int64Deadline(value uint64) (int64, error) {
	if value > math.MaxInt64 {
		return 0, fmt.Errorf("%w: deadline %d exceeds int64", ErrInvalidFrame, value)
	}
	return int64(value), nil
}

func validateFrame(frame Frame) error {
	if !frame.Kind.valid() {
		return fmt.Errorf("%w: kind %d", ErrInvalidFrame, frame.Kind)
	}
	if frame.Flags&^FlagEnd != 0 {
		return fmt.Errorf("%w: flags %d", ErrInvalidFrame, frame.Flags)
	}
	switch frame.Kind {
	case FrameHello, FrameHelloAck:
		if frame.Flags != FlagEnd || frame.ID != 0 || frame.Sequence != 0 || frame.DeadlineUnixMilli != 0 ||
			frame.Op != "" || frame.Tenant != "" || len(frame.Payload) == 0 {
			return fmt.Errorf("%w: handshake frame kind %d", ErrInvalidFrame, frame.Kind)
		}
	case FrameRequest:
		if frame.ID == 0 || frame.Sequence != 0 || frame.DeadlineUnixMilli < 0 || frame.Op == "" {
			return fmt.Errorf("%w: request frame", ErrInvalidFrame)
		}
	case FrameResponse:
		if frame.Flags != FlagEnd || frame.ID == 0 || frame.Sequence != 0 || frame.DeadlineUnixMilli != 0 ||
			frame.Op != "" || frame.Tenant != "" || len(frame.Payload) == 0 {
			return fmt.Errorf("%w: response frame", ErrInvalidFrame)
		}
	case FrameCancel:
		if frame.Flags != FlagEnd || frame.ID == 0 || frame.Sequence != 0 || frame.DeadlineUnixMilli != 0 ||
			frame.Op != "" || frame.Tenant != "" || len(frame.Payload) != 0 {
			return fmt.Errorf("%w: cancel frame", ErrInvalidFrame)
		}
	case FrameEvent:
		if frame.Flags != FlagEnd || frame.ID != 0 || frame.Sequence != 0 || frame.DeadlineUnixMilli != 0 ||
			frame.Op == "" || frame.Tenant != "" {
			return fmt.Errorf("%w: event frame", ErrInvalidFrame)
		}
	case FrameStream:
		if frame.ID == 0 || frame.DeadlineUnixMilli != 0 || frame.Op != "" || frame.Tenant != "" {
			return fmt.Errorf("%w: stream frame", ErrInvalidFrame)
		}
	case FrameGoAway:
		if frame.Flags != FlagEnd || frame.ID != 0 || frame.Sequence != 0 || frame.DeadlineUnixMilli != 0 ||
			frame.Op != "" || frame.Tenant != "" || len(frame.Payload) != 0 {
			return fmt.Errorf("%w: go-away frame", ErrInvalidFrame)
		}
	case FrameWindow:
		if frame.Flags != 0 || frame.Sequence == 0 || frame.DeadlineUnixMilli != 0 ||
			frame.Op != "" || frame.Tenant != "" || len(frame.Payload) != 0 {
			return fmt.Errorf("%w: window frame", ErrInvalidFrame)
		}
	case FrameAck:
		if frame.Flags != FlagEnd || frame.ID == 0 || frame.Sequence != 0 || frame.DeadlineUnixMilli != 0 ||
			frame.Op != "" || frame.Tenant != "" || len(frame.Payload) != sessionGenerationBytes {
			return fmt.Errorf("%w: acknowledgement frame", ErrInvalidFrame)
		}
	case FrameLifecycle:
		if frame.Flags != FlagEnd || frame.ID != 0 || frame.Sequence != 0 || frame.DeadlineUnixMilli != 0 ||
			frame.Op != "" || frame.Tenant != "" || len(frame.Payload) == 0 {
			return fmt.Errorf("%w: lifecycle frame", ErrInvalidFrame)
		}
	}
	return nil
}

func writeFull(w io.Writer, p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		n, err := w.Write(p)
		written += n
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrShortWrite
		}
		p = p[n:]
	}
	return written, nil
}
