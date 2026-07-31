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

// drainPreamble is the two bytes 0x4452 ("DR") a draining server emits instead
// of a hello ack. They can never open a frame: read as the head of a length
// prefix they declare a body over 1 GiB, above every MaxFrame in the fleet.
var drainPreamble = [2]byte{0x44, 0x52}

// Codec reads and writes exact-version length-prefixed frames over one
// connection. Reads and writes are independently serialized and safe from any
// goroutine.
type Codec struct {
	MaxFrame     int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	conn      net.Conn
	rights    frameRightsCodec
	rightsErr error
	peeked    []byte
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

// PeekPreamble reads the next two bytes through the codec's read path and
// reports whether they are the drain preamble. Bytes that are not the preamble
// are stashed and consumed as the first bytes of the next frame read, on the
// SCM_RIGHTS path with any rights received at frame byte zero held intact.
func (c *Codec) PeekPreamble() (drain bool, err error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	if c.rightsErr != nil {
		return false, c.rightsErr
	}
	if c.peeked != nil {
		panic("wire: preamble already peeked")
	}
	if c.ReadTimeout > 0 {
		if err := c.conn.SetReadDeadline(time.Now().Add(c.ReadTimeout)); err != nil {
			return false, fmt.Errorf("wire: set read deadline: %w", err)
		}
		defer func() {
			clearErr := clearReadDeadline(c.conn)
			if err == nil && isCompletedFrameClose(clearErr) {
				clearErr = nil
			}
			err = errors.Join(err, clearErr)
		}()
	}
	head := make([]byte, len(drainPreamble))
	if c.rights != nil {
		if err := c.rights.peek(head); err != nil {
			return false, err
		}
	} else if _, err := io.ReadFull(c.conn, head); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return false, ErrFrameTruncated
		}
		return false, err
	}
	if [2]byte(head) == drainPreamble {
		return true, nil
	}
	c.peeked = head
	return false, nil
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
	peeked := c.peeked
	c.peeked = nil
	if c.rights != nil {
		return c.rights.readFrame(c.MaxFrame, peeked)
	}
	var prefix [4]byte
	n := copy(prefix[:], peeked)
	if _, err := io.ReadFull(c.conn, prefix[n:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || (n > 0 && errors.Is(err, io.EOF)) {
			return Frame{}, nil, ErrFrameTruncated
		}
		return Frame{}, nil, err
	}
	bodyLength := int(binary.BigEndian.Uint32(prefix[:]))
	limit := c.MaxFrame
	if limit <= 0 {
		limit = DefaultMaxFrame
	}
	if bodyLength > limit {
		return Frame{}, nil, fmt.Errorf("%w: %d > %d", ErrFrameTooLarge, bodyLength, limit)
	}
	if bodyLength < frameHeaderSize {
		return Frame{}, nil, fmt.Errorf("%w: body length %d", ErrInvalidFrame, bodyLength)
	}
	body := make([]byte, bodyLength)
	if _, err := io.ReadFull(c.conn, body); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return Frame{}, nil, ErrFrameTruncated
		}
		return Frame{}, nil, err
	}
	frame, err = decodeValidFrame(body)
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
	body, err := encodeValidFrame(frame)
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
	bodyLength, err := uint32Length(len(body))
	if err != nil {
		return false, false, err
	}
	packet := make([]byte, framePrefixSize+len(body))
	binary.BigEndian.PutUint32(packet[:framePrefixSize], bodyLength)
	copy(packet[framePrefixSize:], body)
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

func encodeValidFrame(frame Frame) ([]byte, error) {
	if err := validateFrame(frame); err != nil {
		return nil, err
	}
	return EncodeFrame(frame)
}

func decodeValidFrame(body []byte) (Frame, error) {
	frame, err := DecodeFrame(body)
	if err != nil {
		return Frame{}, err
	}
	if err := validateFrame(frame); err != nil {
		return Frame{}, err
	}
	return frame, nil
}

// validateFrame layers the per-kind semantic contract over the generated
// structural codec: which header fields each kind may populate.
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

func uint32Length(value int) (uint32, error) {
	if value < 0 || uint64(value) > math.MaxUint32 {
		return 0, fmt.Errorf("%w: length %d", ErrInvalidFrame, value)
	}
	return uint32(value), nil
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
