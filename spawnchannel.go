package daemonkit

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/yasyf/daemonkit/internal/proc"
	"github.com/yasyf/daemonkit/internal/trust"
	"github.com/yasyf/daemonkit/internal/wire"
	"golang.org/x/sys/unix"
)

const spawnChannelReasonLimit = 256

// ServeChannel serves a ChannelHandoff child's spawn channel until ctx ends
// or the child closes it: a mint answers with a fresh socketpair end, the
// daemon side admitted as a business session pinned to the spawn-proven
// child; an adopt delivers a child-accepted descriptor into full trust
// admission. Every request echoes the spawn nonce; the take is the one Conn
// and Business share.
func (c Ctx) ServeChannel(ctx context.Context, child *Child) error {
	if c.adoptMinted == nil || c.adoptHandoff == nil {
		return errors.New("daemonkit: ServeChannel requires a serving daemon's Ctx")
	}
	conn, err := child.Conn()
	if err != nil {
		return err
	}
	channel, ok := conn.(*net.UnixConn)
	if !ok {
		_ = conn.Close()
		return fmt.Errorf("daemonkit: the spawn channel is %T, want a unix socketpair end", conn)
	}
	peer := trust.Peer{UID: os.Getuid(), Token: child.token}
	return serveSpawnChannel(ctx, channel, child.nonce, peer, c.adoptMinted, c.adoptHandoff)
}

func serveSpawnChannel(
	ctx context.Context,
	channel *net.UnixConn,
	spawnNonce []byte,
	peer trust.Peer,
	adoptMinted func(*net.UnixConn, trust.Peer, []byte) error,
	adoptHandoff func(*net.UnixConn) error,
) error {
	defer func() { _ = channel.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = channel.Close() })
	defer stop()
	for {
		payload, fd, err := wire.ReadSpawnChannelFrame(channel, true)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("daemonkit: read spawn channel request: %w", err)
		}
		request, err := wire.DecodeSpawnChannelRequest(payload)
		if err != nil {
			closeSpawnChannelFD(fd)
			return err
		}
		if subtle.ConstantTimeCompare(request.Nonce, spawnNonce) != 1 {
			closeSpawnChannelFD(fd)
			return errors.New("daemonkit: spawn channel request nonce mismatch")
		}
		switch request.Op {
		case wire.SpawnChannelOpMint:
			if fd >= 0 {
				closeSpawnChannelFD(fd)
				return errors.New("daemonkit: mint request carries a descriptor")
			}
			err = serveSpawnChannelMint(channel, peer, adoptMinted)
		case wire.SpawnChannelOpAdopt:
			if fd < 0 {
				return errors.New("daemonkit: adopt request carries no descriptor")
			}
			err = serveSpawnChannelAdopt(channel, fd, adoptHandoff)
		}
		if err != nil {
			return err
		}
	}
}

func serveSpawnChannelMint(
	channel *net.UnixConn,
	peer trust.Peer,
	adoptMinted func(*net.UnixConn, trust.Peer, []byte) error,
) error {
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		return fmt.Errorf("daemonkit: mint attach nonce: %w", err)
	}
	payload, err := json.Marshal(wire.SpawnChannelMinted{Nonce: nonce})
	if err != nil {
		return fmt.Errorf("daemonkit: encode mint response: %w", err)
	}
	parentEnd, childEnd, err := proc.SocketpairFiles()
	if err != nil {
		return fmt.Errorf("daemonkit: mint connection pair: %w", err)
	}
	writeErr := wire.WriteSpawnChannelFrame(channel, payload, int(childEnd.Fd()))
	closeErr := childEnd.Close()
	if writeErr != nil || closeErr != nil {
		_ = parentEnd.Close()
		return fmt.Errorf("daemonkit: convey minted connection: %w", errors.Join(writeErr, closeErr))
	}
	conn, err := adoptedUnixConn(parentEnd)
	if err != nil {
		return err
	}
	if err := adoptMinted(conn, peer, nonce); err != nil {
		return fmt.Errorf("daemonkit: admit minted connection: %w", err)
	}
	return nil
}

func serveSpawnChannelAdopt(channel *net.UnixConn, fd int, adoptHandoff func(*net.UnixConn) error) error {
	conn, err := adoptedUnixConn(os.NewFile(uintptr(fd), "daemonkit-spawn-channel-adopt")) //nolint:gosec // ReadSpawnChannelFrame validated the received descriptor
	if err == nil {
		err = adoptHandoff(conn)
	}
	result := wire.SpawnChannelAdopted{Adopted: err == nil}
	if err != nil {
		result.Reason = spawnChannelReason(err)
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("daemonkit: encode adopt response: %w", err)
	}
	return wire.WriteSpawnChannelFrame(channel, payload, -1)
}

func spawnChannelReason(err error) string {
	reason := err.Error()
	if len(reason) > spawnChannelReasonLimit {
		reason = reason[:spawnChannelReasonLimit]
	}
	return reason
}

func adoptedUnixConn(file *os.File) (*net.UnixConn, error) {
	conn, err := net.FileConn(file)
	closeErr := file.Close()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("daemonkit: adopt spawn channel descriptor: %w", err), closeErr)
	}
	if closeErr != nil {
		_ = conn.Close()
		return nil, closeErr
	}
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("daemonkit: spawn channel descriptor is %T, want a unix stream", conn)
	}
	return unixConn, nil
}

func closeSpawnChannelFD(fd int) {
	if fd >= 0 {
		_ = unix.Close(fd)
	}
}
