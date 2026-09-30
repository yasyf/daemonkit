//go:build linux

// Package supervise is the service layer for a linux host with no init system
// to register with: the value-type model for one exact supervised service and
// the foreground supervisor that runs it.
//
// A Service is one desired specification, the counterpart of a LaunchAgent.
// [Run] is the supervisor for one label. It is an ordinary foreground process
// the workspace starts and owns — a container entrypoint, a process manager's
// entry, a terminal — and it needs neither root nor systemd. It holds the
// label's exclusive lock, runs the applied service as a child whose durable
// record is written before the child's first instruction, restarts it by the
// service's own policy, and answers three verbs on a socket in the label's
// private state directory. [Apply] installs or repairs the one named service
// and always starts it; [Verify] reports whether it is already exactly
// applied; [Remove] stops it and forgets it.
//
// The applied service is persisted beside the socket, so a supervisor the
// workspace restarts resumes the service it was running, and a supervisor that
// died without stopping its child reclaims that exact process instance before
// it starts another.
//
// Both ends hold the same-effective-UID floor against the socket's kernel
// credentials and nothing stronger: linux has no code identity to pin, so any
// process of the same user can drive a supervisor or stand in for one. That is
// the whole trust model here, and it is only sound on a host one user owns.
package supervise
