package proc

import (
	"errors"
	"fmt"
	"sort"
)

// ExecutableIdentities returns every live process whose kernel-resolved
// executable is exactly path. It does not use names, argv, or shell process
// discovery, and it revalidates each PID around the identity snapshot: both
// the executable and the {start, boot} instance are re-read afterwards, so a
// PID that was reused mid-inventory by another process running the same
// executable is dropped rather than reported at its dead predecessor's pin.
func ExecutableIdentities(path string) ([]Identity, error) {
	return executableIdentities(path, processIDs, ExecutablePath, ProbeIdentity)
}

func executableIdentities(
	path string,
	list func() ([]int, error),
	executable func(int) (string, error),
	probe func(int) (Identity, error),
) ([]Identity, error) {
	pids, err := list()
	if err != nil {
		return nil, err
	}
	identities := make([]Identity, 0)
	for _, pid := range pids {
		before, err := executable(pid)
		if errors.Is(err, ErrNoProcess) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("inspect executable for pid %d: %w", pid, err)
		}
		if before != path {
			continue
		}
		identity, err := probe(pid)
		if errors.Is(err, ErrNoProcess) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("probe executable pid %d: %w", pid, err)
		}
		after, err := executable(pid)
		if errors.Is(err, ErrNoProcess) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("revalidate executable for pid %d: %w", pid, err)
		}
		if after != before {
			continue
		}
		repinned, err := probe(pid)
		if errors.Is(err, ErrNoProcess) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("revalidate identity for pid %d: %w", pid, err)
		}
		if !instance(identity).matches(instance(repinned)) {
			continue
		}
		identity.Executable = after
		identities = append(identities, identity)
	}
	sort.Slice(identities, func(i, j int) bool { return identities[i].PID < identities[j].PID })
	return identities, nil
}
