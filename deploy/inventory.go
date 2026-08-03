package deploy

import (
	"debug/macho"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/yasyf/daemonkit/internal/proc"
)

// ErrLive means the executable-scoped inventory gate found live processes on
// a deployment's own programs. It is the refusal that stands between a
// half-quiesced daemon and an irreversible step.
var ErrLive = errors.New("deploy: live processes remain on the deployment's executables")

// machOMagics are the thin and fat Mach-O headers, the 64-bit fat variant
// included: debug/macho names every one of them but that last.
var machOMagics = [...]uint32{macho.Magic32, macho.Magic64, macho.MagicFat, 0xcafebabf}

// LiveProcess is one inventory survivor, pinned so a refusal can say exactly
// what remains. Executable is empty for a survivor nothing could name, whose
// pin is then the whole of what is known about it.
type LiveProcess struct {
	PID        int
	Start      uint64
	Boot       uint64
	Executable string
}

// String names the survivor a refusal reports: the pid and its executable, or
// the instance pin alone when nothing could name it.
func (p LiveProcess) String() string { return p.identity().String() }

func (p LiveProcess) identity() proc.Identity {
	return proc.Identity{PID: p.PID, Start: p.Start, Boot: p.Boot, Executable: p.Executable}
}

// Survivors is what one inventory found, in the two sets that carry different
// authority. Live is attributed to the query: each process runs one of the
// executables asked about. Unnameable is attributed to nothing — a live
// same-user process whose executable neither the kernel nor its recorded
// execve path could resolve, which is what a daemon whose binary was unlinked
// under it looks like — so nothing about the process says whose it is. Only a
// caller that recognizes the pin as one it recorded may hold it against
// itself.
type Survivors struct {
	Live       []LiveProcess
	Unnameable []LiveProcess
}

// attributed returns the unnameable survivors whose instance pin is one of
// known. A pid alone is never the answer: the kernel hands it to a stranger
// the moment the process leaves, so the whole pin is compared.
func (s Survivors) attributed(known ...proc.Identity) []LiveProcess {
	attributed := make([]LiveProcess, 0)
	for _, survivor := range s.Unnameable {
		if slices.ContainsFunc(known, func(id proc.Identity) bool {
			return proc.SameInstance(survivor.identity(), id)
		}) {
			attributed = append(attributed, survivor)
		}
	}
	return attributed
}

// Inventory reports every live same-user process the kernel names as running
// one of paths — compared both in the symlink-free form the kernel reports and
// in the literal form the caller wrote — and every live same-user process
// nothing could name at all. It consults no names, no argv, and no shell
// process discovery, and it revalidates each matched PID's executable and
// instance around the identity snapshot, so a PID reused mid-inventory is
// dropped rather than reported at its dead predecessor's pin.
//
// Another user's process is never one of this deployment's: the trust floor is
// the same effective uid, and a root daemon that happens to run a queried path
// would otherwise refuse every irreversible step forever.
//
// An empty Inventory over a daemon's host and app executables is the absence
// proof quiesce and uninstall gate on, and it is the only one of daemonkit's
// absence proofs that reads the kernel's own process table directly. Stopped
// proves that one pinned identity left and says nothing about an orphaned
// child, a second instance, or the app half; Settle's identity comes out of a
// same-UID-writable owner record that a hostile writer can point at a corpse.
// Neither can be substituted for this. Gate every irreversible step on both.
//
// Unnameable comes back beside the answer rather than folded into it. A
// process nothing can name is evidence the scan may not drop — it may be the
// daemon whose bytes were unlinked — but it is evidence about no particular
// executable, and counting it against every query hands one long-lived husk
// anywhere on the machine a veto over every gate. Correlate it against an
// identity you recorded; ignore it when you recorded none.
//
// Each path scans the one process table, so a survivor both an unnameable set
// and a second query would report twice is reported once.
func Inventory(paths ...string) (Survivors, error) {
	found := Survivors{Live: make([]LiveProcess, 0), Unnameable: make([]LiveProcess, 0)}
	for _, path := range paths {
		report, err := proc.ExecutableIdentities(path)
		if err != nil {
			return Survivors{}, fmt.Errorf("deploy: inventory %q: %w", path, err)
		}
		for _, identity := range report.Matched {
			found.Live = append(found.Live, liveProcess(identity))
		}
		for _, identity := range report.Unnameable {
			found.Unnameable = append(found.Unnameable, liveProcess(identity))
		}
	}
	byPID := func(a, b LiveProcess) int { return a.PID - b.PID }
	slices.SortFunc(found.Live, byPID)
	slices.SortFunc(found.Unnameable, byPID)
	found.Live = slices.Compact(found.Live)
	found.Unnameable = slices.Compact(found.Unnameable)
	return found, nil
}

func liveProcess(identity proc.Identity) LiveProcess {
	return LiveProcess{
		PID:        identity.PID,
		Start:      identity.Start,
		Boot:       identity.Boot,
		Executable: identity.Executable,
	}
}

// resolveExecutables holds every declared host binary to the exact form the
// kernel reports for a running process: absolute, cleaned, and free of
// symlinks. The inventory compares a query against that form and against the
// literal one, and against nothing else, so a path in any other form matches
// nothing — and a gate that matches nothing is a gate that always passes,
// which is the one failure mode an absence proof may not have.
func resolveExecutables(declared []string) ([]string, error) {
	resolved := make([]string, 0, len(declared))
	for _, path := range declared {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return nil, fmt.Errorf("%w: executable %q must be an exact absolute path", ErrConfig, path)
		}
		actual, err := filepath.EvalSymlinks(path)
		if err != nil {
			return nil, fmt.Errorf("%w: resolve executable %q: %w", ErrConfig, path, err)
		}
		resolved = append(resolved, actual)
	}
	return resolved, nil
}

// executables is every program this deployment runs: each agent's Program, the
// host binaries the consumer declared outside the bundle, and every Mach-O
// executable a bundle carries at each of the locations a whole generation can
// occupy — [Deployment.generationSlots], which is where those locations are
// enumerated and why. The bundle half is what covers the helper that is neither
// an agent nor declared — nothing else would notice it, and it is the bundle
// under it that the next step deletes.
func (d *Deployment) executables() ([]string, error) {
	paths := make([]string, 0, len(d.config.Agents)+len(d.config.Executables))
	for _, agent := range d.config.Agents {
		paths = append(paths, agent.Program)
	}
	paths = append(paths, d.config.Executables...)
	slots, err := d.generationSlots()
	if err != nil {
		return nil, err
	}
	for _, slot := range slots {
		carried, err := bundleExecutables(slot)
		if err != nil {
			return nil, err
		}
		paths = append(paths, carried...)
	}
	slices.Sort(paths)
	return slices.Compact(paths), nil
}

// bundleExecutables reports every Mach-O file carrying an execute bit the
// generation slot holds: each one in its tree when the slot holds a bundle,
// walked under an os.Root scope so no entry names anything outside it, and the
// slot itself when it holds a plain file. A slot nothing occupies holds nothing,
// which is a state the ladder branches on rather than an error.
//
// Symlinks are skipped, the slot's own included: the kernel reports the file it
// execed, never a link to it, and a step that discards a slot unlinks the link
// rather than what it points at.
//
// A slot that exists and is not a directory holds no bundle, and the bytes it
// does hold are the whole of what the gate can honestly say about it. Failing
// the inventory for it instead refused every gated verb for as long as it stayed
// occupied — Reset included, which is the way out of a state no other verb
// accepts, so a plain file planted at a slot left no way out at all.
func bundleExecutables(slot string) ([]string, error) {
	info, err := os.Lstat(slot)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("deploy: inspect generation slot: %w", err)
	}
	if !info.IsDir() {
		return slotFileExecutable(slot, info)
	}
	handle, err := os.OpenRoot(slot)
	if err != nil {
		return nil, fmt.Errorf("deploy: open bundle root: %w", err)
	}
	defer handle.Close()
	var carried []string
	walkErr := fs.WalkDir(handle.FS(), ".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
			return nil
		}
		executable, err := isMachO(handle, path)
		if err != nil {
			return err
		}
		if executable {
			carried = append(carried, filepath.Join(slot, filepath.FromSlash(path)))
		}
		return nil
	})
	if walkErr != nil {
		return nil, fmt.Errorf("deploy: scan bundle executables: %w", walkErr)
	}
	return carried, nil
}

// slotFileExecutable answers for a generation slot holding a plain file: the
// file itself when it is a Mach-O the kernel could have execed, and nothing
// otherwise. It is read through a root scoped to the directory the slot sits in,
// exactly as a bundle's own entries are.
func slotFileExecutable(slot string, info os.FileInfo) ([]string, error) {
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return nil, nil
	}
	handle, err := os.OpenRoot(filepath.Dir(slot))
	if err != nil {
		return nil, fmt.Errorf("deploy: open generation slot root: %w", err)
	}
	defer handle.Close()
	executable, err := isMachO(handle, filepath.Base(slot))
	if err != nil {
		return nil, fmt.Errorf("deploy: scan generation slot: %w", err)
	}
	if !executable {
		return nil, nil
	}
	return []string{slot}, nil
}

func isMachO(handle *os.Root, path string) (bool, error) {
	file, err := handle.Open(path)
	if err != nil {
		return false, err
	}
	var header [4]byte
	_, readErr := io.ReadFull(file, header[:])
	closeErr := file.Close()
	if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
		return false, closeErr
	}
	if err := errors.Join(readErr, closeErr); err != nil {
		return false, err
	}
	native, swapped := binary.LittleEndian.Uint32(header[:]), binary.BigEndian.Uint32(header[:])
	for _, magic := range machOMagics {
		if native == magic || swapped == magic {
			return true, nil
		}
	}
	return false, nil
}

// requireEmpty is the inventory gate every quiesce arm ends at. A live process
// on one of this deployment's own executables refuses outright. A live process
// nothing could name refuses when its pin is the one this deployment's daemon
// recorded, and only then: its own husk is in the owner record Serve writes
// before it binds, while the long-lived stranger every machine carries — a
// process under some other product's deleted binary — is not this deployment's
// to answer for, and counting it would leave the gate unpassable forever.
//
// The residual is exact and worth stating: a husk that never recorded itself
// is attributable to nothing, and no scan of the process table can attribute
// it. Quiesce's other half is what covers a recorded identity whose executable
// is gone — Stopped's reap observes that identity out of the table directly.
func (d *Deployment) requireEmpty() error {
	paths, err := d.executables()
	if err != nil {
		return err
	}
	found, err := Inventory(paths...)
	if err != nil {
		return err
	}
	recorded, err := d.recordedIdentities()
	if err != nil {
		return err
	}
	remaining := append(slices.Clone(found.Live), found.attributed(recorded...)...)
	if len(remaining) == 0 {
		return nil
	}
	names := make([]string, len(remaining))
	for i, process := range remaining {
		names[i] = process.String()
	}
	return fmt.Errorf("%w: %s", ErrLive, strings.Join(names, ", "))
}

// recordedIdentities is every process instance this deployment wrote down as
// its own: the durable owner record its daemon persists before it binds. It is
// the only thing that says which unnameable process is this deployment's, and
// nothing recorded means nothing to attribute.
func (d *Deployment) recordedIdentities() ([]proc.Identity, error) {
	owner, recorded, err := proc.ReadOwner(d.config.Daemon.RecordPath())
	if err != nil {
		return nil, fmt.Errorf("deploy: read owner record: %w", err)
	}
	if !recorded {
		return nil, nil
	}
	return []proc.Identity{owner.Identity()}, nil
}
