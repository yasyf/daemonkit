package deploy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// A staging tree is named with these and reclaimed by them; nothing else names
// one.
const (
	stagePrefix = ".staging-"
	stageSuffix = ".app"
)

// settleSwap drives the outstanding rename pair to its end: the canonical
// bundle aside to prior, then the staged candidate into canonical.
//
// Each rename carries its own precondition, and that precondition is also its
// idempotency check — a landed rename makes its own guard false. So the
// question "which rename already happened" is recomputed from stat and a
// codesign re-verify every single call and is never stored, which is what
// makes a crash at any point resumable by calling this again.
//
// The pair's completion is asked first and of the canonical path itself,
// because the prior tree is retired after the pair lands: once it is gone,
// "prior is not aside" no longer means "the first rename is pending", and a
// resume that trusted that guard alone would try to move the candidate it
// just landed back out of the way. It is asked of the inode, not the bytes:
// a candidate identical to the prior it replaces answers "these are the
// candidate's bytes" before either rename has run, and taking that for a
// landed pair strands the staged tree in its slot forever.
//
// The canonical path is what answers for the first rename too, and only it
// can: the recorded prior cannot be in two places, so a canonical path still
// holding it is that rename pending and a canonical path that does not is that
// rename done, however occupied or empty the destination looks. Asking the
// destination instead read an occupied one as a landed rename, which left the
// candidate no empty slot to land in and refused — and refused identically on
// every resume after, a permanent wedge no verb could clear. What occupied it
// is nothing any record names, since the generation the record does name is at
// the canonical path, and nothing the gate above this call left live; the
// rename therefore clears its own destination rather than refusing it.
func (d *Deployment) settleSwap(ctx context.Context, record swapRecord) error {
	if fileExists(d.layout.canonical) {
		occupant, err := d.inspect(ctx, d.layout.canonical)
		if err != nil {
			return err
		}
		if occupant.sameTree(record.Candidate) {
			return nil
		}
		if record.Prior == nil || !occupant.sameBytes(*record.Prior) {
			return fmt.Errorf("%w: the canonical app is neither the recorded prior nor the candidate", ErrConflict)
		}
		if err := d.attest(ctx, *record.Prior); err != nil {
			return err
		}
		if err := removeTreeDurable(d.layout.prior); err != nil {
			return err
		}
		if err := renameDurable(d.layout.canonical, d.layout.prior); err != nil {
			return err
		}
	}
	if fileExists(d.layout.candidate) {
		staged := record.Candidate
		staged.Path = d.layout.candidate
		if err := d.attest(ctx, staged); err != nil {
			return err
		}
		if fileExists(d.layout.canonical) {
			return fmt.Errorf("%w: canonical app is occupied during swap", ErrConflict)
		}
		if err := renameDurable(d.layout.candidate, d.layout.canonical); err != nil {
			return err
		}
	}
	landed, err := d.inspect(ctx, d.layout.canonical)
	if err != nil {
		return err
	}
	if !landed.sameTree(record.Candidate) {
		return fmt.Errorf("%w: swapped candidate is not the recorded generation", ErrConflict)
	}
	return nil
}

// recover drives any outstanding swap to its end and retires its record, so
// every verb below starts from a canonical path holding exactly one attested
// generation. A deployment with no swap outstanding is already there.
//
// A resume that still has bytes to move or destroy runs the same gate the
// first pass ran: the daemon is proved gone, its executables are proved empty,
// and its services are taken away so launchd cannot start it back into the
// rename it is about to lose its bundle to. Nothing else may drive a record to
// its end — every verb below calls this first, so an ungated resume would hand
// each of them the destruction the gate exists to refuse.
//
// A resume with nothing left but records to retire is not gated. It destroys
// nothing, and quiescing a healthy daemon to delete a stale record would be
// the larger harm.
func (d *Deployment) recover(ctx context.Context) error {
	var record swapRecord
	err := readRecord(d.layout.swap, &record)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if record.Target != d.layout.canonical {
		return fmt.Errorf("%w: swap record names %q", ErrState, record.Target)
	}
	destroys, err := d.resumeDestroys(ctx, record)
	if err != nil {
		return err
	}
	if destroys {
		if _, err := d.quiesceAndConverge(ctx, nil); err != nil {
			return err
		}
	}
	if err := d.settleSwap(ctx, record); err != nil {
		return err
	}
	return d.retireSwap(record)
}

// resumeDestroys reports whether driving record to its end still moves or
// destroys a generation's bytes. A first install has none to lose: it moves
// its candidate into an empty slot, which is why the first pass gates only a
// supersede. A supersede whose candidate already occupies the canonical path
// and whose prior tree is already gone has nothing left but records.
func (d *Deployment) resumeDestroys(ctx context.Context, record swapRecord) (bool, error) {
	if record.Prior == nil {
		return false, nil
	}
	if fileExists(d.layout.prior) || !fileExists(d.layout.canonical) {
		return true, nil
	}
	occupant, err := d.inspect(ctx, d.layout.canonical)
	if err != nil {
		return false, err
	}
	return !occupant.sameTree(record.Candidate), nil
}

// retireSwap discards everything the landed swap in record invalidates: the
// superseded tree, the sealed activation its readiness no longer describes, and
// the tombstone minted for a generation that is no longer installed.
//
// The prior tree is destroyed only when the record names one, and that is what
// keeps this a gated destruction: a record naming a prior is one whose pass
// proved the deployment empty and whose settle renamed that very generation
// aside, so the bytes destroyed here are the bytes the gate just scanned. A
// record naming none — a first install, whose canonical path was empty and
// which therefore proves nothing absent — leaves a tree some earlier crash
// stranded in the slot alone; reclaiming it is Reset's, under the whole gate.
//
// The swap record goes last, and that order is the crash window's only cover:
// while it is on disk the next verb's resume comes back through here, so a
// crash mid-retirement cannot strand a stale record that no resume revisits.
// A failure is the same window as a crash and gets the same cover, which is
// why the record's removal is a call of its own: errors.Join evaluates every
// argument it is handed, so a prior tree that could not be destroyed would
// still have had its record retired out from under it.
func (d *Deployment) retireSwap(record swapRecord) error {
	retired := []error{
		removeFileDurable(d.layout.activation),
		removeFileDurable(d.layout.removal),
	}
	if record.Prior != nil {
		retired = append(retired, removeTreeDurable(d.layout.prior))
	}
	if err := errors.Join(retired...); err != nil {
		return err
	}
	return removeFileDurable(d.layout.swap)
}

// stage copies the candidate source into the deployment's private candidate
// slot, beside the canonical path so the swap is a rename and never a copy,
// and re-attests the source before and the copy after: bytes that changed
// under the copy never reach the slot.
func (d *Deployment) stage(ctx context.Context, candidate Candidate) (Generation, error) {
	if fileExists(d.layout.candidate) {
		staged, err := d.inspect(ctx, d.layout.candidate)
		if err != nil {
			return Generation{}, err
		}
		return staged, candidate.matches(staged)
	}
	source, err := d.inspect(ctx, candidate.Source)
	if err != nil {
		return Generation{}, err
	}
	if err := candidate.matches(source); err != nil {
		return Generation{}, err
	}
	stagePath, err := os.MkdirTemp(d.layout.metadata, stagePrefix+"*"+stageSuffix)
	if err != nil {
		return Generation{}, fmt.Errorf("deploy: create candidate stage: %w", err)
	}
	staged, stageErr := d.copyIntoStage(ctx, candidate, source, stagePath)
	if stageErr != nil {
		return Generation{}, errors.Join(stageErr, removeTreeDurable(stagePath))
	}
	return staged, nil
}

// generationSlots is every location this deployment can move or copy a whole
// generation into: the canonical path, the three private slots beside it, and
// every staging tree left under the metadata directory. It is the one place
// those locations are named — the inventory gate scans exactly this set and
// every step that destroys a generation draws its targets from it, so a
// location added here is covered by both and one missing from it by neither.
//
// The private slots belong on it because the canonical path is not where a
// bundle lives when the gate matters most. Supersede renames the incumbent
// aside to prior and the staged candidate into place; uninstall renames the
// whole generation into the removal slot; a stage that crashed before its
// rename owns a full bundle copy the swap record does not name and no path
// derives from. Each is bytes a process may still be running and the next step
// destroys. A slot holding no bundle is not an error.
func (d *Deployment) generationSlots() ([]string, error) {
	stages, err := d.stagingTrees()
	if err != nil {
		return nil, err
	}
	fixed := []string{d.layout.canonical, d.layout.prior, d.layout.candidate, d.layout.removed}
	return slices.Concat(fixed, stages), nil
}

// stagingTrees names every staging tree under the metadata directory. A
// deployment that has never held its lock has no metadata directory and so no
// trees, which is a state the ladder branches on rather than an error.
func (d *Deployment) stagingTrees() ([]string, error) {
	entries, err := os.ReadDir(d.layout.metadata)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("deploy: scan staging trees: %w", err)
	}
	trees := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), stagePrefix) || !strings.HasSuffix(entry.Name(), stageSuffix) {
			continue
		}
		trees = append(trees, filepath.Join(d.layout.metadata, entry.Name()))
	}
	return trees, nil
}

// discardGenerations destroys every generation slot but the canonical path,
// whose installed bytes no caller of this asked to lose. Its targets come out
// of the same enumeration the inventory gate scans, so it destroys nothing the
// gate did not just prove empty.
func (d *Deployment) discardGenerations() error {
	slots, err := d.generationSlots()
	if err != nil {
		return err
	}
	discarded := make([]error, 0, len(slots))
	for _, slot := range slots {
		if slot == d.layout.canonical {
			continue
		}
		discarded = append(discarded, removeTreeDurable(slot))
	}
	return errors.Join(discarded...)
}

func (d *Deployment) copyIntoStage(
	ctx context.Context,
	candidate Candidate,
	source Generation,
	stagePath string,
) (Generation, error) {
	if err := copyBundleTree(candidate.Source, stagePath); err != nil {
		return Generation{}, err
	}
	if err := syncBundleTree(stagePath); err != nil {
		return Generation{}, err
	}
	if err := d.attest(ctx, source); err != nil {
		return Generation{}, err
	}
	copied, err := d.inspect(ctx, stagePath)
	if err != nil {
		return Generation{}, err
	}
	if !copied.sameBytes(source) {
		return Generation{}, fmt.Errorf("%w: staged copy differs from its source", ErrConflict)
	}
	if err := renameDurable(stagePath, d.layout.candidate); err != nil {
		return Generation{}, fmt.Errorf("deploy: publish private candidate: %w", err)
	}
	published, err := d.inspect(ctx, d.layout.candidate)
	if err != nil {
		return Generation{}, err
	}
	if !published.sameBytes(source) {
		return Generation{}, fmt.Errorf("%w: published candidate differs from its source", ErrConflict)
	}
	return published, nil
}
