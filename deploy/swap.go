package deploy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	}
	if record.Prior != nil && !fileExists(d.layout.prior) {
		if err := d.attest(ctx, *record.Prior); err != nil {
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
	return d.retireSwap()
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

// retireSwap discards everything a landed swap invalidates: the superseded
// tree, the sealed activation its readiness no longer describes, and the
// tombstone minted for a generation that is no longer installed.
//
// The swap record goes last, and that order is the crash window's only cover:
// while it is on disk the next verb's resume comes back through here, so a
// crash mid-retirement cannot strand a stale record that no resume revisits.
// A failure is the same window as a crash and gets the same cover, which is
// why the removals are two calls and not four arguments to one: errors.Join
// evaluates every argument it is handed, so a prior tree that could not be
// destroyed would still have had its record retired out from under it.
func (d *Deployment) retireSwap() error {
	if err := errors.Join(
		removeTreeDurable(d.layout.prior),
		removeFileDurable(d.layout.activation),
		removeFileDurable(d.layout.removal),
	); err != nil {
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

// discardStages reclaims every staging tree under the metadata directory. A
// stage that crashed before its rename owns a whole bundle copy the swap record
// does not name and no path derives from, so no verb but this one revisits it.
func (d *Deployment) discardStages() error {
	entries, err := os.ReadDir(d.layout.metadata)
	if err != nil {
		return fmt.Errorf("deploy: scan staging trees: %w", err)
	}
	discarded := make([]error, 0, len(entries))
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), stagePrefix) || !strings.HasSuffix(entry.Name(), stageSuffix) {
			continue
		}
		discarded = append(discarded, removeTreeDurable(filepath.Join(d.layout.metadata, entry.Name())))
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
