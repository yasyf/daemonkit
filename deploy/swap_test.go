package deploy

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/yasyf/daemonkit"
)

// crashed builds the on-disk state a crash at one point in the rename pair
// leaves behind: the swap record beside whichever trees had already moved.
// Nothing records which rename landed, so this is the whole input recover
// gets — the point of the test is that it is enough.
type crashed struct {
	name string
	// prior is the generation the canonical path held before the swap; empty
	// on a first install.
	prior string
	// place arranges the trees, given the settled layout, and reports what the
	// canonical path must hold once recover returns.
	place func(t *testing.T, f *fixture)
	want  string
}

func TestRecoverSettlesTheRenamePairFromEveryCrashPoint(t *testing.T) {
	tests := []crashed{
		{
			name:  "supersede: neither rename landed",
			prior: "one",
			place: func(*testing.T, *fixture) {},
			want:  "two",
		},
		{
			name:  "supersede: only the prior moved aside",
			prior: "one",
			place: func(t *testing.T, f *fixture) {
				rename(t, f.deploy.layout.canonical, f.deploy.layout.prior)
			},
			want: "two",
		},
		{
			name:  "supersede: both renames landed, prior still aside",
			prior: "one",
			place: func(t *testing.T, f *fixture) {
				rename(t, f.deploy.layout.canonical, f.deploy.layout.prior)
				rename(t, f.deploy.layout.candidate, f.deploy.layout.canonical)
			},
			want: "two",
		},
		{
			name:  "supersede: both renames landed and the prior was already retired",
			prior: "one",
			place: func(t *testing.T, f *fixture) {
				if err := os.RemoveAll(f.deploy.layout.canonical); err != nil {
					t.Fatal(err)
				}
				rename(t, f.deploy.layout.candidate, f.deploy.layout.canonical)
			},
			want: "two",
		},
		{
			name:  "install: the rename did not land",
			place: func(*testing.T, *fixture) {},
			want:  "two",
		},
		{
			name: "install: the rename landed",
			place: func(t *testing.T, f *fixture) {
				rename(t, f.deploy.layout.candidate, f.deploy.layout.canonical)
			},
			want: "two",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			f.crash(tt.prior, "two", tt.place)
			if err := f.deploy.recover(f.ctx()); err != nil {
				t.Fatalf("recover: %v", err)
			}
			f.wantCanonical(tt.want)
			if fileExists(f.deploy.layout.swap) {
				t.Error("recover kept the swap record after settling it")
			}
			if fileExists(f.deploy.layout.prior) {
				t.Error("recover kept the prior tree after settling the swap")
			}
			if fileExists(f.deploy.layout.candidate) {
				t.Error("recover kept the candidate slot after settling the swap")
			}
			if err := f.deploy.recover(f.ctx()); err != nil {
				t.Fatalf("recover replay: %v", err)
			}
			f.wantCanonical(tt.want)
		})
	}
}

func TestRecoverRefusesATornRenamePair(t *testing.T) {
	tests := []struct {
		name  string
		place func(t *testing.T, f *fixture)
		want  error
	}{
		{
			name: "canonical occupied by a foreign bundle while the candidate waits",
			place: func(t *testing.T, f *fixture) {
				rename(t, f.deploy.layout.canonical, f.deploy.layout.prior)
				foreign := f.bundle("Foreign", "9.9", "foreign")
				rename(t, foreign, f.deploy.layout.canonical)
			},
			want: ErrConflict,
		},
		{
			name: "the recorded prior is not what the canonical path holds",
			place: func(t *testing.T, f *fixture) {
				if err := os.WriteFile(
					f.deploy.layout.canonical+"/Contents/MacOS/example", []byte("tampered"), 0o755,
				); err != nil {
					t.Fatal(err)
				}
			},
			want: ErrConflict,
		},
		{
			name: "the staged candidate is not the recorded one",
			place: func(t *testing.T, f *fixture) {
				rename(t, f.deploy.layout.canonical, f.deploy.layout.prior)
				if err := os.WriteFile(
					f.deploy.layout.candidate+"/Contents/MacOS/example", []byte("tampered"), 0o755,
				); err != nil {
					t.Fatal(err)
				}
			},
			want: ErrConflict,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			f.crash("one", "two", tt.place)
			if err := f.deploy.recover(f.ctx()); !errors.Is(err, tt.want) {
				t.Fatalf("recover err = %v, want %v", err, tt.want)
			}
			if !fileExists(f.deploy.layout.swap) {
				t.Error("a refused recover retired the swap record")
			}
		})
	}
}

// TestRecoverGatesTheResumedDestruction pins the gate on the resume path. The
// record a crash leaves behind drives the rename pair to its end and then
// destroys the generation it superseded, and every verb starts by resuming it
// — so a resume that skipped the gate would hand each of them the destruction
// the first pass refuses while a process of the deployment is still live.
func TestRecoverGatesTheResumedDestruction(t *testing.T) {
	tests := []struct {
		name  string
		place func(t *testing.T, f *fixture)
	}{
		{
			name:  "neither rename landed",
			place: func(*testing.T, *fixture) {},
		},
		{
			name: "only the prior moved aside",
			place: func(t *testing.T, f *fixture) {
				rename(t, f.deploy.layout.canonical, f.deploy.layout.prior)
			},
		},
		{
			name: "both renames landed, prior still aside",
			place: func(t *testing.T, f *fixture) {
				rename(t, f.deploy.layout.canonical, f.deploy.layout.prior)
				rename(t, f.deploy.layout.candidate, f.deploy.layout.canonical)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			f.crash("one", "two", tt.place)
			f.live = []LiveProcess{{PID: 909, Executable: f.agent.Program}}
			if err := f.deploy.recover(f.ctx()); !errors.Is(err, ErrLive) {
				t.Fatalf("recover err = %v, want ErrLive", err)
			}
			f.wantSuperseded("one")
			if !fileExists(f.deploy.layout.swap) {
				t.Error("a refused resume retired the swap record")
			}
		})
	}
}

// TestRecoverResumesASettledSwapWithoutAGate pins the other side of that gate:
// a resume holding nothing but records to retire destroys nothing, and
// quiescing a healthy daemon to delete a stale record would be the larger harm.
func TestRecoverResumesASettledSwapWithoutAGate(t *testing.T) {
	f := newFixture(t)
	f.crash("one", "two", func(t *testing.T, f *fixture) {
		if err := os.RemoveAll(f.deploy.layout.canonical); err != nil {
			t.Fatal(err)
		}
		rename(t, f.deploy.layout.candidate, f.deploy.layout.canonical)
	})
	f.live = []LiveProcess{{PID: 909, Executable: f.agent.Program}}
	if err := f.deploy.recover(f.ctx()); err != nil {
		t.Fatalf("recover: %v", err)
	}
	f.wantCanonical("two")
	if fileExists(f.deploy.layout.swap) {
		t.Error("recover kept the swap record it had nothing left to settle")
	}
}

// TestRecoverRetiresEveryRecordTheSwapInvalidates pins the crash window a land
// leaves between its last rename and its terminal cleanup. The swap record is
// retired last, so while it is on disk the next verb's resume comes back
// through recover — and a resume that retired only the prior tree and the swap
// record would leave the superseded generation's sealed activation and its
// tombstone to wedge the verbs that read them.
func TestRecoverRetiresEveryRecordTheSwapInvalidates(t *testing.T) {
	f := newFixture(t)
	f.crash("one", "two", func(t *testing.T, f *fixture) {
		rename(t, f.deploy.layout.canonical, f.deploy.layout.prior)
		rename(t, f.deploy.layout.candidate, f.deploy.layout.canonical)
	})
	superseded, err := f.deploy.inspect(f.ctx(), f.deploy.layout.prior)
	if err != nil {
		t.Fatal(err)
	}
	superseded.Path = f.deploy.layout.canonical
	if err := writeRecord(f.deploy.layout.activation, activationRecord{
		Identity: activationIdentity, Schema: recordSchema, Generation: superseded,
		Readiness: storedProof{Build: "superseded", Generation: 3, Digest: SHA256{7}.String()},
	}); err != nil {
		t.Fatal(err)
	}
	if err := writeRecord(f.deploy.layout.removal, removalRecord{
		Identity: removalIdentity, Schema: recordSchema, Generation: superseded,
		Runtime: runtimeProof(daemonkit.Stopped{Reap: daemonkit.ReapAbsent}).stored(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := f.deploy.recover(f.ctx()); err != nil {
		t.Fatalf("recover: %v", err)
	}
	for name, path := range map[string]string{
		"activation": f.deploy.layout.activation,
		"removal":    f.deploy.layout.removal,
		"swap":       f.deploy.layout.swap,
		"prior":      f.deploy.layout.prior,
	} {
		if fileExists(path) {
			t.Errorf("recover left the superseded %s behind at %q", name, path)
		}
	}
	f.wantCanonical("two")
}

// TestRetireSwapKeepsTheRecordUntilThePriorTreeIsGone pins that same window
// against an I/O failure rather than a crash. The swap record is what brings
// the next verb's resume back through recover, so a prior tree that could not
// be destroyed has to leave it on disk — and errors.Join evaluates every one of
// its arguments before it joins them, so the removals cannot all be arguments
// to one call.
func TestRetireSwapKeepsTheRecordUntilThePriorTreeIsGone(t *testing.T) {
	f := newFixture(t)
	f.crash("one", "two", func(t *testing.T, f *fixture) {
		rename(t, f.deploy.layout.canonical, f.deploy.layout.prior)
		rename(t, f.deploy.layout.candidate, f.deploy.layout.canonical)
	})
	sealTree(t, f.deploy.layout.prior)
	var record swapRecord
	if err := readRecord(f.deploy.layout.swap, &record); err != nil {
		t.Fatal(err)
	}
	if err := f.deploy.retireSwap(record); err == nil {
		t.Fatal("retireSwap = nil, want the undeletable prior tree's failure")
	}
	if !fileExists(f.deploy.layout.prior) {
		t.Fatal("the prior tree was destroyed after all; the case proves nothing")
	}
	if !fileExists(f.deploy.layout.swap) {
		t.Fatal("retireSwap discarded the record that brings the next resume back to the surviving prior tree")
	}
}

// TestRetireSwapDestroysNoPriorItsRecordDoesNotName holds the retirement to the
// one generation this pass's gate scanned and settle itself renamed aside. An
// install proves nothing absent — it lands into an empty canonical path — so a
// tree some earlier crash stranded in the prior slot is bytes no gate of this
// pass ever asked about, and destroying it is exactly the harm the gate exists
// to refuse. A gated verb is what reclaims it.
func TestRetireSwapDestroysNoPriorItsRecordDoesNotName(t *testing.T) {
	f := newFixture(t)
	if err := f.deploy.layout.ensureMetadata(); err != nil {
		t.Fatal(err)
	}
	stranded := filepath.Join(f.deploy.layout.prior, "Contents", "MacOS", "example")
	writeMachO(t, stranded, 0o755)
	if _, err := f.deploy.Install(f.ctx(), f.candidate("Source", "1.0", "one")); err != nil {
		t.Fatalf("Install: %v", err)
	}
	if !fileExists(stranded) {
		t.Fatal("Install destroyed a generation no gate of its own ever scanned")
	}
	var queried []string
	f.deploy.inventory = func(paths ...string) (Survivors, error) {
		queried = append(queried, paths...)
		return Survivors{}, nil
	}
	if err := f.deploy.Reset(f.ctx()); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if fileExists(f.deploy.layout.prior) {
		t.Fatal("Reset left the stranded prior tree behind")
	}
	if !slices.Contains(queried, stranded) {
		t.Fatalf("Reset destroyed the stranded prior tree without scanning %q: %q", stranded, queried)
	}
}

// sealTree makes path undeletable: os.RemoveAll must enter the directory it
// plants and may not unlink the file inside it.
func sealTree(t *testing.T, path string) {
	t.Helper()
	sealed := filepath.Join(path, "sealed")
	if err := os.MkdirAll(sealed, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sealed, "held"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(sealed, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(sealed, 0o700) })
}

func TestRecoverRefusesARecordForAnotherApp(t *testing.T) {
	f := newFixture(t)
	f.crash("one", "two", func(*testing.T, *fixture) {})
	var record swapRecord
	if err := readRecord(f.deploy.layout.swap, &record); err != nil {
		t.Fatal(err)
	}
	record.Target = f.root + "/Other.app"
	if err := writeRecord(f.deploy.layout.swap, record); err != nil {
		t.Fatal(err)
	}
	if err := f.deploy.recover(f.ctx()); !errors.Is(err, ErrState) {
		t.Fatalf("recover err = %v, want ErrState", err)
	}
}

// crash lays down a swap record and its two bundles in the settled positions
// — prior at the canonical path, candidate in the private slot — then lets
// place move whichever trees the crash point had already moved.
func (f *fixture) crash(prior, candidate string, place func(*testing.T, *fixture)) {
	f.t.Helper()
	record := swapRecord{Identity: swapIdentity, Schema: recordSchema, Target: f.deploy.layout.canonical}
	if prior != "" {
		rename(f.t, f.bundle("Prior", "1.0", prior), f.deploy.layout.canonical)
		generation, err := f.deploy.inspect(f.ctx(), f.deploy.layout.canonical)
		if err != nil {
			f.t.Fatal(err)
		}
		record.Prior = &generation
	}
	rename(f.t, f.bundle("Candidate", "2.0", candidate), f.deploy.layout.candidate)
	staged, err := f.deploy.inspect(f.ctx(), f.deploy.layout.candidate)
	if err != nil {
		f.t.Fatal(err)
	}
	staged.Path = f.deploy.layout.canonical
	record.Candidate = staged
	if err := f.deploy.layout.ensureMetadata(); err != nil {
		f.t.Fatal(err)
	}
	if err := writeRecord(f.deploy.layout.swap, record); err != nil {
		f.t.Fatal(err)
	}
	place(f.t, f)
}

func (f *fixture) wantCanonical(body string) {
	f.t.Helper()
	got, err := os.ReadFile(f.deploy.layout.canonical + "/Contents/MacOS/example")
	if err != nil {
		f.t.Fatalf("read canonical executable: %v", err)
	}
	if string(got) != body {
		f.t.Fatalf("canonical executable = %q, want %q", got, body)
	}
}

// wantSuperseded asserts the generation the swap replaces is still on disk, at
// whichever of the two slots the crash point left it in.
func (f *fixture) wantSuperseded(body string) {
	f.t.Helper()
	for _, path := range []string{f.deploy.layout.canonical, f.deploy.layout.prior} {
		got, err := os.ReadFile(path + "/Contents/MacOS/example")
		if err == nil && string(got) == body {
			return
		}
	}
	f.t.Fatalf("the superseded generation %q survives at neither the canonical path nor the prior slot", body)
}

func rename(t *testing.T, from, to string) {
	t.Helper()
	if err := os.Rename(from, to); err != nil {
		t.Fatal(err)
	}
}
