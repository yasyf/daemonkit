package proc

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/daemonkit/internal/state"
)

func TestObserveClassification(t *testing.T) {
	id := Identity{PID: 4242, Start: 100, Boot: testBoot}
	probeFailure := errors.New("probe failed")
	tests := []struct {
		name        string
		boot        uint64
		probe       func(int) (procInfo, error)
		wantReap    Reap
		wantSettled bool
		wantErr     error
	}{
		{
			name:        "cross boot",
			boot:        testBoot + 1,
			probe:       func(int) (procInfo, error) { t.Fatal("probed across boots"); return procInfo{}, nil },
			wantReap:    ReapCrossBoot,
			wantSettled: true,
		},
		{
			name:        "absent",
			boot:        testBoot,
			probe:       func(int) (procInfo, error) { return procInfo{}, errNoProc },
			wantReap:    ReapAbsent,
			wantSettled: true,
		},
		{
			name:        "reused pid",
			boot:        testBoot,
			probe:       func(int) (procInfo, error) { return procInfo{start: 999}, nil },
			wantReap:    ReapReused,
			wantSettled: true,
		},
		{
			name:        "zombie is absent",
			boot:        testBoot,
			probe:       func(int) (procInfo, error) { return procInfo{start: 100, zombie: true}, nil },
			wantReap:    ReapAbsent,
			wantSettled: true,
		},
		{
			name:        "live exact match",
			boot:        testBoot,
			probe:       func(int) (procInfo, error) { return procInfo{start: 100}, nil },
			wantReap:    reapUndetermined,
			wantSettled: false,
		},
		{
			name:        "probe failure fails closed",
			boot:        testBoot,
			probe:       func(int) (procInfo, error) { return procInfo{}, probeFailure },
			wantReap:    reapUndetermined,
			wantSettled: false,
			wantErr:     probeFailure,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prober := &funcProber{
				probeFn: tt.probe,
				bootFn:  func() (uint64, error) { return tt.boot, nil },
			}
			reap, settled, err := observe(prober, id)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("observe() error = %v, want %v", err, tt.wantErr)
			}
			if reap != tt.wantReap || settled != tt.wantSettled {
				t.Fatalf("observe() = (%d, %t), want (%d, %t)", reap, settled, tt.wantReap, tt.wantSettled)
			}
		})
	}
}

func TestRecordOwnerRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.dkstate")
	store, err := OpenStore(path)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	defer store.Close()

	if _, ok, err := ReadOwner(path); err != nil || ok {
		t.Fatalf("ReadOwner(fresh) = ok=%t err=%v, want no owner", ok, err)
	}

	owner, err := store.RecordOwner("build-digest")
	if err != nil {
		t.Fatalf("RecordOwner: %v", err)
	}
	if owner.PID != os.Getpid() {
		t.Fatalf("Owner.PID = %d, want %d", owner.PID, os.Getpid())
	}
	if owner.Generation != store.Generation() {
		t.Fatalf("Owner.Generation = %d, want %d", owner.Generation, store.Generation())
	}
	if owner.Start == 0 || owner.Boot == 0 {
		t.Fatalf("Owner = %+v, want non-zero start and boot", owner)
	}
	if owner.Build != "build-digest" {
		t.Fatalf("Owner.Build = %q, want %q", owner.Build, "build-digest")
	}

	read, ok, err := ReadOwner(path)
	if err != nil || !ok {
		t.Fatalf("ReadOwner = ok=%t err=%v, want the recorded owner", ok, err)
	}
	if read != owner {
		t.Fatalf("ReadOwner = %+v, want %+v", read, owner)
	}
	if read.Identity() != (Identity{PID: owner.PID, Start: owner.Start, Boot: owner.Boot}) {
		t.Fatalf("Identity() = %+v, want the owner core", read.Identity())
	}
}

func TestReadOwnerRefusesIllFormedRecords(t *testing.T) {
	sound := Owner{PID: 4242, Start: 100, Boot: 200, Generation: 7, Build: "b1"}
	tests := []struct {
		name   string
		owner  Owner
		wantOK bool
	}{
		{"well formed", sound, true},
		{"pid only", Owner{PID: 4242}, false},
		{"zero pid", Owner{Start: 100, Boot: 200, Generation: 7, Build: "b1"}, false},
		{"negative pid", Owner{PID: -1, Start: 100, Boot: 200, Generation: 7, Build: "b1"}, false},
		{"zero start", Owner{PID: 4242, Boot: 200, Generation: 7, Build: "b1"}, false},
		{"zero boot", Owner{PID: 4242, Start: 100, Generation: 7, Build: "b1"}, false},
		{"zero generation", Owner{PID: 4242, Start: 100, Boot: 200, Build: "b1"}, false},
		{"empty build", Owner{PID: 4242, Start: 100, Boot: 200, Generation: 7}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "records.dkstate")
			owner := tt.owner
			if err := state.New[records](path, recordSchema).Store(records{Owner: &owner}); err != nil {
				t.Fatalf("Store: %v", err)
			}
			read, ok, err := ReadOwner(path)
			if err != nil {
				t.Fatalf("ReadOwner() = %v", err)
			}
			if ok != tt.wantOK {
				t.Fatalf("ReadOwner() ok = %t, want %t", ok, tt.wantOK)
			}
			if !tt.wantOK && read != (Owner{}) {
				t.Fatalf("ReadOwner() = %+v, want the zero owner", read)
			}
		})
	}
}

func TestOwnerSurvivesRecordWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.dkstate")
	store, err := OpenStore(path)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	defer store.Close()
	owner, err := store.RecordOwner("build-digest")
	if err != nil {
		t.Fatalf("RecordOwner: %v", err)
	}
	rec := record{PID: 54321, Start: 7, Boot: 9, Generation: store.Generation()}
	if err := store.add(rec); err != nil {
		t.Fatalf("add: %v", err)
	}
	<-store.retire(rec.id())
	read, ok, err := ReadOwner(path)
	if err != nil || !ok {
		t.Fatalf("ReadOwner = ok=%t err=%v, want owner intact after record writes", ok, err)
	}
	if read != owner {
		t.Fatalf("ReadOwner = %+v, want %+v", read, owner)
	}
}
