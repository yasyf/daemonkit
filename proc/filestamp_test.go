package proc

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFileStampClaimsOncePerWindow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "job.stamp")
	stamp := FileStamp{Path: path, Window: time.Hour}

	won, err := stamp.Claim()
	if err != nil {
		t.Fatalf("first Claim() = %v", err)
	}
	if !won {
		t.Fatal("first Claim() = false, want true")
	}

	won, err = stamp.Claim()
	if err != nil {
		t.Fatalf("second Claim() = %v", err)
	}
	if won {
		t.Fatal("second Claim() within window = true, want false")
	}
}

func TestFileStampReclaimsAfterWindow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "job.stamp")
	if won, err := (FileStamp{Path: path, Window: time.Hour}).Claim(); err != nil || !won {
		t.Fatalf("first Claim() = %v, %v; want true, nil", won, err)
	}
	// The stamp's mtime is set at create; advance the virtual clock past Window.
	elapsed := FileStamp{Path: path, Window: time.Hour, now: func() time.Time { return time.Now().Add(2 * time.Hour) }}
	if won, err := elapsed.Claim(); err != nil || !won {
		t.Fatalf("Claim() after window = %v, %v; want true, nil", won, err)
	}
}

func TestFileStampSingleWinnerUnderContention(t *testing.T) {
	path := filepath.Join(t.TempDir(), "job.stamp")
	var wins atomic.Int64
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if won, err := (FileStamp{Path: path, Window: time.Hour}).Claim(); err == nil && won {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := wins.Load(); got != 1 {
		t.Fatalf("winners = %d, want exactly 1", got)
	}
}

func TestFileStampSingleWinnerOnStaleContention(t *testing.T) {
	path := filepath.Join(t.TempDir(), "job.stamp")
	if won, err := (FileStamp{Path: path, Window: time.Hour}).Claim(); err != nil || !won {
		t.Fatalf("seed Claim() = %v, %v; want true, nil", won, err)
	}
	// Backdate the stamp so it is stale for every claimant; racing removes+recreates
	// must still yield exactly one winner.
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	var wins atomic.Int64
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if won, err := (FileStamp{Path: path, Window: time.Hour}).Claim(); err == nil && won {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := wins.Load(); got != 1 {
		t.Fatalf("winners on stale contention = %d, want exactly 1", got)
	}
}

func TestFileStampReclaimsFarFutureStamp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "job.stamp")
	if won, err := (FileStamp{Path: path, Window: time.Hour}).Claim(); err != nil || !won {
		t.Fatalf("seed Claim() = %v, %v; want true, nil", won, err)
	}
	future := time.Now().Add(48 * time.Hour)
	if err := os.Chtimes(path, future, future); err != nil {
		t.Fatal(err)
	}
	if won, err := (FileStamp{Path: path, Window: time.Hour}).Claim(); err != nil || !won {
		t.Fatalf("Claim() on a far-future stamp = %v, %v; want true (treated as stale)", won, err)
	}
}

func TestFileStampNearFutureStampStillLoses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "job.stamp")
	if won, err := (FileStamp{Path: path, Window: time.Hour}).Claim(); err != nil || !won {
		t.Fatalf("seed Claim() = %v, %v; want true, nil", won, err)
	}
	// A small forward skew stays a recent claim; the throttle does not fire early.
	near := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(path, near, near); err != nil {
		t.Fatal(err)
	}
	if won, err := (FileStamp{Path: path, Window: time.Hour}).Claim(); err != nil || won {
		t.Fatalf("Claim() on a near-future stamp = %v, %v; want false (within slack)", won, err)
	}
}

func TestFileStampRejectsInvalidSpec(t *testing.T) {
	abs := filepath.Join(t.TempDir(), "job.stamp")
	tests := map[string]FileStamp{
		"empty path":      {Window: time.Hour},
		"relative path":   {Path: "job.stamp", Window: time.Hour},
		"zero window":     {Path: abs},
		"negative window": {Path: abs, Window: -time.Hour},
	}
	for name, stamp := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := stamp.Claim(); !errors.Is(err, ErrInvalidFileStamp) {
				t.Fatalf("Claim() err = %v, want ErrInvalidFileStamp", err)
			}
		})
	}
}
