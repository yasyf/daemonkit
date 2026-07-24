package proc

import (
	"errors"
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

func TestFileStampRejectsInvalidSpec(t *testing.T) {
	abs := filepath.Join(t.TempDir(), "job.stamp")
	tests := map[string]FileStamp{
		"empty path":    {Window: time.Hour},
		"relative path": {Path: "job.stamp", Window: time.Hour},
		"zero window":   {Path: abs},
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
