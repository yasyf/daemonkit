package proc

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func storeRecord(class RecoveryClass, pid int) Record {
	return Record{
		RecoveryClass: class,
		PID:           pid,
		StartTime:     "start",
		Boot:          "boot",
		Comm:          "worker",
		Generation:    "prior",
	}
}

func commitStoreReceipt(t *testing.T, store *FileStore, record Record) ReapReceipt {
	t.Helper()
	if err := store.Add(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	if err := store.BeginReap(t.Context(), record, "successor"); err != nil {
		t.Fatal(err)
	}
	receipt, err := store.CommitReap(t.Context(), record, "successor", ReapAbsent)
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}

func seedStoreReceipts(t *testing.T, store *FileStore, class RecoveryClass, count int) []ReapReceipt {
	t.Helper()
	db, err := store.open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	receipts := make([]ReapReceipt, count)
	err = db.Update(func(tx *bolt.Tx) error {
		ledger, err := fileStoreLedger(tx)
		if err != nil {
			return err
		}
		for index := range receipts {
			record := storeRecord(class, 1000+index)
			receipt, err := newReapReceipt(ledger, uint64(index+1), record, "successor", ReapAbsent)
			if err != nil {
				return err
			}
			encoded, err := encodeStored(receipt)
			if err != nil {
				return err
			}
			key := receiptKey(class, receipt.Sequence)
			if err := tx.Bucket(fileStoreReceiptsBucket).Put(key, encoded); err != nil {
				return err
			}
			if err := tx.Bucket(fileStoreReceiptIndexBucket).Put([]byte(recordKey(record)), key); err != nil {
				return err
			}
			receipts[index] = receipt
		}
		if err := putBucketSequence(tx.Bucket(fileStoreSequencesBucket), class, uint64(count)); err != nil {
			return err
		}
		return updateOutstanding(tx, int64(count))
	})
	if err != nil {
		t.Fatal(err)
	}
	return receipts
}

func TestFileStoreAddIsExactIdempotent(t *testing.T) {
	store := &FileStore{Path: filepath.Join(t.TempDir(), "recovery.db")}
	record := storeRecord(RecoveryTask, 42)
	if err := store.Add(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	if err := store.Add(t.Context(), record); err != nil {
		t.Fatalf("exact retry: %v", err)
	}
	changed := record
	changed.Generation = "different-owner"
	if err := store.Add(t.Context(), changed); !errors.Is(err, ErrIdentityChanged) {
		t.Fatalf("changed process-instance record = %v", err)
	}
	records, err := store.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0] != record {
		t.Fatalf("records after conflicting add = %+v", records)
	}
}

func TestFileStoreReceiptLedgerPagesClassesAndPersistsFloors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recovery.db")
	store := &FileStore{Path: path, MaxOutstanding: 256}
	tasks := seedStoreReceipts(t, store, RecoveryTask, ReapReceiptPageLimit+2)
	for index := range tasks {
		if tasks[index].Sequence != uint64(index+1) {
			t.Fatalf("task sequence %d = %d", index, tasks[index].Sequence)
		}
	}
	trust := make([]ReapReceipt, 3)
	for index := range trust {
		trust[index] = commitStoreReceipt(t, store, storeRecord(RecoveryTrust, 2000+index))
		if trust[index].Sequence != uint64(index+1) {
			t.Fatalf("trust sequence %d = %d", index, trust[index].Sequence)
		}
		if trust[index].LedgerID != tasks[0].LedgerID {
			t.Fatal("classes did not share the stable store ledger")
		}
	}

	first, err := store.LoadReapReceipts(t.Context(), RecoveryTask, ReapReceiptCursor{}, ReapReceiptPageLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Receipts) != ReapReceiptPageLimit || !first.More || first.Next.Sequence != ReapReceiptPageLimit {
		t.Fatalf("first page = %+v", first)
	}
	second, err := store.LoadReapReceipts(t.Context(), RecoveryTask, first.Next, ReapReceiptPageLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Receipts) != 2 || second.More || second.Receipts[0].Sequence != ReapReceiptPageLimit+1 {
		t.Fatalf("second page = %+v", second)
	}
	trustPage, err := store.LoadReapReceipts(t.Context(), RecoveryTrust, ReapReceiptCursor{}, ReapReceiptPageLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(trustPage.Receipts) != 3 || trustPage.Receipts[0] != trust[0] {
		t.Fatalf("trust page = %+v", trustPage)
	}

	if _, err := store.AcknowledgeReap(t.Context(), tasks[1]); !errors.Is(err, ErrReapReceiptOrder) {
		t.Fatalf("out-of-order acknowledgement = %v", err)
	}
	floor, err := store.AcknowledgeReap(t.Context(), tasks[0])
	if err != nil || floor.Sequence != 1 {
		t.Fatalf("first acknowledgement = %+v, %v", floor, err)
	}
	floor, err = store.AcknowledgeReap(t.Context(), tasks[0])
	if err != nil || floor.Sequence != 1 {
		t.Fatalf("lost acknowledgement retry = %+v, %v", floor, err)
	}
	floor, err = store.AcknowledgeReap(t.Context(), tasks[1])
	if err != nil || floor.Sequence != 2 {
		t.Fatalf("second acknowledgement = %+v, %v", floor, err)
	}
	if _, err := store.AcknowledgeReap(t.Context(), tasks[0]); !errors.Is(err, ErrReapReceiptStale) {
		t.Fatalf("stale acknowledgement = %v", err)
	}

	reopened := &FileStore{Path: path, MaxOutstanding: 256}
	persisted, err := reopened.ReapReceiptFloor(t.Context(), RecoveryTask)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.LedgerID != tasks[0].LedgerID || persisted.Sequence != 2 {
		t.Fatalf("persisted floor = %+v", persisted)
	}
	badCursor := ReapReceiptCursor{LedgerID: ReceiptLedgerID{9}, Sequence: 2}
	if _, err := reopened.LoadReapReceipts(t.Context(), RecoveryTask, badCursor, 1); !errors.Is(err, ErrReapReceiptStale) {
		t.Fatalf("foreign cursor = %v", err)
	}
	if _, err := reopened.LoadReapReceipts(t.Context(), RecoveryTask, ReapReceiptCursor{Sequence: 2}, 1); !errors.Is(err, ErrReapReceiptStale) {
		t.Fatalf("anonymous nonzero cursor = %v", err)
	}
}

func TestFileStoreBackpressureCountsRecordsAndReceipts(t *testing.T) {
	store := &FileStore{Path: filepath.Join(t.TempDir(), "recovery.db"), MaxOutstanding: 2}
	first := storeRecord(RecoveryTask, 1)
	second := storeRecord(RecoveryTask, 2)
	third := storeRecord(RecoveryTask, 3)
	if err := store.Add(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	if err := store.Add(t.Context(), second); err != nil {
		t.Fatal(err)
	}
	if err := store.Add(t.Context(), third); !errors.Is(err, ErrReceiptBacklog) {
		t.Fatalf("third record admission = %v", err)
	}
	if err := store.BeginReap(t.Context(), first, "successor"); err != nil {
		t.Fatal(err)
	}
	receipt, err := store.CommitReap(t.Context(), first, "successor", ReapAbsent)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Add(t.Context(), third); !errors.Is(err, ErrReceiptBacklog) {
		t.Fatalf("receipt incorrectly released backlog admission: %v", err)
	}
	if _, err := store.AcknowledgeReap(t.Context(), receipt); err != nil {
		t.Fatal(err)
	}
	if err := store.Add(t.Context(), third); err != nil {
		t.Fatalf("acknowledgement did not reopen admission: %v", err)
	}
}

func TestRecoverReapReceiptsAcknowledgesOnlySettledPrefix(t *testing.T) {
	store := &FileStore{Path: filepath.Join(t.TempDir(), "recovery.db")}
	receipts := make([]ReapReceipt, 3)
	for index := range receipts {
		receipts[index] = commitStoreReceipt(t, store, storeRecord(RecoveryObserver, 100+index))
	}
	reaper := &Reaper{Store: store, Generation: "successor"}
	settleErr := errors.New("catalog not committed")
	var attempted []uint64
	_, err := reaper.RecoverReapReceipts(t.Context(), RecoveryObserver, func(_ context.Context, receipt ReapReceipt) error {
		attempted = append(attempted, receipt.Sequence)
		if receipt.Sequence == 2 {
			return settleErr
		}
		return nil
	})
	if !errors.Is(err, settleErr) {
		t.Fatalf("first recovery = %v", err)
	}
	floor, err := store.ReapReceiptFloor(t.Context(), RecoveryObserver)
	if err != nil || floor.Sequence != 1 {
		t.Fatalf("failed recovery floor = %+v, %v", floor, err)
	}
	attempted = nil
	floor, err = reaper.RecoverReapReceipts(t.Context(), RecoveryObserver, func(_ context.Context, receipt ReapReceipt) error {
		attempted = append(attempted, receipt.Sequence)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if floor.Sequence != 3 || len(attempted) != 2 || attempted[0] != 2 || attempted[1] != 3 {
		t.Fatalf("retry floor/attempts = %+v/%v", floor, attempted)
	}
	page, err := store.LoadReapReceipts(t.Context(), RecoveryObserver, ReapReceiptCursor{}, 1)
	if err != nil || len(page.Receipts) != 0 || page.Floor.Sequence != 3 {
		t.Fatalf("post-recovery page = %+v, %v", page, err)
	}
}
