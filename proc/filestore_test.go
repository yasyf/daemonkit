package proc

import (
	"context"
	"encoding/binary"
	"errors"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

type unpublishedDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (c unpublishedDeadlineContext) Deadline() (time.Time, bool) { return c.deadline, true }
func (unpublishedDeadlineContext) Done() <-chan struct{}         { return nil }
func (unpublishedDeadlineContext) Err() error                    { return nil }

func TestFileStoreExpiredDeadlineNeverReturnsNilSuccess(t *testing.T) {
	store := &FileStore{Path: filepath.Join(t.TempDir(), "recovery.db")}
	ctx := unpublishedDeadlineContext{Context: context.Background(), deadline: time.Now().Add(-time.Second)}
	db, err := store.open(ctx)
	if db != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("open = %v, %v; want nil, deadline exceeded", db, err)
	}
}

func TestFileStoreSchemaIsExactEpochOne(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recovery.db")
	store := &FileStore{Path: path}
	if err := store.Add(t.Context(), storeRecord(RecoveryTask, 42)); err != nil {
		t.Fatal(err)
	}
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	var schema []byte
	if err := db.View(func(tx *bolt.Tx) error {
		schema = append([]byte(nil), tx.Bucket(fileStoreMetaBucket).Get(fileStoreSchemaKey)...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(schema) != 8 || binary.BigEndian.Uint64(schema) != 1 {
		t.Fatalf("schema = %x, want epoch 1", schema)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(fileStoreMetaBucket).Put(fileStoreSchemaKey, uint64Bytes(2))
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(t.Context()); !errors.Is(err, ErrRecordSchema) {
		t.Fatalf("foreign schema load = %v, want ErrRecordSchema", err)
	}
}

func TestFileStoreRejectsIncompleteOldEpochOneWithoutMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recovery.db")
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	oldBuckets := [][]byte{
		fileStoreMetaBucket, fileStoreRecordsBucket, fileStoreClaimsBucket,
		fileStoreReceiptsBucket, fileStoreReceiptIndexBucket,
		fileStoreSequencesBucket, fileStoreFloorsBucket,
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		for _, name := range oldBuckets {
			if _, err := tx.CreateBucket(name); err != nil {
				return err
			}
		}
		meta := tx.Bucket(fileStoreMetaBucket)
		if err := meta.Put(fileStoreSchemaKey, uint64Bytes(recordSchemaVersion)); err != nil {
			return err
		}
		if err := meta.Put(fileStoreLedgerKey, make([]byte, len(ReceiptLedgerID{}))); err != nil {
			return err
		}
		return meta.Put(fileStoreOutstandingKey, uint64Bytes(0))
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	store := &FileStore{Path: path}
	if _, err := store.Load(t.Context()); !errors.Is(err, ErrRecordSchema) {
		t.Fatalf("old epoch-one load = %v, want ErrRecordSchema", err)
	}
	db, err = bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(fileStoreStopConsumedBucket) != nil {
			t.Fatal("failed schema open mutated the old layout")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

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

func stopControlStoreRecord(pid int, expires time.Time) (Identity, Record) {
	identity := Identity{
		PID: pid, StartTime: "start", Boot: "boot", Comm: "stop-child",
		Executable: "/Applications/Fixed.app/Contents/MacOS/Fixed",
		AuditToken: auditTokenForPID(pid, 17),
	}
	return identity, Record{
		RecoveryClass: RecoveryStopControl,
		PID:           identity.PID, StartTime: identity.StartTime, Boot: identity.Boot, Comm: identity.Comm,
		Executable: identity.Executable, AuditToken: identity.AuditToken,
		Generation: "controller-generation", Role: "com.example.stop", RuntimeBuild: "v2.0.0", RuntimeProtocol: 1,
		TargetProcessGeneration: "runtime-generation", Intent: "upgrade", ExpiresUnixMilli: expires.UnixMilli(),
	}
}

func TestFileStoreStopControlConsumesExactlyOnceConcurrently(t *testing.T) {
	store := &FileStore{Path: filepath.Join(t.TempDir(), "recovery.db")}
	identity, record := stopControlStoreRecord(142, time.Now().Add(time.Minute))
	if err := store.Add(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		record Record
		ok     bool
		err    error
	}
	start := make(chan struct{})
	results := make(chan outcome, 2)
	for range 2 {
		go func() {
			<-start
			got, ok, err := store.ConsumeStopControl(
				context.Background(), identity, record.Role, record.TargetProcessGeneration, time.Now(),
			)
			results <- outcome{record: got, ok: ok, err: err}
		}()
	}
	close(start)
	consumed := 0
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.ok {
			consumed++
			if result.record != record {
				t.Fatalf("consumed record = %+v, want %+v", result.record, record)
			}
		}
	}
	if consumed != 1 {
		t.Fatalf("successful consumes = %d, want 1", consumed)
	}
	if _, ok, err := store.ConsumeStopControl(
		t.Context(), identity, record.Role, record.TargetProcessGeneration, time.Now(),
	); err != nil || ok {
		t.Fatalf("replay = ok %v err %v, want false nil", ok, err)
	}
	records, err := store.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0] != record {
		t.Fatalf("post-consume recovery records = %+v, want exact helper retained", records)
	}
	if err := store.Remove(t.Context(), []Record{record}); err != nil {
		t.Fatal(err)
	}
	records, err = store.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("records after settled untrack = %+v", records)
	}
}

func TestFileStoreStopControlConsumedMarkerSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recovery.db")
	store := &FileStore{Path: path}
	identity, record := stopControlStoreRecord(145, time.Now().Add(time.Minute))
	if err := store.Add(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := store.ConsumeStopControl(
		t.Context(), identity, record.Role, record.TargetProcessGeneration, time.Now(),
	); err != nil || !ok || got != record {
		t.Fatalf("initial consume = %+v, %v, %v", got, ok, err)
	}
	reopened := &FileStore{Path: path}
	if got, ok, err := reopened.ConsumeStopControl(
		t.Context(), identity, record.Role, record.TargetProcessGeneration, time.Now(),
	); err != nil || ok || got != (Record{}) {
		t.Fatalf("reopen replay = %+v, %v, %v; want zero, false, nil", got, ok, err)
	}
	records, err := reopened.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0] != record {
		t.Fatalf("reopen recovery records = %+v, want consumed helper retained", records)
	}
}

func TestFileStoreStopControlRejectsNearMatchesWithoutConsuming(t *testing.T) {
	store := &FileStore{Path: filepath.Join(t.TempDir(), "recovery.db")}
	identity, record := stopControlStoreRecord(143, time.Now().Add(time.Minute))
	if err := store.Add(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	wrongPID := identity
	wrongPID.PID++
	wrongPID.AuditToken = auditTokenForPID(wrongPID.PID, 17)
	wrongStart := identity
	wrongStart.StartTime = "other-start"
	wrongBoot := identity
	wrongBoot.Boot = "other-boot"
	wrongComm := identity
	wrongComm.Comm = "other-child"
	wrongExecutable := identity
	wrongExecutable.Executable = "/Applications/Other.app/Contents/MacOS/Other"
	wrongAudit := identity
	wrongAudit.AuditToken = auditTokenForPID(identity.PID, 18)
	for _, test := range []struct {
		name     string
		identity Identity
		role     string
		target   string
	}{
		{name: "pid", identity: wrongPID, role: record.Role, target: record.TargetProcessGeneration},
		{name: "start", identity: wrongStart, role: record.Role, target: record.TargetProcessGeneration},
		{name: "boot", identity: wrongBoot, role: record.Role, target: record.TargetProcessGeneration},
		{name: "comm", identity: wrongComm, role: record.Role, target: record.TargetProcessGeneration},
		{name: "executable", identity: wrongExecutable, role: record.Role, target: record.TargetProcessGeneration},
		{name: "audit", identity: wrongAudit, role: record.Role, target: record.TargetProcessGeneration},
		{name: "role", identity: identity, role: "com.example.other", target: record.TargetProcessGeneration},
		{name: "target", identity: identity, role: record.Role, target: "other-runtime"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got, ok, err := store.ConsumeStopControl(
				t.Context(), test.identity, test.role, test.target, time.Now(),
			); err != nil || ok || got != (Record{}) {
				t.Fatalf("near match = %+v, %v, %v; want zero, false, nil", got, ok, err)
			}
		})
	}
	got, ok, err := store.ConsumeStopControl(
		t.Context(), identity, record.Role, record.TargetProcessGeneration, time.Now(),
	)
	if err != nil || !ok || got != record {
		t.Fatalf("exact consume = %+v, %v, %v; want record, true, nil", got, ok, err)
	}
}

func TestFileStoreConsumedStopControlRecoversThroughReceiptAcknowledgement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recovery.db")
	store := &FileStore{Path: path}
	identity, record := stopControlStoreRecord(146, time.Now().Add(time.Minute))
	if err := store.Add(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := store.ConsumeStopControl(
		t.Context(), identity, record.Role, record.TargetProcessGeneration, time.Now(),
	); err != nil || !ok {
		t.Fatalf("consume = %v, %v", ok, err)
	}
	if err := store.BeginReap(t.Context(), record, "reaper-generation"); err != nil {
		t.Fatal(err)
	}
	receipt, err := store.CommitReap(t.Context(), record, "reaper-generation", ReapTerminated)
	if err != nil {
		t.Fatal(err)
	}
	reopened := &FileStore{Path: path}
	reaper := &Reaper{Store: reopened, Generation: "successor-generation"}
	var settled []ReapReceipt
	floor, err := reaper.RecoverReapReceipts(
		t.Context(), RecoveryStopControl,
		func(_ context.Context, got ReapReceipt) error {
			settled = append(settled, got)
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(settled) != 1 || settled[0] != receipt || floor.Sequence != receipt.Sequence {
		t.Fatalf("recovery = receipts %+v floor %+v; want receipt %+v", settled, floor, receipt)
	}
	page, err := reaper.ReapReceipts(t.Context(), RecoveryStopControl, ReapReceiptCursor{}, ReapReceiptPageLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Receipts) != 0 || page.Floor.Sequence != receipt.Sequence {
		t.Fatalf("post-ack page = %+v", page)
	}
}

func TestFileStoreStopControlExpiryIsExactAndRetainedForRecovery(t *testing.T) {
	store := &FileStore{Path: filepath.Join(t.TempDir(), "recovery.db")}
	expires := time.UnixMilli(time.Now().Add(time.Minute).UnixMilli())
	identity, record := stopControlStoreRecord(144, expires)
	if err := store.Add(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := store.ConsumeStopControl(
		t.Context(), identity, record.Role, record.TargetProcessGeneration, expires,
	); err != nil || ok || got != (Record{}) {
		t.Fatalf("consume at expiry = %+v, %v, %v; want zero, false, nil", got, ok, err)
	}
	records, err := store.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0] != record {
		t.Fatalf("expired recovery records = %+v, want retained authority", records)
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
