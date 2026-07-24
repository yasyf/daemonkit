package proc

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

type unpublishedDeadlineContext struct {
	context.Context
	deadline time.Time
}

func TestReceiptKeyLengthPrefixesFullRecoveryID(t *testing.T) {
	id, err := ParseRecoveryID("consumer.barrier.v1")
	if err != nil {
		t.Fatal(err)
	}
	const sequence = uint64(42)
	key := receiptKey(id, sequence)
	if got := binary.BigEndian.Uint64(key[:8]); got != uint64(len(id)) {
		t.Fatalf("id length = %d, want %d", got, len(id))
	}
	if got := string(key[8 : 8+len(id)]); got != string(id) {
		t.Fatalf("id = %q, want %q", got, id)
	}
	if got := binary.BigEndian.Uint64(key[8+len(id):]); got != sequence {
		t.Fatalf("sequence = %d, want %d", got, sequence)
	}
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
	if err := store.Add(t.Context(), storeRecord(RecoveryTaskID, 42)); err != nil {
		t.Fatal(err)
	}
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	var identity, schema, fingerprint []byte
	if err := db.View(func(tx *bolt.Tx) error {
		meta := tx.Bucket(fileStoreMetaBucket)
		identity = append([]byte(nil), meta.Get(fileStoreIdentityKey)...)
		schema = append([]byte(nil), meta.Get(fileStoreSchemaKey)...)
		fingerprint = append([]byte(nil), meta.Get(fileStoreFingerprintKey)...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(schema) != 8 || binary.BigEndian.Uint64(schema) != 1 {
		t.Fatalf("schema = %x, want epoch 1", schema)
	}
	if string(identity) != string(fileStoreIdentity) || string(fingerprint) != string(fileStoreFingerprint) {
		t.Fatalf("identity/fingerprint = %q/%q, want %q/%q", identity, fingerprint, fileStoreIdentity, fileStoreFingerprint)
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

func TestFileStoreRejectsForeignMetadata(t *testing.T) {
	tests := map[string]func(*bolt.Bucket) error{
		"identity": func(meta *bolt.Bucket) error {
			return meta.Put(fileStoreIdentityKey, []byte("foreign"))
		},
		"fingerprint": func(meta *bolt.Bucket) error {
			return meta.Put(fileStoreFingerprintKey, []byte("foreign"))
		},
		"missing identity": func(meta *bolt.Bucket) error {
			return meta.Delete(fileStoreIdentityKey)
		},
		"unknown key": func(meta *bolt.Bucket) error {
			return meta.Put([]byte("future"), []byte("1"))
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "recovery.db")
			store := &FileStore{Path: path}
			if err := store.Add(t.Context(), storeRecord(RecoveryTaskID, 42)); err != nil {
				t.Fatal(err)
			}
			db, err := bolt.Open(path, 0o600, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Update(func(tx *bolt.Tx) error {
				return mutate(tx.Bucket(fileStoreMetaBucket))
			}); err != nil {
				_ = db.Close()
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Load(t.Context()); !errors.Is(err, ErrRecordSchema) {
				t.Fatalf("Load error = %v, want ErrRecordSchema", err)
			}
		})
	}
}

func TestFileStoreRejectsMissingRecordField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recovery.db")
	store := &FileStore{Path: path}
	record := storeRecord(RecoveryTaskID, 42)
	if err := store.Add(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(fileStoreRecordsBucket)
		key := []byte(recordKey(record))
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(bucket.Get(key), &fields); err != nil {
			return err
		}
		delete(fields, "executable")
		payload, err := json.Marshal(fields)
		if err != nil {
			return err
		}
		return bucket.Put(key, payload)
	}); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(t.Context()); !errors.Is(err, ErrRecordSchema) {
		t.Fatalf("Load error = %v, want ErrRecordSchema", err)
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

func storeRecord(id RecoveryID, pid int) Record {
	return Record{
		RecoveryID: id,
		PID:        pid,
		StartTime:  "start",
		Boot:       "boot",
		Comm:       "worker",
		Generation: testOwnerGeneration("prior"),
	}
}

func commitStoreReceipt(t *testing.T, store *FileStore, record Record) ReapReceipt {
	t.Helper()
	if err := store.Add(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	if err := store.BeginReap(t.Context(), record, testOwnerGeneration("successor")); err != nil {
		t.Fatal(err)
	}
	receipt, err := store.CommitReap(t.Context(), record, testOwnerGeneration("successor"), ReapAbsent)
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}

func seedStoreReceipts(t *testing.T, store *FileStore, id RecoveryID, count int) []ReapReceipt {
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
			record := storeRecord(id, 1000+index)
			receipt, err := newReapReceipt(ledger, uint64(index+1), record, testOwnerGeneration("successor"), ReapAbsent)
			if err != nil {
				return err
			}
			encoded, err := encodeStored(receipt)
			if err != nil {
				return err
			}
			key := receiptKey(id, receipt.Sequence)
			if err := tx.Bucket(fileStoreReceiptsBucket).Put(key, encoded); err != nil {
				return err
			}
			if err := tx.Bucket(fileStoreReceiptIndexBucket).Put([]byte(recordKey(record)), key); err != nil {
				return err
			}
			receipts[index] = receipt
		}
		if err := putBucketSequence(tx.Bucket(fileStoreSequencesBucket), id, uint64(count)); err != nil {
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
	record := storeRecord(RecoveryTaskID, 42)
	if err := store.Add(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	if err := store.Add(t.Context(), record); err != nil {
		t.Fatalf("exact retry: %v", err)
	}
	changed := record
	changed.Generation = testOwnerGeneration("different-owner")
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
	state := StopAuthorityArmed
	expiresUnixMilli := expires.UnixMilli()
	if expires.IsZero() {
		state = StopAuthorityPending
		expiresUnixMilli = 0
	}
	var stopSession StopSessionID
	stopSession[0] = 1
	var preparationNonce StopPreparationNonce
	preparationNonce[0] = 2
	return identity, Record{
		RecoveryID: RecoveryStopControlID,
		PID:        identity.PID, StartTime: identity.StartTime, Boot: identity.Boot, Comm: identity.Comm,
		Executable: identity.Executable, AuditToken: identity.AuditToken,
		Generation: testOwnerGeneration("controller-generation"), Role: "com.example.stop", OperationID: "stop-operation",
		StopSession: stopSession, PreparationNonce: preparationNonce, RuntimeProtocol: 1,
		TargetProcessGeneration: testOwnerGeneration("runtime-generation"),
		StopAuthorityState:      state, ExpiresUnixMilli: expiresUnixMilli,
	}
}

func TestFileStoreStopControlArmCommitReserve(t *testing.T) {
	const window = 5 * time.Second
	stamp := time.UnixMilli(1_900_000_000_000)
	for _, test := range []struct {
		name       string
		delay      time.Duration
		fullWindow bool
	}{
		{name: "within-reserve", delay: 4 * time.Second, fullWindow: true},
		{name: "reserve-exhausted", delay: 6 * time.Second, fullWindow: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			entered := make(chan struct{})
			release := make(chan struct{})
			store := &FileStore{
				Path:           filepath.Join(t.TempDir(), "recovery.db"),
				stopControlNow: func() time.Time { return stamp },
				stopControlAfterArmStamp: func() {
					close(entered)
					<-release
				},
			}
			identity, pending := stopControlStoreRecord(147, time.Time{})
			durablePending, err := store.addStopControlPending(t.Context(), pending)
			if err != nil {
				t.Fatal(err)
			}
			if got, ok, err := store.ConsumeStopControl(
				t.Context(), identity, pending.Role, pending.OperationID, pending.StopSession,
				pending.PreparationNonce, pending.RuntimeProtocol, pending.TargetProcessGeneration, stamp,
			); err != nil || ok || got != (Record{}) {
				t.Fatalf("pending consume = %+v, %v, %v; want zero, false, nil", got, ok, err)
			}
			type outcome struct {
				record Record
				err    error
			}
			result := make(chan outcome, 1)
			go func() {
				got, err := store.armStopControl(context.Background(), durablePending, window)
				result <- outcome{record: got, err: err}
			}()
			<-entered
			select {
			case got := <-result:
				t.Fatalf("arm committed before injected post-stamp barrier released: %+v", got)
			default:
			}
			releaseNow := stamp.Add(test.delay)
			close(release)
			got := <-result
			if got.err != nil {
				t.Fatal(got.err)
			}
			if got.record.StopAuthorityState != StopAuthorityArmed {
				t.Fatalf("state = %q, want armed", got.record.StopAuthorityState)
			}
			if want := stamp.Add(2 * window).UnixMilli(); got.record.ExpiresUnixMilli != want {
				t.Fatalf("expiry = %d, want private reserve + window = %d", got.record.ExpiresUnixMilli, want)
			}
			fullWindow := !releaseNow.Add(window).After(time.UnixMilli(got.record.ExpiresUnixMilli))
			if fullWindow != test.fullWindow {
				t.Fatalf("full window = %v, want %v", fullWindow, test.fullWindow)
			}
			store.stopControlNow = func() time.Time { return stamp.Add(time.Hour) }
			retry, err := store.armStopControl(t.Context(), durablePending, window)
			if err != nil {
				t.Fatal(err)
			}
			if retry != got.record {
				t.Fatalf("exact arm retry extended authority: got %+v want %+v", retry, got.record)
			}
			if test.fullWindow {
				return
			}
			revoked, err := store.revokeStopControl(t.Context(), got.record)
			if err != nil {
				t.Fatal(err)
			}
			if revoked.StopAuthorityState != StopAuthorityRevoked {
				t.Fatalf("state = %q, want revoked", revoked.StopAuthorityState)
			}
			if consumed, ok, err := store.ConsumeStopControl(
				t.Context(), identity, pending.Role, pending.OperationID, pending.StopSession,
				pending.PreparationNonce, pending.RuntimeProtocol, pending.TargetProcessGeneration, releaseNow,
			); err != nil || ok || consumed != (Record{}) {
				t.Fatalf("revoked consume = %+v, %v, %v; want zero, false, nil", consumed, ok, err)
			}
		})
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
				context.Background(), identity, record.Role, record.OperationID, record.StopSession,
				record.PreparationNonce, record.RuntimeProtocol, record.TargetProcessGeneration, time.Now(),
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
		t.Context(), identity, record.Role, record.OperationID, record.StopSession,
		record.PreparationNonce, record.RuntimeProtocol, record.TargetProcessGeneration, time.Now(),
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

func TestFileStoreConsumedStopControlCannotBeRetracked(t *testing.T) {
	store := &FileStore{Path: filepath.Join(t.TempDir(), "recovery.db")}
	reaper := &Reaper{Store: store, Generation: testOwnerGeneration("controller-generation")}
	identity, _ := stopControlStoreRecord(148, time.Time{})
	record, err := reaper.TrackStopControl(
		t.Context(), identity, "com.example.stop", "stop-operation", StopSessionID{1},
		StopPreparationNonce{2}, 1, testOwnerGeneration("runtime-generation"), time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := store.ConsumeStopControl(
		t.Context(), identity, record.Role, record.OperationID, record.StopSession,
		record.PreparationNonce, record.RuntimeProtocol, record.TargetProcessGeneration, time.Now(),
	); err != nil || !ok {
		t.Fatalf("consume = %v, %v; want true, nil", ok, err)
	}
	if _, err := reaper.TrackStopControl(
		t.Context(), identity, "com.example.stop", "stop-operation", StopSessionID{1},
		StopPreparationNonce{2}, 1, testOwnerGeneration("runtime-generation"), time.Minute,
	); err == nil || !strings.Contains(err.Error(), "already consumed") {
		t.Fatalf("retrack error = %v, want already consumed", err)
	}
}

func TestFileStoreStopControlRevokeRejectsRecoveryOwnership(t *testing.T) {
	for _, state := range []string{"claimed", "receipted"} {
		t.Run(state, func(t *testing.T) {
			store := &FileStore{Path: filepath.Join(t.TempDir(), "recovery.db")}
			_, record := stopControlStoreRecord(149, time.Now().Add(time.Minute))
			if err := store.Add(t.Context(), record); err != nil {
				t.Fatal(err)
			}
			if err := store.BeginReap(t.Context(), record, testOwnerGeneration("successor-generation")); err != nil {
				t.Fatal(err)
			}
			if state == "receipted" {
				if _, err := store.CommitReap(
					t.Context(), record, testOwnerGeneration("successor-generation"), ReapTerminated,
				); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := store.revokeStopControl(t.Context(), record); err == nil ||
				!strings.Contains(err.Error(), state[:len(state)-2]) {
				t.Fatalf("revoke %s error = %v", state, err)
			}
			if state == "claimed" {
				records, err := store.Load(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				if len(records) != 1 || records[0] != record {
					t.Fatalf("claimed record changed = %+v", records)
				}
				if _, err := store.CommitReap(
					t.Context(), record, testOwnerGeneration("successor-generation"), ReapTerminated,
				); err != nil {
					t.Fatalf("successor could not settle unchanged claim: %v", err)
				}
				records, err = store.Load(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				if len(records) != 0 {
					t.Fatalf("records after successor settlement = %+v, want none", records)
				}
			}
		})
	}
}

func TestFileStoreStopControlValidatesStoredRecordBeforeConsume(t *testing.T) {
	store := &FileStore{Path: filepath.Join(t.TempDir(), "recovery.db")}
	identity, record := stopControlStoreRecord(150, time.Now().Add(time.Minute))
	if err := store.Add(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	writeRecord := func(value Record) {
		t.Helper()
		db, err := store.open(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		encoded, err := encodeStored(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Update(func(tx *bolt.Tx) error {
			return tx.Bucket(fileStoreRecordsBucket).Put([]byte(recordKey(record)), encoded)
		}); err != nil {
			t.Fatal(err)
		}
	}
	malformed := record
	malformed.StopAuthorityState = StopAuthorityState("unknown")
	writeRecord(malformed)
	if got, ok, err := store.ConsumeStopControl(
		t.Context(), identity, record.Role, record.OperationID, record.StopSession,
		record.PreparationNonce, record.RuntimeProtocol, record.TargetProcessGeneration, time.Now(),
	); !errors.Is(err, ErrInvalidRecord) || ok || got != (Record{}) {
		t.Fatalf("malformed consume = %+v, %v, %v; want invalid record", got, ok, err)
	}
	writeRecord(record)
	if got, ok, err := store.ConsumeStopControl(
		t.Context(), identity, record.Role, record.OperationID, record.StopSession,
		record.PreparationNonce, record.RuntimeProtocol, record.TargetProcessGeneration, time.Now(),
	); err != nil || !ok || got != record {
		t.Fatalf("repaired consume = %+v, %v, %v; want record, true, nil", got, ok, err)
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
		t.Context(), identity, record.Role, record.OperationID, record.StopSession,
		record.PreparationNonce, record.RuntimeProtocol, record.TargetProcessGeneration, time.Now(),
	); err != nil || !ok || got != record {
		t.Fatalf("initial consume = %+v, %v, %v", got, ok, err)
	}
	reopened := &FileStore{Path: path}
	if got, ok, err := reopened.ConsumeStopControl(
		t.Context(), identity, record.Role, record.OperationID, record.StopSession,
		record.PreparationNonce, record.RuntimeProtocol, record.TargetProcessGeneration, time.Now(),
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
		name             string
		identity         Identity
		role             string
		operationID      string
		stopSession      StopSessionID
		preparationNonce StopPreparationNonce
		protocol         int
		target           OwnerGeneration
	}{
		{name: "pid", identity: wrongPID, role: record.Role, operationID: record.OperationID, stopSession: record.StopSession, preparationNonce: record.PreparationNonce, protocol: record.RuntimeProtocol, target: record.TargetProcessGeneration},
		{name: "start", identity: wrongStart, role: record.Role, operationID: record.OperationID, stopSession: record.StopSession, preparationNonce: record.PreparationNonce, protocol: record.RuntimeProtocol, target: record.TargetProcessGeneration},
		{name: "boot", identity: wrongBoot, role: record.Role, operationID: record.OperationID, stopSession: record.StopSession, preparationNonce: record.PreparationNonce, protocol: record.RuntimeProtocol, target: record.TargetProcessGeneration},
		{name: "comm", identity: wrongComm, role: record.Role, operationID: record.OperationID, stopSession: record.StopSession, preparationNonce: record.PreparationNonce, protocol: record.RuntimeProtocol, target: record.TargetProcessGeneration},
		{name: "executable", identity: wrongExecutable, role: record.Role, operationID: record.OperationID, stopSession: record.StopSession, preparationNonce: record.PreparationNonce, protocol: record.RuntimeProtocol, target: record.TargetProcessGeneration},
		{name: "audit", identity: wrongAudit, role: record.Role, operationID: record.OperationID, stopSession: record.StopSession, preparationNonce: record.PreparationNonce, protocol: record.RuntimeProtocol, target: record.TargetProcessGeneration},
		{name: "role", identity: identity, role: "com.example.other", operationID: record.OperationID, stopSession: record.StopSession, preparationNonce: record.PreparationNonce, protocol: record.RuntimeProtocol, target: record.TargetProcessGeneration},
		{name: "operation", identity: identity, role: record.Role, operationID: "other-operation", stopSession: record.StopSession, preparationNonce: record.PreparationNonce, protocol: record.RuntimeProtocol, target: record.TargetProcessGeneration},
		{name: "session", identity: identity, role: record.Role, operationID: record.OperationID, stopSession: StopSessionID{9}, preparationNonce: record.PreparationNonce, protocol: record.RuntimeProtocol, target: record.TargetProcessGeneration},
		{name: "nonce", identity: identity, role: record.Role, operationID: record.OperationID, stopSession: record.StopSession, preparationNonce: StopPreparationNonce{9}, protocol: record.RuntimeProtocol, target: record.TargetProcessGeneration},
		{name: "protocol", identity: identity, role: record.Role, operationID: record.OperationID, stopSession: record.StopSession, preparationNonce: record.PreparationNonce, protocol: 2, target: record.TargetProcessGeneration},
		{name: "target", identity: identity, role: record.Role, operationID: record.OperationID, stopSession: record.StopSession, preparationNonce: record.PreparationNonce, protocol: record.RuntimeProtocol, target: testOwnerGeneration("other-runtime")},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got, ok, err := store.ConsumeStopControl(
				t.Context(), test.identity, test.role, test.operationID, test.stopSession,
				test.preparationNonce, test.protocol, test.target, time.Now(),
			); err != nil || ok || got != (Record{}) {
				t.Fatalf("near match = %+v, %v, %v; want zero, false, nil", got, ok, err)
			}
		})
	}
	got, ok, err := store.ConsumeStopControl(
		t.Context(), identity, record.Role, record.OperationID, record.StopSession,
		record.PreparationNonce, record.RuntimeProtocol, record.TargetProcessGeneration, time.Now(),
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
		t.Context(), identity, record.Role, record.OperationID, record.StopSession,
		record.PreparationNonce, record.RuntimeProtocol, record.TargetProcessGeneration, time.Now(),
	); err != nil || !ok {
		t.Fatalf("consume = %v, %v", ok, err)
	}
	if err := store.BeginReap(t.Context(), record, testOwnerGeneration("reaper-generation")); err != nil {
		t.Fatal(err)
	}
	receipt, err := store.CommitReap(t.Context(), record, testOwnerGeneration("reaper-generation"), ReapTerminated)
	if err != nil {
		t.Fatal(err)
	}
	reopened := &FileStore{Path: path}
	reaper := &Reaper{Store: reopened, Generation: testOwnerGeneration("successor-generation")}
	var settled []ReapReceipt
	floor, err := reaper.RecoverReapReceipts(
		t.Context(), RecoveryStopControlID,
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
	page, err := reaper.ReapReceipts(t.Context(), RecoveryStopControlID, ReapReceiptCursor{}, ReapReceiptPageLimit)
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
		t.Context(), identity, record.Role, record.OperationID, record.StopSession,
		record.PreparationNonce, record.RuntimeProtocol, record.TargetProcessGeneration, expires,
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
	tasks := seedStoreReceipts(t, store, RecoveryTaskID, ReapReceiptPageLimit+2)
	for index := range tasks {
		if tasks[index].Sequence != uint64(index+1) {
			t.Fatalf("task sequence %d = %d", index, tasks[index].Sequence)
		}
	}
	trust := make([]ReapReceipt, 3)
	for index := range trust {
		trust[index] = commitStoreReceipt(t, store, storeRecord(RecoveryTrustID, 2000+index))
		if trust[index].Sequence != uint64(index+1) {
			t.Fatalf("trust sequence %d = %d", index, trust[index].Sequence)
		}
		if trust[index].LedgerID != tasks[0].LedgerID {
			t.Fatal("ids did not share the stable store ledger")
		}
	}

	first, err := store.LoadReapReceipts(t.Context(), RecoveryTaskID, ReapReceiptCursor{}, ReapReceiptPageLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Receipts) != ReapReceiptPageLimit || !first.More || first.Next.Sequence != ReapReceiptPageLimit {
		t.Fatalf("first page = %+v", first)
	}
	second, err := store.LoadReapReceipts(t.Context(), RecoveryTaskID, first.Next, ReapReceiptPageLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Receipts) != 2 || second.More || second.Receipts[0].Sequence != ReapReceiptPageLimit+1 {
		t.Fatalf("second page = %+v", second)
	}
	trustPage, err := store.LoadReapReceipts(t.Context(), RecoveryTrustID, ReapReceiptCursor{}, ReapReceiptPageLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(trustPage.Receipts) != 3 || trustPage.Receipts[0] != trust[0] {
		t.Fatalf("trust page = %+v", trustPage)
	}
	var all []ReapReceipt
	var scanCursor ReapReceiptScanCursor
	for {
		page, err := store.ScanReapReceipts(t.Context(), scanCursor, ReapReceiptPageLimit)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, page.Receipts...)
		if !page.More {
			break
		}
		scanCursor = page.Next
	}
	counts := make(map[RecoveryID]int)
	for _, receipt := range all {
		counts[receipt.Record.RecoveryID]++
	}
	if len(all) != len(tasks)+len(trust) || counts[RecoveryTaskID] != len(tasks) || counts[RecoveryTrustID] != len(trust) {
		t.Fatalf("exhaustive receipts = %d, counts %v", len(all), counts)
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
	persisted, err := reopened.ReapReceiptFloor(t.Context(), RecoveryTaskID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.LedgerID != tasks[0].LedgerID || persisted.Sequence != 2 {
		t.Fatalf("persisted floor = %+v", persisted)
	}
	badCursor := ReapReceiptCursor{LedgerID: ReceiptLedgerID{9}, Sequence: 2}
	if _, err := reopened.LoadReapReceipts(t.Context(), RecoveryTaskID, badCursor, 1); !errors.Is(err, ErrReapReceiptStale) {
		t.Fatalf("foreign cursor = %v", err)
	}
	if _, err := reopened.LoadReapReceipts(t.Context(), RecoveryTaskID, ReapReceiptCursor{Sequence: 2}, 1); !errors.Is(err, ErrReapReceiptStale) {
		t.Fatalf("anonymous nonzero cursor = %v", err)
	}
}

func TestFileStoreBackpressureCountsRecordsAndReceipts(t *testing.T) {
	store := &FileStore{Path: filepath.Join(t.TempDir(), "recovery.db"), MaxOutstanding: 2}
	first := storeRecord(RecoveryTaskID, 1)
	second := storeRecord(RecoveryTaskID, 2)
	third := storeRecord(RecoveryTaskID, 3)
	if err := store.Add(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	if err := store.Add(t.Context(), second); err != nil {
		t.Fatal(err)
	}
	if err := store.Add(t.Context(), third); !errors.Is(err, ErrReceiptBacklog) {
		t.Fatalf("third record admission = %v", err)
	}
	if err := store.BeginReap(t.Context(), first, testOwnerGeneration("successor")); err != nil {
		t.Fatal(err)
	}
	receipt, err := store.CommitReap(t.Context(), first, testOwnerGeneration("successor"), ReapAbsent)
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
		receipts[index] = commitStoreReceipt(t, store, storeRecord(testConsumerRecoveryID, 100+index))
	}
	reaper := &Reaper{Store: store, Generation: testOwnerGeneration("successor")}
	settleErr := errors.New("catalog not committed")
	var attempted []uint64
	_, err := reaper.RecoverReapReceipts(t.Context(), testConsumerRecoveryID, func(_ context.Context, receipt ReapReceipt) error {
		attempted = append(attempted, receipt.Sequence)
		if receipt.Sequence == 2 {
			return settleErr
		}
		return nil
	})
	if !errors.Is(err, settleErr) {
		t.Fatalf("first recovery = %v", err)
	}
	floor, err := store.ReapReceiptFloor(t.Context(), testConsumerRecoveryID)
	if err != nil || floor.Sequence != 1 {
		t.Fatalf("failed recovery floor = %+v, %v", floor, err)
	}
	attempted = nil
	floor, err = reaper.RecoverReapReceipts(t.Context(), testConsumerRecoveryID, func(_ context.Context, receipt ReapReceipt) error {
		attempted = append(attempted, receipt.Sequence)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if floor.Sequence != 3 || len(attempted) != 2 || attempted[0] != 2 || attempted[1] != 3 {
		t.Fatalf("retry floor/attempts = %+v/%v", floor, attempted)
	}
	page, err := store.LoadReapReceipts(t.Context(), testConsumerRecoveryID, ReapReceiptCursor{}, 1)
	if err != nil || len(page.Receipts) != 0 || page.Floor.Sequence != 3 {
		t.Fatalf("post-recovery page = %+v, %v", page, err)
	}
}
