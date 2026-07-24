package proc

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	bolt "go.etcd.io/bbolt"
)

const (
	defaultMaxOutstanding = 4096
	fileStoreOpenTimeout  = 5 * time.Second
)

var (
	fileStoreMetaBucket         = []byte("meta")
	fileStoreRecordsBucket      = []byte("records")
	fileStoreClaimsBucket       = []byte("claims")
	fileStoreStopConsumedBucket = []byte("stop-consumed")
	fileStoreReceiptsBucket     = []byte("receipts")
	fileStoreReceiptIndexBucket = []byte("receipt-records")
	fileStoreSequencesBucket    = []byte("sequences")
	fileStoreFloorsBucket       = []byte("floors")
	fileStoreIdentityKey        = []byte("identity")
	fileStoreSchemaKey          = []byte("schema")
	fileStoreFingerprintKey     = []byte("fingerprint")
	fileStoreLedgerKey          = []byte("ledger")
	fileStoreOutstandingKey     = []byte("outstanding")
	fileStoreIdentity           = []byte("daemonkit.proc.file-store.v1")
	fileStoreFingerprint        = []byte("2114d0e5e58dfd74db3f61a3adea3ae61d7588e63e240e024c99394d9c45f463")
)

// ErrReceiptBacklog means durable unacknowledged recovery liabilities reached
// the configured admission bound.
var ErrReceiptBacklog = errors.New("proc: recovery receipt backlog is full")

// FileStore is a keyed, transactional process-record and retirement-receipt
// ledger. Path is one bbolt database file and must remain stable across daemon
// generations.
type FileStore struct {
	Path string
	// MaxOutstanding bounds records plus unacknowledged receipts. Zero uses
	// defaultMaxOutstanding.
	MaxOutstanding           uint64
	stopControlNow           func() time.Time
	stopControlAfterArmStamp func()
}

func (s *FileStore) maximumOutstanding() uint64 {
	if s.MaxOutstanding == 0 {
		return defaultMaxOutstanding
	}
	return s.MaxOutstanding
}

func (s *FileStore) open(ctx context.Context) (*bolt.DB, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s == nil || !filepath.IsAbs(s.Path) || filepath.Clean(s.Path) != s.Path {
		return nil, fmt.Errorf("proc: file store path %q is not exact and absolute", s.Path)
	}
	directory := filepath.Dir(s.Path)
	if err := mkdirAllDurable(directory, 0o700, fsyncDir); err != nil {
		return nil, fmt.Errorf("proc: create file store directory: %w", err)
	}
	_, statErr := os.Stat(s.Path)
	created := errors.Is(statErr, os.ErrNotExist)
	if statErr != nil && !created {
		return nil, fmt.Errorf("proc: inspect keyed file store: %w", statErr)
	}
	timeout := fileStoreOpenTimeout
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, context.DeadlineExceeded
		}
		if remaining < timeout {
			timeout = remaining
		}
	}
	db, err := bolt.Open(s.Path, 0o600, &bolt.Options{Timeout: timeout})
	if err != nil {
		return nil, fmt.Errorf("proc: open keyed file store: %w", err)
	}
	if err := db.Update(initializeFileStore); err != nil {
		_ = db.Close()
		return nil, err
	}
	fileInfo, err := os.Stat(s.Path)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("proc: inspect opened keyed file store: %w", err)
	}
	if fileInfo.Mode().Perm() != 0o600 {
		_ = db.Close()
		return nil, fmt.Errorf("proc: keyed file store mode is %04o, want 0600", fileInfo.Mode().Perm())
	}
	if created {
		if err := fsyncDir(directory); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("proc: persist keyed file store directory entry: %w", err)
		}
	}
	return db, nil
}

func initializeFileStore(tx *bolt.Tx) error {
	buckets := [][]byte{
		fileStoreMetaBucket, fileStoreRecordsBucket, fileStoreClaimsBucket,
		fileStoreStopConsumedBucket,
		fileStoreReceiptsBucket, fileStoreReceiptIndexBucket,
		fileStoreSequencesBucket, fileStoreFloorsBucket,
	}
	meta := tx.Bucket(fileStoreMetaBucket)
	if meta == nil {
		key, _ := tx.Cursor().First()
		if key != nil {
			return fmt.Errorf("%w: uninitialized keyed store is not empty", ErrRecordSchema)
		}
		for _, name := range buckets {
			if _, err := tx.CreateBucket(name); err != nil {
				return fmt.Errorf("proc: create keyed store bucket %q: %w", name, err)
			}
		}
		meta = tx.Bucket(fileStoreMetaBucket)
		if err := meta.Put(fileStoreIdentityKey, fileStoreIdentity); err != nil {
			return err
		}
		if err := meta.Put(fileStoreSchemaKey, uint64Bytes(recordSchemaVersion)); err != nil {
			return err
		}
		if err := meta.Put(fileStoreFingerprintKey, fileStoreFingerprint); err != nil {
			return err
		}
		var ledger ReceiptLedgerID
		for ledger == (ReceiptLedgerID{}) {
			if _, err := rand.Read(ledger[:]); err != nil {
				return fmt.Errorf("proc: create receipt ledger identity: %w", err)
			}
		}
		if err := meta.Put(fileStoreLedgerKey, ledger[:]); err != nil {
			return err
		}
		return meta.Put(fileStoreOutstandingKey, uint64Bytes(0))
	}
	expected := make(map[string]struct{}, len(buckets))
	for _, name := range buckets {
		expected[string(name)] = struct{}{}
		if tx.Bucket(name) == nil {
			return fmt.Errorf("%w: keyed store bucket %q is missing", ErrRecordSchema, name)
		}
	}
	if err := tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
		if _, ok := expected[string(name)]; !ok {
			return fmt.Errorf("%w: unknown keyed store bucket %q", ErrRecordSchema, name)
		}
		return nil
	}); err != nil {
		return err
	}
	schema := meta.Get(fileStoreSchemaKey)
	if len(schema) != 8 || binary.BigEndian.Uint64(schema) != recordSchemaVersion {
		return fmt.Errorf("%w: keyed store schema is not %d", ErrRecordSchema, recordSchemaVersion)
	}
	expectedMetadata := map[string]struct{}{
		string(fileStoreIdentityKey): {}, string(fileStoreSchemaKey): {},
		string(fileStoreFingerprintKey): {}, string(fileStoreLedgerKey): {},
		string(fileStoreOutstandingKey): {},
	}
	if err := meta.ForEach(func(key, _ []byte) error {
		if _, ok := expectedMetadata[string(key)]; !ok {
			return fmt.Errorf("%w: unknown keyed store metadata %q", ErrRecordSchema, key)
		}
		return nil
	}); err != nil {
		return err
	}
	if !bytes.Equal(meta.Get(fileStoreIdentityKey), fileStoreIdentity) ||
		!bytes.Equal(meta.Get(fileStoreFingerprintKey), fileStoreFingerprint) {
		return fmt.Errorf("%w: keyed store identity or fingerprint mismatch", ErrRecordSchema)
	}
	if len(meta.Get(fileStoreLedgerKey)) != len(ReceiptLedgerID{}) || len(meta.Get(fileStoreOutstandingKey)) != 8 {
		return fmt.Errorf("%w: keyed store metadata is incomplete", ErrRecordSchema)
	}
	return nil
}

func uint64Bytes(value uint64) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	return encoded[:]
}

func classKey(class RecoveryClass) []byte { return []byte{byte(class)} }

func receiptKey(class RecoveryClass, sequence uint64) []byte {
	key := make([]byte, 9)
	key[0] = byte(class)
	binary.BigEndian.PutUint64(key[1:], sequence)
	return key
}

func recordKey(record Record) string {
	key := strconv.Itoa(record.PID) + "\x00" + record.Boot + "\x00" + record.StartTime
	if record.RecoveryClass == RecoveryStopControl {
		key += "\x00" + record.OperationID + "\x00" + string(record.StopSession[:]) +
			"\x00" + string(record.PreparationNonce[:])
	}
	return key
}

func fileStoreLedger(tx *bolt.Tx) (ReceiptLedgerID, error) {
	var ledger ReceiptLedgerID
	value := tx.Bucket(fileStoreMetaBucket).Get(fileStoreLedgerKey)
	if len(value) != len(ledger) {
		return ReceiptLedgerID{}, fmt.Errorf("%w: invalid receipt ledger identity", ErrRecordSchema)
	}
	copy(ledger[:], value)
	return ledger, nil
}

func bucketSequence(bucket *bolt.Bucket, class RecoveryClass) (uint64, error) {
	value := bucket.Get(classKey(class))
	if value == nil {
		return 0, nil
	}
	if len(value) != 8 {
		return 0, ErrRecordSchema
	}
	return binary.BigEndian.Uint64(value), nil
}

func putBucketSequence(bucket *bolt.Bucket, class RecoveryClass, sequence uint64) error {
	return bucket.Put(classKey(class), uint64Bytes(sequence))
}

func updateOutstanding(tx *bolt.Tx, delta int64) error {
	meta := tx.Bucket(fileStoreMetaBucket)
	current := meta.Get(fileStoreOutstandingKey)
	if len(current) != 8 {
		return ErrRecordSchema
	}
	value := binary.BigEndian.Uint64(current)
	if delta < 0 {
		magnitude := uint64(-delta)
		if magnitude > value {
			return fmt.Errorf("%w: outstanding recovery count underflow", ErrRecordSchema)
		}
		value -= magnitude
	} else {
		if uint64(delta) > ^uint64(0)-value {
			return fmt.Errorf("%w: outstanding recovery count overflow", ErrRecordSchema)
		}
		value += uint64(delta)
	}
	return meta.Put(fileStoreOutstandingKey, uint64Bytes(value))
}

func decodeStored[T any](data []byte, value *T) error {
	if data == nil {
		return os.ErrNotExist
	}
	if err := validateStoredFieldSet(data, value); err != nil {
		return fmt.Errorf("%w: stored value field set: %w", ErrRecordSchema, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return fmt.Errorf("%w: decode keyed store value: %w", ErrRecordSchema, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: trailing keyed store value", ErrRecordSchema)
	}
	return nil
}

var recordJSONFields = []string{
	"recovery_class", "pid", "start_time", "boot", "comm", "executable", "audit_token",
	"generation", "process_group", "session_id", "role", "operation_id", "stop_session",
	"preparation_nonce", "runtime_protocol", "target_process_generation", "stop_authority_state", "expires_unix_milli",
}

func validateStoredFieldSet(data []byte, value any) error {
	switch value.(type) {
	case *Record:
		return exactJSONObject(data, recordJSONFields, nil)
	case *reapClaim:
		return exactJSONObject(data, []string{"record", "reaper_generation"}, map[string][]string{
			"record": recordJSONFields,
		})
	case *ReapReceipt:
		return exactJSONObject(data, []string{
			"ledger_id", "sequence", "record", "reaper_generation", "outcome", "digest",
		}, map[string][]string{"record": recordJSONFields})
	default:
		return fmt.Errorf("unsupported durable value type %T", value)
	}
}

func exactJSONObject(data []byte, fields []string, nested map[string][]string) error {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return err
	}
	if len(object) != len(fields) {
		return errors.New("field count mismatch")
	}
	for _, field := range fields {
		raw, ok := object[field]
		if !ok {
			return fmt.Errorf("field %q is missing", field)
		}
		if nestedFields := nested[field]; nestedFields != nil {
			if err := exactJSONObject(raw, nestedFields, nil); err != nil {
				return fmt.Errorf("field %q: %w", field, err)
			}
		}
	}
	return nil
}

func encodeStored(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("proc: encode keyed store value: %w", err)
	}
	return encoded, nil
}

// Add records rec, rejecting new process admission when the bounded recovery
// backlog is full.
func (s *FileStore) Add(ctx context.Context, rec Record) error {
	if err := rec.Validate(); err != nil {
		return err
	}
	maximum := s.maximumOutstanding()
	db, err := s.open(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Update(func(tx *bolt.Tx) error {
		key := []byte(recordKey(rec))
		if tx.Bucket(fileStoreClaimsBucket).Get(key) != nil {
			return errors.New("proc: process instance is claimed for reap")
		}
		if tx.Bucket(fileStoreReceiptIndexBucket).Get(key) != nil {
			return errors.New("proc: process instance already has a retirement receipt")
		}
		records := tx.Bucket(fileStoreRecordsBucket)
		if value := records.Get(key); value != nil {
			var existing Record
			if err := decodeStored(value, &existing); err != nil {
				return err
			}
			if existing != rec {
				return fmt.Errorf("%w: process instance record changed", ErrIdentityChanged)
			}
			return nil
		}
		outstanding := binary.BigEndian.Uint64(tx.Bucket(fileStoreMetaBucket).Get(fileStoreOutstandingKey))
		if outstanding >= maximum {
			return ErrReceiptBacklog
		}
		if err := updateOutstanding(tx, 1); err != nil {
			return err
		}
		encoded, err := encodeStored(rec)
		if err != nil {
			return err
		}
		return records.Put(key, encoded)
	})
}

func stopControlPending(record Record) Record {
	record.StopAuthorityState = StopAuthorityPending
	record.ExpiresUnixMilli = 0
	return record
}

func (s *FileStore) addStopControlPending(ctx context.Context, rec Record) (Record, error) {
	if rec.RecoveryClass != RecoveryStopControl || rec.StopAuthorityState != StopAuthorityPending ||
		rec.ExpiresUnixMilli != 0 {
		return Record{}, errors.New("proc: pending stop control admission is invalid")
	}
	if err := rec.Validate(); err != nil {
		return Record{}, err
	}
	maximum := s.maximumOutstanding()
	db, err := s.open(ctx)
	if err != nil {
		return Record{}, err
	}
	defer db.Close()
	var durable Record
	err = db.Update(func(tx *bolt.Tx) error {
		key := []byte(recordKey(rec))
		if tx.Bucket(fileStoreClaimsBucket).Get(key) != nil {
			return errors.New("proc: process instance is claimed for reap")
		}
		if tx.Bucket(fileStoreReceiptIndexBucket).Get(key) != nil {
			return errors.New("proc: process instance already has a retirement receipt")
		}
		if tx.Bucket(fileStoreStopConsumedBucket).Get(key) != nil {
			return errors.New("proc: stop control authority was already consumed")
		}
		records := tx.Bucket(fileStoreRecordsBucket)
		if value := records.Get(key); value != nil {
			var existing Record
			if err := decodeStored(value, &existing); err != nil {
				return err
			}
			if stopControlPending(existing) != rec {
				return fmt.Errorf("%w: process instance record changed", ErrIdentityChanged)
			}
			durable = existing
			return nil
		}
		outstanding := binary.BigEndian.Uint64(tx.Bucket(fileStoreMetaBucket).Get(fileStoreOutstandingKey))
		if outstanding >= maximum {
			return ErrReceiptBacklog
		}
		encoded, err := encodeStored(rec)
		if err != nil {
			return err
		}
		if err := updateOutstanding(tx, 1); err != nil {
			return err
		}
		if err := records.Put(key, encoded); err != nil {
			return err
		}
		durable = rec
		return nil
	})
	if err != nil {
		return Record{}, err
	}
	return durable, nil
}

func (s *FileStore) armStopControl(ctx context.Context, pending Record, authorityWindow time.Duration) (Record, error) {
	if pending.RecoveryClass != RecoveryStopControl || pending.StopAuthorityState != StopAuthorityPending ||
		pending.ExpiresUnixMilli != 0 || authorityWindow <= 0 {
		return Record{}, errors.New("proc: stop control arm is invalid")
	}
	if err := pending.Validate(); err != nil {
		return Record{}, err
	}
	db, err := s.open(ctx)
	if err != nil {
		return Record{}, err
	}
	defer db.Close()
	var durable Record
	err = db.Update(func(tx *bolt.Tx) error {
		key := []byte(recordKey(pending))
		if tx.Bucket(fileStoreClaimsBucket).Get(key) != nil {
			return errors.New("proc: pending stop control is claimed for reap")
		}
		if tx.Bucket(fileStoreReceiptIndexBucket).Get(key) != nil {
			return errors.New("proc: pending stop control already has a retirement receipt")
		}
		if tx.Bucket(fileStoreStopConsumedBucket).Get(key) != nil {
			return errors.New("proc: pending stop control was already consumed")
		}
		records := tx.Bucket(fileStoreRecordsBucket)
		value := records.Get(key)
		if value == nil {
			return errors.New("proc: pending stop control record is missing")
		}
		var existing Record
		if err := decodeStored(value, &existing); err != nil {
			return err
		}
		if stopControlPending(existing) != pending {
			return fmt.Errorf("%w: process instance record changed", ErrIdentityChanged)
		}
		switch existing.StopAuthorityState {
		case StopAuthorityArmed:
			durable = existing
			return nil
		case StopAuthorityRevoked:
			return errors.New("proc: stop control authority is revoked")
		case StopAuthorityPending:
		default:
			return errors.New("proc: stop control authority state is invalid")
		}
		now := time.Now
		if s.stopControlNow != nil {
			now = s.stopControlNow
		}
		stamp := now()
		// The first window is private commit reserve. The second is the complete
		// release/consume window that the controller must still observe post-commit.
		expires := stamp.Add(authorityWindow).Add(authorityWindow)
		existing.StopAuthorityState = StopAuthorityArmed
		existing.ExpiresUnixMilli = expires.UnixMilli()
		if err := existing.Validate(); err != nil {
			return err
		}
		encoded, err := encodeStored(existing)
		if err != nil {
			return err
		}
		if err := records.Put(key, encoded); err != nil {
			return err
		}
		durable = existing
		if s.stopControlAfterArmStamp != nil {
			s.stopControlAfterArmStamp()
		}
		return nil
	})
	if err != nil {
		return Record{}, err
	}
	return durable, nil
}

func (s *FileStore) revokeStopControl(ctx context.Context, armed Record) (Record, error) {
	if armed.RecoveryClass != RecoveryStopControl || armed.StopAuthorityState != StopAuthorityArmed ||
		armed.ExpiresUnixMilli <= 0 {
		return Record{}, errors.New("proc: stop control revoke is invalid")
	}
	if err := armed.Validate(); err != nil {
		return Record{}, err
	}
	db, err := s.open(ctx)
	if err != nil {
		return Record{}, err
	}
	defer db.Close()
	var durable Record
	err = db.Update(func(tx *bolt.Tx) error {
		key := []byte(recordKey(armed))
		if tx.Bucket(fileStoreClaimsBucket).Get(key) != nil {
			return errors.New("proc: armed stop control is claimed for reap")
		}
		if tx.Bucket(fileStoreReceiptIndexBucket).Get(key) != nil {
			return errors.New("proc: armed stop control already has a retirement receipt")
		}
		if tx.Bucket(fileStoreStopConsumedBucket).Get(key) != nil {
			return errors.New("proc: consumed stop control cannot be revoked")
		}
		records := tx.Bucket(fileStoreRecordsBucket)
		value := records.Get(key)
		if value == nil {
			return errors.New("proc: armed stop control record is missing")
		}
		var existing Record
		if err := decodeStored(value, &existing); err != nil {
			return err
		}
		if existing == armed {
			existing.StopAuthorityState = StopAuthorityRevoked
		} else {
			revoked := armed
			revoked.StopAuthorityState = StopAuthorityRevoked
			if existing != revoked {
				return fmt.Errorf("%w: process instance record changed", ErrIdentityChanged)
			}
		}
		encoded, err := encodeStored(existing)
		if err != nil {
			return err
		}
		if err := records.Put(key, encoded); err != nil {
			return err
		}
		durable = existing
		return nil
	})
	if err != nil {
		return Record{}, err
	}
	return durable, nil
}

// Load returns every durable process record in stable instance-key order.
func (s *FileStore) Load(ctx context.Context) ([]Record, error) {
	db, err := s.open(ctx)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	var records []Record
	err = db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(fileStoreRecordsBucket).ForEach(func(key, value []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			var record Record
			if err := decodeStored(value, &record); err != nil {
				return err
			}
			if err := record.Validate(); err != nil {
				return err
			}
			if !bytes.Equal(key, []byte(recordKey(record))) {
				return fmt.Errorf("%w: process record key does not match value", ErrRecordSchema)
			}
			records = append(records, record)
			return nil
		})
	})
	return records, err
}

// Remove deletes only exact, unclaimed records.
func (s *FileStore) Remove(ctx context.Context, victims []Record) error {
	for _, record := range victims {
		if err := record.Validate(); err != nil {
			return err
		}
	}
	if len(victims) == 0 {
		return nil
	}
	db, err := s.open(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Update(func(tx *bolt.Tx) error {
		records := tx.Bucket(fileStoreRecordsBucket)
		claims := tx.Bucket(fileStoreClaimsBucket)
		for _, victim := range victims {
			if err := ctx.Err(); err != nil {
				return err
			}
			key := []byte(recordKey(victim))
			if claims.Get(key) != nil {
				continue
			}
			value := records.Get(key)
			if value == nil {
				continue
			}
			var existing Record
			if err := decodeStored(value, &existing); err != nil {
				return err
			}
			if existing != victim {
				continue
			}
			if err := records.Delete(key); err != nil {
				return err
			}
			if err := tx.Bucket(fileStoreStopConsumedBucket).Delete(key); err != nil {
				return err
			}
			if err := updateOutstanding(tx, -1); err != nil {
				return err
			}
		}
		return nil
	})
}

// ConsumeStopControl atomically marks and returns one exact unexpired authority.
// The process record remains durable until synchronous untracking or recovery reaping.
func (s *FileStore) ConsumeStopControl(
	ctx context.Context,
	identity Identity,
	role, operationID string,
	stopSession StopSessionID,
	preparationNonce StopPreparationNonce,
	runtimeProtocol int,
	targetProcessGeneration string,
	now time.Time,
) (Record, bool, error) {
	if identity.PID <= 0 || identity.StartTime == "" || identity.Boot == "" ||
		role == "" || operationID == "" || stopSession == (StopSessionID{}) ||
		preparationNonce == (StopPreparationNonce{}) || runtimeProtocol <= 0 || targetProcessGeneration == "" {
		return Record{}, false, errors.New("proc: stop authority lookup is incomplete")
	}
	db, err := s.open(ctx)
	if err != nil {
		return Record{}, false, err
	}
	defer db.Close()
	authority := Record{
		RecoveryClass: RecoveryStopControl,
		PID:           identity.PID, StartTime: identity.StartTime, Boot: identity.Boot,
		OperationID: operationID, StopSession: stopSession, PreparationNonce: preparationNonce,
	}
	var consumed Record
	err = db.Update(func(tx *bolt.Tx) error {
		records := tx.Bucket(fileStoreRecordsBucket)
		consumedRecords := tx.Bucket(fileStoreStopConsumedBucket)
		key := []byte(recordKey(authority))
		if consumedRecords.Get(key) != nil {
			return nil
		}
		value := records.Get(key)
		if value == nil {
			return nil
		}
		var stored Record
		if err := decodeStored(value, &stored); err != nil {
			return err
		}
		if err := stored.Validate(); err != nil {
			return err
		}
		if stored.RecoveryClass != RecoveryStopControl || stored.PID != identity.PID ||
			stored.StartTime != identity.StartTime || stored.Boot != identity.Boot ||
			stored.Comm != identity.Comm || stored.Executable != identity.Executable ||
			stored.AuditToken != identity.AuditToken || stored.Role != role ||
			stored.OperationID != operationID || stored.StopSession != stopSession ||
			stored.PreparationNonce != preparationNonce ||
			stored.RuntimeProtocol != runtimeProtocol ||
			stored.TargetProcessGeneration != targetProcessGeneration ||
			stored.StopAuthorityState != StopAuthorityArmed {
			return nil
		}
		if now.UnixMilli() >= stored.ExpiresUnixMilli {
			return nil
		}
		if err := consumedRecords.Put(key, []byte{1}); err != nil {
			return err
		}
		consumed = stored
		return nil
	})
	return consumed, consumed.RecoveryClass == RecoveryStopControl, err
}

// BeginReap durably fences graceful untracking of rec.
func (s *FileStore) BeginReap(ctx context.Context, rec Record, reaperGeneration string) error {
	claim := reapClaim{Record: rec, ReaperGeneration: reaperGeneration}
	if err := claim.validate(); err != nil {
		return err
	}
	db, err := s.open(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Update(func(tx *bolt.Tx) error {
		key := []byte(recordKey(rec))
		value := tx.Bucket(fileStoreRecordsBucket).Get(key)
		var existing Record
		if err := decodeStored(value, &existing); err != nil {
			return err
		}
		if existing != rec {
			return errors.New("proc: reap claim has no exact durable process record")
		}
		claims := tx.Bucket(fileStoreClaimsBucket)
		if prior := claims.Get(key); prior != nil {
			var existingClaim reapClaim
			if err := decodeStored(prior, &existingClaim); err != nil {
				return err
			}
			if existingClaim != claim {
				return errors.New("proc: process instance has a different reap claim")
			}
			return nil
		}
		encoded, err := encodeStored(claim)
		if err != nil {
			return err
		}
		return claims.Put(key, encoded)
	})
}

// CommitReap replaces one claimed record with the next ordered class receipt.
func (s *FileStore) CommitReap(
	ctx context.Context,
	rec Record,
	reaperGeneration string,
	outcome ReapOutcome,
) (ReapReceipt, error) {
	if err := rec.Validate(); err != nil {
		return ReapReceipt{}, err
	}
	db, err := s.open(ctx)
	if err != nil {
		return ReapReceipt{}, err
	}
	defer db.Close()
	var receipt ReapReceipt
	err = db.Update(func(tx *bolt.Tx) error {
		recordKeyBytes := []byte(recordKey(rec))
		index := tx.Bucket(fileStoreReceiptIndexBucket)
		if existingKey := index.Get(recordKeyBytes); existingKey != nil {
			var existing ReapReceipt
			if err := decodeStored(tx.Bucket(fileStoreReceiptsBucket).Get(existingKey), &existing); err != nil {
				return err
			}
			if err := existing.Validate(); err != nil {
				return err
			}
			if !bytes.Equal(existingKey, receiptKey(existing.Record.RecoveryClass, existing.Sequence)) {
				return fmt.Errorf("%w: receipt index key does not match value", ErrRecordSchema)
			}
			if existing.Record != rec || existing.ReaperGeneration != reaperGeneration || existing.Outcome != outcome {
				return fmt.Errorf("%w: receipt commit changed", ErrInvalidReapReceipt)
			}
			receipt = existing
			return nil
		}
		var record Record
		if err := decodeStored(tx.Bucket(fileStoreRecordsBucket).Get(recordKeyBytes), &record); err != nil {
			return err
		}
		if record != rec {
			return errors.New("proc: reap receipt has no exact durable process record")
		}
		var claim reapClaim
		if err := decodeStored(tx.Bucket(fileStoreClaimsBucket).Get(recordKeyBytes), &claim); err != nil {
			return err
		}
		if claim.Record != rec || claim.ReaperGeneration != reaperGeneration {
			return errors.New("proc: reap receipt has no exact durable claim")
		}
		sequences := tx.Bucket(fileStoreSequencesBucket)
		sequence, err := bucketSequence(sequences, rec.RecoveryClass)
		if err != nil {
			return err
		}
		if sequence == ^uint64(0) {
			return errors.New("proc: reap receipt sequence exhausted")
		}
		sequence++
		ledger, err := fileStoreLedger(tx)
		if err != nil {
			return err
		}
		receipt, err = newReapReceipt(ledger, sequence, rec, reaperGeneration, outcome)
		if err != nil {
			return err
		}
		encoded, err := encodeStored(receipt)
		if err != nil {
			return err
		}
		key := receiptKey(rec.RecoveryClass, sequence)
		if err := tx.Bucket(fileStoreReceiptsBucket).Put(key, encoded); err != nil {
			return err
		}
		if err := index.Put(recordKeyBytes, key); err != nil {
			return err
		}
		if err := putBucketSequence(sequences, rec.RecoveryClass, sequence); err != nil {
			return err
		}
		if err := tx.Bucket(fileStoreClaimsBucket).Delete(recordKeyBytes); err != nil {
			return err
		}
		if err := tx.Bucket(fileStoreStopConsumedBucket).Delete(recordKeyBytes); err != nil {
			return err
		}
		return tx.Bucket(fileStoreRecordsBucket).Delete(recordKeyBytes)
	})
	return receipt, err
}

// LoadReapReceipts returns an oldest-first class page without scanning another
// class or rewriting prior receipts.
func (s *FileStore) LoadReapReceipts(
	ctx context.Context,
	class RecoveryClass,
	after ReapReceiptCursor,
	limit int,
) (ReapReceiptPage, error) {
	if err := class.Validate(); err != nil {
		return ReapReceiptPage{}, err
	}
	if limit <= 0 || limit > ReapReceiptPageLimit {
		return ReapReceiptPage{}, fmt.Errorf("proc: reap receipt limit %d is out of bounds", limit)
	}
	db, err := s.open(ctx)
	if err != nil {
		return ReapReceiptPage{}, err
	}
	defer db.Close()
	var page ReapReceiptPage
	err = db.View(func(tx *bolt.Tx) error {
		ledger, err := fileStoreLedger(tx)
		if err != nil {
			return err
		}
		if after.LedgerID != (ReceiptLedgerID{}) && after.LedgerID != ledger {
			return fmt.Errorf("%w: receipt cursor ledger changed", ErrReapReceiptStale)
		}
		if after.Sequence != 0 && after.LedgerID == (ReceiptLedgerID{}) {
			return fmt.Errorf("%w: receipt cursor sequence has no ledger", ErrReapReceiptStale)
		}
		floorSequence, err := bucketSequence(tx.Bucket(fileStoreFloorsBucket), class)
		if err != nil {
			return err
		}
		page.Floor = ReapReceiptFloor{LedgerID: ledger, RecoveryClass: class, Sequence: floorSequence}
		if after.Sequence == ^uint64(0) {
			return fmt.Errorf("%w: receipt cursor sequence exhausted", ErrReapReceiptStale)
		}
		if floorSequence == ^uint64(0) {
			page.Next = ReapReceiptCursor{LedgerID: ledger, Sequence: floorSequence}
			return nil
		}
		start := after.Sequence + 1
		if start <= floorSequence {
			start = floorSequence + 1
		}
		cursor := tx.Bucket(fileStoreReceiptsBucket).Cursor()
		key, value := cursor.Seek(receiptKey(class, start))
		for len(key) == 9 && key[0] == byte(class) && len(page.Receipts) <= limit {
			if err := ctx.Err(); err != nil {
				return err
			}
			var receipt ReapReceipt
			if err := decodeStored(value, &receipt); err != nil {
				return err
			}
			if err := receipt.Validate(); err != nil {
				return err
			}
			if receipt.LedgerID != ledger || receipt.Record.RecoveryClass != class ||
				receipt.Sequence != binary.BigEndian.Uint64(key[1:]) {
				return fmt.Errorf("%w: receipt key does not match value", ErrRecordSchema)
			}
			page.Receipts = append(page.Receipts, receipt)
			key, value = cursor.Next()
		}
		if len(page.Receipts) > limit {
			page.Receipts = page.Receipts[:limit]
			page.More = true
		}
		if len(page.Receipts) != 0 {
			last := page.Receipts[len(page.Receipts)-1]
			page.Next = ReapReceiptCursor{LedgerID: last.LedgerID, Sequence: last.Sequence}
		} else {
			page.Next = ReapReceiptCursor{LedgerID: ledger, Sequence: after.Sequence}
		}
		return nil
	})
	return page, err
}

// HasReapReceipt reports whether the exact unacknowledged receipt is present.
func (s *FileStore) HasReapReceipt(ctx context.Context, receipt ReapReceipt) (bool, error) {
	if err := receipt.Validate(); err != nil {
		return false, err
	}
	db, err := s.open(ctx)
	if err != nil {
		return false, err
	}
	defer db.Close()
	var found bool
	err = db.View(func(tx *bolt.Tx) error {
		ledger, err := fileStoreLedger(tx)
		if err != nil {
			return err
		}
		if ledger != receipt.LedgerID {
			return nil
		}
		floor, err := bucketSequence(tx.Bucket(fileStoreFloorsBucket), receipt.Record.RecoveryClass)
		if err != nil {
			return err
		}
		if receipt.Sequence <= floor {
			return ErrReapReceiptStale
		}
		var existing ReapReceipt
		if err := decodeStored(
			tx.Bucket(fileStoreReceiptsBucket).Get(receiptKey(receipt.Record.RecoveryClass, receipt.Sequence)),
			&existing,
		); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if err := existing.Validate(); err != nil {
			return err
		}
		if existing.LedgerID != ledger || existing.Sequence != receipt.Sequence ||
			existing.Record.RecoveryClass != receipt.Record.RecoveryClass {
			return fmt.Errorf("%w: receipt key does not match value", ErrRecordSchema)
		}
		found = existing == receipt
		return nil
	})
	return found, err
}

// FindReapReceipt returns the exact receipt indexed by one process record.
func (s *FileStore) FindReapReceipt(ctx context.Context, record Record) (ReapReceipt, bool, error) {
	if err := record.Validate(); err != nil {
		return ReapReceipt{}, false, err
	}
	db, err := s.open(ctx)
	if err != nil {
		return ReapReceipt{}, false, err
	}
	defer db.Close()
	var receipt ReapReceipt
	var found bool
	err = db.View(func(tx *bolt.Tx) error {
		key := tx.Bucket(fileStoreReceiptIndexBucket).Get([]byte(recordKey(record)))
		if key == nil {
			return nil
		}
		if err := decodeStored(tx.Bucket(fileStoreReceiptsBucket).Get(key), &receipt); err != nil {
			return err
		}
		if err := receipt.Validate(); err != nil {
			return err
		}
		if !bytes.Equal(key, receiptKey(receipt.Record.RecoveryClass, receipt.Sequence)) {
			return fmt.Errorf("%w: receipt index key does not match value", ErrRecordSchema)
		}
		if receipt.Record != record {
			return fmt.Errorf("%w: receipt index does not match record", ErrRecordSchema)
		}
		found = true
		return nil
	})
	return receipt, found, err
}

// AcknowledgeReap deletes only the exact next class receipt and advances its
// durable contiguous floor.
func (s *FileStore) AcknowledgeReap(
	ctx context.Context,
	receipt ReapReceipt,
) (ReapReceiptFloor, error) {
	if err := receipt.Validate(); err != nil {
		return ReapReceiptFloor{}, err
	}
	db, err := s.open(ctx)
	if err != nil {
		return ReapReceiptFloor{}, err
	}
	defer db.Close()
	class := receipt.Record.RecoveryClass
	var result ReapReceiptFloor
	err = db.Update(func(tx *bolt.Tx) error {
		ledger, err := fileStoreLedger(tx)
		if err != nil {
			return err
		}
		if receipt.LedgerID != ledger {
			return ErrReapReceiptStale
		}
		floors := tx.Bucket(fileStoreFloorsBucket)
		floor, err := bucketSequence(floors, class)
		if err != nil {
			return err
		}
		result = ReapReceiptFloor{LedgerID: ledger, RecoveryClass: class, Sequence: floor}
		switch {
		case receipt.Sequence < floor:
			return ErrReapReceiptStale
		case receipt.Sequence == floor:
			return nil
		case receipt.Sequence != floor+1:
			return ErrReapReceiptOrder
		}
		key := receiptKey(class, receipt.Sequence)
		var existing ReapReceipt
		if err := decodeStored(tx.Bucket(fileStoreReceiptsBucket).Get(key), &existing); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return ErrUnrecognizedReapReceipt
			}
			return err
		}
		if existing != receipt {
			return ErrUnrecognizedReapReceipt
		}
		if err := tx.Bucket(fileStoreReceiptsBucket).Delete(key); err != nil {
			return err
		}
		if err := tx.Bucket(fileStoreReceiptIndexBucket).Delete([]byte(recordKey(receipt.Record))); err != nil {
			return err
		}
		if err := putBucketSequence(floors, class, receipt.Sequence); err != nil {
			return err
		}
		if err := updateOutstanding(tx, -1); err != nil {
			return err
		}
		result.Sequence = receipt.Sequence
		return nil
	})
	return result, err
}

// ReapReceiptFloor returns the retained contiguous class floor.
func (s *FileStore) ReapReceiptFloor(ctx context.Context, class RecoveryClass) (ReapReceiptFloor, error) {
	if err := class.Validate(); err != nil {
		return ReapReceiptFloor{}, err
	}
	db, err := s.open(ctx)
	if err != nil {
		return ReapReceiptFloor{}, err
	}
	defer db.Close()
	var floor ReapReceiptFloor
	err = db.View(func(tx *bolt.Tx) error {
		ledger, err := fileStoreLedger(tx)
		if err != nil {
			return err
		}
		sequence, err := bucketSequence(tx.Bucket(fileStoreFloorsBucket), class)
		if err != nil {
			return err
		}
		floor = ReapReceiptFloor{LedgerID: ledger, RecoveryClass: class, Sequence: sequence}
		return nil
	})
	return floor, err
}

var (
	_ Store            = (*FileStore)(nil)
	_ StopControlStore = (*FileStore)(nil)
)
