package supervise

import "sync"

// KeyedMutex hands out one lock per key, so every caller naming the same subject
// serializes on the same mutex. A Supervisor and a consumer's manual-kill path
// share one KeyedMutex so a subject's kill and respawn never interleave — it is
// the natural backing for a Subject.Lock implementation. The zero value is ready
// to use; safe for concurrent use.
type KeyedMutex struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

// Get returns the lock for key, creating it on first use. The same key always
// returns the same *sync.Mutex, so both the supervisor and the consumer's
// manual-kill path address one lock per subject identity.
func (k *KeyedMutex) Get(key string) sync.Locker {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.locks == nil {
		k.locks = make(map[string]*sync.Mutex)
	}
	m := k.locks[key]
	if m == nil {
		m = &sync.Mutex{}
		k.locks[key] = m
	}
	return m
}
