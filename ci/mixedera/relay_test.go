//go:build mixedera

package mixedera

import (
	"fmt"
	"io"
	"net"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"
)

const (
	leadCapture = 16
	relaySettle = 500 * time.Millisecond
	quiesceWait = 30 * time.Second
)

type exchange struct {
	opened   []byte
	answered []byte
}

type relay struct {
	path     string
	upstream string
	mu       sync.Mutex
	accepted []*exchange
	live     int
}

func newRelay(t *testing.T, upstream string) *relay {
	t.Helper()
	r := &relay{path: filepath.Join(socketDir(t), "relay.sock"), upstream: upstream}
	listener, err := net.Listen("unix", r.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go r.pipe(conn, r.open())
		}
	}()
	return r
}

func (r *relay) open() *exchange {
	r.mu.Lock()
	defer r.mu.Unlock()
	crossing := &exchange{}
	r.accepted = append(r.accepted, crossing)
	r.live++
	return crossing
}

func (r *relay) closed() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.live--
}

func (r *relay) pipe(down net.Conn, crossing *exchange) {
	defer r.closed()
	up, err := net.Dial("unix", r.upstream)
	if err != nil {
		_ = down.Close()
		return
	}
	var once sync.Once
	shut := func() {
		once.Do(func() {
			_ = down.Close()
			_ = up.Close()
		})
	}
	defer shut()
	var answering sync.WaitGroup
	answering.Add(1)
	go func() {
		defer answering.Done()
		defer shut()
		_, _ = io.Copy(io.MultiWriter(down, tap{r, &crossing.answered}), up)
	}()
	_, _ = io.Copy(io.MultiWriter(up, tap{r, &crossing.opened}), down)
	shut()
	answering.Wait()
}

type tap struct {
	relay *relay
	into  *[]byte
}

func (t tap) Write(payload []byte) (int, error) {
	t.relay.mu.Lock()
	defer t.relay.mu.Unlock()
	if room := leadCapture - len(*t.into); room > 0 {
		*t.into = append(*t.into, payload[:min(room, len(payload))]...)
	}
	return len(payload), nil
}

func (r *relay) sample() (crossings []exchange, live int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, crossing := range r.accepted {
		crossings = append(crossings, exchange{
			opened: slices.Clone(crossing.opened), answered: slices.Clone(crossing.answered),
		})
	}
	return crossings, r.live
}

// witness attributes every frame prefix the relay copied to the era whose
// frozen identity it carries, so an era's frame layout is redeemable against
// bytes that crossed a real connection rather than against a peer's word.
func (r *relay) witness(t *testing.T, crossings []exchange) {
	t.Helper()
	for _, era := range []string{cutEra, precutEra} {
		prefix := frozen(t, frameFixture(era))
		for i, crossing := range crossings {
			sides := []struct {
				name     string
				observed []byte
			}{{"opened", crossing.opened}, {"answered", crossing.answered}}
			for _, side := range sides {
				if !carriesFramePrefix(side.observed, prefix) {
					continue
				}
				observedPresent(t, era, mechanismFrame, fromWire, fmt.Sprintf(
					"the relay at %s copied the frozen %s frame prefix %#x on the %s side of connection %d of %d",
					r.path, era, prefix, side.name, i+1, len(crossings),
				))
			}
		}
	}
}

func (r *relay) quiesce(t *testing.T) []exchange {
	t.Helper()
	deadline := time.Now().Add(quiesceWait)
	for {
		settled, live := r.sample()
		if len(settled) > 0 && live == 0 {
			time.Sleep(relaySettle)
			if again, live := r.sample(); live == 0 && len(again) == len(settled) {
				r.witness(t, again)
				return again
			}
			continue
		}
		if time.Now().After(deadline) {
			t.Fatalf("the relay at %s saw %d connections with %d still in flight after %s",
				r.path, len(settled), live, quiesceWait)
		}
		time.Sleep(relaySettle)
	}
}
