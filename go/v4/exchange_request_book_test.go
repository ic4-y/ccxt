package ccxt_test

import (
	"sync"
	"sync/atomic"
	"testing"

	ccxt "github.com/ccxt/ccxt/go/v4"
)

// TestNextRequestIdConcurrent exercises the request-id counter from many
// goroutines. Kucoin's SubscribeMultipleAsync calls RequestId concurrently, and
// the unguarded map crashed the process with "concurrent map writes". Run with
// -race: the mutex must make the counter safe and each returned id unique.
func TestNextRequestIdConcurrent(t *testing.T) {
	exchange := &ccxt.BaseExchange{Options: map[string]any{}}

	const goroutines = 32
	const perGoroutine = 500
	total := goroutines * perGoroutine

	ids := make([]int64, total)
	var next atomic.Int64
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				ids[next.Add(1)-1] = exchange.NextRequestId("test")
			}
		}()
	}
	wg.Wait()

	seen := make(map[int64]struct{}, total)
	for _, id := range ids {
		if id <= 0 {
			t.Fatalf("non-positive request id %d", id)
		}
		if _, ok := seen[id]; ok {
			t.Fatalf("duplicate request id %d", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != total {
		t.Fatalf("got %d unique ids, want %d", len(seen), total)
	}
}
