package ccxt_test

// Experiment: a per-frame phase hook that gives both an OTel adapter and plain
// latency histograms a place to attach: received (raw bytes in), decoded (venue
// handler input ready) and dispatched (venue handler returned). Driven against a
// real pro venue and a local TLS websocket server.

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	ccxt "github.com/ccxt/ccxt/go/v4"
	ccxtpro "github.com/ccxt/ccxt/go/v4/pro"
	"github.com/gorilla/websocket"
)

func hookTradeFrame(id int64, padding int) []byte {
	pad := ""
	if padding > 0 {
		pad = `,"pad":"` + strings.Repeat("x", padding) + `"`
	}
	return []byte(fmt.Sprintf(`{"e":"trade","E":%d,"s":"BTCUSDT","t":%d,"p":"100.50","q":"0.500","T":%d,"m":false%s}`,
		time.Now().UnixMilli(), id, time.Now().UnixMilli(), pad))
}

func newHookBinance(t *testing.T, wsBase string) *ccxtpro.Binance {
	t.Helper()
	ex := ccxtpro.NewBinance(map[string]any{"options": map[string]any{}})
	ex.SetValidateServerSsl(false)
	wsUrls := ex.Urls["api"].(map[string]any)["ws"].(map[string]any)
	wsUrls["spot"] = wsBase
	ex.SetMarkets(map[string]any{
		"BTC/USDT": map[string]any{
			"id": "BTCUSDT", "lowercaseId": "btcusdt", "symbol": "BTC/USDT",
			"base": "BTC", "quote": "USDT", "baseId": "BTC", "quoteId": "USDT",
			"type": "spot", "spot": true, "active": true,
			"precision": map[string]any{"price": 0.01, "amount": 0.00001},
			"limits":    map[string]any{},
		},
	})
	return ex
}

// hookServer upgrades one connection, consumes the SUBSCRIBE payload and writes the
// given payloads with a pause before each one. It reports the instant each frame was
// handed to the socket, so a test can compare the hook's received timestamp with a
// known send instant.
func hookServer(t *testing.T, payloads [][]byte) (string, <-chan time.Time) {
	t.Helper()
	upgrader := websocket.Upgrader{}
	sentAt := make(chan time.Time, len(payloads))
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		// the first client write is the SUBSCRIBE payload
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		for index, payload := range payloads {
			if index == 0 {
				time.Sleep(5 * time.Millisecond)
			} else {
				// a synthetic slow frame: the client must timestamp this one when it
				// arrives, not when the previous frame or the subscribe happened
				time.Sleep(40 * time.Millisecond)
			}
			sentAt <- time.Now()
			if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}))
	t.Cleanup(server.Close)
	wsBase := "wss" + strings.TrimPrefix(server.URL, "https") + "/stream"
	return wsBase, sentAt
}

func TestFrameHookPhases(t *testing.T) {
	payloads := [][]byte{
		hookTradeFrame(1, 0),
		hookTradeFrame(2, 0),
		// an expensive frame makes the decode span visible in the printed evidence
		hookTradeFrame(3, 40000),
	}
	wsBase, sentAt := hookServer(t, payloads)
	ex := newHookBinance(t, wsBase)

	events := make(chan ccxt.WsFrameEvent, 16)
	ex.SetWsFrameHook(func(event ccxt.WsFrameEvent) {
		events <- event
	})

	type result struct {
		at     time.Time
		trades []ccxt.Trade
		err    error
	}
	returned := make(chan result, 3)
	go func() {
		for i := 0; i < len(payloads); i++ {
			trades, err := ex.WatchTrades("BTC/USDT")
			returned <- result{at: time.Now(), trades: trades, err: err}
			if err != nil {
				return
			}
		}
	}()

	var results []result
	var collected []ccxt.WsFrameEvent
	deadline := time.After(5 * time.Second)
	for len(results) < len(payloads) || len(collected) < 3*len(payloads) {
		select {
		case r := <-returned:
			if r.err != nil {
				t.Fatalf("watch error: %v", r.err)
			}
			if len(r.trades) < 1 {
				t.Fatalf("want at least one trade, got %d", len(r.trades))
			}
			results = append(results, r)
		case event := <-events:
			collected = append(collected, event)
		case <-deadline:
			t.Fatalf("timeout: %d results, %d events", len(results), len(collected))
		}
	}

	byID := map[uint64][]ccxt.WsFrameEvent{}
	for _, event := range collected {
		byID[event.ID] = append(byID[event.ID], event)
	}
	if len(byID) != len(payloads) {
		t.Fatalf("each frame must get one id, got %d ids for %d frames", len(byID), len(payloads))
	}
	sents := make([]time.Time, len(payloads))
	for i := range sents {
		sents[i] = <-sentAt
	}
	// frames are read in order, so ids are 1..N on this single connection
	for id := uint64(1); id <= uint64(len(payloads)); id++ {
		phases := byID[id]
		if len(phases) != 3 {
			t.Fatalf("frame %d: want received/decoded/dispatched, got %d events", id, len(phases))
		}
		received, decoded, dispatched := phases[0], phases[1], phases[2]
		if received.Phase != ccxt.WsFrameReceived || decoded.Phase != ccxt.WsFrameDecoded || dispatched.Phase != ccxt.WsFrameDispatched {
			t.Fatalf("frame %d: phase order is %v, %v, %v", id, received.Phase, decoded.Phase, dispatched.Phase)
		}
		if received.Received.IsZero() || received.At != received.Received {
			t.Fatalf("frame %d: received phase must carry its own instant, got %+v", id, received)
		}
		if decoded.At != decoded.Decoded || decoded.Decoded.Before(received.Received) {
			t.Fatalf("frame %d: decoded phase out of order, got %+v", id, decoded)
		}
		if dispatched.At != dispatched.Dispatched || dispatched.Dispatched.Before(decoded.Decoded) {
			t.Fatalf("frame %d: dispatched phase out of order, got %+v", id, dispatched)
		}
		for _, event := range phases {
			if event.Client == nil {
				t.Fatalf("frame %d: the event must name its client", id)
			}
			if !strings.HasPrefix(event.URL, "wss://") || !strings.Contains(event.URL, "/stream") {
				t.Fatalf("frame %d: unexpected client url %q", id, event.URL)
			}
			if event.Bytes <= 0 {
				t.Fatalf("frame %d: the received bytes must travel with every phase", id)
			}
		}
		if decoded.Kind != "json-object" || dispatched.Kind != "json-object" {
			t.Fatalf("frame %d: decoded kind %q/%q", id, decoded.Kind, dispatched.Kind)
		}
		if got := received.Timing(); got.Received != received.Received || got.Bytes != received.Bytes {
			t.Fatalf("frame %d: Timing() must project the event, got %+v", id, got)
		}
		if received.Duration() < 0 || dispatched.Duration() < 0 {
			t.Fatalf("frame %d: duration must be non-negative", id)
		}
		// the synthetic slow frame: receipt is stamped when the bytes arrive, after
		// the server handed them to the socket, not when the client called watch
		sent := sents[id-1]
		if received.Received.Before(sent) {
			t.Fatalf("frame %d: received %v precedes the server send %v", id, received.Received, sent)
		}
		if lag := received.Received.Sub(sent); lag > 150*time.Millisecond {
			t.Fatalf("frame %d: received lag %v is implausible for a local socket", id, lag)
		}
		// the caller wakes after the dispatch phase of its frame
		if len(results) >= int(id) && results[id-1].at.Before(dispatched.Dispatched) {
			t.Fatalf("frame %d: caller woke at %v, before dispatch at %v", id, results[id-1].at, dispatched.Dispatched)
		}
		fmt.Printf("frame %d %-12s bytes=%6d decode=%9v dispatch=%9v total=%9v caller-wakeup=%9v\n",
			id, dispatched.Kind, dispatched.Bytes,
			decoded.Decoded.Sub(received.Received),
			dispatched.Dispatched.Sub(decoded.Decoded),
			dispatched.Duration(),
			results[id-1].at.Sub(dispatched.Dispatched))
	}
}

func TestFrameHookNilIsNoop(t *testing.T) {
	wsBase, _ := hookServer(t, [][]byte{hookTradeFrame(1, 0)})
	ex := newHookBinance(t, wsBase)
	// no hook at all: the phases stay off and watch still delivers
	ex.SetWsFrameHook(nil)
	trades, err := ex.WatchTrades("BTC/USDT")
	if err != nil {
		t.Fatalf("watch with a nil hook: %v", err)
	}
	if len(trades) < 1 {
		t.Fatalf("want at least one trade, got %d", len(trades))
	}
}
