package ccxt_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	ccxt "github.com/ccxt/ccxt/go/v4"
)

func TestRestHookPhases(t *testing.T) {
	body := `{"serverTime":1700000000000,"rows":[{"p":"1.5","q":"2"}]}`
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		fmt.Fprint(writer, body)
	}))
	defer server.Close()

	ex := ccxt.NewBinance(nil)
	ex.SetValidateServerSsl(false)

	events := make(chan ccxt.RestEvent, 4)
	ex.SetRestHook(func(event ccxt.RestEvent) {
		events <- event
	})

	if result := <-ex.FetchAsync(server.URL+"/api/v3/time", "GET", nil, nil); result.Err != nil {
		t.Fatalf("fetch: %v", result.Err)
	}

	var collected []ccxt.RestEvent
	for len(collected) < 4 {
		select {
		case event := <-events:
			collected = append(collected, event)
		case <-time.After(3 * time.Second):
			t.Fatalf("hook fired %d phases, want 4", len(collected))
		}
	}
	request, headers, bodyPhase, decoded := collected[0], collected[1], collected[2], collected[3]
	if request.Phase != ccxt.RestRequest || headers.Phase != ccxt.RestHeaders || bodyPhase.Phase != ccxt.RestBody || decoded.Phase != ccxt.RestDecoded {
		t.Fatalf("phase order is %v, %v, %v, %v", request.Phase, headers.Phase, bodyPhase.Phase, decoded.Phase)
	}
	for _, event := range collected {
		if event.ID != request.ID {
			t.Fatalf("one request must keep one id, got %d and %d", request.ID, event.ID)
		}
		if event.URL != server.URL+"/api/v3/time" || event.Method != "GET" {
			t.Fatalf("the request identity must travel with every phase, got %s %s", event.Method, event.URL)
		}
	}
	if request.RequestAt.IsZero() || request.At != request.RequestAt {
		t.Fatalf("request phase must carry its own instant: %+v", request)
	}
	if headers.Status != http.StatusOK || headers.HeadersAt.Before(request.RequestAt) {
		t.Fatalf("headers phase: %+v", headers)
	}
	if bodyPhase.Bytes != len(body) || bodyPhase.BodyAt.Before(headers.HeadersAt) {
		t.Fatalf("body phase: bytes=%d at=%v", bodyPhase.Bytes, bodyPhase.BodyAt)
	}
	if decoded.DecodedAt.Before(bodyPhase.BodyAt) || decoded.Status != http.StatusOK || decoded.Bytes != len(body) {
		t.Fatalf("decoded phase must close the body span and carry status/bytes: %+v", decoded)
	}
	if got := decoded.Timing(); got.Url != decoded.URL || got.DecodedAt != decoded.DecodedAt {
		t.Fatalf("Timing() must project the event, got %+v", got)
	}
	if decoded.Duration() <= 0 {
		t.Fatalf("duration must cover request to decoded, got %v", decoded.Duration())
	}
	fmt.Printf("rest %s %s status=%d bytes=%d\n  wire(request->body)=%v decode=%v total=%v\n",
		decoded.Method, decoded.URL, decoded.Status, decoded.Bytes,
		decoded.BodyAt.Sub(decoded.RequestAt), decoded.DecodedAt.Sub(decoded.BodyAt), decoded.Duration())
}

func TestRestHookNilIsNoop(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		fmt.Fprint(writer, `{"serverTime":1700000000000}`)
	}))
	defer server.Close()

	ex := ccxt.NewBinance(nil)
	ex.SetValidateServerSsl(false)
	events := make(chan ccxt.RestEvent, 4)
	ex.SetRestHook(func(event ccxt.RestEvent) { events <- event })
	// removing the hook before the request must leave the round trip silent
	ex.SetRestHook(nil)
	if result := <-ex.FetchAsync(server.URL+"/api/v3/time", "GET", nil, nil); result.Err != nil {
		t.Fatalf("fetch with a nil hook: %v", result.Err)
	}
	select {
	case event := <-events:
		t.Fatalf("a nil hook must not fire, got %+v", event)
	case <-time.After(50 * time.Millisecond):
	}
}
