package ccxt

import (
	"sync"
	"testing"
)

const (
	requestBookWriters    = 8
	requestBookReaders    = 8
	requestBookIterations = 100
)

func TestRequestBookConcurrent(t *testing.T) {
	ex := &BaseExchange{}
	runRequestBookConcurrency(ex)
}

func TestRequestBookConcurrentZeroValue(t *testing.T) {
	var ex BaseExchange
	runRequestBookConcurrency(&ex)

	ex.handOffRequestBook(requestBookOp{kind: requestBookRequest, request: map[string]any{
		"headers": map[string]any{"x-final": "1"},
		"body":    "final",
		"url":     "https://example.invalid/final",
	}})
	if got := ex.readRequestBook().requestUrl; got != "https://example.invalid/final" {
		t.Fatalf("request book lost the final request: %v", got)
	}
}

func runRequestBookConcurrency(ex *BaseExchange) {
	start := make(chan struct{})
	var wg sync.WaitGroup

	for w := 0; w < requestBookWriters; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			for i := 0; i < requestBookIterations; i++ {
				switch (w + i) % 4 {
				case 0:
					ex.handOffRequestBook(requestBookOp{kind: requestBookStamp, stamp: int64(i)})
				case 1:
					ex.handOffRequestBook(requestBookOp{
						kind: requestBookRequest,
						request: map[string]any{
							"headers": map[string]any{"x-writer": w},
							"body":    "body",
							"url":     "https://example.invalid/request",
						},
					})
				case 2:
					ex.handOffRequestBook(requestBookOp{
						kind:    requestBookResponse,
						headers: map[string]any{"x-response": w},
					})
				case 3:
					ex.handOffRequestBook(requestBookOp{kind: requestBookReset})
				}
			}
		}(w)
	}

	for r := 0; r < requestBookReaders; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < requestBookIterations; i++ {
				ex.readRequestBook()
			}
		}()
	}

	close(start)
	wg.Wait()
}
