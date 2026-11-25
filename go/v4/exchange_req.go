package ccxt

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

// mockBodyForUrl picks the response-test body whose url fragment the request url
// contains. Map iteration is unordered, so the no-match fallback sorts the
// fragments and takes the first for a deterministic result.
func mockBodyForUrl(responsesByUrl any, url any) any {
	byUrl, ok := responsesByUrl.(map[string]any)
	if !ok {
		return responsesByUrl
	}
	urlStr, _ := url.(string)
	fragments := make([]string, 0, len(byUrl))
	for fragment := range byUrl {
		fragments = append(fragments, fragment)
	}
	sort.Strings(fragments)
	for _, fragment := range fragments {
		if strings.Contains(urlStr, fragment) {
			return byUrl[fragment]
		}
	}
	// Go urlencodes a map[string]any in random key order, while the fixtures spell
	// fragments in TS insertion order: match a query fragment as a set of k=v pairs
	queryPairs := map[string]bool{}
	if q := strings.IndexByte(urlStr, '?'); q >= 0 {
		for _, pair := range strings.Split(urlStr[q+1:], "&") {
			queryPairs[pair] = true
		}
	}
	for _, fragment := range fragments {
		pairs := strings.Split(fragment, "&")
		if len(pairs) < 2 {
			continue
		}
		matched := true
		for _, pair := range pairs {
			if !queryPairs[pair] {
				matched = false
				break
			}
		}
		if matched {
			return byUrl[fragment]
		}
	}
	if len(fragments) > 0 {
		return byUrl[fragments[0]]
	}
	return nil
}

func (this *BaseExchange) FetchAsync(url any, method any, headers any, body any) chan AsyncResult[any] {
	ch := make(chan AsyncResult[any])
	go func() {
		defer close(ch)
		defer ReturnPanicError(ch)

		if this.FetchResponseByUrl != nil {
			ch <- AsyncResult[any]{Value: mockBodyForUrl(this.FetchResponseByUrl, url)}
			return
		}

		if this.FetchResponse != nil {
			ch <- AsyncResult[any]{Value: this.FetchResponse}
			return
		}
		this.UpdateProxySettings() // for now this needs to be here
		// sign() may return a typed nil pointer (*string) as body; unwrap it, otherwise `body != nil` is true and a literal "null" body is sent (rejected with 403 by cloudfront on GET)
		body = derefScalar(body)

		// Convert url to string: a venue sign() may build the url as a *string local
		urlStr, ok := derefScalar(url).(string)
		if !ok {
			panic("url must be a string")
		}

		// Convert method to string
		methodStr, ok := derefScalar(method).(string)
		if !ok {
			panic("method must be a string")
		}

		// Convert headers to map[string]string
		// unsigned requests carry no headers (sign returns undefined)
		headersStrMap := make(map[string]string)
		switch h := headers.(type) {
		case nil:
		case map[string]any:
			for k, v := range h {
				headersStrMap[k] = fmt.Sprintf("%v", v)
			}
		case map[string]string:
			for k, v := range h {
				headersStrMap[k] = v
			}
		default:
			panic("headers must be a map[string]any")
		}

		headersOptions, ok := this.Options["headers"]
		if ok {
			if headersOptions != nil {
				for key, value := range headersOptions.(map[string]any) {
					if _, exists := headersStrMap[key]; !exists {
						headersStrMap[key] = fmt.Sprintf("%v", value)
					}
				}
			} else {
				panic("headersOptions should be a map[string]any")
			}

		}

		// sign() may hand the body over as a *string (a typed nil when there is no body):
		// a nil pointer boxed in an interface is not nil, so it would be marshalled as
		// the JSON literal null, and a non-nil pointer would be encoded a second time
		if bodyPtr, ok := body.(*string); ok {
			if bodyPtr == nil {
				body = nil
			} else {
				body = *bodyPtr
			}
		}

		// check content type for multipart/form-data, used in lighter
		for k, v := range headersStrMap {
			lk := strings.ToLower(k)
			if lk == "content-type" {
				if v == "multipart/form-data" {
					var formBody bytes.Buffer
					writer := multipart.NewWriter(&formBody)

					for fk, fv := range body.(map[string]any) {
						writer.WriteField(fk, fmt.Sprintf("%v", fv))
					}

					headersStrMap[k] = writer.FormDataContentType()
					writer.Close()
					body = formBody.String()
				}
				break
			}
			headersStrMap[k] = fmt.Sprintf("%v", v)
		}

		// Marshal the body to JSON if not nil
		// var requestBody []byte
		// var err error
		// if body != nil {
		// 	requestBody, err = json.Marshal(body)
		// 	if err != nil {
		// 		panic(fmt.Sprintf("failed to marshal body: %v", err))
		// 	}
		// }

		var req *http.Request
		var err error

		if body != nil {
			switch v := body.(type) {
			case string:
				// If the body is a string, use URL encoding
				// data := urlLib.Values{}
				// parsedData, err := urlLib.ParseQuery(v)
				// if err != nil {
				// 	panic(fmt.Sprintf("error parsing query string: %w", v))
				// }
				// for key, values := range parsedData {
				// 	for _, value := range values {
				// 		data.Set(key, value)
				// 	}
				// }
				req, err = http.NewRequest(methodStr, urlStr, strings.NewReader(v))
				if err != nil {
					panic("error creating request")
				}
			default:
				requestBody, err := json.Marshal(body)
				if err != nil {
					panic("error marshalling JSON")
				}
				req, err = http.NewRequest(methodStr, urlStr, bytes.NewBuffer(requestBody))
				if err != nil {
					panic("error creating request")
				}
			}
		} else {
			req, err = http.NewRequest(methodStr, urlStr, nil)
			if err != nil {
				panic("error creating request")
			}
		}
		// Create the HTTP request
		// req, err := http.NewRequest(methodStr, urlStr, bytes.NewBuffer(requestBody))
		// if err != nil {
		// 	panic(fmt.Sprintf("failed to create request: %v", err))
		// }

		//set default headers
		defaultHeaders := this.Headers.(map[string]any)
		for key, value := range defaultHeaders {
			req.Header.Set(key, derefScalar(value).(string))
		}

		// Set headers
		for key, value := range headersStrMap {
			req.Header.Set(key, value)
		}

		if this.Verbose {
			fmt.Println("Headers:", req.Header)
			fmt.Printf("\n\n\n")
			fmt.Printf("Request: %s %s\n", methodStr, urlStr)
			fmt.Printf("\n\n\n")
			fmt.Printf("Body: %v\n", body)
			fmt.Printf("\n\n\n")
		}

		// strings.NewReader()
		// Send the request
		var timing RestResponseTiming
		var event RestEvent
		hook := this.restHook
		if hook != nil {
			now := time.Now()
			timing = RestResponseTiming{Url: urlStr, Method: methodStr, RequestAt: now}
			event = RestEvent{
				ID:        atomic.AddUint64(&this.restRequestID, 1),
				Phase:     RestRequest,
				URL:       urlStr,
				Method:    methodStr,
				At:        now,
				RequestAt: now,
			}
			hook(event)
		}
		resp, err := this.httpClient.Do(req)
		if hook != nil && resp != nil {
			// Do returns once the response head is in: everything after this is
			// body transfer plus decode, which is the caller-visible remainder
			now := time.Now()
			timing.HeadersAt = now
			timing.Status = resp.StatusCode
			event.HeadersAt = now
			event.At = now
			event.Phase = RestHeaders
			event.Status = resp.StatusCode
			hook(event)
		}

		// Read the response body
		// respBody, err := ioutil.ReadAll(resp.Body)
		var respBody []byte

		if resp == nil {
			// when resp is nil: The request failed at the transport level (e.g., network issue, DNS lookup failure).
			networkError := NetworkError(fmt.Sprintf("Network error: %v", err))
			panic(networkError)
		}
		respHeadersMap := HeaderToMap(resp.Header)
		// hand the response headers to the request book's owner; the request
		// goroutine never writes the shared bookkeeping fields
		this.handOffRequestBook(requestBookOp{kind: requestBookResponse, headers: respHeadersMap})
		if err == nil {
			defer resp.Body.Close()
			if resp.Header.Get("Content-Encoding") == "gzip" {
				gzipReader, err := gzip.NewReader(resp.Body)
				if err != nil {
					panic(fmt.Sprintf("Error creating gzip reader: %s", err))
				}
				defer gzipReader.Close()

				decompressedData, err := io.ReadAll(gzipReader)
				if err != nil {
					panic(fmt.Sprintf("Error reading decompressed data: %s", err))
				}
				respBody = decompressedData
			} else {
				respBodyAux, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					panic(fmt.Sprintf("failed to read response body: %v", err))
				}
				respBody = respBodyAux
			}
		}

		responseHeaders := HeaderToMap(resp.Header)
		if hook != nil {
			now := time.Now()
			timing.BodyAt = now
			timing.Bytes = len(respBody)
			event.BodyAt = now
			event.At = now
			event.Phase = RestBody
			event.Bytes = len(respBody)
			hook(event)
		}

		// Use ParseJSON to handle JSON parsing with proper number normalization
		var result any
		result = ParseJSON(string(respBody))

		if result == nil {
			// If ParseJSON failed, fallback to raw string
			result = string(respBody)
		} else {
			if this.ReturnResponseHeaders {
				if resultMap, ok := result.(map[string]any); ok {
					resultMap["responseHeaders"] = responseHeaders
					result = resultMap
				}
			}
		}
		if hook != nil {
			// the decoded value the venue handler will see is final at this point
			now := time.Now()
			timing.DecodedAt = now
			event.DecodedAt = now
			event.At = now
			event.Phase = RestDecoded
			hook(event)
		}

		// Log the response (for debugging purposes)
		if this.Verbose {
			fmt.Printf("Response: %s\n", respBody)
		}

		statusText := http.StatusText(resp.StatusCode)
		// handleErrorResult := <-this.callInternal("handleErrors", resp.StatusCode, statusText, urlStr, methodStr, headers, string(respBody), result, headersStrMap, body)
		handleErrorResult := this.DerivedExchange.HandleErrors(int64(resp.StatusCode), statusText, urlStr, methodStr, responseHeaders, string(respBody), result, headersStrMap, body)
		PanicOnError(handleErrorResult)

		if handleErrorResult == nil {
			this.HandleHttpStatusCode(resp.StatusCode, statusText, urlStr, methodStr, result)
		}

		// Check for HTTP errors
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			// add handle http status()
			panic(fmt.Sprintf("HTTP request failed with status code %d: %s", resp.StatusCode, string(respBody)))
		}

		// only handling failure here
		if err != nil {
			panic(fmt.Sprintf("request failed: %v", err))
		}

		ch <- AsyncResult[any]{Value: result}
	}()
	return ch
}

func (this *BaseExchange) HandleHttpStatusCode(code any, reason any, url any, method any, body any) {

	codeString := ToString(code)
	codeinHttpExceptions := SafeValue(this.HttpExceptions, codeString, nil)

	if codeinHttpExceptions != nil {
		errorMessage := this.Id + " " + ToString(method) + " " + ToString(url) + " " + ToString(code) + " " + ToString(reason) + " " + ToString(body)
		functionError := codeinHttpExceptions.(func(...any) error)
		panic(functionError(errorMessage))
	}

}

func HeaderToMap(header http.Header) map[string]any {
	result := make(map[string]any)
	for key, values := range header {
		if len(values) == 1 {
			result[key] = values[0]
		} else {
			result[key] = values
		}
	}
	return result
}

// RestPhase names one point of a REST round trip's timeline.
type RestPhase int

const (
	// RestRequest fires just before the transport call, with the request URL and
	// method: the span's starting point.
	RestRequest RestPhase = iota
	// RestHeaders fires once httpClient.Do returns and the response head is in.
	RestHeaders
	// RestBody fires once the body has been read (and inflated, if gzipped).
	RestBody
	// RestDecoded fires once the body has become the value the venue handler sees
	// (ParseJSON, or the raw string when the body is not JSON).
	RestDecoded
)

func (phase RestPhase) String() string {
	switch phase {
	case RestRequest:
		return "request"
	case RestHeaders:
		return "headers"
	case RestBody:
		return "body"
	case RestDecoded:
		return "decoded"
	}
	return "unknown"
}

// RestEvent is one phase notification for one REST round trip. ID is stable
// across the four phases of a request; At is the phase's instant, while
// RequestAt/HeadersAt/BodyAt/DecodedAt carry the timeline known so far. ccxt takes
// no tracing dependency: an adapter starts a span at RestRequest, annotates
// Status/Bytes at RestHeaders/RestBody and ends it at RestDecoded.
type RestEvent struct {
	ID        uint64
	Phase     RestPhase
	URL       string
	Method    string
	Status    int
	Bytes     int
	At        time.Time
	RequestAt time.Time
	HeadersAt time.Time
	BodyAt    time.Time
	DecodedAt time.Time
}

// Timing projects the event's timeline, so timing-only consumers keep the shape
// the earlier observer exposed.
func (event RestEvent) Timing() RestResponseTiming {
	return RestResponseTiming{
		Url:       event.URL,
		Method:    event.Method,
		Status:    event.Status,
		Bytes:     event.Bytes,
		RequestAt: event.RequestAt,
		HeadersAt: event.HeadersAt,
		BodyAt:    event.BodyAt,
		DecodedAt: event.DecodedAt,
	}
}

// Duration returns the whole observed span known so far, request start to
// decoded value.
func (event RestEvent) Duration() time.Duration {
	return event.Timing().Duration()
}

// RestResponseTiming is one REST exchange's receive-side timeline. RequestAt is
// taken just before the transport call, HeadersAt when the response head is in,
// BodyAt when the body has been read (and inflated, when the response was
// gzipped) and DecodedAt when the body has been turned into the value the venue
// handler sees. The library's own share is BodyAt->DecodedAt (decode); the
// network's share is RequestAt->BodyAt.
type RestResponseTiming struct {
	Url       string
	Method    string
	Status    int
	Bytes     int
	RequestAt time.Time
	HeadersAt time.Time
	BodyAt    time.Time
	DecodedAt time.Time
}

// Duration returns the whole observe span, request start to decoded value.
func (t RestResponseTiming) Duration() time.Duration {
	if t.RequestAt.IsZero() || t.DecodedAt.IsZero() {
		return 0
	}
	return t.DecodedAt.Sub(t.RequestAt)
}
