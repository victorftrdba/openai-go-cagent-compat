package ssestream

import (
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

func TestStreamSkipsEventsWithoutData(t *testing.T) {
	tests := []struct {
		name string
		body string
		want []string
	}{
		{"comment", ": OPENROUTER PROCESSING\n\ndata: {\"value\":\"first\"}\n\ndata: [DONE]\n\n", []string{"first"}},
		{"retry directive", "retry: 3000\n\ndata: {\"value\":\"first\"}\n\ndata: [DONE]\n\n", []string{"first"}},
		{"CRLF", ": OPENROUTER PROCESSING\r\n\r\ndata: {\"value\":\"first\"}\r\n\r\ndata: [DONE]\r\n\r\n", []string{"first"}},
		{"multiple empty events", "data: {\"value\":\"first\"}\n\n: keepalive\n\nretry: 3000\n\ndata: {\"value\":\"second\"}\n\ndata: [DONE]\n\n", []string{"first", "second"}},
		{"only empty events", ": keepalive\n\nretry: 3000\n\n", nil},
		{"blank blocks then chunk and EOF", "\n\n\ndata: {\"value\":\"first\"}\n\n", []string{"first"}},
		{"event-only block resets type", "event: thread.run.created\n\ndata: {\"value\":\"first\"}\n\n", []string{"first"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := &http.Response{Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(tt.body))}
			stream := NewStream[struct {
				Value string `json:"value"`
			}](NewDecoder(res), nil)
			var got []string
			for stream.Next() {
				got = append(got, stream.Current().Value)
			}
			if err := stream.Err(); err != nil {
				t.Fatalf("unexpected stream error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDecoderSkipsBlocksWithoutData(t *testing.T) {
	res := &http.Response{Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(": keepalive\n\nretry: 3000\n\ndata: {\"value\":\"first\"}\n\n"))}
	decoder := NewDecoder(res)
	if !decoder.Next() {
		t.Fatalf("expected data event: %v", decoder.Err())
	}
	if got := string(decoder.Event().Data); got != "{\"value\":\"first\"}\n" {
		t.Fatalf("got data %q", got)
	}
	if decoder.Next() {
		t.Fatal("expected EOF")
	}
	if err := decoder.Err(); err != nil {
		t.Fatalf("unexpected decoder error: %v", err)
	}
}

func TestStreamPreservesErrorAfterBlockWithoutData(t *testing.T) {
	res := &http.Response{Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(": keepalive\n\ndata: {\"error\":{\"message\":\"bad\",\"type\":\"invalid_request_error\"}}\n\n"))}
	stream := NewStream[map[string]any](NewDecoder(res), nil)
	if stream.Next() {
		t.Fatal("expected API error")
	}
	if err := stream.Err(); err == nil || !strings.Contains(err.Error(), "received error while streaming:") || !strings.Contains(err.Error(), "bad") {
		t.Fatalf("expected preserved API error, got %v", err)
	}
}

var errTestReader = errors.New("test reader error")

type testErrorReadCloser struct {
	*strings.Reader
}

func (r testErrorReadCloser) Read(p []byte) (int, error) {
	if r.Len() == 0 {
		return 0, errTestReader
	}
	return r.Reader.Read(p)
}

func (r testErrorReadCloser) Close() error { return nil }

func TestStreamPreservesReaderErrorAfterBlockWithoutData(t *testing.T) {
	for _, tt := range []struct {
		name string
		body string
		want []string
	}{
		{"empty blocks", ": keepalive\n\nretry: 3000\n\n", nil},
		{"chunk before empty block", "data: {\"value\":\"first\"}\n\n: keepalive\n\n", []string{"first"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			res := &http.Response{Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: testErrorReadCloser{strings.NewReader(tt.body)}}
			stream := NewStream[struct {
				Value string `json:"value"`
			}](NewDecoder(res), nil)
			var got []string
			for stream.Next() {
				got = append(got, stream.Current().Value)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			if err := stream.Err(); !errors.Is(err, errTestReader) {
				t.Fatalf("expected reader error, got %v", err)
			}
		})
	}
}

func TestStreamRejectsEmptyDataField(t *testing.T) {
	res := &http.Response{Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader("data:\n\n"))}
	stream := NewStream[map[string]any](NewDecoder(res), nil)
	if stream.Next() {
		t.Fatal("expected invalid JSON error")
	}
	if err := stream.Err(); err == nil || !strings.Contains(err.Error(), "unexpected end of JSON input") {
		t.Fatalf("expected invalid JSON error, got %v", err)
	}
}
