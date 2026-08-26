package tts

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestSynthesizeStreamSendsMultipartAndReturnsAudio(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/tts/infer-stream" || r.Header.Get("api_key") != "test-key" {
			t.Fatalf("unexpected request %s", r.URL)
		}
		if err := r.ParseMultipartForm(1024); err != nil {
			t.Fatal(err)
		}
		if got := r.FormValue("text"); got != "hello" {
			t.Fatalf("text=%q", got)
		}
		if got := r.FormValue("speed"); got != "1.25" {
			t.Fatalf("speed=%q", got)
		}
		w.Header().Set("X-Request-Id", "req-1")
		w.Header().Set("X-Audio-Type", "mp3")
		_, _ = w.Write([]byte("audio"))
	}))
	defer server.Close()
	speed := float32(1.25)
	stream, err := (Config{BaseURL: server.URL, APIKey: "test-key"}).SynthesizeStream(context.Background(), SynthesisRequest{Text: "hello", AudioType: "mp3", Speed: &speed})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	body, _ := io.ReadAll(stream)
	if string(body) != "audio" || stream.RequestID != "req-1" {
		t.Fatalf("body=%q request=%q", body, stream.RequestID)
	}
}

func TestSynthesizeStreamRetriesServerFailureBeforeAudio(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("audio"))
	}))
	defer server.Close()
	stream, err := (Config{BaseURL: server.URL, APIKey: "test-key", Retry: RetryPolicy{MaxAttempts: 2, InitialBackoff: time.Millisecond}}).SynthesizeStream(context.Background(), SynthesisRequest{Text: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if calls.Load() != 2 {
		t.Fatalf("calls=%d", calls.Load())
	}
}
