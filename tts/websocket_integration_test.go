package tts

import (
	"context"
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lycheeAIc/voice-openapi-go-sdk/internal/protocol"
	"nhooyr.io/websocket"
)

func TestWebSocketConnectionEventAndNormalClose(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("api_key") != "test-key" {
			t.Error("missing api key")
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		_, frame, err := conn.Read(r.Context())
		if err != nil {
			t.Error(err)
			return
		}
		request, err := protocol.Decode(frame)
		if err != nil || request.Event != protocol.EventStartConnection {
			t.Errorf("request=%#v err=%v", request, err)
			return
		}
		response := make([]byte, 4+4+4+4+2)
		response[0], response[1], response[2] = 0x11, 0x94, 0x10
		binary.BigEndian.PutUint32(response[4:], protocol.EventConnectionStarted)
		binary.BigEndian.PutUint32(response[8:], 0) // connection id size
		binary.BigEndian.PutUint32(response[12:], 2)
		copy(response[16:], "{}")
		_ = conn.Write(r.Context(), websocket.MessageBinary, response)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := (Config{BaseURL: server.URL, APIKey: "test-key"}).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.StartConnection(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-stream.Events():
		if event.Event != protocol.EventConnectionStarted || event.Err != nil {
			t.Fatalf("event=%#v", event)
		}
	case <-ctx.Done():
		t.Fatal("event timed out")
	}
	<-stream.Done()
	if stream.State() != StateClosed {
		t.Fatalf("state=%v", stream.State())
	}
}

func TestWebSocketConnectionFinishedMarksClosedBeforeCallerCleanup(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		_, frame, err := conn.Read(r.Context())
		if err != nil {
			t.Error(err)
			return
		}
		request, err := protocol.Decode(frame)
		if err != nil || request.Event != protocol.EventStartConnection {
			t.Errorf("request=%#v err=%v", request, err)
			return
		}
		if err := conn.Write(r.Context(), websocket.MessageBinary,
			testServerFrame(protocol.EventConnectionFinished, "", []byte("{}"))); err != nil {
			t.Error(err)
			return
		}
		// The SDK must close after receiving ConnectionFinished. Keep the server open
		// until that close is observed so this test exercises the business-frame path,
		// rather than a peer-initiated WebSocket close.
		_, _, _ = conn.Read(r.Context())
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := (Config{BaseURL: server.URL, APIKey: "test-key"}).Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.StartConnection(ctx); err != nil {
		t.Fatal(err)
	}
	for event := range stream.Events() {
		if event.Event != protocol.EventConnectionFinished || event.Err != nil {
			t.Fatalf("event=%#v", event)
		}
		// This mirrors callers that defer Close. It must not emit a second close frame.
		if stream.State() != StateClosed {
			t.Fatalf("state at ConnectionFinished=%v", stream.State())
		}
		if err := stream.Close(websocket.StatusNormalClosure, "done"); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-stream.Done():
		if stream.State() != StateClosed {
			t.Fatalf("final state=%v", stream.State())
		}
	case <-ctx.Done():
		t.Fatal("stream did not finish")
	}
}

func TestWebSocketIdleTimeoutMarksStreamFailed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		time.Sleep(100 * time.Millisecond)
	}))
	defer server.Close()
	stream, err := (Config{BaseURL: server.URL, APIKey: "test-key", IdleTimeout: 10 * time.Millisecond}).Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-stream.Done():
		if stream.State() != StateFailed {
			t.Fatalf("state=%v", stream.State())
		}
	case <-time.After(time.Second):
		t.Fatal("idle timeout did not fire")
	}
}

func TestWebSocketAudioChunksArriveInOrder(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		_, _, _ = conn.Read(r.Context())
		for _, payload := range [][]byte{[]byte("one"), []byte("two")} {
			_ = conn.Write(r.Context(), websocket.MessageBinary, testServerFrame(protocol.EventAudio, "session", payload))
		}
	}))
	defer server.Close()
	stream, err := (Config{BaseURL: server.URL, APIKey: "test-key"}).Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = stream.StartConnection(context.Background()); err != nil {
		t.Fatal(err)
	}
	var audio []byte
	for event := range stream.Events() {
		if event.Event == protocol.EventAudio {
			audio = append(audio, event.Audio...)
		}
	}
	if string(audio) != "onetwo" {
		t.Fatalf("audio=%q", audio)
	}
	if stream.State() != StateClosed {
		t.Fatalf("state=%v", stream.State())
	}
}

func TestWebSocketSessionFailureExposesBusinessError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		_, _, _ = conn.Read(r.Context())
		_ = conn.Write(r.Context(), websocket.MessageBinary, testServerFrame(protocol.EventSessionFailed, "session", []byte(`{"error_code":45000001,"error_message":"invalid speaker"}`)))
	}))
	defer server.Close()
	stream, err := (Config{BaseURL: server.URL, APIKey: "test-key"}).Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = stream.StartConnection(context.Background()); err != nil {
		t.Fatal(err)
	}
	event := <-stream.Events()
	if event.Err == nil || event.ErrorCode != 45000001 || stream.State() != StateFailed {
		t.Fatalf("event=%#v state=%v", event, stream.State())
	}
}

func testServerFrame(event int, sessionID string, payload []byte) []byte {
	id := []byte(sessionID)
	result := make([]byte, 4+4+4+len(id)+4+len(payload))
	result[0], result[1], result[2] = 0x11, 0xB4, 0x10
	binary.BigEndian.PutUint32(result[4:], uint32(event))
	binary.BigEndian.PutUint32(result[8:], uint32(len(id)))
	copy(result[12:], id)
	offset := 12 + len(id)
	binary.BigEndian.PutUint32(result[offset:], uint32(len(payload)))
	copy(result[offset+4:], payload)
	return result
}
