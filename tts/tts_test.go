package tts

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"nhooyr.io/websocket"
)

func TestDefaultRetrySkipsDeterministicHTTPFailures(t *testing.T) {
	attempts := 0
	err := retry(context.Background(), RetryPolicy{MaxAttempts: 3}, func() error {
		attempts++
		return &Error{Transport: "http", HTTPStatus: http.StatusUnauthorized}
	})
	if err == nil || attempts != 1 {
		t.Fatalf("attempts=%d, want 1", attempts)
	}
}

func TestDefaultRetryRetriesNetworkFailure(t *testing.T) {
	attempts := 0
	err := retry(context.Background(), RetryPolicy{MaxAttempts: 2, InitialBackoff: time.Millisecond}, func() error {
		attempts++
		if attempts == 1 {
			return timeoutError{}
		}
		return nil
	})
	if err != nil || attempts != 2 {
		t.Fatalf("err=%v attempts=%d, want nil/2", err, attempts)
	}
}

func TestTimeoutContext(t *testing.T) {
	ctx, cancel := timeoutContext(context.Background(), time.Millisecond)
	defer cancel()
	<-ctx.Done()
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("got %v", ctx.Err())
	}
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "temporary timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

type testTTSGRPCServer struct {
	UnimplementedTtsStreamServiceServer
}

type testTTSGRPCFailureServer struct {
	UnimplementedTtsStreamServiceServer
}

func (testTTSGRPCFailureServer) StreamTts(stream TtsStreamService_StreamTtsServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	return stream.Send(&TtsStreamResponse{Payload: testServerFrame(EventSessionFailed, "session-grpc", []byte(`{"code":1500,"message":"TTS 算法未返回音频","type":"ALGORITHM_ERROR","retryable":true,"request_id":"req-grpc-1","session_id":"session-grpc","upstream_code":55000000,"error_code":1500,"error_message":"TTS 算法未返回音频"}`))})
}

func (testTTSGRPCServer) StreamTts(stream TtsStreamService_StreamTtsServer) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	if request.GetApiKey() != "test-key" || request.GetRequestId() == "" {
		return nil
	}
	frame, err := Decode(request.GetPayload())
	if err != nil || frame.Event != EventStartConnection {
		return err
	}
	response := make([]byte, 18)
	response[0], response[1], response[2] = 0x11, 0x94, 0x10
	binary.BigEndian.PutUint32(response[4:], EventConnectionStarted)
	binary.BigEndian.PutUint32(response[8:], 0)
	binary.BigEndian.PutUint32(response[12:], 2)
	copy(response[16:], "{}")
	if err := stream.Send(&TtsStreamResponse{Payload: response}); err != nil {
		return err
	}
	if err := stream.Send(&TtsStreamResponse{Payload: testServerFrame(EventAudio, "session", []byte("one"))}); err != nil {
		return err
	}
	return stream.Send(&TtsStreamResponse{Payload: testServerFrame(EventAudio, "session", []byte("two"))})
}

func TestGRPCAudioChunksArriveInOrder(t *testing.T) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()
	server := grpc.NewServer()
	RegisterTtsStreamServiceServer(server, testTTSGRPCServer{})
	go server.Serve(listener)
	defer server.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := (Config{GRPCAddress: listener.Addr().String(), APIKey: "test-key", ConnectTimeout: time.Second}).OpenGRPC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if err = stream.StartConnection(ctx); err != nil {
		t.Fatal(err)
	}
	var audio []byte
	for event := range stream.Events() {
		if event.Event == EventAudio {
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

func TestGRPCFirstFrameAndConnectionEvent(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	RegisterTtsStreamServiceServer(server, testTTSGRPCServer{})
	go server.Serve(listener)
	defer server.Stop()
	defer listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := (Config{GRPCAddress: listener.Addr().String(), APIKey: "test-key", ConnectTimeout: time.Second}).OpenGRPC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if err = stream.StartConnection(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-stream.Events():
		if event.Event != EventConnectionStarted || event.Err != nil {
			t.Fatalf("event=%#v", event)
		}
	case <-ctx.Done():
		t.Fatal("event timed out")
	}
}

func TestGRPCSessionFailureExposesUnifiedBusinessError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	RegisterTtsStreamServiceServer(server, testTTSGRPCFailureServer{})
	go server.Serve(listener)
	defer server.Stop()
	defer listener.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := (Config{GRPCAddress: listener.Addr().String(), APIKey: "test-key", ConnectTimeout: time.Second}).OpenGRPC(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if err = stream.StartConnection(ctx); err != nil {
		t.Fatal(err)
	}
	event := <-stream.Events()
	apiErr, ok := event.Err.(*Error)
	if !ok || event.ErrorCode != 1500 || apiErr.BusinessCode != 1500 || apiErr.Type != "ALGORITHM_ERROR" || !apiErr.Retryable || apiErr.RequestID != "req-grpc-1" || apiErr.SessionID != "session-grpc" || apiErr.UpstreamCode != 55000000 {
		t.Fatalf("event=%#v", event)
	}
}

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

func TestSynthesizeStreamExposesUnifiedHTTPBusinessError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Request-Id", "req-http-1")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"code":1500,"info":"TTS 算法未返回音频","data":{"type":"ALGORITHM_ERROR","retryable":true,"request_id":"req-http-1","session_id":"session-http-1","upstream_code":55000000}}`))
	}))
	defer server.Close()

	_, err := (Config{BaseURL: server.URL, APIKey: "test-key"}).SynthesizeStream(context.Background(), SynthesisRequest{Text: "hello"})
	var apiErr *Error
	if !errors.As(err, &apiErr) {
		t.Fatalf("err=%v", err)
	}
	if apiErr.HTTPStatus != http.StatusBadGateway || apiErr.BusinessCode != 1500 || apiErr.Type != "ALGORITHM_ERROR" || !apiErr.Retryable || apiErr.RequestID != "req-http-1" || apiErr.SessionID != "session-http-1" || apiErr.UpstreamCode != 55000000 {
		t.Fatalf("error=%#v", apiErr)
	}
}

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
		request, err := Decode(frame)
		if err != nil || request.Event != EventStartConnection {
			t.Errorf("request=%#v err=%v", request, err)
			return
		}
		response := make([]byte, 4+4+4+4+2)
		response[0], response[1], response[2] = 0x11, 0x94, 0x10
		binary.BigEndian.PutUint32(response[4:], EventConnectionStarted)
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
		if event.Event != EventConnectionStarted || event.Err != nil {
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

func TestWebSocketConnectionFinishedWaitsForServerClose(t *testing.T) {
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
		request, err := Decode(frame)
		if err != nil || request.Event != EventStartConnection {
			t.Errorf("request=%#v err=%v", request, err)
			return
		}
		if err := conn.Write(r.Context(), websocket.MessageBinary,
			testServerFrame(EventConnectionFinished, "", []byte("{}"))); err != nil {
			t.Error(err)
			return
		}
		// This mirrors the TTS server: it emits the terminal business frame and then
		// initiates the WebSocket close handshake. The SDK must not race it with a
		// second client-initiated close.
		if err := conn.Close(websocket.StatusNormalClosure, ""); err != nil {
			t.Error(err)
		}
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
		if event.Event != EventConnectionFinished || event.Err != nil {
			t.Fatalf("event=%#v", event)
		}
		// Callers commonly defer Close. At the business terminal frame, transport
		// shutdown is still owned by the server, so Close must be a no-op.
		if stream.State() != StateClosing {
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
			_ = conn.Write(r.Context(), websocket.MessageBinary, testServerFrame(EventAudio, "session", payload))
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
		if event.Event == EventAudio {
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
		_ = conn.Write(r.Context(), websocket.MessageBinary, testServerFrame(EventSessionFailed, "session", []byte(`{"code":1001,"message":"invalid speaker","type":"PARAM_ERROR","retryable":false,"request_id":"req-ws-1","session_id":"session","error_code":1001,"error_message":"invalid speaker"}`)))
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
	apiErr, ok := event.Err.(*Error)
	if !ok || event.ErrorCode != 1001 || apiErr.BusinessCode != 1001 || apiErr.Type != "PARAM_ERROR" || apiErr.Retryable || apiErr.RequestID != "req-ws-1" || apiErr.SessionID != "session" || stream.State() != StateFailed {
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

func TestEncodeDecodeClientTaskFrame(t *testing.T) {
	input := Frame{Event: EventTaskRequest, SessionID: "session-1", Payload: []byte(`{"text":"hello"}`)}
	encoded, err := EncodeClient(input)
	if err != nil {
		t.Fatalf("EncodeClient: %v", err)
	}
	got, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Event != input.Event || got.SessionID != input.SessionID || string(got.Payload) != string(input.Payload) {
		t.Fatalf("round trip mismatch: %#v", got)
	}
}
