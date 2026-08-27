package tts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"mime/multipart"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lycheeAIc/voice-openapi-go-sdk/internal/protocol"
	"github.com/lycheeAIc/voice-openapi-go-sdk/tts/grpcpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"nhooyr.io/websocket"
)

type RetryPolicy struct {
	MaxAttempts                int
	InitialBackoff, MaxBackoff time.Duration
	Multiplier                 float64
	Jitter                     float64
	Retryable                  func(error) bool
}

func (p RetryPolicy) normalized() RetryPolicy {
	if p.MaxAttempts < 1 {
		p.MaxAttempts = 1
	}
	if p.InitialBackoff <= 0 {
		p.InitialBackoff = 200 * time.Millisecond
	}
	if p.MaxBackoff <= 0 {
		p.MaxBackoff = 5 * time.Second
	}
	if p.Multiplier < 1 {
		p.Multiplier = 2
	}
	return p
}

type Config struct {
	BaseURL, GRPCAddress, APIKey                           string
	HTTPClient                                             *http.Client
	ConnectTimeout, ReadTimeout, WriteTimeout, IdleTimeout time.Duration
	Retry                                                  RetryPolicy
}

func (c Config) validate() error {
	if c.BaseURL == "" && c.GRPCAddress == "" {
		return errors.New("tts: BaseURL or GRPCAddress is required")
	}
	if c.APIKey == "" {
		return errors.New("tts: APIKey is required")
	}
	return nil
}
func (c Config) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	timeout := c.ReadTimeout
	if timeout <= 0 {
		timeout = 100 * time.Second
	}
	return &http.Client{Timeout: timeout}
}
func retry(ctx context.Context, p RetryPolicy, fn func() error) error {
	p = p.normalized()
	var err error
	delay := p.InitialBackoff
	for attempt := 1; attempt <= p.MaxAttempts; attempt++ {
		if err = fn(); err == nil || attempt == p.MaxAttempts || !shouldRetry(p, err) {
			return err
		}
		jitter := 1 + (rand.Float64()*2-1)*p.Jitter
		wait := time.Duration(float64(delay) * jitter)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		delay = time.Duration(float64(delay) * p.Multiplier)
		if delay > p.MaxBackoff {
			delay = p.MaxBackoff
		}
	}
	return err
}

func shouldRetry(p RetryPolicy, err error) bool {
	if p.Retryable != nil {
		return p.Retryable(err)
	}
	if e, ok := err.(*Error); ok && e.HTTPStatus > 0 {
		if e.retryableSet {
			return e.Retryable
		}
		return e.HTTPStatus == http.StatusTooManyRequests || e.HTTPStatus >= 500
	}
	var networkError net.Error
	return errors.As(err, &networkError)
}

func timeoutContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return parent, func() {}
	}
	return context.WithTimeout(parent, timeout)
}

type Error struct {
	Transport                                            string
	HTTPStatus, GRPCCode, CloseCode, BusinessCode, Event int
	RequestID, SessionID, Type, Message                  string
	UpstreamCode                                         int
	Retryable                                            bool
	Cause                                                error
	retryableSet                                         bool
}

func (e *Error) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("tts %s error: %s", e.Transport, e.Message)
	}
	if e.Cause != nil {
		return fmt.Sprintf("tts %s error: %v", e.Transport, e.Cause)
	}
	return "tts error"
}
func (e *Error) Unwrap() error { return e.Cause }

type streamErrorPayload struct {
	Code         int    `json:"code"`
	Message      string `json:"message"`
	Type         string `json:"type"`
	Retryable    *bool  `json:"retryable"`
	RequestID    string `json:"request_id"`
	SessionID    string `json:"session_id"`
	UpstreamCode int    `json:"upstream_code"`
	ErrorCode    int    `json:"error_code"`
	ErrorMessage string `json:"error_message"`
}

func parseStreamErrorPayload(payload []byte) (streamErrorPayload, bool) {
	var details streamErrorPayload
	if json.Unmarshal(payload, &details) != nil {
		return streamErrorPayload{}, false
	}
	if details.Code == 0 {
		details.Code = details.ErrorCode
	}
	if details.Message == "" {
		details.Message = details.ErrorMessage
	}
	return details, details.Code != 0 || details.Message != "" || details.Type != "" || details.Retryable != nil
}

func newStreamError(transport string, event, fallbackCode int, fallbackMessage string, payload []byte) *Error {
	details, ok := parseStreamErrorPayload(payload)
	if !ok {
		return &Error{Transport: transport, Event: event, BusinessCode: fallbackCode, Message: fallbackMessage}
	}
	code := details.Code
	if code == 0 {
		code = fallbackCode
	}
	message := details.Message
	if message == "" {
		message = fallbackMessage
	}
	err := &Error{
		Transport: transport, Event: event, BusinessCode: code, Message: message,
		Type: details.Type, RequestID: details.RequestID, SessionID: details.SessionID,
		UpstreamCode: details.UpstreamCode,
	}
	if details.Retryable != nil {
		err.Retryable = *details.Retryable
		err.retryableSet = true
	}
	return err
}

type SynthesisRequest struct {
	Text, SpeakerID, AudioType string
	Audio                      io.Reader
	AudioName                  string
	Speed, Volume              *float32
	SampleRate                 *int
	TextNormalizer             *bool
}
type AudioStream struct {
	io.ReadCloser
	RequestID, AudioType, SampleRate, Channels, SampleFormat string
}

func (c Config) SynthesizeStream(ctx context.Context, input SynthesisRequest) (*AudioStream, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(input.Text) == "" {
		return nil, fmt.Errorf("tts: text is required")
	}
	var response *http.Response
	policy := c.Retry
	if input.Audio != nil {
		policy.MaxAttempts = 1 // A generic io.Reader cannot be safely replayed.
	}
	err := retry(ctx, policy, func() error {
		r, err := c.newHTTPStreamRequest(ctx, input)
		if err != nil {
			return err
		}
		response, err = c.httpClient().Do(r)
		if err != nil {
			return &Error{Transport: "http", Message: "request failed", Cause: err}
		}
		if response.StatusCode/100 != 2 {
			requestID := response.Header.Get("X-Request-Id")
			body, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			if readErr != nil {
				return &Error{Transport: "http", HTTPStatus: response.StatusCode, RequestID: requestID,
					Message: response.Status, Cause: readErr}
			}
			return parseHTTPError(response.StatusCode, requestID, response.Status, body)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &AudioStream{ReadCloser: response.Body, RequestID: response.Header.Get("X-Request-Id"), AudioType: response.Header.Get("X-Audio-Type"), SampleRate: response.Header.Get("X-Audio-Sample-Rate"), Channels: response.Header.Get("X-Audio-Channels"), SampleFormat: response.Header.Get("X-Audio-Sample-Format")}, nil
}

func parseHTTPError(status int, requestID, fallbackMessage string, body []byte) *Error {
	var payload struct {
		Code int             `json:"code"`
		Info string          `json:"info"`
		Data json.RawMessage `json:"data"`
	}
	if json.Unmarshal(body, &payload) != nil {
		return &Error{Transport: "http", HTTPStatus: status, RequestID: requestID, Message: fallbackMessage}
	}
	err := newStreamError("http", 0, payload.Code, payload.Info, payload.Data)
	err.HTTPStatus = status
	if err.RequestID == "" {
		err.RequestID = requestID
	}
	if err.Message == "" {
		err.Message = fallbackMessage
	}
	return err
}
func (c Config) newHTTPStreamRequest(ctx context.Context, in SynthesisRequest) (*http.Request, error) {
	pr, pw := io.Pipe()
	w := multipart.NewWriter(pw)
	go func() {
		defer pw.Close()
		defer w.Close()
		_ = w.WriteField("text", in.Text)
		if in.SpeakerID != "" {
			_ = w.WriteField("speaker_id", in.SpeakerID)
		}
		if in.AudioType != "" {
			_ = w.WriteField("audio_type", in.AudioType)
		}
		if in.Speed != nil {
			_ = w.WriteField("speed", strconv.FormatFloat(float64(*in.Speed), 'f', -1, 32))
		}
		if in.Volume != nil {
			_ = w.WriteField("volume", strconv.FormatFloat(float64(*in.Volume), 'f', -1, 32))
		}
		if in.SampleRate != nil {
			_ = w.WriteField("sample_rate", strconv.Itoa(*in.SampleRate))
		}
		if in.TextNormalizer != nil {
			_ = w.WriteField("text_normalizer", strconv.FormatBool(*in.TextNormalizer))
		}
		if in.Audio != nil {
			part, e := w.CreateFormFile("audio", in.AudioName)
			if e == nil {
				_, e = io.Copy(part, in.Audio)
			}
			if e != nil {
				_ = pw.CloseWithError(e)
			}
		}
	}()
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.BaseURL, "/")+"/tts/infer-stream", pr)
	if err != nil {
		return nil, err
	}
	r.Header.Set("Content-Type", w.FormDataContentType())
	r.Header.Set("api_key", c.APIKey)
	return r, nil
}

type ConnectionState uint8

const (
	StateConnecting ConnectionState = iota
	StateConnected
	StateClosing
	StateClosed
	StateFailed
)

type Event struct {
	State            ConnectionState
	Event, ErrorCode int
	SessionID        string
	Audio, Metadata  []byte
	Err              error
}
type Stream struct {
	conn         *websocket.Conn
	events       chan Event
	done         chan struct{}
	mu           sync.RWMutex
	writeMu      sync.Mutex
	state        ConnectionState
	err          error
	readTimeout  time.Duration
	writeTimeout time.Duration
}

func (c Config) Connect(ctx context.Context) (*Stream, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	var conn *websocket.Conn
	var err error
	err = retry(ctx, c.Retry, func() error {
		dialCtx, cancel := timeoutContext(ctx, c.ConnectTimeout)
		defer cancel()
		h := http.Header{}
		h.Set("api_key", c.APIKey)
		endpoint := strings.TrimRight(c.BaseURL, "/")
		endpoint = strings.TrimPrefix(endpoint, "http://")
		endpoint = strings.TrimPrefix(endpoint, "https://")
		if strings.HasPrefix(c.BaseURL, "https://") {
			endpoint = "wss://" + endpoint
		} else {
			endpoint = "ws://" + endpoint
		}
		conn, _, err = websocket.Dial(dialCtx, endpoint+"/tts/ws_binary/v2", &websocket.DialOptions{HTTPHeader: h})
		return err
	})
	if err != nil {
		return nil, &Error{Transport: "websocket", Message: "connect failed", Cause: err}
	}
	readTimeout := c.ReadTimeout
	if c.IdleTimeout > 0 {
		readTimeout = c.IdleTimeout
	}
	s := &Stream{conn: conn, events: make(chan Event, 64), done: make(chan struct{}), state: StateConnected, readTimeout: readTimeout, writeTimeout: c.WriteTimeout}
	go s.readLoop()
	return s, nil
}
func (s *Stream) Events() <-chan Event   { return s.events }
func (s *Stream) Done() <-chan struct{}  { return s.done }
func (s *Stream) State() ConnectionState { s.mu.RLock(); defer s.mu.RUnlock(); return s.state }
func (s *Stream) Err() error             { s.mu.RLock(); defer s.mu.RUnlock(); return s.err }
func (s *Stream) Send(ctx context.Context, event int, sessionID string, payload []byte) error {
	b, err := protocol.EncodeClient(protocol.Frame{Event: event, SessionID: sessionID, Payload: payload})
	if err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.RLock()
	state := s.state
	s.mu.RUnlock()
	if state != StateConnected {
		return &Error{Transport: "websocket", Message: "cannot send after stream is closing"}
	}
	writeCtx, cancel := timeoutContext(ctx, s.writeTimeout)
	defer cancel()
	return s.conn.Write(writeCtx, websocket.MessageBinary, b)
}
func (s *Stream) StartConnection(ctx context.Context) error {
	return s.Send(ctx, protocol.EventStartConnection, "", []byte("{}"))
}
func (s *Stream) StartSession(ctx context.Context, id string, payload []byte) error {
	return s.Send(ctx, protocol.EventStartSession, id, payload)
}
func (s *Stream) SendTask(ctx context.Context, id string, payload []byte) error {
	return s.Send(ctx, protocol.EventTaskRequest, id, payload)
}
func (s *Stream) FinishSession(ctx context.Context, id string) error {
	return s.Send(ctx, protocol.EventFinishSession, id, []byte("{}"))
}
func (s *Stream) FinishConnection(ctx context.Context) error {
	return s.Send(ctx, protocol.EventFinishConnection, "", []byte("{}"))
}
func (s *Stream) Close(code websocket.StatusCode, reason string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.Lock()
	if s.state >= StateClosing {
		s.mu.Unlock()
		return nil
	}
	s.state = StateClosing
	s.mu.Unlock()
	return s.conn.Close(code, reason)
}
func (s *Stream) readLoop() {
	serverInitiatedClose := false
	defer func() {
		if !serverInitiatedClose {
			s.closeTransport(websocket.StatusNormalClosure, "")
		}
		close(s.events)
		close(s.done)
	}()
	for {
		readCtx, cancel := timeoutContext(context.Background(), s.readTimeout)
		_, b, e := s.conn.Read(readCtx)
		cancel()
		if e != nil {
			if websocket.CloseStatus(e) == websocket.StatusNormalClosure || websocket.CloseStatus(e) == websocket.StatusGoingAway {
				s.closed()
				return
			}
			s.fail(e)
			return
		}
		f, e := protocol.Decode(b)
		if e != nil {
			s.emit(Event{State: StateFailed, Err: &Error{Transport: "websocket", Message: "invalid server frame", Cause: e}})
			continue
		}
		ev := Event{State: StateConnected, Event: f.Event, ErrorCode: f.ErrorCode, SessionID: f.SessionID, Metadata: f.Payload}
		if f.Event == protocol.EventAudio {
			ev.Audio = f.Payload
			ev.Metadata = nil
		}
		if f.Event == protocol.EventConnectionFailed || f.Event == protocol.EventSessionFailed || f.ErrorCode != 0 {
			failure := newStreamError("websocket", f.Event, f.ErrorCode, string(f.Payload), f.Payload)
			ev.Err = failure
			ev.ErrorCode = failure.BusinessCode
		}
		if ev.Err != nil {
			s.markFailed(ev.Err)
		}
		if f.Event == protocol.EventConnectionFinished {
			// The TTS server sends this terminal business frame immediately before
			// initiating the WebSocket close handshake. Keep reading for that close
			// control frame instead of racing it with a client-initiated close.
			serverInitiatedClose = true
			s.closing()
			s.emit(ev)
			continue
		}
		s.emit(ev)
	}
}
func (s *Stream) closeTransport(code websocket.StatusCode, reason string) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_ = s.conn.Close(code, reason)
}
func (s *Stream) closing() {
	s.mu.Lock()
	if s.state == StateConnected {
		s.state = StateClosing
	}
	s.mu.Unlock()
}
func (s *Stream) closed() { s.mu.Lock(); s.state = StateClosed; s.mu.Unlock() }
func (s *Stream) emit(e Event) {
	select {
	case s.events <- e:
	case <-s.done:
	}
}
func (s *Stream) fail(e error) {
	s.mu.Lock()
	s.state = StateFailed
	s.err = e
	s.mu.Unlock()
	s.emit(Event{State: StateFailed, Err: e})
}
func (s *Stream) markFailed(e error) { s.mu.Lock(); s.state = StateFailed; s.err = e; s.mu.Unlock() }

type GRPCStream struct {
	conn      *grpc.ClientConn
	stream    grpcpb.TtsStreamService_StreamTtsClient
	events    chan Event
	done      chan struct{}
	once      sync.Once
	mu        sync.Mutex
	apiKey    string
	requestID string
	sentFirst bool
	cancel    context.CancelFunc
	state     ConnectionState
	err       error
}

func (c Config) OpenGRPC(ctx context.Context) (*GRPCStream, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	var conn *grpc.ClientConn
	var err error
	err = retry(ctx, c.Retry, func() error {
		dialCtx, cancel := timeoutContext(ctx, c.ConnectTimeout)
		defer cancel()
		options := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock()}
		if c.IdleTimeout > 0 {
			options = append(options, grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: c.IdleTimeout, Timeout: c.ConnectTimeout, PermitWithoutStream: true}))
		}
		conn, err = grpc.DialContext(dialCtx, c.GRPCAddress, options...)
		return err
	})
	if err != nil {
		return nil, &Error{Transport: "grpc", Message: "dial failed", Cause: err}
	}
	streamCtx, cancel := timeoutContext(ctx, c.ReadTimeout)
	stream, err := grpcpb.NewTtsStreamServiceClient(conn).StreamTts(streamCtx)
	if err != nil {
		conn.Close()
		cancel()
		return nil, &Error{Transport: "grpc", Message: "open stream failed", Cause: err}
	}
	s := &GRPCStream{conn: conn, stream: stream, events: make(chan Event, 64), done: make(chan struct{}), apiKey: c.APIKey, requestID: fmt.Sprintf("go%d", time.Now().UnixNano()), cancel: cancel, state: StateConnected}
	go s.readLoop()
	return s, nil
}
func (s *GRPCStream) Events() <-chan Event   { return s.events }
func (s *GRPCStream) Done() <-chan struct{}  { return s.done }
func (s *GRPCStream) State() ConnectionState { s.mu.Lock(); defer s.mu.Unlock(); return s.state }
func (s *GRPCStream) Err() error             { s.mu.Lock(); defer s.mu.Unlock(); return s.err }
func (s *GRPCStream) Send(ctx context.Context, event int, sessionID string, payload []byte) error {
	b, e := protocol.EncodeClient(protocol.Frame{Event: event, SessionID: sessionID, Payload: payload})
	if e != nil {
		return e
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		s.mu.Lock()
		defer s.mu.Unlock()
		req := &grpcpb.TtsStreamRequest{Payload: b}
		if !s.sentFirst {
			req.RequestId = s.requestID
			req.ApiKey = s.apiKey
			s.sentFirst = true
		}
		return s.stream.Send(req)
	}
}
func (s *GRPCStream) StartConnection(ctx context.Context) error {
	return s.Send(ctx, protocol.EventStartConnection, "", []byte("{}"))
}
func (s *GRPCStream) StartSession(ctx context.Context, id string, p []byte) error {
	return s.Send(ctx, protocol.EventStartSession, id, p)
}
func (s *GRPCStream) SendTask(ctx context.Context, id string, p []byte) error {
	return s.Send(ctx, protocol.EventTaskRequest, id, p)
}
func (s *GRPCStream) FinishSession(ctx context.Context, id string) error {
	return s.Send(ctx, protocol.EventFinishSession, id, []byte("{}"))
}
func (s *GRPCStream) FinishConnection(ctx context.Context) error {
	return s.Send(ctx, protocol.EventFinishConnection, "", []byte("{}"))
}
func (s *GRPCStream) Close() error {
	var e error
	s.once.Do(func() {
		s.mu.Lock()
		s.state = StateClosing
		s.mu.Unlock()
		s.cancel()
		e = s.stream.CloseSend()
		_ = s.conn.Close()
	})
	return e
}
func (s *GRPCStream) readLoop() {
	defer close(s.done)
	defer close(s.events)
	defer s.conn.Close()
	for {
		r, e := s.stream.Recv()
		if e == io.EOF {
			s.closed()
			return
		}
		if e != nil {
			st, _ := status.FromError(e)
			failure := &Error{Transport: "grpc", GRPCCode: int(st.Code()), Message: st.Message(), Cause: e}
			s.failed(failure)
			s.events <- Event{State: StateFailed, Err: failure}
			return
		}
		f, e := protocol.Decode(r.Payload)
		if e != nil {
			s.events <- Event{State: StateFailed, Err: e}
			continue
		}
		ev := Event{State: StateConnected, Event: f.Event, ErrorCode: f.ErrorCode, SessionID: f.SessionID, Metadata: f.Payload}
		if f.Event == protocol.EventAudio {
			ev.Audio = f.Payload
			ev.Metadata = nil
		}
		if f.ErrorCode != 0 || f.Event == protocol.EventSessionFailed || f.Event == protocol.EventConnectionFailed {
			failure := newStreamError("grpc", f.Event, f.ErrorCode, string(f.Payload), f.Payload)
			ev.Err = failure
			ev.ErrorCode = failure.BusinessCode
		}
		if ev.Err != nil {
			s.failed(ev.Err)
		}
		s.events <- ev
	}
}
func (s *GRPCStream) closed() {
	s.mu.Lock()
	if s.state != StateFailed {
		s.state = StateClosed
	}
	s.mu.Unlock()
}
func (s *GRPCStream) failed(err error) {
	s.mu.Lock()
	s.state = StateFailed
	s.err = err
	s.mu.Unlock()
}

// Version is the SDK version. It is updated as part of each release.
const Version = "v0.1.0"
