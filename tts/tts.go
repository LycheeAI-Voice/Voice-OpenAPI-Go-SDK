package tts

import (
	"context"
	"encoding/binary"
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

	"google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	"nhooyr.io/websocket"
	reflect "reflect"
	unsafe "unsafe"
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
	b, err := EncodeClient(Frame{Event: event, SessionID: sessionID, Payload: payload})
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
	return s.Send(ctx, EventStartConnection, "", []byte("{}"))
}
func (s *Stream) StartSession(ctx context.Context, id string, payload []byte) error {
	return s.Send(ctx, EventStartSession, id, payload)
}
func (s *Stream) SendTask(ctx context.Context, id string, payload []byte) error {
	return s.Send(ctx, EventTaskRequest, id, payload)
}
func (s *Stream) FinishSession(ctx context.Context, id string) error {
	return s.Send(ctx, EventFinishSession, id, []byte("{}"))
}
func (s *Stream) FinishConnection(ctx context.Context) error {
	return s.Send(ctx, EventFinishConnection, "", []byte("{}"))
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
		f, e := Decode(b)
		if e != nil {
			s.emit(Event{State: StateFailed, Err: &Error{Transport: "websocket", Message: "invalid server frame", Cause: e}})
			continue
		}
		ev := Event{State: StateConnected, Event: f.Event, ErrorCode: f.ErrorCode, SessionID: f.SessionID, Metadata: f.Payload}
		if f.Event == EventAudio {
			ev.Audio = f.Payload
			ev.Metadata = nil
		}
		if f.Event == EventConnectionFailed || f.Event == EventSessionFailed || f.ErrorCode != 0 {
			failure := newStreamError("websocket", f.Event, f.ErrorCode, string(f.Payload), f.Payload)
			ev.Err = failure
			ev.ErrorCode = failure.BusinessCode
		}
		if ev.Err != nil {
			s.markFailed(ev.Err)
		}
		if f.Event == EventConnectionFinished {
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
	stream    TtsStreamService_StreamTtsClient
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
	stream, err := NewTtsStreamServiceClient(conn).StreamTts(streamCtx)
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
	b, e := EncodeClient(Frame{Event: event, SessionID: sessionID, Payload: payload})
	if e != nil {
		return e
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		s.mu.Lock()
		defer s.mu.Unlock()
		req := &TtsStreamRequest{Payload: b}
		if !s.sentFirst {
			req.RequestId = s.requestID
			req.ApiKey = s.apiKey
			s.sentFirst = true
		}
		return s.stream.Send(req)
	}
}
func (s *GRPCStream) StartConnection(ctx context.Context) error {
	return s.Send(ctx, EventStartConnection, "", []byte("{}"))
}
func (s *GRPCStream) StartSession(ctx context.Context, id string, p []byte) error {
	return s.Send(ctx, EventStartSession, id, p)
}
func (s *GRPCStream) SendTask(ctx context.Context, id string, p []byte) error {
	return s.Send(ctx, EventTaskRequest, id, p)
}
func (s *GRPCStream) FinishSession(ctx context.Context, id string) error {
	return s.Send(ctx, EventFinishSession, id, []byte("{}"))
}
func (s *GRPCStream) FinishConnection(ctx context.Context) error {
	return s.Send(ctx, EventFinishConnection, "", []byte("{}"))
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
		f, e := Decode(r.Payload)
		if e != nil {
			s.events <- Event{State: StateFailed, Err: e}
			continue
		}
		ev := Event{State: StateConnected, Event: f.Event, ErrorCode: f.ErrorCode, SessionID: f.SessionID, Metadata: f.Payload}
		if f.Event == EventAudio {
			ev.Audio = f.Payload
			ev.Metadata = nil
		}
		if f.ErrorCode != 0 || f.Event == EventSessionFailed || f.Event == EventConnectionFailed {
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

// Binary protocol implementation retained inside the public TTS module.
const (
	messageFullClient = 1
	messageFullServer = 9
	messageAudioOnly  = 11
	messageError      = 15
	flagWithEvent     = 4

	EventStartConnection    = 1
	EventFinishConnection   = 2
	EventConnectionStarted  = 50
	EventConnectionFailed   = 51
	EventConnectionFinished = 52
	EventStartSession       = 100
	EventFinishSession      = 102
	EventSessionStarted     = 150
	EventSessionFinished    = 152
	EventSessionFailed      = 153
	EventTaskRequest        = 200
	EventSentenceStart      = 350
	EventSentenceEnd        = 351
	EventAudio              = 352
)

// Frame is a decoded protocol message. Payload is audio for audio messages and JSON for metadata/errors.
type Frame struct {
	MessageType  int
	Event        int
	SessionID    string
	ConnectionID string
	ErrorCode    int
	Payload      []byte
}

func EncodeClient(frame Frame) ([]byte, error) {
	if frame.Event == 0 {
		return nil, fmt.Errorf("tts client frame requires an event")
	}
	if len(frame.SessionID) > int(^uint32(0)) || len(frame.Payload) > int(^uint32(0)) {
		return nil, fmt.Errorf("tts frame is too large")
	}
	result := make([]byte, 4+4)
	result[0] = 0x11 // protocol version 1, four-byte header
	result[1] = byte(messageFullClient<<4 | flagWithEvent)
	result[2] = 0x10 // JSON serialization, no compression
	binary.BigEndian.PutUint32(result[4:], uint32(frame.Event))
	if frame.SessionID != "" {
		id := []byte(frame.SessionID)
		tail := make([]byte, 4+len(id))
		binary.BigEndian.PutUint32(tail, uint32(len(id)))
		copy(tail[4:], id)
		result = append(result, tail...)
	}
	payloadSize := make([]byte, 4)
	binary.BigEndian.PutUint32(payloadSize, uint32(len(frame.Payload)))
	result = append(result, payloadSize...)
	result = append(result, frame.Payload...)
	return result, nil
}

func Decode(data []byte) (Frame, error) {
	if len(data) < 4 {
		return Frame{}, fmt.Errorf("tts frame shorter than header")
	}
	headerSize := int(data[0]&0x0f) * 4
	if data[0]>>4 != 1 || headerSize < 4 || len(data) < headerSize {
		return Frame{}, fmt.Errorf("invalid tts frame header")
	}
	f := Frame{MessageType: int(data[1] >> 4)}
	offset := headerSize
	if f.MessageType == messageError {
		if len(data) < offset+8 {
			return Frame{}, fmt.Errorf("truncated tts error frame")
		}
		f.ErrorCode = int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
		return readPayload(data, offset, f)
	}
	if f.MessageType != messageFullServer && f.MessageType != messageAudioOnly && f.MessageType != messageFullClient {
		return Frame{}, fmt.Errorf("unsupported tts message type %d", f.MessageType)
	}
	if data[1]&0x0f == flagWithEvent {
		if len(data) < offset+4 {
			return Frame{}, fmt.Errorf("truncated tts event")
		}
		f.Event = int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
	}
	if f.Event == EventConnectionStarted || f.Event == EventConnectionFailed {
		if len(data) < offset+4 {
			return Frame{}, fmt.Errorf("truncated tts connection id size")
		}
		n := int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
		if n < 0 || len(data) < offset+n {
			return Frame{}, fmt.Errorf("truncated tts connection id")
		}
		f.ConnectionID = string(data[offset : offset+n])
		offset += n
		return readPayload(data, offset, f)
	}
	if len(data) < offset+4 {
		return Frame{}, fmt.Errorf("truncated tts session id size")
	}
	idSize := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	if idSize < 0 || len(data) < offset+idSize {
		return Frame{}, fmt.Errorf("truncated tts session id")
	}
	f.SessionID = string(data[offset : offset+idSize])
	offset += idSize
	return readPayload(data, offset, f)
}

func readPayload(data []byte, offset int, f Frame) (Frame, error) {
	if len(data) == offset {
		return f, nil
	}
	if len(data) < offset+4 {
		return Frame{}, fmt.Errorf("truncated tts payload size")
	}
	n := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	if n < 0 || len(data) < offset+n {
		return Frame{}, fmt.Errorf("truncated tts payload")
	}
	f.Payload = append([]byte(nil), data[offset:offset+n]...)
	return f, nil
}

// Protobuf and gRPC bindings retained inside the public TTS module.
const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type TtsStreamRequest struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	RequestId     string                 `protobuf:"bytes,1,opt,name=request_id,json=requestId,proto3" json:"request_id,omitempty"`
	ApiKey        string                 `protobuf:"bytes,2,opt,name=api_key,json=apiKey,proto3" json:"api_key,omitempty"`
	Payload       []byte                 `protobuf:"bytes,3,opt,name=payload,proto3" json:"payload,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *TtsStreamRequest) Reset() {
	*x = TtsStreamRequest{}
	mi := &file_proto_lychee_openapi_tts_tts_stream_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *TtsStreamRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TtsStreamRequest) ProtoMessage() {}

func (x *TtsStreamRequest) ProtoReflect() protoreflect.Message {
	mi := &file_proto_lychee_openapi_tts_tts_stream_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TtsStreamRequest.ProtoReflect.Descriptor instead.
func (*TtsStreamRequest) Descriptor() ([]byte, []int) {
	return file_proto_lychee_openapi_tts_tts_stream_proto_rawDescGZIP(), []int{0}
}

func (x *TtsStreamRequest) GetRequestId() string {
	if x != nil {
		return x.RequestId
	}
	return ""
}

func (x *TtsStreamRequest) GetApiKey() string {
	if x != nil {
		return x.ApiKey
	}
	return ""
}

func (x *TtsStreamRequest) GetPayload() []byte {
	if x != nil {
		return x.Payload
	}
	return nil
}

type TtsStreamResponse struct {
	state         protoimpl.MessageState `protogen:"open.v1"`
	RequestId     string                 `protobuf:"bytes,1,opt,name=request_id,json=requestId,proto3" json:"request_id,omitempty"`
	Payload       []byte                 `protobuf:"bytes,2,opt,name=payload,proto3" json:"payload,omitempty"`
	unknownFields protoimpl.UnknownFields
	sizeCache     protoimpl.SizeCache
}

func (x *TtsStreamResponse) Reset() {
	*x = TtsStreamResponse{}
	mi := &file_proto_lychee_openapi_tts_tts_stream_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *TtsStreamResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TtsStreamResponse) ProtoMessage() {}

func (x *TtsStreamResponse) ProtoReflect() protoreflect.Message {
	mi := &file_proto_lychee_openapi_tts_tts_stream_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TtsStreamResponse.ProtoReflect.Descriptor instead.
func (*TtsStreamResponse) Descriptor() ([]byte, []int) {
	return file_proto_lychee_openapi_tts_tts_stream_proto_rawDescGZIP(), []int{1}
}

func (x *TtsStreamResponse) GetRequestId() string {
	if x != nil {
		return x.RequestId
	}
	return ""
}

func (x *TtsStreamResponse) GetPayload() []byte {
	if x != nil {
		return x.Payload
	}
	return nil
}

var File_proto_lychee_openapi_tts_tts_stream_proto protoreflect.FileDescriptor

const file_proto_lychee_openapi_tts_tts_stream_proto_rawDesc = "" +
	"\n" +
	")proto/lychee/openapi/tts/tts_stream.proto\x12\x12lychee.openapi.tts\"d\n" +
	"\x10TtsStreamRequest\x12\x1d\n" +
	"\n" +
	"request_id\x18\x01 \x01(\tR\trequestId\x12\x17\n" +
	"\aapi_key\x18\x02 \x01(\tR\x06apiKey\x12\x18\n" +
	"\apayload\x18\x03 \x01(\fR\apayload\"L\n" +
	"\x11TtsStreamResponse\x12\x1d\n" +
	"\n" +
	"request_id\x18\x01 \x01(\tR\trequestId\x12\x18\n" +
	"\apayload\x18\x02 \x01(\fR\apayload2p\n" +
	"\x10TtsStreamService\x12\\\n" +
	"\tStreamTts\x12$.lychee.openapi.tts.TtsStreamRequest\x1a%.lychee.openapi.tts.TtsStreamResponse(\x010\x01B=Z;github.com/lycheeAIc/voice-openapi-go-sdk/tts/grpcpb;grpcpbb\x06proto3"

var (
	file_proto_lychee_openapi_tts_tts_stream_proto_rawDescOnce sync.Once
	file_proto_lychee_openapi_tts_tts_stream_proto_rawDescData []byte
)

func file_proto_lychee_openapi_tts_tts_stream_proto_rawDescGZIP() []byte {
	file_proto_lychee_openapi_tts_tts_stream_proto_rawDescOnce.Do(func() {
		file_proto_lychee_openapi_tts_tts_stream_proto_rawDescData = protoimpl.X.CompressGZIP(unsafe.Slice(unsafe.StringData(file_proto_lychee_openapi_tts_tts_stream_proto_rawDesc), len(file_proto_lychee_openapi_tts_tts_stream_proto_rawDesc)))
	})
	return file_proto_lychee_openapi_tts_tts_stream_proto_rawDescData
}

var file_proto_lychee_openapi_tts_tts_stream_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_proto_lychee_openapi_tts_tts_stream_proto_goTypes = []any{
	(*TtsStreamRequest)(nil),  // 0: lychee.openapi.tts.TtsStreamRequest
	(*TtsStreamResponse)(nil), // 1: lychee.openapi.tts.TtsStreamResponse
}
var file_proto_lychee_openapi_tts_tts_stream_proto_depIdxs = []int32{
	0, // 0: lychee.openapi.tts.TtsStreamService.StreamTts:input_type -> lychee.openapi.tts.TtsStreamRequest
	1, // 1: lychee.openapi.tts.TtsStreamService.StreamTts:output_type -> lychee.openapi.tts.TtsStreamResponse
	1, // [1:2] is the sub-list for method output_type
	0, // [0:1] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_proto_lychee_openapi_tts_tts_stream_proto_init() }
func file_proto_lychee_openapi_tts_tts_stream_proto_init() {
	if File_proto_lychee_openapi_tts_tts_stream_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_proto_lychee_openapi_tts_tts_stream_proto_rawDesc), len(file_proto_lychee_openapi_tts_tts_stream_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_proto_lychee_openapi_tts_tts_stream_proto_goTypes,
		DependencyIndexes: file_proto_lychee_openapi_tts_tts_stream_proto_depIdxs,
		MessageInfos:      file_proto_lychee_openapi_tts_tts_stream_proto_msgTypes,
	}.Build()
	File_proto_lychee_openapi_tts_tts_stream_proto = out.File
	file_proto_lychee_openapi_tts_tts_stream_proto_goTypes = nil
	file_proto_lychee_openapi_tts_tts_stream_proto_depIdxs = nil
}

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	TtsStreamService_StreamTts_FullMethodName = "/lychee.openapi.tts.TtsStreamService/StreamTts"
)

// TtsStreamServiceClient is the client API for TtsStreamService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type TtsStreamServiceClient interface {
	StreamTts(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[TtsStreamRequest, TtsStreamResponse], error)
}

type ttsStreamServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewTtsStreamServiceClient(cc grpc.ClientConnInterface) TtsStreamServiceClient {
	return &ttsStreamServiceClient{cc}
}

func (c *ttsStreamServiceClient) StreamTts(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[TtsStreamRequest, TtsStreamResponse], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &TtsStreamService_ServiceDesc.Streams[0], TtsStreamService_StreamTts_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[TtsStreamRequest, TtsStreamResponse]{ClientStream: stream}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type TtsStreamService_StreamTtsClient = grpc.BidiStreamingClient[TtsStreamRequest, TtsStreamResponse]

// TtsStreamServiceServer is the server API for TtsStreamService service.
// All implementations must embed UnimplementedTtsStreamServiceServer
// for forward compatibility.
type TtsStreamServiceServer interface {
	StreamTts(grpc.BidiStreamingServer[TtsStreamRequest, TtsStreamResponse]) error
	mustEmbedUnimplementedTtsStreamServiceServer()
}

// UnimplementedTtsStreamServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedTtsStreamServiceServer struct{}

func (UnimplementedTtsStreamServiceServer) StreamTts(grpc.BidiStreamingServer[TtsStreamRequest, TtsStreamResponse]) error {
	return status.Error(codes.Unimplemented, "method StreamTts not implemented")
}
func (UnimplementedTtsStreamServiceServer) mustEmbedUnimplementedTtsStreamServiceServer() {}
func (UnimplementedTtsStreamServiceServer) testEmbeddedByValue()                          {}

// UnsafeTtsStreamServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to TtsStreamServiceServer will
// result in compilation errors.
type UnsafeTtsStreamServiceServer interface {
	mustEmbedUnimplementedTtsStreamServiceServer()
}

func RegisterTtsStreamServiceServer(s grpc.ServiceRegistrar, srv TtsStreamServiceServer) {
	// If the following call panics, it indicates UnimplementedTtsStreamServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&TtsStreamService_ServiceDesc, srv)
}

func _TtsStreamService_StreamTts_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(TtsStreamServiceServer).StreamTts(&grpc.GenericServerStream[TtsStreamRequest, TtsStreamResponse]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type TtsStreamService_StreamTtsServer = grpc.BidiStreamingServer[TtsStreamRequest, TtsStreamResponse]

// TtsStreamService_ServiceDesc is the grpc.ServiceDesc for TtsStreamService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var TtsStreamService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "lychee.openapi.tts.TtsStreamService",
	HandlerType: (*TtsStreamServiceServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "StreamTts",
			Handler:       _TtsStreamService_StreamTts_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "proto/tts_stream.proto",
}
