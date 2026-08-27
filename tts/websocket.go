package tts

import (
	"context"
	"github.com/lycheeAIc/voice-openapi-go-sdk/internal/protocol"
	"net/http"
	"nhooyr.io/websocket"
	"strings"
	"sync"
	"time"
)

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
