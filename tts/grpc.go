package tts

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/lycheeAIc/voice-openapi-go-sdk/internal/protocol"
	"github.com/lycheeAIc/voice-openapi-go-sdk/tts/grpcpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"io"
	"sync"
	"time"
)

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
		if (f.Event == protocol.EventConnectionFailed || f.Event == protocol.EventSessionFailed) && ev.ErrorCode == 0 {
			var meta struct {
				ErrorCode    int    `json:"error_code"`
				ErrorMessage string `json:"error_message"`
			}
			if json.Unmarshal(f.Payload, &meta) == nil {
				ev.ErrorCode = meta.ErrorCode
				if meta.ErrorMessage != "" {
					ev.Err = &Error{Transport: "grpc", Event: f.Event, BusinessCode: meta.ErrorCode, Message: meta.ErrorMessage}
				}
			}
		}
		if f.Event == protocol.EventAudio {
			ev.Audio = f.Payload
			ev.Metadata = nil
		}
		if (f.ErrorCode != 0 || f.Event == protocol.EventSessionFailed || f.Event == protocol.EventConnectionFailed) && ev.Err == nil {
			ev.Err = &Error{Transport: "grpc", Event: f.Event, BusinessCode: f.ErrorCode, Message: string(f.Payload)}
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
