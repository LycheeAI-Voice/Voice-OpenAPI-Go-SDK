package tts

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/lycheeAIc/voice-openapi-go-sdk/internal/protocol"
	"github.com/lycheeAIc/voice-openapi-go-sdk/tts/grpcpb"
	"google.golang.org/grpc"
)

type testTTSGRPCServer struct {
	grpcpb.UnimplementedTtsStreamServiceServer
}

type testTTSGRPCFailureServer struct {
	grpcpb.UnimplementedTtsStreamServiceServer
}

func (testTTSGRPCFailureServer) StreamTts(stream grpcpb.TtsStreamService_StreamTtsServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	return stream.Send(&grpcpb.TtsStreamResponse{Payload: testServerFrame(protocol.EventSessionFailed, "session-grpc", []byte(`{"code":1500,"message":"TTS 算法未返回音频","type":"ALGORITHM_ERROR","retryable":true,"request_id":"req-grpc-1","session_id":"session-grpc","upstream_code":55000000,"error_code":1500,"error_message":"TTS 算法未返回音频"}`))})
}

func (testTTSGRPCServer) StreamTts(stream grpcpb.TtsStreamService_StreamTtsServer) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	if request.GetApiKey() != "test-key" || request.GetRequestId() == "" {
		return nil
	}
	frame, err := protocol.Decode(request.GetPayload())
	if err != nil || frame.Event != protocol.EventStartConnection {
		return err
	}
	response := make([]byte, 18)
	response[0], response[1], response[2] = 0x11, 0x94, 0x10
	binary.BigEndian.PutUint32(response[4:], protocol.EventConnectionStarted)
	binary.BigEndian.PutUint32(response[8:], 0)
	binary.BigEndian.PutUint32(response[12:], 2)
	copy(response[16:], "{}")
	if err := stream.Send(&grpcpb.TtsStreamResponse{Payload: response}); err != nil {
		return err
	}
	if err := stream.Send(&grpcpb.TtsStreamResponse{Payload: testServerFrame(protocol.EventAudio, "session", []byte("one"))}); err != nil {
		return err
	}
	return stream.Send(&grpcpb.TtsStreamResponse{Payload: testServerFrame(protocol.EventAudio, "session", []byte("two"))})
}

func TestGRPCAudioChunksArriveInOrder(t *testing.T) {
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	defer listener.Close()
	server := grpc.NewServer()
	grpcpb.RegisterTtsStreamServiceServer(server, testTTSGRPCServer{})
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

func TestGRPCFirstFrameAndConnectionEvent(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	grpcpb.RegisterTtsStreamServiceServer(server, testTTSGRPCServer{})
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
		if event.Event != protocol.EventConnectionStarted || event.Err != nil {
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
	grpcpb.RegisterTtsStreamServiceServer(server, testTTSGRPCFailureServer{})
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
