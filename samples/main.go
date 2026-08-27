package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/lycheeAIc/voice-openapi-go-sdk/tts"
	"nhooyr.io/websocket"
)

const (
	defaultBaseURL     = "https://voice.lycheeai.net/openapi"
	defaultGRPCAddress = "voice.lycheeai.net:46060"
)

func main() {
	transport := flag.String("transport", "http", "http, ws, or grpc")
	text := flag.String("text", "你好，这是 Voice OpenAPI Go SDK 示例。", "text to synthesize")
	speaker := flag.String("speaker", "", "speaker_id")
	output := flag.String("output", "output.mp3", "output audio path")
	flag.Parse()

	loadDotEnv(".env")
	if *speaker == "" {
		*speaker = os.Getenv("VOICE_OPENAPI_SPEAKER_ID")
	}
	apiKey := os.Getenv("VOICE_OPENAPI_API_KEY")
	if apiKey == "" || apiKey == "your-api-key-here" {
		log.Fatal("请先在 samples/.env 中填写 VOICE_OPENAPI_API_KEY")
	}
	cfg := tts.Config{
		BaseURL:     defaultBaseURL,
		GRPCAddress: defaultGRPCAddress,
		APIKey:      apiKey,
		ReadTimeout: 100 * time.Second,
		Retry:       tts.RetryPolicy{MaxAttempts: 3, InitialBackoff: 300 * time.Millisecond, MaxBackoff: 3 * time.Second, Jitter: .2},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	switch *transport {
	case "http":
		if err := runHTTP(ctx, cfg, *text, *speaker, *output); err != nil {
			log.Fatal(err)
		}
	case "ws":
		if *speaker == "" {
			log.Fatal("-speaker is required for ws/grpc")
		}
		stream, err := cfg.Connect(ctx)
		if err != nil {
			log.Fatal(err)
		}
		defer stream.Close(websocket.StatusNormalClosure, "sample complete")
		if err := runDuplex(ctx, stream, *text, *speaker, *output); err != nil {
			log.Fatal(err)
		}
	case "grpc":
		if *speaker == "" {
			log.Fatal("-speaker is required for ws/grpc")
		}
		stream, err := cfg.OpenGRPC(ctx)
		if err != nil {
			log.Fatal(err)
		}
		defer stream.Close()
		if err := runDuplex(ctx, stream, *text, *speaker, *output); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatalf("unsupported transport %q", *transport)
	}
}

type duplexStream interface {
	Events() <-chan tts.Event
	StartConnection(context.Context) error
	StartSession(context.Context, string, []byte) error
	SendTask(context.Context, string, []byte) error
	FinishSession(context.Context, string) error
	FinishConnection(context.Context) error
}

func runDuplex(ctx context.Context, stream duplexStream, text, speaker, output string) error {
	sessionID := fmt.Sprintf("sample%d", time.Now().UnixNano())
	if err := stream.StartConnection(ctx); err != nil {
		return err
	}
	var audio []byte
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-stream.Events():
			if !ok {
				return writeAudio(output, audio)
			}
			if event.Err != nil {
				return event.Err
			}
			switch event.Event {
			case 50:
				payload, _ := json.Marshal(map[string]any{"event": 100, "codec": "mp3", "sample_rate": 24000, "speaker_id": speaker, "text_normalizer": true})
				if err := stream.StartSession(ctx, sessionID, payload); err != nil {
					return err
				}
			case 150:
				payload, _ := json.Marshal(map[string]any{"event": 200, "text": text, "text_normalizer": true})
				if err := stream.SendTask(ctx, sessionID, payload); err != nil {
					return err
				}
				if err := stream.FinishSession(ctx, sessionID); err != nil {
					return err
				}
			case 352:
				audio = append(audio, event.Audio...)
			case 152:
				if err := stream.FinishConnection(ctx); err != nil {
					return err
				}
			case 52:
				return writeAudio(output, audio)
			}
		}
	}
}

func runHTTP(ctx context.Context, cfg tts.Config, text, speaker, output string) error {
	stream, err := cfg.SynthesizeStream(ctx, tts.SynthesisRequest{Text: text, SpeakerID: speaker, AudioType: "mp3"})
	if err != nil {
		return err
	}
	defer stream.Close()
	file, err := os.Create(output)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err = io.Copy(file, stream); err != nil {
		return err
	}
	fmt.Printf("saved %s (request_id=%s, type=%s)\n", output, stream.RequestID, stream.AudioType)
	return nil
}

func writeAudio(output string, audio []byte) error {
	if err := os.WriteFile(output, audio, 0644); err != nil {
		return err
	}
	fmt.Printf("saved %s (%d bytes)\n", output, len(audio))
	return nil
}

func loadDotEnv(name string) {
	file, err := os.Open(name)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		log.Printf("load %s: %v", name, err)
		return
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if ok && os.Getenv(key) == "" {
			_ = os.Setenv(strings.TrimSpace(key), strings.TrimSpace(value))
		}
	}
}
