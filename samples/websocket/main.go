package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/lycheeAIc/voice-openapi-go-sdk/tts"
	"nhooyr.io/websocket"
)

func main() {
	ctx := context.Background()
	cfg := tts.Config{BaseURL: os.Getenv("VOICE_OPENAPI_BASE_URL"), APIKey: os.Getenv("VOICE_OPENAPI_API_KEY"), IdleTimeout: 30 * time.Second}
	stream, err := cfg.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer stream.Close(websocket.StatusNormalClosure, "done")
	sessionID := "sample"
	if err = stream.StartConnection(ctx); err != nil {
		log.Fatal(err)
	}
	for event := range stream.Events() {
		if event.Err != nil {
			log.Fatal(event.Err)
		}
		switch event.Event {
		case 50:
			payload, _ := json.Marshal(map[string]any{"event": 100, "codec": "mp3", "sample_rate": 24000, "speaker_id": os.Getenv("VOICE_OPENAPI_SPEAKER_ID")})
			_ = stream.StartSession(ctx, sessionID, payload)
		case 150:
			payload, _ := json.Marshal(map[string]any{"event": 200, "text": "你好，这是 WebSocket 示例。"})
			_ = stream.SendTask(ctx, sessionID, payload)
			_ = stream.FinishSession(ctx, sessionID)
		case 352:
			_, _ = os.Stdout.Write(event.Audio)
		case 152:
			_ = stream.FinishConnection(ctx)
		}
	}
}
