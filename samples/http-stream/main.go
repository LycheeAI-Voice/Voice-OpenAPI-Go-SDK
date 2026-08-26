package main

import (
	"context"
	"io"
	"log"
	"os"
	"time"

	"github.com/lycheeAIc/voice-openapi-go-sdk/tts"
)

func main() {
	cfg := tts.Config{BaseURL: os.Getenv("VOICE_OPENAPI_BASE_URL"), APIKey: os.Getenv("VOICE_OPENAPI_API_KEY"), ReadTimeout: 100 * time.Second}
	stream, err := cfg.SynthesizeStream(context.Background(), tts.SynthesisRequest{Text: "你好，这是 HTTP 流式合成示例。", SpeakerID: os.Getenv("VOICE_OPENAPI_SPEAKER_ID"), AudioType: "mp3"})
	if err != nil {
		log.Fatal(err)
	}
	defer stream.Close()
	file, err := os.Create("output.mp3")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	if _, err = io.Copy(file, stream); err != nil {
		log.Fatal(err)
	}
}
