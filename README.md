# Voice OpenAPI Go SDK

Go SDK for `/tts/infer-stream`, `/tts/ws_binary/v2`, and `TtsStreamService/StreamTts`.

```go
client := tts.Config{BaseURL: "https://api.example.com/openapi", APIKey: os.Getenv("API_KEY"), ReadTimeout: 100*time.Second}
audio, err := client.SynthesizeStream(ctx, tts.SynthesisRequest{Text: "你好", AudioType: "mp3"})
if err != nil { log.Fatal(err) }
defer audio.Close()
_, _ = io.Copy(file, audio)
```

For WebSocket, call `Connect`, invoke `StartConnection`, then consume `Events()` and use `StartSession`, `SendTask`, `FinishSession`, and `FinishConnection`. gRPC offers the same methods through `OpenGRPC`.

`Config.Retry` controls bounded exponential retry only for opening HTTP requests/connections. An already submitted synthesis task is never silently replayed.
