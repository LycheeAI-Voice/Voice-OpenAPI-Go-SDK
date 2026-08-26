# Voice OpenAPI Go SDK

第三方 Go 客户端，支持 HTTP 流式、WebSocket 双向流和 gRPC 双向流 TTS。

## 安装

```bash
go get github.com/lycheeAIc/voice-openapi-go-sdk@v0.1.0
```

```go
import "github.com/lycheeAIc/voice-openapi-go-sdk/tts"
```

## 接口与用途

| 方式 | 接口 | 适用场景 |
| --- | --- | --- |
| HTTP | `POST /tts/infer-stream` | 单段文本，直接读取音频流 |
| WebSocket | `/tts/ws_binary/v2` | 长连接、边发边收 |
| gRPC | `TtsStreamService/StreamTts` | 服务端双向流调用 |

## 配置、超时与重试

```go
cfg := tts.Config{
    BaseURL: "https://shanhaistudio.lycheeai.com.cn/openapi",
    GRPCAddress: "shanhaistudio.lycheeai.com.cn:443",
    APIKey: os.Getenv("VOICE_OPENAPI_API_KEY"),
    ConnectTimeout: 10 * time.Second, ReadTimeout: 100 * time.Second,
    WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second,
    Retry: tts.RetryPolicy{MaxAttempts: 3},
}
```

默认仅重试网络错误、HTTP 429、HTTP 5xx；不会重试鉴权、参数、余额等确定性错误，也不会重放已发送的合成任务。

## HTTP 流式合成

```go
stream, err := cfg.SynthesizeStream(ctx, tts.SynthesisRequest{
    Text: "你好，这是流式语音合成。", SpeakerID: "your-speaker-id", AudioType: "mp3",
})
if err != nil { log.Fatal(err) }
defer stream.Close()
file, _ := os.Create("output.mp3")
defer file.Close()
_, err = io.Copy(file, stream)
```

支持 `Text`、`SpeakerID`、`Audio`、`AudioType`、`Speed`、`Volume`、`SampleRate`、`TextNormalizer`。

## WebSocket 与 gRPC

WebSocket 使用 `cfg.Connect(ctx)`，gRPC 使用 `cfg.OpenGRPC(ctx)`。两者都通过 `Events()` 接收事件，并提供 `StartConnection`、`StartSession`、`SendTask`、`FinishSession`、`FinishConnection`。

```text
StartConnection (1) → ConnectionStarted (50)
StartSession (100) → SessionStarted (150)
TaskRequest (200，可多次) → FinishSession (102)
TTSResponse (352，持续接收音频) → SessionFinished (152)
FinishConnection (2) → ConnectionFinished (52)
```

失败事件为 `ConnectionFailed (51)` 或 `SessionFailed (153)`。检查 `Event.Err`、`Event.ErrorCode`、`State()` 和 `Err()`。

## 音频格式

MP3 和 Opus 分片可直接拼接。WAV 分片应先提取每包 PCM 后重新封装 WAV；PCM 需要按实际采样率写文件头。

## 参考

- 完整参数、排错说明：[SDK_GUIDE.md](SDK_GUIDE.md)
- 可运行样例：[samples/](samples/)
- 协议定义：[tts_stream.proto](proto/lychee/openapi/tts/tts_stream.proto)
- 变更记录：[CHANGELOG.md](CHANGELOG.md)
