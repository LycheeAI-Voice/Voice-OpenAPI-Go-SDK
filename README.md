# Voice OpenAPI Go SDK

lychee 语音开放平台 **TTS 流式合成**的官方 Go 客户端 SDK，支持三种传输方式：

- **HTTP 流式**：`POST /tts/infer-stream`，单段文本，直接读取音频流
- **WebSocket**：`/tts/ws_binary/v2`，长连接、边发边收、连续多段合成
- **gRPC**：`TtsStreamService/StreamTts`，服务端双向流

> 📖 完整接口文档（请求参数、事件协议、错误码、计费）：**[https://voice-api-4an.pages.dev](https://voice-api-4an.pages.dev)**

> 范围：本 SDK 目前仅封装 TTS 流式合成三通道；平台其余能力（ASR、视频压制、字幕翻译等）请直接调用接口文档中的 HTTP 接口。

## 安装

```bash
go get github.com/lycheeAIc/voice-openapi-go-sdk@v0.1.2
```

```go
import "github.com/lycheeAIc/voice-openapi-go-sdk/tts"
```

要求 Go 1.26+（见 `go.mod`）。`v0.1.1` 已废弃，请使用 `v0.1.2`。

## 快速开始

```go
package main

import (
	"context"
	"io"
	"log"
	"os"

	"github.com/lycheeAIc/voice-openapi-go-sdk/tts"
)

func main() {
	cfg := tts.Config{
		BaseURL: os.Getenv("VOICE_OPENAPI_BASE_URL"),
		APIKey:  os.Getenv("VOICE_OPENAPI_API_KEY"),
	}
	stream, err := cfg.SynthesizeStream(context.Background(), tts.SynthesisRequest{
		Text:      "你好，这是流式语音合成。",
		SpeakerID: os.Getenv("VOICE_OPENAPI_SPEAKER_ID"),
		AudioType: "mp3",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer stream.Close()

	out, err := os.Create("output.mp3")
	if err != nil {
		log.Fatal(err)
	}
	defer out.Close()
	if _, err = io.Copy(out, stream); err != nil {
		log.Fatal(err)
	}
}
```

## 配置

```go
cfg := tts.Config{
	BaseURL:        "https://…",                        // HTTP / WebSocket 必填
	GRPCAddress:    "voice.lycheeai.com.cn:443",        // gRPC 必填
	APIKey:         os.Getenv("VOICE_OPENAPI_API_KEY"), // 必填
	ConnectTimeout: 10 * time.Second,
	ReadTimeout:    100 * time.Second,
	WriteTimeout:   10 * time.Second,
	IdleTimeout:    30 * time.Second,
	Retry: tts.RetryPolicy{
		MaxAttempts:    3,
		InitialBackoff: 200 * time.Millisecond,
		MaxBackoff:     2 * time.Second,
		Multiplier:     2,
		Jitter:         0.2,
	},
}
```

| 配置 | 含义 |
| --- | --- |
| `ConnectTimeout` | 建连上限（WebSocket / gRPC） |
| `ReadTimeout` | HTTP 请求、gRPC 流的上限 |
| `WriteTimeout` | WebSocket 单帧写入上限 |
| `IdleTimeout` | WebSocket 无服务端帧等待上限；gRPC keepalive 周期 |
| `Retry` | 最大次数、退避与自定义可重试判定 |

`context.Context` 可随时取消任意请求或流。

### 重试语义

- 默认只重试**网络错误、HTTP 429、HTTP 5xx**；鉴权、参数、余额等确定性错误不重试；
- 重试只发生在建连/请求阶段，**不会重放已发送的合成任务**（避免重复计费）；
- 含参考音频的 HTTP 请求不可重读，不自动重试。

## 三种传输方式

### HTTP 流式

`cfg.SynthesizeStream(ctx, SynthesisRequest)` 返回 `*AudioStream`（`io.ReadCloser`，可直接 `io.Copy` 落盘），并携带 `RequestID` 与音频格式信息。`SynthesisRequest` 支持 `Text`、`SpeakerID`、`Audio`、`AudioType`、`Speed`、`Volume`、`SampleRate`、`TextNormalizer`，字段含义见[接口文档](https://voice-api-4an.pages.dev)。

### WebSocket

`cfg.Connect(ctx)` 返回 `*Stream`，通过 `Events() <-chan Event` 接收服务端事件。

#### Event 定义（与接口文档一致）

| Event | 说明 | 方向 |
| --- | --- | --- |
| 1 | StartConnection，建立连接 | 上行 |
| 2 | FinishConnection，结束连接 | 上行 |
| 50 | ConnectionStarted，建连成功 | 下行 |
| 51 | ConnectionFailed，建连失败 | 下行 |
| 52 | ConnectionFinished，连接结束 | 下行 |
| 100 | StartSession，开始会话 | 上行 |
| 102 | FinishSession，结束会话 | 上行 |
| 150 | SessionStarted，会话开始 | 下行 |
| 152 | SessionFinished，会话结束 | 下行 |
| 153 | SessionFailed，会话失败 | 下行 |
| 200 | TaskRequest，发送文本请求 | 上行 |
| 350 | TTSSentenceStart，句子开始 | 下行 |
| 351 | TTSSentenceEnd，句子结束 | 下行 |
| 352 | TTSResponse，音频数据响应 | 下行 |

#### 连接流程

```text
1. 建立 WebSocket 连接，携带 api_key 请求头
2. 发送 EVENT_StartConnection (1)
3. 收到 EVENT_ConnectionStarted (50) 后，发送 EVENT_StartSession (100)（包含 speaker_id）
4. 收到 EVENT_SessionStarted (150) 后，发送 EVENT_TaskRequest (200)（包含文本）
5. 接收 EVENT_TTSResponse (352) 获取音频数据
6. 发送 EVENT_FinishSession (102) 结束会话
7. 发送 EVENT_FinishConnection (2) 结束连接
```

事件流转简图：

```text
StartConnection (1) → ConnectionStarted (50)
StartSession (100)  → SessionStarted (150)
TaskRequest (200，可多次) → 持续接收音频 (352) → SessionFinished (152)
FinishConnection (2) → ConnectionFinished (52)
```

提供 `StartConnection`、`StartSession`、`SendTask`、`FinishSession`、`FinishConnection`、`Close`。失败事件为 `ConnectionFailed (51)` 或 `SessionFailed (153)`，检查 `Event.Err` / `Event.ErrorCode`，流状态变为 `StateFailed`。

### gRPC

`cfg.OpenGRPC(ctx)` 返回 `*GRPCStream`，事件 API 与 WebSocket 相同；SDK 自动在首条消息中携带 `api_key`，无需手动处理。

> ⚠️ 当前 gRPC 客户端为明文连接（`insecure`），仅适合开发环境；接口文档要求生产使用 TLS 安全通道，接入前需先补齐 TLS 支持。

## 错误处理

失败统一为 `*tts.Error`（含 `Transport`、`HTTPStatus`、`GRPCCode`、`BusinessCode`、`Event`、`Message`、`Cause`，支持 `errors.As` / `Unwrap`）；流式错误通过事件上报。业务错误码见[接口文档](https://voice-api-4an.pages.dev)。

```go
var e *tts.Error
if errors.As(err, &e) {
	// e.HTTPStatus: 401 检查 API Key；429 / 5xx 可重试
}
```

## 音频格式

- MP3 / Opus：分片可直接拼接；
- WAV：分片需提取每包 PCM 后重新封装文件头；
- PCM：按实际采样率、声道自行写文件头。

## 示例与参考

- 可运行样例：[samples/](samples/)（凭据全部从环境变量读取）
- 接入指南与排错：[SDK_GUIDE.md](SDK_GUIDE.md)
- **在线接口文档：[https://voice-api-4an.pages.dev](https://voice-api-4an.pages.dev)**
- 协议定义：[proto/lychee/openapi/tts/tts_stream.proto](proto/lychee/openapi/tts/tts_stream.proto)
- 变更记录：[CHANGELOG.md](CHANGELOG.md)
