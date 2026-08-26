# Go SDK 接入指南

## 配置

`tts.Config` 需要 `APIKey`，HTTP/WebSocket 使用 `BaseURL`，gRPC 使用 `GRPCAddress`。`context.Context` 可取消任意请求或流。

| 配置 | 含义 |
| --- | --- |
| `ConnectTimeout` | WebSocket/gRPC 建连上限 |
| `ReadTimeout` | HTTP 响应或 gRPC 流 context 上限 |
| `WriteTimeout` | WebSocket 单帧写入上限 |
| `IdleTimeout` | WebSocket 无服务端帧等待上限；gRPC keepalive 周期 |
| `Retry` | 最大次数、退避和自定义可重试判定 |

默认只重试网络错误、HTTP 429、HTTP 5xx；认证、参数、余额等错误不重试。含不可重读 `audio` 的 HTTP 请求不会自动重放。

## HTTP 参数

`SynthesisRequest` 支持 `Text`、`SpeakerID`、`Audio`、`AudioType`、`Speed`、`Volume`、`SampleRate`、`TextNormalizer`。返回 `AudioStream`，可直接用 `io.Copy` 保存音频，并读取 `RequestID` 和音频 header。

## 流式事件

WebSocket 与 gRPC 都通过 `Events() <-chan tts.Event` 接收事件。

| 事件 | 说明 |
| --- | --- |
| 50 | ConnectionStarted |
| 51 | ConnectionFailed |
| 150 | SessionStarted |
| 152 | SessionFinished |
| 153 | SessionFailed |
| 350 / 351 | 句子开始 / 结束 |
| 352 | 音频分片，读取 `Event.Audio` |
| 52 | ConnectionFinished |

收到 51 或 153 时检查 `Event.Err`、`Event.ErrorCode`；流状态变为 `StateFailed`。成功关闭为 `StateClosed`。不要自动重放已经发送过 `TaskRequest(200)` 的任务，以免重复合成或计费。

## 排错

- 401：检查 API Key；
- 429 或 5xx：可由默认重试处理；
- 收到空音频：确认 `speaker_id`、文本和会话完成事件；
- 空闲超时：提高 `IdleTimeout`，或确认服务端是否持续输出帧；
- gRPC 直连开发端口使用明文连接，生产环境应按部署方式配置 TLS。
