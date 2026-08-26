# Samples

所有示例均通过环境变量读取凭据：`VOICE_OPENAPI_API_KEY`、`VOICE_OPENAPI_SPEAKER_ID`；HTTP/WebSocket 还需 `VOICE_OPENAPI_BASE_URL`，gRPC 需 `VOICE_OPENAPI_GRPC_ADDRESS`。

```bash
go run ./samples/http-stream
go run ./samples/websocket > output.mp3
go run ./samples/grpc > output.mp3
```

请勿将真实 API Key 写入示例或提交到版本库。
