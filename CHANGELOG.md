# Changelog

## Unreleased

- Added public `samples/http-stream`, `samples/websocket`, and `samples/grpc` examples that read credentials only from environment variables.
- Added SDK integration guide and expanded README with transport selection, configuration, event flow, and audio-format guidance.

## v0.1.2

- Fixed WebSocket client close sequencing: serializes business-frame writes and Close control frames.
- Rejects new WebSocket business frames once a stream enters the closing state.
- Fixed the public module import used by the WebSocket client.

## v0.1.1

- Superseded by v0.1.2 due to an incorrect temporary module import in the WebSocket client. Do not use this version.

## v0.1.0

- Added HTTP, WebSocket, and gRPC TTS streaming clients.
- Added binary protocol handling, event channels, retry policy, and timeout configuration.
- Added HTTP, WebSocket, and gRPC integration tests, including retry, timeout, error, and audio-chunk coverage.
