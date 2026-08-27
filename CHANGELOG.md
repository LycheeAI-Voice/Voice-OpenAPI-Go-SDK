# Changelog

## v0.1.0

- Added HTTP, WebSocket, and gRPC TTS streaming clients.
- Added binary protocol handling, event channels, retry policy, and timeout configuration.
- Added HTTP, WebSocket, and gRPC integration tests, including retry, timeout, error, and audio-chunk coverage.
- Unified TTS stream error fields across HTTP, WebSocket, and gRPC.
- Fixed WebSocket terminal-event close sequencing by waiting for the server close frame.
- Consolidated the public `tts` package implementation into `tts/tts.go`.
- Added public samples and SDK usage documentation.
