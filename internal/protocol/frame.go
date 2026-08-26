// Package protocol implements the binary framing used by the public TTS stream APIs.
package protocol

import (
	"encoding/binary"
	"fmt"
)

const (
	messageFullClient = 1
	messageFullServer = 9
	messageAudioOnly  = 11
	messageError      = 15
	flagWithEvent     = 4

	EventStartConnection    = 1
	EventFinishConnection   = 2
	EventConnectionStarted  = 50
	EventConnectionFailed   = 51
	EventConnectionFinished = 52
	EventStartSession       = 100
	EventFinishSession      = 102
	EventSessionStarted     = 150
	EventSessionFinished    = 152
	EventSessionFailed      = 153
	EventTaskRequest        = 200
	EventSentenceStart      = 350
	EventSentenceEnd        = 351
	EventAudio              = 352
)

// Frame is a decoded protocol message. Payload is audio for audio messages and JSON for metadata/errors.
type Frame struct {
	MessageType  int
	Event        int
	SessionID    string
	ConnectionID string
	ErrorCode    int
	Payload      []byte
}

func EncodeClient(frame Frame) ([]byte, error) {
	if frame.Event == 0 {
		return nil, fmt.Errorf("tts client frame requires an event")
	}
	if len(frame.SessionID) > int(^uint32(0)) || len(frame.Payload) > int(^uint32(0)) {
		return nil, fmt.Errorf("tts frame is too large")
	}
	result := make([]byte, 4+4)
	result[0] = 0x11 // protocol version 1, four-byte header
	result[1] = byte(messageFullClient<<4 | flagWithEvent)
	result[2] = 0x10 // JSON serialization, no compression
	binary.BigEndian.PutUint32(result[4:], uint32(frame.Event))
	if frame.SessionID != "" {
		id := []byte(frame.SessionID)
		tail := make([]byte, 4+len(id))
		binary.BigEndian.PutUint32(tail, uint32(len(id)))
		copy(tail[4:], id)
		result = append(result, tail...)
	}
	payloadSize := make([]byte, 4)
	binary.BigEndian.PutUint32(payloadSize, uint32(len(frame.Payload)))
	result = append(result, payloadSize...)
	result = append(result, frame.Payload...)
	return result, nil
}

func Decode(data []byte) (Frame, error) {
	if len(data) < 4 {
		return Frame{}, fmt.Errorf("tts frame shorter than header")
	}
	headerSize := int(data[0]&0x0f) * 4
	if data[0]>>4 != 1 || headerSize < 4 || len(data) < headerSize {
		return Frame{}, fmt.Errorf("invalid tts frame header")
	}
	f := Frame{MessageType: int(data[1] >> 4)}
	offset := headerSize
	if f.MessageType == messageError {
		if len(data) < offset+8 {
			return Frame{}, fmt.Errorf("truncated tts error frame")
		}
		f.ErrorCode = int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
		return readPayload(data, offset, f)
	}
	if f.MessageType != messageFullServer && f.MessageType != messageAudioOnly && f.MessageType != messageFullClient {
		return Frame{}, fmt.Errorf("unsupported tts message type %d", f.MessageType)
	}
	if data[1]&0x0f == flagWithEvent {
		if len(data) < offset+4 {
			return Frame{}, fmt.Errorf("truncated tts event")
		}
		f.Event = int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
	}
	if f.Event == EventConnectionStarted || f.Event == EventConnectionFailed {
		if len(data) < offset+4 {
			return Frame{}, fmt.Errorf("truncated tts connection id size")
		}
		n := int(binary.BigEndian.Uint32(data[offset:]))
		offset += 4
		if n < 0 || len(data) < offset+n {
			return Frame{}, fmt.Errorf("truncated tts connection id")
		}
		f.ConnectionID = string(data[offset : offset+n])
		offset += n
		return readPayload(data, offset, f)
	}
	if len(data) < offset+4 {
		return Frame{}, fmt.Errorf("truncated tts session id size")
	}
	idSize := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	if idSize < 0 || len(data) < offset+idSize {
		return Frame{}, fmt.Errorf("truncated tts session id")
	}
	f.SessionID = string(data[offset : offset+idSize])
	offset += idSize
	return readPayload(data, offset, f)
}

func readPayload(data []byte, offset int, f Frame) (Frame, error) {
	if len(data) == offset {
		return f, nil
	}
	if len(data) < offset+4 {
		return Frame{}, fmt.Errorf("truncated tts payload size")
	}
	n := int(binary.BigEndian.Uint32(data[offset:]))
	offset += 4
	if n < 0 || len(data) < offset+n {
		return Frame{}, fmt.Errorf("truncated tts payload")
	}
	f.Payload = append([]byte(nil), data[offset:offset+n]...)
	return f, nil
}
