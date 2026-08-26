package protocol

import "testing"

func TestEncodeDecodeClientTaskFrame(t *testing.T) {
	input := Frame{Event: EventTaskRequest, SessionID: "session-1", Payload: []byte(`{"text":"hello"}`)}
	encoded, err := EncodeClient(input)
	if err != nil {
		t.Fatalf("EncodeClient: %v", err)
	}
	got, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Event != input.Event || got.SessionID != input.SessionID || string(got.Payload) != string(input.Payload) {
		t.Fatalf("round trip mismatch: %#v", got)
	}
}
