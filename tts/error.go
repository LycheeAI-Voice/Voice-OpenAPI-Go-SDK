package tts

import "fmt"

type Error struct {
	Transport                                            string
	HTTPStatus, GRPCCode, CloseCode, BusinessCode, Event int
	RequestID, Message                                   string
	Cause                                                error
}

func (e *Error) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("tts %s error: %s", e.Transport, e.Message)
	}
	if e.Cause != nil {
		return fmt.Sprintf("tts %s error: %v", e.Transport, e.Cause)
	}
	return "tts error"
}
func (e *Error) Unwrap() error { return e.Cause }
