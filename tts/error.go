package tts

import (
	"encoding/json"
	"fmt"
)

type Error struct {
	Transport                                            string
	HTTPStatus, GRPCCode, CloseCode, BusinessCode, Event int
	RequestID, SessionID, Type, Message                  string
	UpstreamCode                                         int
	Retryable                                            bool
	Cause                                                error
	retryableSet                                         bool
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

type streamErrorPayload struct {
	Code         int    `json:"code"`
	Message      string `json:"message"`
	Type         string `json:"type"`
	Retryable    *bool  `json:"retryable"`
	RequestID    string `json:"request_id"`
	SessionID    string `json:"session_id"`
	UpstreamCode int    `json:"upstream_code"`
	ErrorCode    int    `json:"error_code"`
	ErrorMessage string `json:"error_message"`
}

func parseStreamErrorPayload(payload []byte) (streamErrorPayload, bool) {
	var details streamErrorPayload
	if json.Unmarshal(payload, &details) != nil {
		return streamErrorPayload{}, false
	}
	if details.Code == 0 {
		details.Code = details.ErrorCode
	}
	if details.Message == "" {
		details.Message = details.ErrorMessage
	}
	return details, details.Code != 0 || details.Message != "" || details.Type != "" || details.Retryable != nil
}

func newStreamError(transport string, event, fallbackCode int, fallbackMessage string, payload []byte) *Error {
	details, ok := parseStreamErrorPayload(payload)
	if !ok {
		return &Error{Transport: transport, Event: event, BusinessCode: fallbackCode, Message: fallbackMessage}
	}
	code := details.Code
	if code == 0 {
		code = fallbackCode
	}
	message := details.Message
	if message == "" {
		message = fallbackMessage
	}
	err := &Error{
		Transport: transport, Event: event, BusinessCode: code, Message: message,
		Type: details.Type, RequestID: details.RequestID, SessionID: details.SessionID,
		UpstreamCode: details.UpstreamCode,
	}
	if details.Retryable != nil {
		err.Retryable = *details.Retryable
		err.retryableSet = true
	}
	return err
}
