package tts

import (
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
)

type SynthesisRequest struct {
	Text, SpeakerID, AudioType string
	Audio                      io.Reader
	AudioName                  string
	Speed, Volume              *float32
	SampleRate                 *int
	TextNormalizer             *bool
}
type AudioStream struct {
	io.ReadCloser
	RequestID, AudioType, SampleRate, Channels, SampleFormat string
}

func (c Config) SynthesizeStream(ctx context.Context, input SynthesisRequest) (*AudioStream, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(input.Text) == "" {
		return nil, fmt.Errorf("tts: text is required")
	}
	var response *http.Response
	policy := c.Retry
	if input.Audio != nil {
		policy.MaxAttempts = 1 // A generic io.Reader cannot be safely replayed.
	}
	err := retry(ctx, policy, func() error {
		r, err := c.newHTTPStreamRequest(ctx, input)
		if err != nil {
			return err
		}
		response, err = c.httpClient().Do(r)
		if err != nil {
			return &Error{Transport: "http", Message: "request failed", Cause: err}
		}
		if response.StatusCode/100 != 2 {
			response.Body.Close()
			return &Error{Transport: "http", HTTPStatus: response.StatusCode, Message: response.Status}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &AudioStream{ReadCloser: response.Body, RequestID: response.Header.Get("X-Request-Id"), AudioType: response.Header.Get("X-Audio-Type"), SampleRate: response.Header.Get("X-Audio-Sample-Rate"), Channels: response.Header.Get("X-Audio-Channels"), SampleFormat: response.Header.Get("X-Audio-Sample-Format")}, nil
}
func (c Config) newHTTPStreamRequest(ctx context.Context, in SynthesisRequest) (*http.Request, error) {
	pr, pw := io.Pipe()
	w := multipart.NewWriter(pw)
	go func() {
		defer pw.Close()
		defer w.Close()
		_ = w.WriteField("text", in.Text)
		if in.SpeakerID != "" {
			_ = w.WriteField("speaker_id", in.SpeakerID)
		}
		if in.AudioType != "" {
			_ = w.WriteField("audio_type", in.AudioType)
		}
		if in.Speed != nil {
			_ = w.WriteField("speed", strconv.FormatFloat(float64(*in.Speed), 'f', -1, 32))
		}
		if in.Volume != nil {
			_ = w.WriteField("volume", strconv.FormatFloat(float64(*in.Volume), 'f', -1, 32))
		}
		if in.SampleRate != nil {
			_ = w.WriteField("sample_rate", strconv.Itoa(*in.SampleRate))
		}
		if in.TextNormalizer != nil {
			_ = w.WriteField("text_normalizer", strconv.FormatBool(*in.TextNormalizer))
		}
		if in.Audio != nil {
			part, e := w.CreateFormFile("audio", in.AudioName)
			if e == nil {
				_, e = io.Copy(part, in.Audio)
			}
			if e != nil {
				_ = pw.CloseWithError(e)
			}
		}
	}()
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.BaseURL, "/")+"/tts/infer-stream", pr)
	if err != nil {
		return nil, err
	}
	r.Header.Set("Content-Type", w.FormDataContentType())
	r.Header.Set("api_key", c.APIKey)
	return r, nil
}
