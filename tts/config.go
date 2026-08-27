package tts

import (
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"net/http"
	"time"
)

type RetryPolicy struct {
	MaxAttempts                int
	InitialBackoff, MaxBackoff time.Duration
	Multiplier                 float64
	Jitter                     float64
	Retryable                  func(error) bool
}

func (p RetryPolicy) normalized() RetryPolicy {
	if p.MaxAttempts < 1 {
		p.MaxAttempts = 1
	}
	if p.InitialBackoff <= 0 {
		p.InitialBackoff = 200 * time.Millisecond
	}
	if p.MaxBackoff <= 0 {
		p.MaxBackoff = 5 * time.Second
	}
	if p.Multiplier < 1 {
		p.Multiplier = 2
	}
	return p
}

type Config struct {
	BaseURL, GRPCAddress, APIKey                           string
	HTTPClient                                             *http.Client
	ConnectTimeout, ReadTimeout, WriteTimeout, IdleTimeout time.Duration
	Retry                                                  RetryPolicy
}

func (c Config) validate() error {
	if c.BaseURL == "" && c.GRPCAddress == "" {
		return errors.New("tts: BaseURL or GRPCAddress is required")
	}
	if c.APIKey == "" {
		return errors.New("tts: APIKey is required")
	}
	return nil
}
func (c Config) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	timeout := c.ReadTimeout
	if timeout <= 0 {
		timeout = 100 * time.Second
	}
	return &http.Client{Timeout: timeout}
}
func retry(ctx context.Context, p RetryPolicy, fn func() error) error {
	p = p.normalized()
	var err error
	delay := p.InitialBackoff
	for attempt := 1; attempt <= p.MaxAttempts; attempt++ {
		if err = fn(); err == nil || attempt == p.MaxAttempts || !shouldRetry(p, err) {
			return err
		}
		jitter := 1 + (rand.Float64()*2-1)*p.Jitter
		wait := time.Duration(float64(delay) * jitter)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		delay = time.Duration(float64(delay) * p.Multiplier)
		if delay > p.MaxBackoff {
			delay = p.MaxBackoff
		}
	}
	return err
}

func shouldRetry(p RetryPolicy, err error) bool {
	if p.Retryable != nil {
		return p.Retryable(err)
	}
	if e, ok := err.(*Error); ok && e.HTTPStatus > 0 {
		if e.retryableSet {
			return e.Retryable
		}
		return e.HTTPStatus == http.StatusTooManyRequests || e.HTTPStatus >= 500
	}
	var networkError net.Error
	return errors.As(err, &networkError)
}

func timeoutContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return parent, func() {}
	}
	return context.WithTimeout(parent, timeout)
}
