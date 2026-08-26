package tts

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestDefaultRetrySkipsDeterministicHTTPFailures(t *testing.T) {
	attempts := 0
	err := retry(context.Background(), RetryPolicy{MaxAttempts: 3}, func() error {
		attempts++
		return &Error{Transport: "http", HTTPStatus: http.StatusUnauthorized}
	})
	if err == nil || attempts != 1 {
		t.Fatalf("attempts=%d, want 1", attempts)
	}
}

func TestDefaultRetryRetriesNetworkFailure(t *testing.T) {
	attempts := 0
	err := retry(context.Background(), RetryPolicy{MaxAttempts: 2, InitialBackoff: time.Millisecond}, func() error {
		attempts++
		if attempts == 1 {
			return timeoutError{}
		}
		return nil
	})
	if err != nil || attempts != 2 {
		t.Fatalf("err=%v attempts=%d, want nil/2", err, attempts)
	}
}

func TestTimeoutContext(t *testing.T) {
	ctx, cancel := timeoutContext(context.Background(), time.Millisecond)
	defer cancel()
	<-ctx.Done()
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("got %v", ctx.Err())
	}
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "temporary timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }
