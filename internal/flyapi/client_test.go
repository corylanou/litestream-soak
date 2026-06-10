package flyapi

import (
	"fmt"
	"net/http"
	"testing"
)

func TestIsNotFound(t *testing.T) {
	t.Parallel()

	err := fmt.Errorf("wrapped: %w", &APIError{
		StatusCode: http.StatusNotFound,
		Body:       "not found",
	})
	if !IsNotFound(err) {
		t.Fatal("IsNotFound() = false, want true")
	}
}

func TestIsNotFoundRejectsOtherStatus(t *testing.T) {
	t.Parallel()

	err := &APIError{
		StatusCode: http.StatusInternalServerError,
		Body:       "server error",
	}
	if IsNotFound(err) {
		t.Fatal("IsNotFound() = true, want false")
	}
}
