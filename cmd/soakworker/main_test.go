package main

import (
	"os"
	"syscall"
	"testing"
)

func TestShutdownSignalsIncludeSIGHUP(t *testing.T) {
	signals := shutdownSignals()
	for _, sig := range signals {
		if sig == syscall.SIGHUP {
			return
		}
	}
	t.Fatalf("shutdownSignals() = %v, want %v included", signals, syscall.SIGHUP)
}

func TestShutdownSignalsKeepInterruptAndSIGTERM(t *testing.T) {
	tests := []os.Signal{os.Interrupt, syscall.SIGTERM}

	for _, want := range tests {
		t.Run(want.String(), func(t *testing.T) {
			signals := shutdownSignals()
			for _, sig := range signals {
				if sig == want {
					return
				}
			}
			t.Fatalf("shutdownSignals() = %v, want %v included", signals, want)
		})
	}
}
