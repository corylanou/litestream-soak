package orchestrator

import (
	"context"
	"strings"
	"testing"
)

func TestTrimSHA(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"shorter_than_12", "abc123", "abc123"},
		{"exactly_12", "abc123def456", "abc123def456"},
		{"longer_than_12_40char", "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0", "a1b2c3d4e5f6"},
		{"13_chars", "abc123def4567", "abc123def456"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := trimSHA(tc.in)
			if got != tc.want {
				t.Fatalf("trimSHA(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestNotifyDeploymentReadyRejectsInvalidSHA(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sha  string
	}{
		{"empty_sha", ""},
		{"too_short", "abc12"},
		{"non_hex", "xyz!@#$%^&*()"},
		{"whitespace_only", "   "},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			deployer := &Deployer{db: openTestDB(t)}
			_, err := deployer.NotifyDeploymentReady(context.Background(), "main", tc.sha, "", "registry.fly.io/app:latest", "test")
			if err == nil {
				t.Fatalf("NotifyDeploymentReady(%q) succeeded, want error", tc.sha)
			}
		})
	}
}

func TestDeployNewSHARequiresRuntimeBuild(t *testing.T) {
	t.Parallel()

	deployer := &Deployer{db: openTestDB(t), allowRuntimeBuild: false}
	err := deployer.DeployNewSHA("a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0")
	if err == nil || !strings.Contains(err.Error(), "runtime builds are disabled") {
		t.Fatalf("DeployNewSHA() error = %v, want runtime-builds-disabled error", err)
	}
}

func TestResolveLitestreamBuildSHAPassesThroughFullSHA(t *testing.T) {
	t.Parallel()

	full := "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0"
	got, err := resolveLitestreamBuildSHA(context.Background(), full)
	if err != nil {
		t.Fatalf("resolveLitestreamBuildSHA(%q) error = %v", full, err)
	}
	if got != full {
		t.Fatalf("resolveLitestreamBuildSHA(%q) = %q, want passthrough", full, got)
	}
}

func TestDeployNewSHARejectsInvalidSHA(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sha  string
	}{
		{"empty_sha", ""},
		{"too_short", "abc12"},
		{"non_hex", "not-a-sha!"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			deployer := &Deployer{db: openTestDB(t), allowRuntimeBuild: true}
			err := deployer.DeployNewSHA(tc.sha)
			if err == nil {
				t.Fatalf("DeployNewSHA(%q) succeeded, want error", tc.sha)
			}
		})
	}
}
