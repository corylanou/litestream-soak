package litestreamsoak

import (
	"os"
	"strings"
	"testing"
)

func TestFlyConfigsDeclareHealthzChecks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		file   string
		header string
		want   map[string]string
	}{
		{
			name:   "worker",
			file:   "fly.toml",
			header: "[checks.healthz]",
			want: map[string]string{
				"type":         "http",
				"port":         "9091",
				"method":       "GET",
				"path":         "/healthz",
				"interval":     "30s",
				"timeout":      "5s",
				"grace_period": "5m",
			},
		},
		{
			name:   "control",
			file:   "fly.control.toml",
			header: "[[http_service.checks]]",
			want: map[string]string{
				"method":       "GET",
				"path":         "/healthz",
				"interval":     "30s",
				"timeout":      "5s",
				"grace_period": "30s",
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			contents, err := os.ReadFile(tt.file)
			if err != nil {
				t.Fatalf("read %s: %v", tt.file, err)
			}

			table, ok := tomlTable(string(contents), tt.header)
			if !ok {
				t.Fatalf("%s is missing %s", tt.file, tt.header)
			}

			for key, want := range tt.want {
				got, ok := tomlValue(table, key)
				if !ok {
					t.Fatalf("%s %s is missing %s", tt.file, tt.header, key)
				}
				if got != want {
					t.Fatalf("%s %s %s = %q, want %q", tt.file, tt.header, key, got, want)
				}
			}
		})
	}
}

func TestControlFlyConfigDeclaresDormantFleetAlertPolicy(t *testing.T) {
	t.Parallel()

	contents, err := os.ReadFile("fly.control.toml")
	if err != nil {
		t.Fatalf("read fly.control.toml: %v", err)
	}
	env, ok := tomlTable(string(contents), "[env]")
	if !ok {
		t.Fatal("fly.control.toml is missing [env]")
	}
	for key, want := range map[string]string{
		"SOAK_DORMANT_FLEET_ALERT_THRESHOLD":      "2h",
		"SOAK_DORMANT_FLEET_ALERT_CHECK_INTERVAL": "10m",
	} {
		got, ok := tomlValue(env, key)
		if !ok {
			t.Fatalf("fly.control.toml [env] is missing %s", key)
		}
		if got != want {
			t.Fatalf("fly.control.toml [env] %s = %q, want %q", key, got, want)
		}
	}
}

func tomlTable(contents, header string) (string, bool) {
	lines := strings.Split(contents, "\n")
	for i, line := range lines {
		if strings.TrimSpace(line) != header {
			continue
		}

		var block []string
		for _, next := range lines[i+1:] {
			trimmed := strings.TrimSpace(next)
			if strings.HasPrefix(trimmed, "[") {
				break
			}
			block = append(block, next)
		}
		return strings.Join(block, "\n"), true
	}
	return "", false
}

func tomlValue(table, key string) (string, bool) {
	for _, line := range strings.Split(table, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, raw, ok := strings.Cut(line, "=")
		if !ok || strings.TrimSpace(name) != key {
			continue
		}
		return strings.Trim(strings.TrimSpace(raw), `"`), true
	}
	return "", false
}
