package workload

import (
	"strings"
	"testing"
)

func TestParseConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		input     string
		want      Config
		wantError bool
	}{
		{
			name:      "empty string",
			input:     "",
			want:      Config{},
			wantError: false,
		},
		{
			name:      "whitespace only",
			input:     "   \n\t  ",
			want:      Config{},
			wantError: false,
		},
		{
			name:  "valid JSON",
			input: `{"write_rate":100,"load_mode":"synthetic"}`,
			want: Config{
				WriteRate: 100,
				LoadMode:  "synthetic",
			},
			wantError: false,
		},
		{
			name:  "many database knobs",
			input: `{"load_mode":"many-db","num_databases":1000,"active_percent":2,"config_mode":"dir","verify_sample_size":5,"replication_lag_threshold":3}`,
			want: Config{
				LoadMode:                "many-db",
				NumDatabases:            1000,
				ActivePercent:           2,
				ActivePercentSet:        true,
				ConfigMode:              "dir",
				VerifySampleSize:        5,
				ReplicationLagThreshold: 3,
			},
			wantError: false,
		},
		{
			name:  "many database explicit pure idle",
			input: `{"load_mode":"many-db","num_databases":100,"active_percent":0}`,
			want: Config{
				LoadMode:         "many-db",
				NumDatabases:     100,
				ActivePercent:    0,
				ActivePercentSet: true,
			},
			wantError: false,
		},
		{
			name:      "corrupt JSON truncated",
			input:     `{"write_rate":`,
			want:      Config{},
			wantError: true,
		},
		{
			name:      "wrong field type",
			input:     `{"write_rate":"fast"}`,
			want:      Config{},
			wantError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseConfig(tc.input)

			if tc.wantError && err == nil {
				t.Fatalf("ParseConfig(%q) error = nil, want non-nil error", tc.input)
			}
			if !tc.wantError && err != nil {
				t.Fatalf("ParseConfig(%q) error = %v, want nil", tc.input, err)
			}
			if got != tc.want {
				t.Fatalf("ParseConfig(%q) = %+v, want %+v", tc.input, got, tc.want)
			}
		})
	}
}

func TestConfigJSONIncludesExplicitZeroActivePercent(t *testing.T) {
	t.Parallel()

	cfg := Config{
		LoadMode:         "many-db",
		NumDatabases:     100,
		ActivePercent:    0,
		ActivePercentSet: true,
	}

	got := cfg.JSON()
	if !strings.Contains(got, `"active_percent":0`) {
		t.Fatalf("JSON() = %s, want explicit active_percent zero", got)
	}

	parsed, err := ParseConfig(got)
	if err != nil {
		t.Fatalf("ParseConfig(%q) error = %v", got, err)
	}
	if parsed.ActivePercent != 0 || !parsed.ActivePercentSet {
		t.Fatalf("parsed active percent = %v set=%v, want 0 set=true", parsed.ActivePercent, parsed.ActivePercentSet)
	}
}
