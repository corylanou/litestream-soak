package workload

import (
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
