package worker

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

func loadLogicalConfig(c *Config) error {
	for _, setting := range []struct {
		name   string
		target *int64
	}{
		{"VERIFY_LOGICAL_MAX_ROWS", &c.LogicalMaxRows},
		{"VERIFY_LOGICAL_MAX_BYTES", &c.LogicalMaxBytes},
	} {
		if value := os.Getenv(setting.name); value != "" {
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil || n <= 0 {
				return fmt.Errorf("invalid %s: must be a positive integer", setting.name)
			}
			*setting.target = n
		}
	}
	for _, setting := range []struct {
		name   string
		target *int
	}{
		{"VERIFY_LOGICAL_MAX_OBJECTS", &c.LogicalMaxObjects},
		{"VERIFY_LOGICAL_MAX_VALUE_BYTES", &c.LogicalMaxValueBytes},
	} {
		if value := os.Getenv(setting.name); value != "" {
			n, err := strconv.ParseInt(value, 10, 32)
			if err != nil || n <= 0 {
				return fmt.Errorf("invalid %s: must be a positive 32-bit integer", setting.name)
			}
			*setting.target = int(n)
		}
	}
	if value := os.Getenv("VERIFY_LOGICAL_TIMEOUT"); value != "" {
		duration, err := time.ParseDuration(value)
		if err != nil || duration <= 0 {
			return fmt.Errorf("invalid VERIFY_LOGICAL_TIMEOUT: must be a positive duration")
		}
		c.LogicalTimeout = duration
	}
	return nil
}
