package worker

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestTenantLifecyclePinnedRestore(t *testing.T) {
	binary := os.Getenv("SOAK_TENANT_LITESTREAM_BINARY")
	if binary == "" {
		t.Skip("opt-in: set SOAK_TENANT_LITESTREAM_BINARY to the pinned executable")
	}
	for _, mode := range []string{"watch", "static"} {
		t.Run(mode, func(t *testing.T) {
			report, err := RunTenantLifecycle(context.Background(), TenantLifecycleOptions{SHA: tenantLitestreamSHA, Capabilities: "directory-v1", Binary: binary, Root: t.TempDir(), Mode: mode, Tenants: 2, SyncTimeout: time.Second, Timeout: 30 * time.Second})
			if err != nil {
				t.Fatalf("run: %v report=%+v", err, report)
			}
			if report.Status != "passed" || len(report.Failures) != 0 {
				t.Fatalf("report=%+v", report)
			}
			for _, phase := range []string{"initial", "runtime-create", "skew", "retained", "recreate", "restart", "cleanup"} {
				found := false
				for _, event := range report.Events {
					if event.Phase == phase && event.Status == "passed" {
						found = true
					}
				}
				if !found {
					t.Errorf("no passing evidence for %s", phase)
				}
			}
			if len(report.Resources) < 4 {
				t.Fatal("resource scaling observations missing")
			}
			t.Logf("pinned=%s mode=%s events=%d resources=%+v", report.LitestreamSHA, mode, len(report.Events), report.Resources)
		})
	}
}
