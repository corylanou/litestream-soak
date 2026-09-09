package orchestrator

import (
	"encoding/json"
	"testing"

	"github.com/corylanou/litestream-soak/internal/churn"
	"github.com/corylanou/litestream-soak/internal/model"
	"github.com/corylanou/litestream-soak/internal/workload"
)

func TestChurnFleetEnvironment(t *testing.T) {
	cfg := workload.Config{LoadMode: "queue", Churn: churn.Config{Slots: 32, HotPercent: 75, Workers: 4, Rate: 100, PayloadSize: 64, Seed: 9}}
	parsed, err := workload.ParseConfig(cfg.JSON())
	if err != nil {
		t.Fatal(err)
	}
	m := &Manager{}
	env := m.workerEnv(model.Worker{ID: "churn-test"}, parsed)
	var actual churn.Config
	if err := json.Unmarshal([]byte(env["CHURN_CONFIG"]), &actual); err != nil {
		t.Fatal(err)
	}
	if actual != cfg.Churn || env["LOAD_MODE"] != "queue" {
		t.Fatalf("churn configuration lost: %+v", actual)
	}
}

func TestDefaultFleetExcludesChurn(t *testing.T) {
	for _, worker := range DefaultMainFleet().Workers {
		if worker.Workload.LoadMode == "queue" || worker.Workload.LoadMode == "cache" {
			t.Fatalf("churn added to default fleet: %s", worker.Name)
		}
	}
}

func TestSyntheticEnvironmentOmitsChurn(t *testing.T) {
	m := &Manager{}
	env := m.workerEnv(model.Worker{}, workload.Config{LoadMode: "synthetic"})
	if _, ok := env["CHURN_CONFIG"]; ok {
		t.Fatal("churn configuration added to existing worker environment")
	}
}
