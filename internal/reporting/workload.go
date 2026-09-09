package reporting

type WorkloadCounters struct {
	WorkloadCounterEpoch    string `json:"workload_counter_epoch,omitempty"`
	WorkloadCountersPresent bool   `json:"workload_counters_present,omitempty"`
	WorkloadAttemptsTotal   uint64 `json:"workload_attempts_total,omitempty"`
	WorkloadMutationsTotal  uint64 `json:"workload_mutations_total,omitempty"`
	WorkloadBusyTotal       uint64 `json:"workload_busy_total,omitempty"`
	WorkloadErrorsTotal     uint64 `json:"workload_errors_total,omitempty"`
}
