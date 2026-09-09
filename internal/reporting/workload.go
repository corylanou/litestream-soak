package reporting

type WorkloadCounters struct {
	WorkloadCounterEpoch    string `json:"workload_counter_epoch,omitempty"`
	WorkloadCountersPresent bool   `json:"workload_counters_present,omitempty"`
	WorkloadAttemptsTotal   uint64 `json:"workload_attempts_total,omitempty"`
	WorkloadMutationsTotal  uint64 `json:"workload_mutations_total,omitempty"`
	WorkloadBusyTotal       uint64 `json:"workload_busy_total,omitempty"`
	WorkloadErrorsTotal     uint64 `json:"workload_errors_total,omitempty"`
}

type WorkloadEvent struct {
	WorkloadEventID        string  `json:"workload_event_id,omitempty"`
	WorkloadMode           string  `json:"workload_mode,omitempty"`
	WorkloadOperation      string  `json:"workload_operation,omitempty"`
	WorkloadErrorKind      string  `json:"workload_error_kind,omitempty"`
	WorkloadLatencySeconds float64 `json:"workload_latency_seconds,omitempty"`
}
