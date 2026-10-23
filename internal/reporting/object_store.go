package reporting

type ObjectStoreCounters struct {
	ObjectStoreCounterEpoch  string            `json:"object_store_counter_epoch,omitempty"`
	ReplicaOperationTotals   map[string]uint64 `json:"replica_operation_totals,omitempty"`
	ReplicaOperationBytes    map[string]uint64 `json:"replica_operation_bytes,omitempty"`
	RestoreRequestTotals     map[string]uint64 `json:"restore_request_totals,omitempty"`
	EvidenceUploadsTotal     uint64            `json:"evidence_uploads_total,omitempty"`
	EvidenceUploadBytesTotal uint64            `json:"evidence_upload_bytes_total,omitempty"`
}
