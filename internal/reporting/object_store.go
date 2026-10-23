package reporting

type ObjectStoreCounters struct {
	ObjectStoreCounterEpoch         string            `json:"object_store_counter_epoch,omitempty"`
	LitestreamCounterEpoch          string            `json:"litestream_counter_epoch,omitempty"`
	LitestreamObjectOperationTotals map[string]uint64 `json:"litestream_object_operation_totals,omitempty"`
	LitestreamObjectOperationBytes  map[string]uint64 `json:"litestream_object_operation_bytes,omitempty"`
	ReplicaRequestTotals            map[string]uint64 `json:"replica_request_totals,omitempty"`
	ReplicaRequestBytesSent         map[string]uint64 `json:"replica_request_bytes_sent,omitempty"`
	ReplicaResponseBytesReceived    map[string]uint64 `json:"replica_response_bytes_received,omitempty"`
	RestoreRequestTotals            map[string]uint64 `json:"restore_request_totals,omitempty"`
	EvidenceObjectUploadsTotal      uint64            `json:"evidence_object_uploads_total,omitempty"`
	EvidenceUploadPayloadBytesTotal uint64            `json:"evidence_upload_payload_bytes_total,omitempty"`
}
