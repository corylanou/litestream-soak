package reporting

import "testing"

func TestClassifyVerificationFailureS3ListRequestCanceled(t *testing.T) {
	errMsg := "validation failed (exit 1): time=2026-04-26T19:00:05.722Z level=ERROR msg=\"Validation failed\" check_type=restore error=\"restore failed: exit status 1\\nOutput: time=2026-04-26T19:00:05.719Z level=ERROR msg=\\\"failed to run\\\" error=\\\"get LTX time bounds: operation error S3: ListObjectsV2, https response error StatusCode: 408, RequestID: 1777230002707552565, HostID: , api error RequestCanceled: Request is canceled.\\\"\\n\""

	got := ClassifyVerificationFailure("integrity", errMsg)
	if got.Stage != "restore" {
		t.Fatalf("Stage = %q, want restore", got.Stage)
	}
	if got.Signature != "restore_s3_list_request_canceled" {
		t.Fatalf("Signature = %q, want restore_s3_list_request_canceled", got.Signature)
	}
	if got.ObjectStore == nil {
		t.Fatal("ObjectStore = nil")
	}
	if got.ObjectStore.Operation != "ListObjectsV2" {
		t.Fatalf("Operation = %q, want ListObjectsV2", got.ObjectStore.Operation)
	}
	if got.ObjectStore.HTTPStatus != 408 {
		t.Fatalf("HTTPStatus = %d, want 408", got.ObjectStore.HTTPStatus)
	}
	if got.ObjectStore.APICode != "RequestCanceled" {
		t.Fatalf("APICode = %q, want RequestCanceled", got.ObjectStore.APICode)
	}
	if got.ObjectStore.RequestID != "1777230002707552565" {
		t.Fatalf("RequestID = %q, want 1777230002707552565", got.ObjectStore.RequestID)
	}
	if got.ObjectStore.Phase != "TimeBounds" {
		t.Fatalf("Phase = %q, want TimeBounds", got.ObjectStore.Phase)
	}
}

func TestClassifyVerificationFailureSyncDecodeError(t *testing.T) {
	got := ClassifyVerificationFailure("integrity", "wait for sync: decode sync response: invalid character '<' looking for beginning of value")
	if got.Stage != "sync" {
		t.Fatalf("Stage = %q, want sync", got.Stage)
	}
	if got.Signature != "sync_decode_error" {
		t.Fatalf("Signature = %q, want sync_decode_error", got.Signature)
	}
	if got.Restore != nil {
		t.Fatalf("Restore = %#v, want nil", got.Restore)
	}
}

func TestClassifyVerificationFailureSyncObjectStore(t *testing.T) {
	got := ClassifyVerificationFailure("integrity", `wait for sync: sync request: operation error S3: GetObject, https response error StatusCode: 408, RequestID: 1777230002707552565, HostID: , api error RequestCanceled: Request is canceled.`)
	if got.Stage != "sync" {
		t.Fatalf("Stage = %q, want sync", got.Stage)
	}
	if got.Signature != "sync_s3_get_request_canceled" {
		t.Fatalf("Signature = %q, want sync_s3_get_request_canceled", got.Signature)
	}
	if got.ObjectStore == nil {
		t.Fatal("ObjectStore = nil")
	}
	if got.ObjectStore.Operation != "GetObject" {
		t.Fatalf("Operation = %q, want GetObject", got.ObjectStore.Operation)
	}
	if got.Restore != nil {
		t.Fatalf("Restore = %#v, want nil", got.Restore)
	}
}

func TestClassifyVerificationFailureRestoreAttachesRestoreFailure(t *testing.T) {
	got := ClassifyVerificationFailure("restore", `validation failed (exit 1): error="restore failed: get LTX time bounds: operation error S3: ListObjectsV2, https response error StatusCode: 408, RequestID: 1777230002707552565, HostID: , api error RequestCanceled: Request is canceled."`)
	if got.Stage != "restore" {
		t.Fatalf("Stage = %q, want restore", got.Stage)
	}
	if got.Signature != "restore_s3_list_request_canceled" {
		t.Fatalf("Signature = %q, want restore_s3_list_request_canceled", got.Signature)
	}
	if got.Restore == nil {
		t.Fatal("Restore = nil, want attached RestoreFailure")
	}
	if got.Restore.Phase != "TimeBounds" {
		t.Fatalf("Restore.Phase = %q, want TimeBounds", got.Restore.Phase)
	}
}

func TestClassifyVerificationFailureIntegrityCheckTypeDecodeError(t *testing.T) {
	got := ClassifyVerificationFailure("integrity", "read page header: unexpected EOF")
	if got.Stage != "restore" {
		t.Fatalf("Stage = %q, want restore", got.Stage)
	}
	if got.Signature != "restore_decode_error" {
		t.Fatalf("Signature = %q, want restore_decode_error", got.Signature)
	}
	if got.Restore == nil {
		t.Fatal("Restore = nil, want attached RestoreFailure")
	}
	if got.Restore.Phase != "Decode" {
		t.Fatalf("Restore.Phase = %q, want Decode", got.Restore.Phase)
	}
}

func TestClassifyVerificationFailureMissingLTXKeepsRestoreMetadata(t *testing.T) {
	got := ClassifyVerificationFailure("integrity", "open ltx file: file does not exist")
	if got.Stage != "restore" {
		t.Fatalf("Stage = %q, want restore", got.Stage)
	}
	if got.Signature != "restore_missing_ltx" {
		t.Fatalf("Signature = %q, want restore_missing_ltx", got.Signature)
	}
	if got.Restore == nil {
		t.Fatal("Restore = nil, want attached RestoreFailure")
	}
}

func TestClassifyVerificationFailureBareDecodeKeepsRestoreMetadata(t *testing.T) {
	got := ClassifyVerificationFailure("integrity", "decode ltx header: unexpected EOF")
	if got.Stage != "restore" {
		t.Fatalf("Stage = %q, want restore", got.Stage)
	}
	if got.Signature != "restore_decode_error" {
		t.Fatalf("Signature = %q, want restore_decode_error", got.Signature)
	}
	if got.Restore == nil {
		t.Fatal("Restore = nil, want attached RestoreFailure")
	}
	if got.Restore.Phase != "Decode" {
		t.Fatalf("Restore.Phase = %q, want Decode", got.Restore.Phase)
	}
}

func TestClassifyVerificationFailureIntegrityCheckObjectStore(t *testing.T) {
	got := ClassifyVerificationFailure("integrity", `integrity check failed: operation error S3: GetObject, https response error StatusCode: 408, RequestID: 1777230002707552565, HostID: , api error RequestCanceled: Request is canceled.`)
	if got.Stage != "restore" {
		t.Fatalf("Stage = %q, want restore", got.Stage)
	}
	if got.Signature != "restore_s3_get_request_canceled" {
		t.Fatalf("Signature = %q, want restore_s3_get_request_canceled", got.Signature)
	}
	if got.ObjectStore == nil {
		t.Fatal("ObjectStore = nil")
	}
	if got.Restore == nil {
		t.Fatal("Restore = nil, want attached RestoreFailure")
	}
}

func TestClassifyVerificationFailureRestoreDecodeError(t *testing.T) {
	got := ClassifyVerificationFailure("restore", "validation failed: restore failed: read page header: unexpected EOF")
	if got.Stage != "restore" {
		t.Fatalf("Stage = %q, want restore", got.Stage)
	}
	if got.Signature != "restore_decode_error" {
		t.Fatalf("Signature = %q, want restore_decode_error", got.Signature)
	}
}

func TestClassifyVerificationFailureDBSyncExecutor(t *testing.T) {
	got := ClassifyVerificationFailure("integrity", `wait for sync: sync returned 500: sync database: db sync: wait for db sync executor: context deadline exceeded`)
	if got.Stage != "sync" {
		t.Fatalf("Stage = %q, want sync", got.Stage)
	}
	if got.Signature != "litestream_db_sync_executor_timeout" {
		t.Fatalf("Signature = %q, want litestream_db_sync_executor_timeout", got.Signature)
	}
}

func TestClassifyVerificationFailureDiskCapacity(t *testing.T) {
	got := ClassifyVerificationFailure("integrity", `checkpoint failed: database or disk is full (13); sync failed: write /data/.test.db-litestream/ltx/0/000000000001.ltx.tmp: no space left on device`)
	if got.Stage != "disk_capacity" {
		t.Fatalf("Stage = %q, want disk_capacity", got.Stage)
	}
	if got.Signature != "disk_capacity_full" {
		t.Fatalf("Signature = %q, want disk_capacity_full", got.Signature)
	}
	if got.ObjectStore != nil {
		t.Fatalf("ObjectStore = %#v, want nil", got.ObjectStore)
	}
}

func TestParseObjectStoreFailureStructuredFields(t *testing.T) {
	got := ParseObjectStoreFailure(`restore failed operation=ListObjectsV2 http_status=408 api_code=RequestCanceled request_id=req-123 bucket=litestream-soak prefix=pr-1228/worker/0001 phase=CalcRestorePlan`)
	if got == nil {
		t.Fatal("ParseObjectStoreFailure() = nil")
	}
	if got.Operation != "ListObjectsV2" {
		t.Fatalf("Operation = %q, want ListObjectsV2", got.Operation)
	}
	if got.HTTPStatus != 408 {
		t.Fatalf("HTTPStatus = %d, want 408", got.HTTPStatus)
	}
	if got.APICode != "RequestCanceled" {
		t.Fatalf("APICode = %q, want RequestCanceled", got.APICode)
	}
	if got.RequestID != "req-123" {
		t.Fatalf("RequestID = %q, want req-123", got.RequestID)
	}
	if got.Bucket != "litestream-soak" {
		t.Fatalf("Bucket = %q, want litestream-soak", got.Bucket)
	}
	if got.RedactedPrefix != "pr-1228/.../0001" {
		t.Fatalf("RedactedPrefix = %q, want pr-1228/.../0001", got.RedactedPrefix)
	}
	if got.Phase != "CalcRestorePlan" {
		t.Fatalf("Phase = %q, want CalcRestorePlan", got.Phase)
	}
}

func TestRedactObjectPrefix(t *testing.T) {
	if got := RedactObjectPrefix("soak/worker-pr-1228-burst-vol/0001"); got != "soak/.../0001" {
		t.Fatalf("RedactObjectPrefix() = %q", got)
	}
}
