package reporting

import (
	"testing"
	"time"
)

func TestFailureClassificationValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		classification *FailureClassification
		want           bool
	}{
		{name: "nil", want: false},
		{name: "missing stage", classification: &FailureClassification{Signature: "soak_fixture_disk_exhausted"}, want: false},
		{name: "missing signature", classification: &FailureClassification{Stage: "disk_capacity"}, want: false},
		{name: "non-canonical whitespace", classification: &FailureClassification{Stage: " disk_capacity", Signature: "disk_capacity_full"}, want: false},
		{name: "fixture", classification: &FailureClassification{Stage: "disk_capacity", Signature: "soak_fixture_disk_exhausted"}, want: true},
		{name: "fixture wrong stage", classification: &FailureClassification{Stage: "restore", Signature: "soak_fixture_disk_exhausted"}, want: false},
		{name: "fixture with nested restore", classification: &FailureClassification{Stage: "disk_capacity", Signature: "soak_fixture_disk_exhausted", Restore: &RestoreFailure{}}, want: false},
		{name: "disk capacity", classification: &FailureClassification{Stage: "disk_capacity", Signature: "disk_capacity_full"}, want: true},
		{name: "unknown disk signature", classification: &FailureClassification{Stage: "disk_capacity", Signature: "unknown"}, want: false},
		{name: "object store wrong stage", classification: &FailureClassification{Stage: "integrity", Signature: "s3_failed", ObjectStore: &ObjectStoreFailure{}}, want: false},
		{name: "restore details wrong stage", classification: &FailureClassification{Stage: "sync", Signature: "s3_transport", Restore: &RestoreFailure{}}, want: false},
		{name: "sync failure", classification: &FailureClassification{Stage: "sync", Signature: "litestream_sync_timeout"}, want: true},
		{name: "restore failure", classification: &FailureClassification{Stage: "restore", Signature: "restore_s3_failed", ObjectStore: &ObjectStoreFailure{}, Restore: &RestoreFailure{}}, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.classification.Valid(); got != tt.want {
				t.Fatalf("Valid() = %t, want %t", got, tt.want)
			}
		})
	}
}

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

func TestClassifyVerificationFailureValidationEOFNotRestoreStage(t *testing.T) {
	got := ClassifyVerificationFailure("integrity", "validation failed (exit 1): unexpected EOF reading stdout")
	if got.Stage != "validation" {
		t.Fatalf("Stage = %q, want validation", got.Stage)
	}
	if got.Restore != nil {
		t.Fatalf("Restore = %#v, want nil", got.Restore)
	}
}

func TestClassifyVerificationFailureCalcRestoreKeepsMetadata(t *testing.T) {
	got := ClassifyVerificationFailure("integrity", "calc restore target: context deadline exceeded")
	if got.Stage != "restore" {
		t.Fatalf("Stage = %q, want restore", got.Stage)
	}
	if got.Signature != "restore_plan_failed" {
		t.Fatalf("Signature = %q, want restore_plan_failed", got.Signature)
	}
	if got.Restore == nil {
		t.Fatal("Restore = nil, want attached RestoreFailure")
	}
}

func TestClassifyVerificationFailureSyncMissingLTX(t *testing.T) {
	got := ClassifyVerificationFailure("integrity", "wait for sync: sync request: no such key")
	if got.Stage != "sync" {
		t.Fatalf("Stage = %q, want sync", got.Stage)
	}
	if got.Signature != "sync_missing_ltx" {
		t.Fatalf("Signature = %q, want sync_missing_ltx", got.Signature)
	}
	if got.Restore != nil {
		t.Fatalf("Restore = %#v, want nil", got.Restore)
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

func TestClassifyVerificationFailureS3Transport(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		msg  string
	}{
		{
			name: "unexpected EOF",
			msg:  `wait for sync: sync returned 500: sync database: replica sync: operation error S3: PutObject, https response error StatusCode: 0, RequestID: , request send failed, Put "https://fly.storage.tigris.dev/litestream-soak": unexpected EOF`,
		},
		{
			name: "retry quota exceeded",
			msg:  `wait for sync: sync returned 500: sync database: replica sync: retry quota exceeded, 0 available, 5 requested`,
		},
		{
			name: "replica sync context deadline",
			msg:  `wait for sync: sync returned 500: sync database: replica sync: context deadline exceeded`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ClassifyVerificationFailure("integrity", tt.msg)
			if got.Stage != "sync" {
				t.Fatalf("Stage = %q, want sync", got.Stage)
			}
			if got.Signature != "s3_transport" {
				t.Fatalf("Signature = %q, want s3_transport", got.Signature)
			}
		})
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

func TestClassifyVerificationFailureBareDiskFullRetainsLegacyInference(t *testing.T) {
	errorMessage := "sync database: db sync: stage-write ltx file: disk full: write header"

	got := ClassifyVerificationFailure("integrity", errorMessage)

	if got.Stage != "integrity" {
		t.Fatalf("Stage = %q, want integrity", got.Stage)
	}
	if got.Signature != errorMessage {
		t.Fatalf("Signature = %q, want %q", got.Signature, errorMessage)
	}
}

func TestClassifyVerificationFailureWithRuntimeDiskCapacity(t *testing.T) {
	t.Parallel()

	completedAt := time.Date(2026, 7, 26, 1, 29, 13, 0, time.UTC)
	errorMessage := `sync database: db sync: stage-write ltx file /data/dbs/.db-00005.db-litestream/ltx/0/000000000000f6b4.ltx.tmp: disk full: write header`

	tests := []struct {
		name         string
		profileName  string
		errorMessage string
		runtime      *RuntimePayload
		want         string
		wantStage    string
	}{
		{
			name: "99.6 percent source usage is fixture exhaustion",
			runtime: &RuntimePayload{
				DataDiskUsedBytes:   9_894_000_000,
				DBTotalSizeBytes:    9_700_000_000,
				WALTotalSizeBytes:   158_000_000,
				SnapshotCollectedAt: completedAt.Add(-15 * time.Second),
			},
			want:      "soak_fixture_disk_exhausted",
			wantStage: "disk_capacity",
		},
		{
			name: "exactly 95 percent source usage is fixture exhaustion",
			runtime: &RuntimePayload{
				DataDiskUsedBytes:   1_000,
				DBTotalSizeBytes:    900,
				WALTotalSizeBytes:   50,
				SnapshotCollectedAt: completedAt,
			},
			want:      "soak_fixture_disk_exhausted",
			wantStage: "disk_capacity",
		},
		{
			name: "just below 95 percent remains actionable",
			runtime: &RuntimePayload{
				DataDiskUsedBytes:   1_000,
				DBTotalSizeBytes:    900,
				WALTotalSizeBytes:   49,
				SnapshotCollectedAt: completedAt,
			},
			want:      "disk_capacity_full",
			wantStage: "disk_capacity",
		},
		{
			name: "non-dominant source usage remains actionable",
			runtime: &RuntimePayload{
				DataDiskUsedBytes:   1_000,
				DBTotalSizeBytes:    500,
				WALTotalSizeBytes:   50,
				SnapshotCollectedAt: completedAt,
			},
			want:      "disk_capacity_full",
			wantStage: "disk_capacity",
		},
		{
			name:      "missing telemetry remains actionable",
			want:      "disk_capacity_full",
			wantStage: "disk_capacity",
		},
		{
			name: "missing collection time remains actionable",
			runtime: &RuntimePayload{
				DataDiskUsedBytes: 1_000,
				DBTotalSizeBytes:  950,
			},
			want:      "disk_capacity_full",
			wantStage: "disk_capacity",
		},
		{
			name: "stale telemetry remains actionable",
			runtime: &RuntimePayload{
				DataDiskUsedBytes:   1_000,
				DBTotalSizeBytes:    950,
				SnapshotCollectedAt: completedAt.Add(-61 * time.Second),
			},
			want:      "disk_capacity_full",
			wantStage: "disk_capacity",
		},
		{
			name: "future-skewed telemetry remains actionable",
			runtime: &RuntimePayload{
				DataDiskUsedBytes:   1_000,
				DBTotalSizeBytes:    950,
				SnapshotCollectedAt: completedAt.Add(61 * time.Second),
			},
			want:      "disk_capacity_full",
			wantStage: "disk_capacity",
		},
		{
			name: "source bytes exceeding used bytes remains actionable",
			runtime: &RuntimePayload{
				DataDiskUsedBytes:   1_000,
				DBTotalSizeBytes:    1_001,
				SnapshotCollectedAt: completedAt,
			},
			want:      "disk_capacity_full",
			wantStage: "disk_capacity",
		},
		{
			name: "single database telemetry is supported",
			runtime: &RuntimePayload{
				DataDiskUsedBytes:   1_000,
				DBSizeBytes:         925,
				WALSizeBytes:        25,
				SnapshotCollectedAt: completedAt,
			},
			want:      "soak_fixture_disk_exhausted",
			wantStage: "disk_capacity",
		},
		{
			name:         "non many-db workload remains actionable when source dominates",
			profileName:  "overload-truncate0",
			errorMessage: "checkpoint failed: database or disk is full",
			runtime: &RuntimePayload{
				DataDiskUsedBytes:   1_000,
				DBTotalSizeBytes:    950,
				SnapshotCollectedAt: completedAt,
			},
			want:      "disk_capacity_full",
			wantStage: "disk_capacity",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			profileName := tt.profileName
			if profileName == "" {
				profileName = "many-dbs-100-dir"
			}
			failureMessage := tt.errorMessage
			if failureMessage == "" {
				failureMessage = errorMessage
			}
			got := ClassifyVerificationFailureWithRuntime("integrity", failureMessage, profileName, tt.runtime, completedAt)
			if got.Stage != tt.wantStage {
				t.Errorf("Stage = %q, want %q", got.Stage, tt.wantStage)
			}
			if got.Signature != tt.want {
				t.Errorf("Signature = %q, want %q", got.Signature, tt.want)
			}
		})
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

func TestClassifyVerificationFailureTransientBucketAndSlowdown(t *testing.T) {
	t.Parallel()

	noSuchBucket := `wait for sync: sync returned 500: {"error":"sync database: db sync: replica sync: list objects: operation error S3: ListObjectsV2, https response error StatusCode: 404, RequestID: 01H8, api error NoSuchBucket: The specified bucket does not exist"}`
	classification := ClassifyVerificationFailure("integrity", noSuchBucket)
	if classification.Stage != "sync" {
		t.Fatalf("Stage = %q, want sync", classification.Stage)
	}
	if classification.Signature != "sync_s3_bucket_missing" {
		t.Fatalf("Signature = %q, want sync_s3_bucket_missing", classification.Signature)
	}
	if classification.ObjectStore == nil || classification.ObjectStore.HTTPStatus != 404 || classification.ObjectStore.APICode != "NoSuchBucket" {
		t.Fatalf("ObjectStore = %+v, want 404/NoSuchBucket", classification.ObjectStore)
	}

	slowdown := `restore failed: operation error S3: GetObject, https response error StatusCode: 503, api error SlowDown: Please reduce your request rate`
	classification = ClassifyVerificationFailure("integrity", slowdown)
	if classification.Signature != "restore_s3_slowdown" {
		t.Fatalf("Signature = %q, want restore_s3_slowdown", classification.Signature)
	}
}

func TestRestoreReplanFailureSignatures(t *testing.T) {
	for _, tc := range []struct{ message, signature string }{
		{"cannot recalculate restore plan: permission denied", "restore_plan_recalculation_failed"},
		{"cannot recalculate restore plan: context deadline exceeded", "restore_plan_recalculation_failed"},
		{"restore plan retries exhausted: decode ltx: open ltx file: file does not exist", "restore_plan_retries_exhausted"},
		{"cannot recalculate restore plan for txid 0000000000000042: transaction not available", "restore_target_unavailable"},
		{"cannot recalculate restore plan: operation error S3: ListObjectsV2: StatusCode: 503, api error SlowDown: retry", "restore_s3_slowdown"},
	} {
		t.Run(tc.signature, func(t *testing.T) {
			got := ClassifyVerificationFailure("logical", tc.message)
			if got.Stage != "restore" || got.Signature != tc.signature {
				t.Fatalf("classification=%+v", got)
			}
		})
	}
}

func TestRestoreFailureIgnoresDebugOutput(t *testing.T) {
	for _, tc := range []struct {
		name, final, signature string
		transport              bool
	}{
		{"reopen race", "Error: decode header for input 24: reopen ltx file at offset 0: file does not exist", "restore_decode_error", false},
		{"target missing", "Error: no matching backup files available", "restore_target_unavailable", false},
		{"transaction missing", "Error: transaction not available", "restore_target_unavailable", false},
		{"recalculate", "Error: cannot recalculate restore plan", "restore_target_unavailable", false},
		{"transport", "Error: cannot recalculate restore plan: operation error S3: GetObject, https response error StatusCode: 408, api error RequestCanceled: Request is canceled.", "restore_s3_get_request_canceled", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output := "validation failed (exit 1): time=2026-10-22T10:00:00Z level=DEBUG msg=\"s3 request\" s3.operation=GetObject s3.method=GET\n" +
				"time=2026-10-22T10:00:01Z level=WARN msg=\"restore file disappeared, recalculating plan\"\n" + tc.final + "\n"
			got := ClassifyVerificationFailure("integrity", output)
			if got.Stage != "restore" || got.Signature != tc.signature || (got.ObjectStore != nil) != tc.transport {
				t.Fatalf("classification=%+v", got)
			}
		})
	}
}

func TestRestoreErrorOutput(t *testing.T) {
	for _, tc := range []struct{ name, output, want string }{
		{"multiline", "time=now level=DEBUG msg=read\nError: restore failed:\n  operation error S3: GetObject\n  StatusCode: 503\n", "Error: restore failed:\n  operation error S3: GetObject\n  StatusCode: 503"},
		{"unindented multiline", "time=now level=DEBUG msg=read\nError: restore failed:\noperation error S3: GetObject\nStatusCode: 503\n", "Error: restore failed:\noperation error S3: GetObject\nStatusCode: 503"},
		{"plain", "starting restore\nfinal failure", "final failure"},
		{"trailing diagnostic", "Error: decode failed\ntime=now level=DEBUG msg=closed\n", "Error: decode failed"},
		{"no final error", "time=now level=DEBUG msg=read\n", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := RestoreErrorOutput(tc.output); got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}

func TestMultilineSyncFailurePreservesContext(t *testing.T) {
	got := ClassifyVerificationFailure("integrity", "wait for sync:\noperation error S3: GetObject, StatusCode: 503, api error SlowDown: retry")
	if got.Stage != "sync" || got.Signature != "sync_s3_slowdown" {
		t.Fatalf("classification=%+v", got)
	}
}

func TestPinnedRestoreTargetUnavailable(t *testing.T) {
	const unavailable = "restore failed: validation failed (exit 1): Error: no matching backup files available: restore target txid 0000000000003d9d is no longer available: txid 0000000000003d9d unavailable (nearest covered TXIDs: before=0000000000003c48, after=0000000000003dcf): transaction not available"
	got := ClassifyVerificationFailure("restore_validate", unavailable)
	if got.Stage != "restore" || got.Signature != SignatureRestoreTargetUnavailable || got.ObjectStore != nil {
		t.Fatalf("classification=%+v", got)
	}
	for _, tc := range []struct {
		name, mode, message string
		want                bool
	}{
		{"txid", "txid", unavailable, true},
		{"timestamp", "timestamp", unavailable, true},
		{"unreported mode defaults to txid", "", unavailable, true},
		{"latest", "latest", unavailable, false},
		{"plan retries exhausted", "txid", "restore failed: restore plan retries exhausted: decode ltx: open ltx file: file does not exist", false},
		{"object store transport", "txid", "restore failed: cannot recalculate restore plan: operation error S3: GetObject, https response error StatusCode: 408, api error RequestCanceled: Request is canceled.", false},
		{"integrity mismatch", "txid", "restore failed: integrity check: wrong # of entries in index", false},
		{"sync stage", "txid", "wait for sync: transaction not available", false},
		{"timestamp target lost", "timestamp", "restore failed: validation failed (exit 1): Error: no matching backup files available: restore target timestamp 2026-10-23T12:00:00.123000001Z is no longer available: transaction not available", true},
		{"bare missing backup files", "txid", "restore failed: validation failed (exit 1): Error: no matching backup files available", false},
		{"bare transaction not available", "txid", "restore failed: validation failed (exit 1): Error: transaction not available", false},
		{"bare recalculate", "timestamp", "restore failed: validation failed (exit 1): Error: cannot recalculate restore plan", false},
		{"recalculate for txid", "txid", "restore failed: cannot recalculate restore plan for txid 0000000000000042: transaction not available", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := PinnedRestoreTargetUnavailable(tc.mode, tc.message); got != tc.want {
				t.Fatalf("PinnedRestoreTargetUnavailable(%q) = %v, want %v", tc.mode, got, tc.want)
			}
		})
	}
}

func TestRestoreOutputDiskFull(t *testing.T) {
	for _, tc := range []struct {
		name, message string
		want          bool
	}{
		{"restore tmp page write", "restore failed: validation failed (exit 1): Error: decode database: write page 501132: write /data/scenarios/overload-truncate0/test.db.restored.tmp: no space left on device", true},
		{"restore target write", "restore failed: validation failed (exit 1): Error: write /data/test.db.restored: no space left on device", true},
		{"restore tmp create", "restore failed: validation failed (exit 1): Error: create /data/test.db.restored.tmp: no space left on device", true},
		{"wal write", "restore failed: validation failed (exit 1): Error: write /data/test.db-wal: no space left on device", false},
		{"source db write", "populate: write /data/test.db: no space left on device", false},
		{"litestream staging", "wait for sync: disk full while staging ltx file: write /data/.test.db-litestream/ltx/0/0000000000000042.ltx: no space left on device", false},
		{"replica sync", "replica sync: write /data/.test.db-litestream/ltx/1/x.ltx: no space left on device", false},
		{"database or disk is full", "restore failed: integrity check: database or disk is full", false},
		{"restore tmp other error", "restore failed: validation failed (exit 1): Error: write /data/test.db.restored.tmp: input/output error", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := RestoreOutputDiskFull(tc.message); got != tc.want {
				t.Fatalf("RestoreOutputDiskFull() = %v, want %v", got, tc.want)
			}
			if classification := ClassifyVerificationFailure("restore_validate", tc.message); tc.want && classification.Signature != "disk_capacity_full" {
				t.Fatalf("classifier signature = %q, want disk_capacity_full for the raw failure", classification.Signature)
			}
		})
	}
	if c := HarnessCapacityClassification("txid"); !c.Valid() || c.Stage != StageHarnessCapacity || c.Signature != SignatureHarnessCapacityInconclusive || c.RestoreMode != "txid" {
		t.Fatalf("harness capacity classification = %+v", c)
	}
}

func TestRestoreRequestCanceled(t *testing.T) {
	const (
		listCanceled = "restore failed: validation failed (exit 1): Error: cannot calc restore plan: operation error S3: ListObjectsV2, exceeded maximum number of attempts, 10, https response error StatusCode: 408, RequestID: 1761761212849403, HostID: , api error RequestCanceled: Request is canceled."
		getCanceled  = "restore failed: validation failed (exit 1): Error: operation error S3: GetObject, exceeded maximum number of attempts, 10, https response error StatusCode: 408, RequestID: 1761761212849404, HostID: , api error RequestCanceled: Request is canceled."
	)
	for _, tc := range []struct {
		name, message, signature string
		want                     bool
	}{
		{"list request canceled", listCanceled, SignatureRestoreS3ListRequestCanceled, true},
		{"get request canceled", getCanceled, SignatureRestoreS3GetRequestCanceled, true},
		{"decode wrapping canceled get", "restore failed: decode ltx: operation error S3: GetObject, https response error StatusCode: 408, RequestID: 1, HostID: , api error RequestCanceled: Request is canceled.", SignatureRestoreS3GetRequestCanceled, true},
		{"access denied", "restore failed: validation failed (exit 1): Error: cannot calc restore plan: operation error S3: ListObjectsV2, https response error StatusCode: 403, RequestID: 2, HostID: , api error AccessDenied: Access Denied", "restore_s3_access_denied", false},
		{"server error", "restore failed: validation failed (exit 1): Error: cannot calc restore plan: operation error S3: ListObjectsV2, https response error StatusCode: 500, RequestID: 3, HostID: , api error InternalError: We encountered an internal error.", "restore_s3_list_failed", false},
		{"status 408 without api code", "restore failed: validation failed (exit 1): Error: operation error S3: GetObject, https response error StatusCode: 408, RequestID: 4, HostID: ", SignatureRestoreS3GetRequestCanceled, false},
		{"request canceled without 408", "restore failed: validation failed (exit 1): Error: operation error S3: GetObject, https response error StatusCode: 0, RequestID: , HostID: , api error RequestCanceled: Request is canceled.", SignatureRestoreS3GetRequestCanceled, false},
		{"sync stage", "wait for sync: operation error S3: ListObjectsV2, exceeded maximum number of attempts, 10, https response error StatusCode: 408, RequestID: 5, HostID: , api error RequestCanceled: Request is canceled.", "sync_s3_list_request_canceled", false},
		{"decode error", "restore failed: validation failed (exit 1): Error: read page header: unexpected EOF", "restore_decode_error", false},
		{"data mismatch", "restore failed: compare: logical snapshot mismatch: table t rows 10 != 12", "", false},
		{"pinned target lost", "restore failed: validation failed (exit 1): Error: no matching backup files available: restore target txid 0000000000003d9d is no longer available: transaction not available", SignatureRestoreTargetUnavailable, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.signature != "" {
				if got := ClassifyVerificationFailure("restore_validate", tc.message); got.Signature != tc.signature {
					t.Fatalf("signature = %q, want %q (%+v)", got.Signature, tc.signature, got)
				}
			}
			if got := RestoreRequestCanceled(tc.message); got != tc.want {
				t.Fatalf("RestoreRequestCanceled() = %v, want %v", got, tc.want)
			}
		})
	}
}
