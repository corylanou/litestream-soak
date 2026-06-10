package reporting

import (
	"regexp"
	"strconv"
	"strings"
)

var (
	s3OperationPattern = regexp.MustCompile(`(?i)operation error S3:\s*([A-Za-z0-9]+)`)
	httpStatusPattern  = regexp.MustCompile(`(?i)StatusCode:\s*([0-9]+)`)
	apiCodePattern     = regexp.MustCompile(`(?i)api error\s+([A-Za-z0-9]+):`)
	requestIDPattern   = regexp.MustCompile(`(?i)RequestID:\s*([^,\s]+)`)
	s3URLPattern       = regexp.MustCompile(`s3://([^/\s]+)/([^"'\s]+)`)
	keyValuePattern    = regexp.MustCompile(`(?i)\b(operation|http_status|api_code|request_id|bucket|prefix|phase)=("[^"]+"|'[^']+'|[^\s,]+)`)
)

func ClassifyVerificationFailure(checkType, errorMessage string) FailureClassification {
	text := strings.ToLower(errorMessage)
	classification := FailureClassification{
		Stage: InferFailureStage(checkType, errorMessage),
	}
	if isDiskCapacityFailure(text) {
		classification.Stage = "disk_capacity"
		classification.Signature = "disk_capacity_full"
		return classification
	}
	objectStore := ParseObjectStoreFailure(errorMessage)
	if objectStore != nil && (classification.Stage == "" || classification.Stage == "validation") {
		classification.Stage = "restore"
	}
	classification.Signature = inferFailureSignature(classification.Stage, text, errorMessage)
	if objectStore != nil {
		objectStore.Phase = firstNonEmpty(objectStore.Phase, inferRestorePhase(text))
		classification.ObjectStore = objectStore
		if signature := objectStoreSignature(classification.Stage, *objectStore); signature != "" {
			classification.Signature = signature
		}
	}
	if classification.Stage == "restore" {
		classification.Restore = &RestoreFailure{Phase: inferRestorePhase(text)}
	}
	return classification
}

func InferFailureStage(checkType, errorMessage string) string {
	text := strings.ToLower(errorMessage)
	switch {
	case isDiskCapacityFailure(text):
		return "disk_capacity"
	case strings.Contains(text, "wait for sync") || strings.Contains(text, "sync request") || strings.Contains(text, "decode sync response") || strings.Contains(text, "litestream.sock"):
		return "sync"
	case strings.Contains(text, "restore failed") || strings.Contains(text, "check_type=restore") || strings.Contains(text, "get ltx time bounds") || strings.Contains(text, "restore plan") || strings.Contains(text, "read page header"):
		return "restore"
	case strings.Contains(text, "integrity check") || strings.Contains(text, "check_type=integrity_check") || strings.Contains(text, "wrong # of entries in index"):
		return "integrity_check"
	case strings.Contains(text, "validation failed"):
		return "validation"
	case strings.TrimSpace(checkType) != "":
		return strings.TrimSpace(checkType)
	default:
		return ""
	}
}

func ParseObjectStoreFailure(errorMessage string) *ObjectStoreFailure {
	text := strings.ToLower(errorMessage)
	if !strings.Contains(text, "operation error s3") && !strings.Contains(text, "listobjectsv2") && !strings.Contains(text, "getobject") && !strings.Contains(text, "requestcanceled") && !strings.Contains(text, "accessdenied") {
		return nil
	}

	failure := &ObjectStoreFailure{
		Operation: firstRegexMatch(s3OperationPattern, errorMessage),
		APICode:   firstRegexMatch(apiCodePattern, errorMessage),
		RequestID: firstRegexMatch(requestIDPattern, errorMessage),
		Phase:     inferRestorePhase(text),
	}
	applyObjectStoreKeyValues(failure, errorMessage)
	if failure.Operation == "" {
		switch {
		case strings.Contains(text, "listobjectsv2"):
			failure.Operation = "ListObjectsV2"
		case strings.Contains(text, "getobject"):
			failure.Operation = "GetObject"
		}
	}
	if match := firstRegexMatch(httpStatusPattern, errorMessage); match != "" {
		status, _ := strconv.Atoi(match)
		failure.HTTPStatus = status
	}
	if matches := s3URLPattern.FindStringSubmatch(errorMessage); len(matches) == 3 {
		failure.Bucket = matches[1]
		failure.Prefix = strings.Trim(matches[2], "/")
		failure.RedactedPrefix = RedactObjectPrefix(failure.Prefix)
	}
	if failure.Operation == "" && failure.HTTPStatus == 0 && failure.APICode == "" && failure.RequestID == "" {
		return nil
	}
	return failure
}

func applyObjectStoreKeyValues(failure *ObjectStoreFailure, errorMessage string) {
	for _, match := range keyValuePattern.FindAllStringSubmatch(errorMessage, -1) {
		if len(match) != 3 {
			continue
		}
		key := strings.ToLower(match[1])
		value := strings.Trim(strings.TrimSpace(match[2]), `"'`)
		switch key {
		case "operation":
			failure.Operation = firstNonEmpty(value, failure.Operation)
		case "http_status":
			if status, err := strconv.Atoi(value); err == nil && failure.HTTPStatus == 0 {
				failure.HTTPStatus = status
			}
		case "api_code":
			failure.APICode = firstNonEmpty(value, failure.APICode)
		case "request_id":
			failure.RequestID = firstNonEmpty(value, failure.RequestID)
		case "bucket":
			failure.Bucket = firstNonEmpty(value, failure.Bucket)
		case "prefix":
			failure.Prefix = firstNonEmpty(strings.Trim(value, "/"), failure.Prefix)
			failure.RedactedPrefix = RedactObjectPrefix(failure.Prefix)
		case "phase":
			failure.Phase = firstNonEmpty(value, failure.Phase)
		}
	}
}

func RedactObjectPrefix(prefix string) string {
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	if prefix == "" {
		return ""
	}
	parts := strings.Split(prefix, "/")
	if len(parts) <= 2 {
		return prefix
	}
	return parts[0] + "/.../" + parts[len(parts)-1]
}

func firstRegexMatch(pattern *regexp.Regexp, text string) string {
	matches := pattern.FindStringSubmatch(text)
	if len(matches) < 2 {
		return ""
	}
	return strings.TrimSpace(matches[1])
}

func inferFailureSignature(stage, text, original string) string {
	switch {
	case isDiskCapacityFailure(text):
		return "disk_capacity_full"
	case strings.Contains(text, "wait for db sync executor"):
		return "litestream_db_sync_executor_timeout"
	case strings.Contains(text, "litestream.sock") && strings.Contains(text, "too many open files"):
		return "litestream_sync_fd_exhausted"
	case strings.Contains(text, "litestream.sock") && strings.Contains(text, "connect: connection refused"):
		return "litestream_sync_socket_refused"
	case strings.Contains(text, "wait for sync") && (strings.Contains(text, "context deadline exceeded") || strings.Contains(text, "client.timeout exceeded")):
		return "litestream_sync_timeout"
	case strings.Contains(text, "wrong # of entries in index"):
		return "sqlite_index_mismatch"
	case strings.Contains(text, "open ltx file: file does not exist") || strings.Contains(text, "no such key") || strings.Contains(text, "missing ltx"):
		return "restore_missing_ltx"
	case strings.Contains(text, "read page header") || strings.Contains(text, "decode") || strings.Contains(text, "unexpected eof"):
		return signatureStagePrefix(stage) + "_decode_error"
	case strings.Contains(text, "restore plan") || strings.Contains(text, "calc restore"):
		return "restore_plan_failed"
	case strings.Contains(text, "ltx continuity"):
		return "ltx_continuity"
	case strings.Contains(text, "validation failed"):
		return "validation_failed"
	default:
		return firstMeaningfulLine(original)
	}
}

func isDiskCapacityFailure(text string) bool {
	return strings.Contains(text, "no space left on device") ||
		strings.Contains(text, "database or disk is full") ||
		strings.Contains(text, "disk is full") ||
		strings.Contains(text, "enospc") ||
		strings.Contains(text, "sqlite_full")
}

func objectStoreSignature(stage string, failure ObjectStoreFailure) string {
	prefix := signatureStagePrefix(stage)
	operation := strings.ToLower(failure.Operation)
	apiCode := strings.ToLower(failure.APICode)
	switch {
	case operation == "listobjectsv2" && (failure.HTTPStatus == 408 || apiCode == "requestcanceled"):
		return prefix + "_s3_list_request_canceled"
	case operation == "getobject" && (failure.HTTPStatus == 408 || apiCode == "requestcanceled"):
		return prefix + "_s3_get_request_canceled"
	case failure.HTTPStatus == 403 || apiCode == "accessdenied":
		return prefix + "_s3_access_denied"
	case operation == "listobjectsv2":
		return prefix + "_s3_list_failed"
	case operation == "getobject":
		return prefix + "_s3_get_failed"
	default:
		return prefix + "_s3_failed"
	}
}

func signatureStagePrefix(stage string) string {
	if stage == "sync" {
		return "sync"
	}
	return "restore"
}

func inferRestorePhase(text string) string {
	switch {
	case strings.Contains(text, "get ltx time bounds") || strings.Contains(text, "time bounds"):
		return "TimeBounds"
	case strings.Contains(text, "calc restore plan") || strings.Contains(text, "restore plan"):
		return "CalcRestorePlan"
	case strings.Contains(text, "getobject") || strings.Contains(text, "object fetch") || strings.Contains(text, "download"):
		return "ObjectFetch"
	case strings.Contains(text, "read page header") || strings.Contains(text, "decode") || strings.Contains(text, "unexpected eof"):
		return "Decode"
	case strings.Contains(text, "integrity") || strings.Contains(text, "wrong # of entries in index"):
		return "IntegrityCheck"
	case strings.Contains(text, "restore"):
		return "Restore"
	default:
		return ""
	}
}

func firstMeaningfulLine(msg string) string {
	for _, line := range strings.Split(msg, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if len(line) > 120 {
			return line[:120]
		}
		return line
	}
	return "unknown_failure"
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}
