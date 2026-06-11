package orchestrator

import (
	"embed"
	"html/template"
	"net/http"
	"net/url"
	"strings"
)

//go:embed templates/*.html
var templateFS embed.FS

var uiTemplates = template.Must(template.New("ui").Funcs(template.FuncMap{
	"confidenceClass":         confidenceClass,
	"comparisonBaseLabel":     comparisonBaseLabel,
	"comparisonCopyText":      comparisonCopyText,
	"comparisonHeadLabel":     comparisonHeadLabel,
	"comparisonModeSummary":   comparisonModeSummary,
	"comparisonTitle":         comparisonTitle,
	"copyLabel":               copyLabel,
	"deploymentSourceLabel":   deploymentSourceLabel,
	"deploymentSourceSummary": deploymentSourceSummary,
	"deploymentSourceURL":     deploymentSourceURL,
	"deploymentCopyText":      deploymentCopyText,
	"deploymentClass":         deploymentStatusClass,
	"eventClass":              eventClass,
	"failureText":             failureText,
	"formatDuration":          formatDurationMS,
	"formatTime":              formatUITime,
	"formatTimePtr":           formatUITimePtr,
	"heartbeatClass":          heartbeatClass,
	"json":                    mustJSON,
	"jsonCompact":             jsonCompact,
	"joinList":                strings.Join,
	"pathEscape":              url.PathEscape,
	"queryEscape":             url.QueryEscape,
	"runtimeClass":            runtimeSnapshotClass,
	"runtimeLabel":            runtimeSnapshotLabel,
	"shorten":                 shortenText,
	"soakCommitURL":           soakCommitURL,
	"sourceLabel":             sourceLabel,
	"sourceURL":               sourceURL,
	"statusClass":             statusClass,
	"tickClass":               tickClass,
	"tickLabel":               tickLabel,
	"timeAgo":                 formatTimeAgoPtr,
	"timeAgoValue":            formatTimeAgo,
	"litestreamCommitURL":     litestreamCommitURL,
	"trimSHA":                 trimSHA,
	"verificationClass":       verificationClass,
	"verificationLabel":       verificationLabel,
	"workerPromptURL":         workerPromptURL,
	"workerName":              workerName,
}).ParseFS(templateFS, "templates/*.html"))

func renderHTML(w http.ResponseWriter, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := uiTemplates.ExecuteTemplate(w, name, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
