package litestreamsoak

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestOverlapSamplerFinalEvidence(t *testing.T) {
	source, err := parser.ParseFile(token.NewFileSet(), "scripts/local-rig-one-shot/main.go.tmpl", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var body bytes.Buffer
	body.WriteString("package sampler\nimport (\"log/slog\"; \"os\"; \"path/filepath\"; \"runtime\"; \"runtime/debug\"; \"runtime/pprof\"; \"sync\"; \"time\")\nconst (overlapSampleInterval=time.Second; overlapProfileMinGrowth=1.1; overlapProfileMinSpacing=time.Second)\n")
	for _, decl := range source.Decls {
		keep := false
		switch d := decl.(type) {
		case *ast.FuncDecl:
			keep = d.Name.Name == "Measure" || d.Name.Name == "writeHeapProfile"
		case *ast.GenDecl:
			if len(d.Specs) == 1 {
				if spec, ok := d.Specs[0].(*ast.TypeSpec); ok {
					keep = spec.Name.Name == "memPhase" || spec.Name.Name == "memSampler"
				}
			}
		}
		if keep {
			if err := format.Node(&body, token.NewFileSet(), decl); err != nil {
				t.Fatal(err)
			}
			body.WriteByte('\n')
		}
	}
	dir := t.TempDir()
	files := map[string]string{
		"go.mod":     "module sampler\ngo 1.26.10\n",
		"sampler.go": body.String(),
		"sampler_test.go": `package sampler
import ("errors"; "os"; "testing")
func TestFinal(t *testing.T) {
 s := memSampler{profileDir:t.TempDir()}
 failure := errors.New("incident")
 phase := s.Measure("short", func() error { return failure })
 if phase.Error != failure.Error() { t.Fatalf("lost incident: %+v", phase) }
 if phase.FinalProfile == "" { t.Fatal("missing final evidence") }
 if stat, err := os.Stat(phase.FinalProfile); err != nil || stat.Size()==0 { t.Fatalf("final artifact: %v", err) }
 if phase.ProfileSemantics == "" { t.Fatal("missing sampled profile semantics") }
}
`,
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command("go", "test", "./...")
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("sampler test: %v\n%s", err, output)
	}
}

func TestPullProfilesIncludesTextAndMatchingMetadata(t *testing.T) {
	script, err := filepath.Abs("scripts/pull-profiles.sh")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for _, name := range []string{"python3", "tr", "awk", "sort", "tail", "sed", "mkdir", "mktemp", "mv", "rm", "basename"} {
		executable, err := exec.LookPath(name)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(executable, filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	}
	mock := `#!/bin/sh
case "$1 $2" in
"machines list") printf '[{"name":"worker-main-example","state":"started","id":"example"}]' ;;
"ssh console") printf '%s\n' status.json 20260909T120000.000000001Z_final_memstats.txt 20260909T120000.000000001Z_final_memstats.txt.json ;;
"ssh sftp") for arg in "$@"; do file="$arg"; done; printf evidence > "$(basename "$file")" ;;
esac
`
	if err := os.WriteFile(filepath.Join(dir, "fly"), []byte(mock), 0700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", script, "main", "example", "1")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "PATH="+dir, "FLY_ACCESS_TOKEN=example")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("retrieval: %v\n%s", err, output)
	}
	for _, file := range []string{"status.json", "20260909T120000.000000001Z_final_memstats.txt", "20260909T120000.000000001Z_final_memstats.txt.json"} {
		if _, err := os.Stat(filepath.Join(dir, "tmp/profiles/main/example", file)); err != nil {
			t.Errorf("missing %s: %v", file, err)
		}
	}
}

func TestRestoreRetentionReplanJSONL(t *testing.T) {
	source, err := parser.ParseFile(token.NewFileSet(), "scripts/local-rig-one-shot/main.go.tmpl", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var body bytes.Buffer
	body.WriteString("package fixture\nimport (\"context\";\"log/slog\";\"sync\")\n")
	for _, decl := range source.Decls {
		keep := false
		switch d := decl.(type) {
		case *ast.FuncDecl:
			if d.Recv != nil {
				for _, field := range d.Recv.List {
					if star, ok := field.Type.(*ast.StarExpr); ok {
						if ident, ok := star.X.(*ast.Ident); ok && ident.Name == "raceLogCounter" {
							keep = true
						}
					}
				}
			}
		case *ast.GenDecl:
			for _, spec := range d.Specs {
				if typ, ok := spec.(*ast.TypeSpec); ok && (typ.Name.Name == "raceLogCounter" || typ.Name.Name == "raceLogCounts") {
					keep = true
				}
			}
		}
		if keep {
			if err := format.Node(&body, token.NewFileSet(), decl); err != nil {
				t.Fatal(err)
			}
			body.WriteByte('\n')
		}
	}
	dir := t.TempDir()
	files := map[string]string{
		"go.mod":     "module fixture\ngo 1.26.10\n",
		"fixture.go": body.String(),
		"fixture_test.go": `package fixture
import ("bytes";"context";"encoding/json";"io";"log/slog";"testing")
func TestAttemptJSONL(t *testing.T) {
 counter:=&raceLogCounter{Handler:slog.NewTextHandler(io.Discard,nil),counts:&raceLogCounts{m:make(map[string]int)}}
 logger:=slog.New(counter).With("component","restore")
 var output bytes.Buffer
 for _,outcome:=range []string{"scenario_success","unrelated_failure","aborted"} {
  before:=counter.count("restore file disappeared, recalculating plan")
  logger.DebugContext(context.Background(),"restore file disappeared, recalculating plan")
  logger.DebugContext(context.Background(),"restore file disappeared, recalculating plan")
  record:=map[string]any{"outcome":outcome}
  counter.recordRestoreReplans(record,before,outcome)
  if err:=json.NewEncoder(&output).Encode(record);err!=nil { t.Fatal(err) }
 }
 decoder:=json.NewDecoder(&output)
 for _,outcome:=range []string{"scenario_success","unrelated_failure","aborted"} {
  var record map[string]any
  if err:=decoder.Decode(&record);err!=nil { t.Fatal(err) }
  if record["restore_replan_count"]!=float64(2) || record["outcome_after_replan"]!=outcome { t.Fatalf("record=%v",record) }
 }
 record:=map[string]any{}
 counter.recordRestoreReplans(record,counter.count("restore file disappeared, recalculating plan"),"scenario_success")
 if record["restore_replan_count"]!=0 || record["outcome_after_replan"]!=nil { t.Fatalf("empty record=%v",record) }
}
`,
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command("go", "test", "./...")
	cmd.Dir = dir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("rig fixture: %v\n%s", err, output)
	}
}
