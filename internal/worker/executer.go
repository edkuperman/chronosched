package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/edkuperman/chronosched/internal/db"
)

// execHTTP executes a job whose kind is "http".
// Example payload:
// { "url": "https://example.com/hook", "method": "POST", "body": {...}, "timeout": 10 }
func (r *Runner) execHTTP(ctx context.Context, j *db.Job) error {
	var p struct {
		URL     string      `json:"url"`
		Method  string      `json:"method"`
		Body    interface{} `json:"body"`
		Timeout int         `json:"timeout"`
	}
	if err := json.Unmarshal(j.PayloadJSON, &p); err != nil {
		return fmt.Errorf("invalid http payload: %w", err)
	}
	if p.URL == "" {
		return fmt.Errorf("missing url")
	}
	if p.Method == "" {
		p.Method = http.MethodPost
	}
	runCtx := ctx
	if p.Timeout > 0 {
		runCtx, _ = context.WithTimeout(ctx, time.Duration(p.Timeout)*time.Second)
	}

	bodyBytes, _ := json.Marshal(p.Body)
	req, err := http.NewRequestWithContext(runCtx, p.Method, p.URL, bytes.NewReader(bodyBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("http job failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	log.Printf("[job %d] http %s -> %d: %s", j.ID, p.URL, resp.StatusCode, string(respBody))

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("http job returned status %d", resp.StatusCode)
	}
	return nil
}

// execCommand executes an arbitrary shell command.
// Example payload:
// { "cmd": "python3 scripts/hello.py", "timeout": 10, "cwd": "/tmp", "env": {"FOO":"BAR"} }
func (r *Runner) execCommand(ctx context.Context, j *db.Job) error {
	var p struct {
		Cmd     string            `json:"cmd"`
		Timeout int               `json:"timeout"`
		Cwd     string            `json:"cwd"`
		Env     map[string]string `json:"env"`
	}
	if err := json.Unmarshal(j.PayloadJSON, &p); err != nil {
		return fmt.Errorf("invalid command payload: %w", err)
	}
	if p.Cmd == "" {
		return fmt.Errorf("missing cmd")
	}

	runCtx := ctx
	if p.Timeout > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, time.Duration(p.Timeout)*time.Second)
		defer cancel()
	}

	cmd := exec.CommandContext(runCtx, "sh", "-c", p.Cmd)
	if p.Cwd != "" {
		cmd.Dir = p.Cwd
	}
	for k, v := range p.Env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}

	var out, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &stderr

	log.Printf("[job %d] execCommand: %s", j.ID, p.Cmd)
	err := cmd.Run()
	if runCtx.Err() == context.DeadlineExceeded {
		return fmt.Errorf("command timed out")
	}
	if err != nil {
		log.Printf("[stderr]\n%s", stderr.String())
		return fmt.Errorf("command failed: %w", err)
	}
	log.Printf("[stdout]\n%s", out.String())
	return nil
}

// execBinary runs a binary stored in the database as bytes.
// Example payload:
// { "filename": "worker.exe", "args": ["--flag"], "timeout": 30 }
func (r *Runner) execBinary(ctx context.Context, j *db.Job) error {
	var p struct {
		Filename string   `json:"filename"`
		Args     []string `json:"args"`
		Timeout  int      `json:"timeout"`
	}
	if err := json.Unmarshal(j.PayloadJSON, &p); err != nil {
		return fmt.Errorf("invalid binary payload: %w", err)
	}
	if p.Filename == "" {
		return fmt.Errorf("missing filename in payload")
	}

	// Retrieve the binary bytes from DB (you must implement this in JobRepo)
	bin, err := r.Jobs.LoadBinary(ctx, j.ID)
	if err != nil {
		return fmt.Errorf("load binary: %w", err)
	}

	tmpDir := os.TempDir()
	tmpPath := filepath.Join(tmpDir, p.Filename)
	if err := os.WriteFile(tmpPath, bin, 0755); err != nil {
		return fmt.Errorf("write temp binary: %w", err)
	}
	defer os.Remove(tmpPath)

	runCtx := ctx
	if p.Timeout > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, time.Duration(p.Timeout)*time.Second)
		defer cancel()
	}

	cmd := exec.CommandContext(runCtx, tmpPath, p.Args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	log.Printf("[job %d] execBinary: %s %v", j.ID, tmpPath, p.Args)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("binary run failed: %w", err)
	}
	return nil
}
