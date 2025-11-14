package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/edkuperman/chronosched/internal/db"
)

type HTTPParams struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
	Timeout int               `json:"timeout"` // seconds
}

type BinaryParams struct {
	Path    string   `json:"path"`
	Args    []string `json:"args"`
	Timeout int      `json:"timeout"`
}

func (r *Runner) execHTTP(ctx context.Context, j db.Job) error {
	var p HTTPParams
	if err := json.Unmarshal(j.PayloadJSON, &p); err != nil {
		return fmt.Errorf("bad http payload: %w", err)
	}
	if p.Method == "" {
		p.Method = "POST"
	}

	req, err := http.NewRequestWithContext(ctx, p.Method, p.URL, strings.NewReader(p.Body))
	if err != nil {
		return err
	}
	for k, v := range p.Headers {
		req.Header.Set(k, v)
	}

	client := &http.Client{Timeout: time.Duration(p.Timeout) * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("http status %d", resp.StatusCode)
	}
	return nil
}

func (r *Runner) execBinary(ctx context.Context, j db.Job) error {
	var p BinaryParams
	if err := json.Unmarshal(j.PayloadJSON, &p); err != nil {
		return fmt.Errorf("bad binary payload: %w", err)
	}
	realCtx := ctx
	var cancel context.CancelFunc
	if p.Timeout > 0 {
		realCtx, cancel = context.WithTimeout(ctx, time.Duration(p.Timeout)*time.Second)
		defer cancel()
	}

	cmd := exec.CommandContext(realCtx, p.Path, p.Args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	log.Printf("[job %d] execBinary: %s %v", j.ID, p.Path, p.Args)
	return cmd.Run()
}
