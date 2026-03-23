package ai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/edkuperman/chronosched/internal/repository"
)

type RunSummary struct {
	RunID        int64                `json:"run_id"`
	RunStatus    repository.RunStatus `json:"run_status"`
	DAGID        string               `json:"dag_id"`
	DAGVersionID string               `json:"dag_version_id"`
	FailedJobID  *int64               `json:"failed_job_id,omitempty"`
	FailedNode   string               `json:"failed_node,omitempty"`
	Type         string               `json:"type"`
	Cause        string               `json:"cause"`
	Impact       string               `json:"impact"`
	NextSteps    []string             `json:"next_steps"`
	Confidence   float64              `json:"confidence"`
	Retry        string               `json:"retry"`
	Source       string               `json:"source"`
	Model        string               `json:"model,omitempty"`
	GeneratedAt  time.Time            `json:"generated_at"`
	Context      SummaryContext       `json:"context"`
	AIError      string               `json:"ai_error,omitempty"`
}

type SummaryContext struct {
	FocusRunID         int64                     `json:"focus_run_id"`
	FailedNodeKey      string                    `json:"failed_node_key,omitempty"`
	FailedStatus       string                    `json:"failed_status,omitempty"`
	FailedReasonCode   string                    `json:"failed_reason_code,omitempty"`
	FailedReason       string                    `json:"failed_reason,omitempty"`
	RootCauseNodeKey   string                    `json:"root_cause_node_key,omitempty"`
	RootCauseStatus    string                    `json:"root_cause_status,omitempty"`
	RootCauseReason    string                    `json:"root_cause_reason,omitempty"`
	UpstreamCompleted  []string                  `json:"upstream_completed"`
	DownstreamImpacted []string                  `json:"downstream_impacted"`
	StatusCounts       map[string]int            `json:"status_counts"`
	WorkflowRuns       []SummaryWorkflowRun      `json:"workflow_runs"`
	Nodes              []SummaryNode             `json:"nodes"`
	Edges              []repository.RunGraphEdge `json:"edges"`
}

type SummaryWorkflowRun struct {
	RunID   int64  `json:"run_id"`
	NodeKey string `json:"node_key"`
	Status  string `json:"status"`
	IsFocus bool   `json:"is_focus"`
}

type SummaryNode struct {
	JobID       int64      `json:"job_id"`
	RunID       int64      `json:"run_id"`
	NodeKey     string     `json:"node_key"`
	DisplayName string     `json:"display_name"`
	Status      string     `json:"status"`
	IsFocusRun  bool       `json:"is_focus_run"`
	ReasonCode  *string    `json:"reason_code,omitempty"`
	LastError   *string    `json:"last_error,omitempty"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
}

type Summarizer struct {
	client *OpenAIClient
}

func NewSummarizerFromEnv() *Summarizer {
	key := strings.TrimSpace(os.Getenv("OPENAI_API_KEY"))
	if key == "" {
		return &Summarizer{}
	}
	model := strings.TrimSpace(os.Getenv("OPENAI_MODEL"))
	if model == "" {
		model = "gpt-5.4-mini"
	}
	baseURL := strings.TrimSpace(os.Getenv("OPENAI_BASE_URL"))
	if baseURL == "" {
		baseURL = "https://api.openai.com/v1"
	}
	return &Summarizer{client: &OpenAIClient{APIKey: key, Model: model, BaseURL: strings.TrimRight(baseURL, "/"), HTTPClient: &http.Client{Timeout: 45 * time.Second}}}
}

func (s *Summarizer) Enabled() bool { return s != nil && s.client != nil }

func (s *Summarizer) SummarizeRun(ctx context.Context, graph *repository.RunGraph) (*RunSummary, error) {
	if graph == nil {
		return nil, errors.New("run graph is required")
	}
	summary := heuristicSummary(graph)
	if !s.Enabled() {
		summary.Source = "heuristic"
		return summary, nil
	}
	aiResp, err := s.client.Summarize(ctx, summary.Context)
	if err != nil {
		summary.Source = "heuristic"
		summary.Model = s.client.Model
		summary.AIError = err.Error()
		return summary, nil
	}
	summary.Type = normalizeFailureType(aiResp.Type, summary.Type)
	if strings.TrimSpace(aiResp.Cause) != "" {
		summary.Cause = strings.TrimSpace(aiResp.Cause)
	}
	if strings.TrimSpace(aiResp.Impact) != "" {
		summary.Impact = strings.TrimSpace(aiResp.Impact)
	}
	if len(aiResp.NextSteps) > 0 {
		summary.NextSteps = compactStrings(aiResp.NextSteps)
	}
	if aiResp.Confidence > 0 {
		summary.Confidence = aiResp.Confidence
	}
	summary.Retry = normalizeRetry(aiResp.Retry, summary.Retry)
	summary.Source = "openai"
	summary.Model = s.client.Model
	return summary, nil
}

func heuristicSummary(graph *repository.RunGraph) *RunSummary {
	ctx := buildSummaryContext(graph)
	failed := focusProblemNode(graph)
	rootCause := rootCauseNode(graph, failed)
	summary := &RunSummary{
		RunID:        graph.Run.ID,
		RunStatus:    graph.Run.Status,
		DAGID:        graph.Run.DAGID,
		DAGVersionID: graph.Run.DAGVersionID,
		Type:         heuristicType(failed, rootCause),
		Cause:        heuristicCause(failed, rootCause),
		Impact:       heuristicImpact(ctx.DownstreamImpacted),
		NextSteps:    heuristicNextSteps(failed, rootCause),
		Confidence:   0.55,
		Retry:        heuristicRetry(failed, rootCause),
		Source:       "heuristic",
		GeneratedAt:  time.Now().UTC(),
		Context:      ctx,
	}
	if failed != nil {
		summary.FailedJobID = &failed.JobID
		summary.FailedNode = failed.NodeKey
	}
	return summary
}

func buildSummaryContext(graph *repository.RunGraph) SummaryContext {
	nodeByID := map[int64]repository.RunJob{}
	children := map[int64][]int64{}
	parents := map[int64][]int64{}
	statusCounts := map[string]int{}
	nodes := make([]SummaryNode, 0, len(graph.Nodes))
	workflowRuns := make([]SummaryWorkflowRun, 0)
	runSeen := map[int64]bool{}
	for _, n := range graph.Nodes {
		nodeByID[n.JobID] = n
		statusCounts[string(n.Status)]++
		nodes = append(nodes, SummaryNode{JobID: n.JobID, RunID: n.RunID, NodeKey: n.NodeKey, DisplayName: n.DisplayName, Status: string(n.Status), IsFocusRun: n.RunID == graph.Run.ID, ReasonCode: n.ReasonCode, LastError: n.LastError, StartedAt: n.StartedAt, FinishedAt: n.FinishedAt})
		if !runSeen[n.RunID] {
			runSeen[n.RunID] = true
			workflowRuns = append(workflowRuns, SummaryWorkflowRun{RunID: n.RunID, NodeKey: n.NodeKey, Status: string(n.Status), IsFocus: n.RunID == graph.Run.ID})
		}
	}
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].RunID != nodes[j].RunID {
			return nodes[i].RunID < nodes[j].RunID
		}
		return nodes[i].JobID < nodes[j].JobID
	})
	sort.Slice(workflowRuns, func(i, j int) bool { return workflowRuns[i].RunID < workflowRuns[j].RunID })
	for _, e := range graph.Edges {
		children[e.FromJobID] = append(children[e.FromJobID], e.ToJobID)
		parents[e.ToJobID] = append(parents[e.ToJobID], e.FromJobID)
	}
	focus := focusProblemNode(graph)
	rootCause := rootCauseNode(graph, focus)
	upstreamCompleted := []string{}
	downstreamImpacted := []string{}
	failedNodeKey := ""
	failedStatus := ""
	failedReasonCode := ""
	failedReason := ""
	rootCauseNodeKey := ""
	rootCauseStatus := ""
	rootCauseReason := ""
	if focus != nil {
		failedNodeKey = focus.NodeKey
		failedStatus = string(focus.Status)
		if focus.ReasonCode != nil {
			failedReasonCode = *focus.ReasonCode
		}
		if focus.LastError != nil {
			failedReason = *focus.LastError
		}
		seenUp := map[int64]bool{}
		var walkUp func(int64)
		walkUp = func(id int64) {
			for _, p := range parents[id] {
				if seenUp[p] {
					continue
				}
				seenUp[p] = true
				parent := nodeByID[p]
				if parent.Status == repository.JobStatusSucceeded {
					upstreamCompleted = append(upstreamCompleted, parent.NodeKey)
				}
				walkUp(p)
			}
		}
		seenDown := map[int64]bool{}
		var walkDown func(int64)
		walkDown = func(id int64) {
			for _, c := range children[id] {
				if seenDown[c] {
					continue
				}
				seenDown[c] = true
				child := nodeByID[c]
				if child.Status != repository.JobStatusSucceeded {
					downstreamImpacted = append(downstreamImpacted, child.NodeKey)
				}
				walkDown(c)
			}
		}
		walkUp(focus.JobID)
		walkDown(focus.JobID)
		sort.Strings(upstreamCompleted)
		sort.Strings(downstreamImpacted)
	}
	if rootCause != nil {
		rootCauseNodeKey = rootCause.NodeKey
		rootCauseStatus = string(rootCause.Status)
		rootCauseReason = strings.TrimSpace(joinNonEmpty(ptrString(rootCause.ReasonCode), ptrString(rootCause.LastError)))
	}
	return SummaryContext{
		FocusRunID:         graph.Run.ID,
		FailedNodeKey:      failedNodeKey,
		FailedStatus:       failedStatus,
		FailedReasonCode:   failedReasonCode,
		FailedReason:       failedReason,
		RootCauseNodeKey:   rootCauseNodeKey,
		RootCauseStatus:    rootCauseStatus,
		RootCauseReason:    rootCauseReason,
		UpstreamCompleted:  upstreamCompleted,
		DownstreamImpacted: downstreamImpacted,
		StatusCounts:       statusCounts,
		WorkflowRuns:       workflowRuns,
		Nodes:              nodes,
		Edges:              graph.Edges,
	}
}

func focusProblemNode(graph *repository.RunGraph) *repository.RunJob {
	if graph == nil {
		return nil
	}
	focusNodes := make([]repository.RunJob, 0)
	for _, n := range graph.Nodes {
		if n.RunID == graph.Run.ID {
			focusNodes = append(focusNodes, n)
		}
	}
	if focus := firstProblemNode(focusNodes); focus != nil {
		return focus
	}
	return firstProblemNode(graph.Nodes)
}

func rootCauseNode(graph *repository.RunGraph, focus *repository.RunJob) *repository.RunJob {
	if graph == nil || focus == nil {
		return focus
	}
	if focus.Status != repository.JobStatusBlocked {
		return focus
	}
	nodeByID := map[int64]repository.RunJob{}
	parents := map[int64][]int64{}
	for _, n := range graph.Nodes {
		nodeByID[n.JobID] = n
	}
	for _, e := range graph.Edges {
		parents[e.ToJobID] = append(parents[e.ToJobID], e.FromJobID)
	}
	seen := map[int64]bool{}
	var best *repository.RunJob
	var walk func(int64)
	walk = func(id int64) {
		for _, p := range parents[id] {
			if seen[p] {
				continue
			}
			seen[p] = true
			parent := nodeByID[p]
			if parent.Status != repository.JobStatusSucceeded && parent.Status != repository.JobStatusWaiting && parent.Status != repository.JobStatusQueued && parent.Status != repository.JobStatusRunning && parent.Status != repository.JobStatusDispatching && parent.Status != repository.JobStatusDispatched {
				candidate := parent
				if best == nil || problemRank(candidate.Status) < problemRank(best.Status) || (problemRank(candidate.Status) == problemRank(best.Status) && candidate.JobID < best.JobID) {
					best = &candidate
				}
			}
			walk(p)
		}
	}
	walk(focus.JobID)
	if best != nil {
		return best
	}
	return focus
}

func problemRank(status repository.JobStatus) int {
	switch status {
	case repository.JobStatusFailed:
		return 1
	case repository.JobStatusLost:
		return 2
	case repository.JobStatusMissed:
		return 3
	case repository.JobStatusBlocked:
		return 4
	case repository.JobStatusCancelled:
		return 5
	default:
		return 999
	}
}

func firstProblemNode(nodes []repository.RunJob) *repository.RunJob {
	var best *repository.RunJob
	bestRank := 999
	for i := range nodes {
		n := &nodes[i]
		r := problemRank(n.Status)
		ok := r < 999
		if !ok {
			continue
		}
		if best == nil || r < bestRank || (r == bestRank && n.JobID < best.JobID) {
			best = n
			bestRank = r
		}
	}
	return best
}

func heuristicType(n *repository.RunJob, rootCause *repository.RunJob) string {
	if n == nil {
		return "unknown"
	}
	if n.Status == repository.JobStatusBlocked && rootCause != nil && rootCause != n {
		return normalizeFailureType(statusOrReasonType(rootCause), "dependency")
	}
	return normalizeFailureType(statusOrReasonType(n), "unknown")
}

func statusOrReasonType(n *repository.RunJob) string {
	if n == nil {
		return "unknown"
	}
	text := strings.ToLower(joinNonEmpty(string(n.Status), ptrString(n.ReasonCode), ptrString(n.LastError)))
	switch {
	case strings.Contains(text, "timeout") || strings.Contains(text, "heartbeat"):
		return "timeout"
	case strings.Contains(text, "validation") || strings.Contains(text, "invalid") || strings.Contains(text, "payload"):
		return "validation"
	case strings.Contains(text, "dependency") || n.Status == repository.JobStatusBlocked:
		return "dependency"
	default:
		return "unknown"
	}
}

func heuristicCause(n *repository.RunJob, rootCause *repository.RunJob) string {
	if n == nil {
		return "No failed or blocked node was found in the run graph."
	}
	if n.Status == repository.JobStatusBlocked && rootCause != nil && rootCause != n {
		causeText := strings.TrimSpace(joinNonEmpty(ptrString(rootCause.ReasonCode), ptrString(rootCause.LastError)))
		if causeText == "" {
			causeText = "An upstream dependency did not complete successfully."
		}
		return fmt.Sprintf("%s was blocked because upstream node %s failed first. %s", n.NodeKey, rootCause.NodeKey, causeText)
	}
	if s := strings.TrimSpace(ptrString(n.LastError)); s != "" {
		return s
	}
	if s := strings.TrimSpace(ptrString(n.ReasonCode)); s != "" {
		return s
	}
	switch n.Status {
	case repository.JobStatusBlocked:
		return "The node could not run because an upstream dependency did not complete successfully."
	case repository.JobStatusLost:
		return "The job started or was dispatched but stopped reporting progress before completion."
	case repository.JobStatusMissed:
		return "The job did not execute within its expected execution window."
	case repository.JobStatusFailed:
		return "The job reported terminal failure."
	default:
		return "Unable to determine a specific cause from the recorded job state."
	}
}

func heuristicImpact(impacted []string) string {
	if len(impacted) == 0 {
		return "No downstream blocked or incomplete nodes were detected from the failed node."
	}
	return fmt.Sprintf("Downstream nodes that may not complete because of this failure: %s.", strings.Join(impacted, ", "))
}

func heuristicNextSteps(n *repository.RunJob, rootCause *repository.RunJob) []string {
	steps := []string{"Inspect the failed node's reason_code and last_error fields.", "Review the run graph to confirm which downstream nodes are blocked or still waiting."}
	if n == nil {
		return steps
	}
	if n.Status == repository.JobStatusBlocked && rootCause != nil && rootCause != n {
		steps = append(steps,
			fmt.Sprintf("Inspect upstream node %s because it is the likely root cause for blocked node %s.", rootCause.NodeKey, n.NodeKey),
			"After the upstream failure is fixed, restart the failed branch or rerun the workflow window.",
		)
		text := strings.ToLower(joinNonEmpty(ptrString(rootCause.ReasonCode), ptrString(rootCause.LastError)))
		if strings.Contains(text, "timeout") || rootCause.Status == repository.JobStatusLost {
			steps = append(steps, "Check service latency, worker heartbeats, and retry policy before rerunning.")
		}
		return compactStrings(steps)
	}
	text := strings.ToLower(joinNonEmpty(ptrString(n.ReasonCode), ptrString(n.LastError)))
	switch {
	case strings.Contains(text, "timeout") || n.Status == repository.JobStatusLost:
		steps = append(steps, "Check the downstream service latency, worker heartbeats, and network availability.")
	case strings.Contains(text, "payload") || strings.Contains(text, "validation"):
		steps = append(steps, "Validate the payload for the failed node and confirm upstream output shape.")
	case n.Status == repository.JobStatusBlocked:
		steps = append(steps, "Fix the upstream failed node first, then restart this job or the affected branch.")
	default:
		steps = append(steps, "Check the worker logs and the target service response for the failed job.")
	}
	return compactStrings(steps)
}

func heuristicRetry(n *repository.RunJob, rootCause *repository.RunJob) string {
	if n == nil {
		return "requires_manual_review"
	}
	if n.Status == repository.JobStatusBlocked && rootCause != nil && rootCause != n {
		return heuristicRetry(rootCause, nil)
	}
	text := strings.ToLower(joinNonEmpty(ptrString(n.ReasonCode), ptrString(n.LastError)))
	switch {
	case strings.Contains(text, "timeout") || strings.Contains(text, "http 5") || strings.Contains(text, "tempor") || n.Status == repository.JobStatusLost:
		return "safe"
	case strings.Contains(text, "validation") || strings.Contains(text, "invalid"):
		return "unsafe"
	default:
		return "requires_manual_review"
	}
}

func normalizeFailureType(v, fallback string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "timeout", "validation", "dependency", "unknown":
		return strings.ToLower(strings.TrimSpace(v))
	default:
		if fallback != "" {
			return fallback
		}
		return "unknown"
	}
}

func normalizeRetry(v, fallback string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "safe", "unsafe", "requires_manual_review":
		return strings.ToLower(strings.TrimSpace(v))
	default:
		if fallback != "" {
			return fallback
		}
		return "requires_manual_review"
	}
}

func compactStrings(items []string) []string {
	out := make([]string, 0, len(items))
	seen := map[string]bool{}
	for _, item := range items {
		s := strings.TrimSpace(item)
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

func ptrString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func joinNonEmpty(parts ...string) string {
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if strings.TrimSpace(p) != "" {
			out = append(out, p)
		}
	}
	return strings.Join(out, " ")
}

type OpenAIClient struct {
	APIKey     string
	Model      string
	BaseURL    string
	HTTPClient *http.Client
}

type openAIRequest struct {
	Model          string            `json:"model"`
	Temperature    float64           `json:"temperature,omitempty"`
	ResponseFormat map[string]string `json:"response_format,omitempty"`
	Messages       []openAIMessage   `json:"messages"`
}

type openAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error,omitempty"`
}

type aiSummaryPayload struct {
	Type       string   `json:"type"`
	Cause      string   `json:"cause"`
	Impact     string   `json:"impact"`
	NextSteps  []string `json:"next_steps"`
	Confidence float64  `json:"confidence"`
	Retry      string   `json:"retry"`
}

type aiSummaryRaw struct {
	Type       string      `json:"type"`
	Cause      string      `json:"cause"`
	Impact     string      `json:"impact"`
	NextSteps  interface{} `json:"next_steps"`
	Confidence float64     `json:"confidence"`
	Retry      string      `json:"retry"`
}

func (c *OpenAIClient) Summarize(ctx context.Context, summaryCtx SummaryContext) (*aiSummaryPayload, error) {
	if c == nil || strings.TrimSpace(c.APIKey) == "" {
		return nil, errors.New("OPENAI_API_KEY is not configured")
	}
	if c.HTTPClient == nil {
		c.HTTPClient = &http.Client{Timeout: 45 * time.Second}
	}
	promptBytes, _ := json.MarshalIndent(summaryCtx, "", "  ")
	system := "You are a workflow debugging assistant. Return strict JSON only with keys: type, cause, impact, next_steps, confidence, retry. Do not include markdown, code fences, or explanatory text outside the JSON object. type must be one of timeout, validation, dependency, unknown. retry must be one of safe, unsafe, requires_manual_review. confidence must be between 0 and 1. next_steps must be a JSON array of strings, never a single string. Keep cause and impact concise and actionable."
	user := "Summarize this failed or problematic workflow run context. Explain where the failure occurred, the likely cause, what did not run or was impacted, and the best next steps.\n\nRun context JSON:\n" + string(promptBytes)
	reqBody := openAIRequest{
		Model:          c.Model,
		Temperature:    0.0,
		ResponseFormat: map[string]string{"type": "json_object"},
		Messages:       []openAIMessage{{Role: "system", Content: system}, {Role: "user", Content: user}},
	}
	body, _ := json.Marshal(reqBody)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.APIKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var parsed openAIResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, fmt.Errorf("openai decode failed: %w", err)
	}
	if resp.StatusCode >= 300 {
		if parsed.Error != nil && parsed.Error.Message != "" {
			return nil, fmt.Errorf("openai error: %s", parsed.Error.Message)
		}
		return nil, fmt.Errorf("openai returned status %d", resp.StatusCode)
	}
	if len(parsed.Choices) == 0 {
		return nil, errors.New("openai returned no choices")
	}
	content := strings.TrimSpace(parsed.Choices[0].Message.Content)
	if content == "" {
		return nil, errors.New("openai returned empty content")
	}
	content = stripJSONCodeFence(content)
	var raw aiSummaryRaw
	if err := json.Unmarshal([]byte(content), &raw); err != nil {
		return nil, fmt.Errorf("openai returned non-json content: %w", err)
	}
	out := aiSummaryPayload{
		Type:       raw.Type,
		Cause:      raw.Cause,
		Impact:     raw.Impact,
		NextSteps:  normalizeNextSteps(raw.NextSteps),
		Confidence: raw.Confidence,
		Retry:      raw.Retry,
	}
	if out.Confidence < 0 {
		out.Confidence = 0
	}
	if out.Confidence > 1 {
		out.Confidence = 1
	}
	return &out, nil
}

func stripJSONCodeFence(content string) string {
	trimmed := strings.TrimSpace(content)
	if !strings.HasPrefix(trimmed, "```") {
		return trimmed
	}
	trimmed = strings.TrimPrefix(trimmed, "```json")
	trimmed = strings.TrimPrefix(trimmed, "```")
	trimmed = strings.TrimSuffix(strings.TrimSpace(trimmed), "```")
	return strings.TrimSpace(trimmed)
}

func normalizeNextSteps(v interface{}) []string {
	switch t := v.(type) {
	case nil:
		return nil
	case string:
		return compactStrings([]string{t})
	case []string:
		return compactStrings(t)
	case []interface{}:
		steps := make([]string, 0, len(t))
		for _, item := range t {
			switch x := item.(type) {
			case string:
				steps = append(steps, x)
			case map[string]interface{}:
				if s, ok := x["step"].(string); ok {
					steps = append(steps, s)
				}
			default:
				steps = append(steps, fmt.Sprint(x))
			}
		}
		return compactStrings(steps)
	default:
		return compactStrings([]string{fmt.Sprint(v)})
	}
}
