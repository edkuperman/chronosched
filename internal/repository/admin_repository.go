package repository

import "context"

type CycleCheckResult struct {
    DAGID  string     `json:"dag_id"`
    Cycles [][]string `json:"cycles"`
}

type CycleCheckResponse struct {
    Count   int                `json:"count"`
    Results []CycleCheckResult `json:"results"`
}

type PruneSummary struct {
    JobDefinitionsArchived int `json:"job_definitions_archived"`
    DagsArchived           int `json:"dags_archived"`
    JobsArchived           int `json:"jobs_archived"`
    DependenciesArchived   int `json:"dependencies_archived"`
}

type AdminRepository interface {
    CheckGlobalCycles(ctx context.Context) (*CycleCheckResponse, error)
    Prune(ctx context.Context) (*PruneSummary, error)
}
