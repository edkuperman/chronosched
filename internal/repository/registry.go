package repository

// Repos aggregates all repository interfaces used by higher layers.
type Repos struct {
	Namespaces  NamespaceRepository
	Definitions JobDefinitionRepository
	DAGs        DAGRepository
	Runs        RunRepository
	Jobs        JobRepository
	Queue       QueueRepository
	Admin       AdminRepository
}
