package repository

// Repos aggregates all repository interfaces used by higher layers.
type Repos struct {
    Namespaces NamespaceRepository
    DAGs       DAGRepository
    Definitions JobDefinitionRepository
    Jobs       JobRepository
    Queue      QueueRepository
    Deps       DependencyRepository
    Admin      AdminRepository
}
