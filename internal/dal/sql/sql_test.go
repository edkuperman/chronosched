package sql

import (
	"testing"

	"github.com/edkuperman/chronosched/internal/repository"
)

func TestNewSQLDAL(t *testing.T) {
	dal := NewSQLDAL(nil)
	if dal == nil {
		t.Fatal("expected non-nil dal")
	}
	if dal.DB != nil {
		t.Fatal("expected nil DB when constructed with nil")
	}
}

func TestConstructorsReturnNonNilAndImplementInterfaces(t *testing.T) {
	dal := NewSQLDAL(nil)
	var _ repository.NamespaceRepository = NewNamespaceSQL(dal)
	var _ repository.JobDefinitionRepository = NewJobDefinitionSQL(dal)
	var _ repository.DAGRepository = NewDAGSQL(dal)
	var _ repository.RunRepository = NewRunSQL(dal)
	var _ repository.JobRepository = NewJobSQL(dal)
	var _ repository.QueueRepository = NewQueueSQL(dal)
	var _ repository.AdminRepository = NewAdminSQL(dal)

	if NewStore(dal) == nil || NewNamespaceSQL(dal) == nil || NewJobDefinitionSQL(dal) == nil || NewDAGSQL(dal) == nil || NewRunSQL(dal) == nil || NewJobSQL(dal) == nil || NewQueueSQL(dal) == nil || NewAdminSQL(dal) == nil {
		t.Fatal("expected constructors to return non-nil values")
	}
}
