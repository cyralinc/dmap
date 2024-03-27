package sqlserver

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/cyralinc/dmap/discovery/repository/genericsql"
)

func initRepoTest(t *testing.T) (context.Context, *sql.DB, sqlmock.Sqlmock, *SqlServerRepository) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	return ctx, db, mock, &SqlServerRepository{
		genericSqlRepo: genericsql.NewGenericSqlRepositoryFromDB("repoName", RepoTypeSqlServer, "dbName", db),
	}
}

func TestListDatabases(t *testing.T) {
	ctx, db, mock, r := initRepoTest(t)
	defer db.Close()
	dbRows := sqlmock.NewRows([]string{"name"}).AddRow("db1").AddRow("db2")
	mock.ExpectQuery(DatabaseQuery).WillReturnRows(dbRows)
	dbs, err := r.ListDatabases(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"db1", "db2"}, dbs)
}
