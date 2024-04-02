package discovery

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestSqlServerRepository_ListDatabases(t *testing.T) {
	ctx, db, mock, r := initSqlServerRepoTest(t)
	defer func() { _ = db.Close() }()
	dbRows := sqlmock.NewRows([]string{"name"}).AddRow("db1").AddRow("db2")
	mock.ExpectQuery(SqlServerDatabaseQuery).WillReturnRows(dbRows)
	dbs, err := r.ListDatabases(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"db1", "db2"}, dbs)
}

func initSqlServerRepoTest(t *testing.T) (context.Context, *sql.DB, sqlmock.Sqlmock, *SqlServerRepository) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	return ctx, db, mock, &SqlServerRepository{
		genericSqlRepo: NewGenericRepositoryFromDB("repoName", RepoTypeSqlServer, "dbName", db),
	}
}
