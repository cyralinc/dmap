package discovery

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestMySqlRepository_ListDatabases(t *testing.T) {
	ctx, db, mock, r := initMySqlRepoTest(t)
	defer func() { _ = db.Close() }()
	dbRows := sqlmock.NewRows([]string{"name"}).AddRow("db1").AddRow("db2")
	mock.ExpectQuery(MySqlDatabaseQuery).WillReturnRows(dbRows)
	dbs, err := r.ListDatabases(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"db1", "db2"}, dbs)
}

func initMySqlRepoTest(t *testing.T) (context.Context, *sql.DB, sqlmock.Sqlmock, *MySqlRepository) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	return ctx, db, mock, &MySqlRepository{
		genericSqlRepo: NewGenericRepositoryFromDB("repoName", RepoTypeMysql, "dbName", db),
	}
}
