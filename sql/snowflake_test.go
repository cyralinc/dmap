package sql

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestSnowflakeRepository_ListDatabases(t *testing.T) {
	ctx, db, mock, r := initSnowflakeRepoTest(t)
	defer func() { _ = db.Close() }()
	dbRows := sqlmock.NewRows([]string{"name"}).AddRow("db1").AddRow("db2")
	mock.ExpectQuery(SnowflakeDatabaseQuery).WillReturnRows(dbRows)
	dbs, err := r.ListDatabases(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"db1", "db2"}, dbs)
}

func initSnowflakeRepoTest(t *testing.T) (context.Context, *sql.DB, sqlmock.Sqlmock, *SnowflakeRepository) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	return ctx, db, mock, &SnowflakeRepository{
		generic: NewGenericRepositoryFromDB(RepoTypeSnowflake, "dbName", db),
	}
}
