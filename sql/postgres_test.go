package sql

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestPostgresRepository_ListDatabases(t *testing.T) {
	ctx, db, mock, r := initPostgresRepoTest(t)
	defer func() { _ = db.Close() }()
	dbRows := sqlmock.NewRows([]string{"name"}).AddRow("db1").AddRow("db2")
	mock.ExpectQuery(postgresDatabaseQuery).WillReturnRows(dbRows)
	dbs, err := r.ListDatabases(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"db1", "db2"}, dbs)
}

func initPostgresRepoTest(t *testing.T) (context.Context, *sql.DB, sqlmock.Sqlmock, *PostgresRepository) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	return ctx, db, mock, &PostgresRepository{
		generic: NewGenericRepositoryFromDB(RepoTypePostgres, "dbName", db),
	}
}
