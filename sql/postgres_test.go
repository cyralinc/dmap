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
	mock.ExpectQuery(PostgresDatabaseQuery).WillReturnRows(dbRows)
	dbs, err := r.ListDatabases(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"db1", "db2"}, dbs)
}

func TestBuildConnOptionsSucc(t *testing.T) {
	sampleRepoCfg := getSampleRepoConfig()
	connOptsStr, err := buildConnOptsStr(sampleRepoCfg)
	require.NoError(t, err)
	require.Equal(t, connOptsStr, "?sslmode=disable")
}

func TestBuildConnOptionsFail(t *testing.T) {
	invalidRepoCfg := RepoConfig{
		Advanced: map[string]any{
			// Invalid: map instead of string
			configConnOpts: []any{
				map[string]string{"sslmode": "disable"},
			},
		},
	}
	connOptsStr, err := buildConnOptsStr(invalidRepoCfg)
	require.Error(t, err)
	require.Empty(t, connOptsStr)
}

func TestMapConnOptionsSucc(t *testing.T) {
	sampleRepoCfg := getSampleRepoConfig()
	connOptsMap, err := mapFromConnOpts(sampleRepoCfg)
	require.NoError(t, err)
	require.EqualValues(
		t, connOptsMap, map[string]string{
			"sslmode": "disable",
		},
	)
}

// The mapping should only fail if the config is malformed, not if it is missing
func TestMapConnOptionsMissing(t *testing.T) {
	sampleCfg := RepoConfig{}
	optsMap, err := mapFromConnOpts(sampleCfg)
	require.NoError(t, err)
	require.Empty(t, optsMap)
}

func TestMapConnOptionsMalformedMap(t *testing.T) {
	sampleCfg := RepoConfig{
		Advanced: map[string]any{
			// Let's put a map instead of the required list
			configConnOpts: map[string]any{
				"testKey": "testValue",
			},
		},
	}
	_, err := mapFromConnOpts(sampleCfg)
	require.Error(t, err)
}

func TestMapConnOptionsMalformedColon(t *testing.T) {
	sampleCfg := RepoConfig{
		Advanced: map[string]any{
			// Let's use a colon instead of '=' to divide options
			configConnOpts: []string{"sslmode:disable"},
		},
	}
	_, err := mapFromConnOpts(sampleCfg)
	require.Error(t, err)
}

func initPostgresRepoTest(t *testing.T) (context.Context, *sql.DB, sqlmock.Sqlmock, *PostgresRepository) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	return ctx, db, mock, &PostgresRepository{
		generic: NewGenericRepositoryFromDB(RepoTypePostgres, "dbName", db),
	}
}

// Returns a correct repo config
func getSampleRepoConfig() RepoConfig {
	return RepoConfig{
		Advanced: map[string]any{
			configConnOpts: []any{"sslmode=disable"},
		},
	}
}
