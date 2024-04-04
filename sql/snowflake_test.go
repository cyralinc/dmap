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
	mock.ExpectQuery(snowflakeDatabaseQuery).WillReturnRows(dbRows)
	dbs, err := r.ListDatabases(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"db1", "db2"}, dbs)
}

func TestNewSnowflakeConfigFromMap(t *testing.T) {
	tests := []struct {
		name    string
		cfg     map[string]any
		want    SnowflakeConfig
		wantErr require.ErrorAssertionFunc
	}{
		{
			name: "Returns config when all keys are present",
			cfg: map[string]any{
				configAccount:   "testAccount",
				configRole:      "testRole",
				configWarehouse: "testWarehouse",
			},
			want: SnowflakeConfig{
				Account:   "testAccount",
				Role:      "testRole",
				Warehouse: "testWarehouse",
			},
		},
		{
			name: "Returns error when account key is missing",
			cfg: map[string]any{
				configRole:      "testRole",
				configWarehouse: "testWarehouse",
			},
			wantErr: require.Error,
		},
		{
			name: "Returns error when role key is missing",
			cfg: map[string]any{
				configAccount:   "testAccount",
				configWarehouse: "testWarehouse",
			},
			wantErr: require.Error,
		},
		{
			name: "Returns error when warehouse key is missing",
			cfg: map[string]any{
				configAccount: "testAccount",
				configRole:    "testRole",
			},
			wantErr: require.Error,
		},
	}
	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				got, err := NewSnowflakeConfigFromMap(tt.cfg)
				if tt.wantErr == nil {
					tt.wantErr = require.NoError
				}
				tt.wantErr(t, err)
				require.Equal(t, tt.want, got)
			},
		)
	}
}

func initSnowflakeRepoTest(t *testing.T) (context.Context, *sql.DB, sqlmock.Sqlmock, *SnowflakeRepository) {
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	return ctx, db, mock, &SnowflakeRepository{
		generic: NewGenericRepositoryFromDB(RepoTypeSnowflake, "dbName", db),
	}
}
