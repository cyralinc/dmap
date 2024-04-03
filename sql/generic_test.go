package sql

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gobwas/glob"
	"github.com/stretchr/testify/require"
)

func Test_Introspect_IsSuccessful(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	repoType := "genericSql"
	database := "exampleDb"

	repo := GenericRepository{
		repoType: repoType,
		database: database,
		db:       db,
	}

	cols := []string{
		"table_schema",
		"table_name",
		"column_name",
		"data_type",
	}

	rows := sqlmock.NewRows(cols).
		AddRow("schema1", "table1", "column1", "varchar").
		AddRow("schema1", "table1", "column2", "decimal").
		AddRow("schema1", "table2", "column1", "integer").
		AddRow("schema2", "table1", "column1", "date")

	mock.ExpectQuery("SELECT (.+) FROM information_schema.columns WHERE (.+)").
		WillReturnRows(rows)

	ctx := context.Background()

	params := IntrospectParameters{
		IncludePaths: []glob.Glob{glob.MustCompile("exampleDb.*")},
	}
	meta, err := repo.Introspect(ctx, params)

	expectedMetadata := Metadata{
		Database: database,
		Schemas: map[string]*SchemaMetadata{
			"schema1": {
				Name: "schema1",
				Tables: map[string]*TableMetadata{
					"table1": {
						Schema: "schema1",
						Name:   "table1",
						Attributes: []*AttributeMetadata{
							{
								Schema:   "schema1",
								Table:    "table1",
								Name:     "column1",
								DataType: "varchar",
							},
							{
								Schema:   "schema1",
								Table:    "table1",
								Name:     "column2",
								DataType: "decimal",
							},
						},
					},
					"table2": {
						Schema: "schema1",
						Name:   "table2",
						Attributes: []*AttributeMetadata{
							{
								Schema:   "schema1",
								Table:    "table2",
								Name:     "column1",
								DataType: "integer",
							},
						},
					},
				},
			},
			"schema2": {
				Name: "schema2",
				Tables: map[string]*TableMetadata{
					"table1": {
						Schema: "schema2",
						Name:   "table1",
						Attributes: []*AttributeMetadata{
							{
								Schema:   "schema2",
								Table:    "table1",
								Name:     "column1",
								DataType: "date",
							},
						},
					},
				},
			},
		},
	}

	require.NoError(t, err)
	require.EqualValues(t, &expectedMetadata, meta)
}

func Test_Introspect_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	repoType := "genericSql"
	database := "exampleDb"

	repo := GenericRepository{
		repoType: repoType,
		database: database,
		db:       db,
	}

	expectedErr := errors.New("dummy error")

	mock.ExpectQuery("SELECT (.+) FROM information_schema.columns WHERE (.+)").
		WillReturnError(expectedErr)

	ctx := context.Background()

	meta, err := repo.Introspect(ctx, IntrospectParameters{})

	require.Nil(t, meta)
	require.ErrorIs(t, err, expectedErr)
}

func Test_Introspect_RowError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	repoType := "genericSql"
	database := "exampleDb"

	repo := GenericRepository{
		repoType: repoType,
		database: database,
		db:       db,
	}

	cols := []string{
		"table_schema",
		"table_name",
		"column_name",
		"data_type",
	}

	expectedErr := errors.New("dummy error")

	rows := sqlmock.NewRows(cols).
		AddRow("schema1", "table1", "column1", "varchar").
		RowError(0, expectedErr)

	mock.ExpectQuery("SELECT (.+) FROM information_schema.columns WHERE (.+)").
		WillReturnRows(rows)

	ctx := context.Background()

	meta, err := repo.Introspect(ctx, IntrospectParameters{})

	require.Nil(t, meta)
	require.ErrorIs(t, err, expectedErr)
}

func Test_Introspect_Filtered(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	repoType := "genericSql"
	database := "exampleDb"

	repo := GenericRepository{
		repoType: repoType,
		database: database,
		db:       db,
	}

	cols := []string{
		"table_schema",
		"table_name",
		"column_name",
		"data_type",
	}

	rows := sqlmock.NewRows(cols).
		AddRow("schema1", "table1", "column1", "varchar").
		AddRow("schema1", "table1", "column2", "decimal").
		AddRow("schema1", "table2", "column1", "integer").
		AddRow("schema2", "table1", "column1", "date").
		AddRow("schema2", "table1", "column2", "decimal").
		AddRow("schema2", "table2", "column1", "varchar").
		AddRow("schema3", "table1", "column2", "decimal").
		AddRow("schema3", "table2", "column1", "integer")

	mock.ExpectQuery("SELECT (.+) FROM information_schema.columns WHERE (.+)").
		WillReturnRows(rows)

	ctx := context.Background()

	params := IntrospectParameters{
		IncludePaths: []glob.Glob{
			glob.MustCompile("exampleDb.schema1.*"),
			glob.MustCompile("exampleDb.*.table1"),
		},
		ExcludePaths: []glob.Glob{
			glob.MustCompile("exampleDb.schema3.*"),
		},
	}
	meta, err := repo.Introspect(ctx, params)

	expectedMetadata := Metadata{
		Database: database,
		Schemas: map[string]*SchemaMetadata{
			"schema1": {
				Name: "schema1",
				Tables: map[string]*TableMetadata{
					"table1": {
						Schema: "schema1",
						Name:   "table1",
						Attributes: []*AttributeMetadata{
							{
								Schema:   "schema1",
								Table:    "table1",
								Name:     "column1",
								DataType: "varchar",
							},
							{
								Schema:   "schema1",
								Table:    "table1",
								Name:     "column2",
								DataType: "decimal",
							},
						},
					},
					"table2": {
						Schema: "schema1",
						Name:   "table2",
						Attributes: []*AttributeMetadata{
							{
								Schema:   "schema1",
								Table:    "table2",
								Name:     "column1",
								DataType: "integer",
							},
						},
					},
				},
			},
			"schema2": {
				Name: "schema2",
				Tables: map[string]*TableMetadata{
					"table1": {
						Schema: "schema2",
						Name:   "table1",
						Attributes: []*AttributeMetadata{
							{
								Schema:   "schema2",
								Table:    "table1",
								Name:     "column1",
								DataType: "date",
							},
							{
								Schema:   "schema2",
								Table:    "table1",
								Name:     "column2",
								DataType: "decimal",
							},
						},
					},
				},
			},
		},
	}

	require.NoError(t, err)
	require.EqualValues(t, &expectedMetadata, meta)
}
