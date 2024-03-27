// Package genericsql provides a generic implementation of SQL functionalities
// that works for a subset of ANSI SQL compatible databases. Many
// repository.Repository implementations may partially or fully delegate to this
// implementation. In that respect, it acts somewhat as a base implementation
// which can be used by SQL-compatible repositories.
package genericsql

import (
	"context"
	"database/sql"
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/cyralinc/dmap/discovery/repository"

	"github.com/gobwas/glob"
)

const (
	IntrospectQuery = "SELECT " +
		"table_schema, " +
		"table_name, " +
		"column_name, " +
		"data_type " +
		"FROM " +
		"information_schema.columns " +
		"WHERE " +
		"table_schema NOT IN " +
		"(" +
		"'INFORMATION_SCHEMA', " +
		"'information_schema', " +
		"'mysql', " +
		"'sys', " +
		"'performance_schema', " +
		"'pg_catalog'" +
		")"
	PingQuery           = "SELECT 1"
	SampleQueryTemplate = "SELECT %s FROM %s.%s LIMIT ? OFFSET ?"
)

// GenericSqlRepository implements generic SQL functionalities that
// works for a subset of ANSI SQL compatible databases and may be
// useful for some repository.Repository implementations.
type GenericSqlRepository struct {
	repoName     string
	repoType     string
	database     string
	db           *sql.DB
	includePaths []glob.Glob
	excludePaths []glob.Glob
}

// NewGenericSqlRepository is a constructor for the GenericSqlRepository type.
// It establishes a database connection for a given repoType and returns a
// pointer to a GenericSqlRepository instance.
func NewGenericSqlRepository(
	repoName, repoType, database, connStr string, maxOpenConns uint,
	repoIncludePaths, repoExcludePaths []glob.Glob,
) (
	*GenericSqlRepository,
	error,
) {
	db, err := getDbHandle(repoType, connStr, maxOpenConns)
	if err != nil {
		return nil, fmt.Errorf("error retrieving DB handle for repo type %s: %w", repoType, err)
	}
	return &GenericSqlRepository{
		repoName:     repoName,
		repoType:     repoType,
		database:     database,
		db:           db,
		includePaths: repoIncludePaths,
		excludePaths: repoExcludePaths,
	}, nil
}

// NewGenericSqlRepositoryFromDB instantiate a new *GenericSqlRepository based
// on a given db connection. This can be used by tests to mock the database.
func NewGenericSqlRepositoryFromDB(
	repoName, repoType, database string,
	db *sql.DB,
) *GenericSqlRepository {
	return &GenericSqlRepository{
		repoName: repoName,
		repoType: repoType,
		database: database,
		db:       db,
	}
}

// ListDatabasesWithQuery returns a list of the names of all databases on the
// server, as determined by the given query. The query is expected to return
// a row set containing a single column corresponding to the database name. If
// the query returns more than one column, an error will be returned.
func (repo *GenericSqlRepository) ListDatabasesWithQuery(
	ctx context.Context,
	query string,
	params ...any,
) ([]string, error) {
	log.Tracef("Query: %s", query)
	rows, err := repo.db.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, fmt.Errorf("error querying databases: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var dbs []string
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			return nil, fmt.Errorf("error scanning database query result row: %w", err)
		}
		dbs = append(dbs, dbName)
	}
	// Something broke while iterating the row set
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating database query rows: %w", err)
	}
	return dbs, nil
}

// Introspect calls IntrospectWithQuery with a default query string
func (repo *GenericSqlRepository) Introspect(ctx context.Context) (*repository.Metadata, error) {
	return repo.IntrospectWithQuery(ctx, IntrospectQuery)
}

/*
IntrospectWithQuery executes a query against the information_schema table in the
database which returns a four-column (all varchar) row set (of N rows, depending
on the number of tables in the database) in the form:

table_schema, table_name, column_name, data_type

This row set represents all the columns of all the tables in the repository.
The row set is then parsed into an instance of repository.Metadata and
returned. Additionally, any errors which occur during the query execution or
parsing process will be returned.
*/
func (repo *GenericSqlRepository) IntrospectWithQuery(
	ctx context.Context,
	query string,
) (*repository.Metadata, error) {
	log.Tracef("Query: %s", query)
	rows, err := repo.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	repoMeta, err := newMetadataFromQueryResult(
		repo.repoType, repo.repoName,
		repo.database, repo.includePaths, repo.excludePaths, rows,
	)
	if err != nil {
		return nil, err
	}
	return repoMeta, nil
}

func (repo *GenericSqlRepository) SampleTable(
	ctx context.Context,
	meta *repository.TableMetadata,
	params repository.SampleParameters,
) (repository.Sample, error) {
	// ANSI SQL uses double-quotes to quote identifiers
	attrStr := meta.QuotedAttributeNamesString("\"")
	query := fmt.Sprintf(SampleQueryTemplate, attrStr, meta.Schema, meta.Name)
	return repo.SampleTableWithQuery(ctx, meta, query, params.SampleSize, params.Offset)
}

// SampleTableWithQuery calls SampleTable with a custom SQL query. Any
// placeholder parameters in the query should be passed via params.
func (repo *GenericSqlRepository) SampleTableWithQuery(
	ctx context.Context,
	meta *repository.TableMetadata,
	query string,
	params ...any,
) (repository.Sample, error) {
	log.Tracef("Query: %s", query)
	rows, err := repo.db.QueryContext(ctx, query, params...)
	if err != nil {
		return repository.Sample{},
			fmt.Errorf("error sampling %s.%s.%s: %w", repo.database, meta.Schema, meta.Name, err)
	}
	defer func() { _ = rows.Close() }()

	sample := repository.Sample{
		Metadata: repository.SampleMetadata{
			Repo:     repo.repoName,
			Database: repo.database,
			Schema:   meta.Schema,
			Table:    meta.Name,
		},
	}

	for rows.Next() {
		data, err := GetCurrentRowAsMap(rows)
		if err != nil {
			return repository.Sample{}, err
		}
		sample.Results = append(sample.Results, data)
	}

	// Something broke while iterating the row set
	err = rows.Err()
	if err != nil {
		return repository.Sample{}, fmt.Errorf("error iterating sample data row set: %w", err)
	}

	return sample, nil
}

func (repo *GenericSqlRepository) Ping(ctx context.Context) error {
	log.Tracef("Query: %s", PingQuery)
	rows, err := repo.db.QueryContext(ctx, PingQuery)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	return nil
}

// GetDb is a getter for the repository's sql.DB handle.
func (repo *GenericSqlRepository) GetDb() *sql.DB {
	return repo.db
}

func (repo *GenericSqlRepository) Close() error {
	return repo.db.Close()
}

func getDbHandle(repoType, connStr string, maxOpenConns uint) (*sql.DB, error) {
	db, err := sql.Open(repoType, connStr)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(int(maxOpenConns))
	return db, nil
}

// newMetadataFromQueryResult builds the repository metadata from the results
// of a query to the INFORMATION_SCHEMA columns view.
func newMetadataFromQueryResult(
	repoType, repoName, db string,
	includePaths, excludePaths []glob.Glob, rows *sql.Rows,
) (
	*repository.Metadata,
	error,
) {
	repo := repository.NewMetadata(repoType, repoName, db)

	for rows.Next() {
		var attr repository.AttributeMetadata
		err := rows.Scan(&attr.Schema, &attr.Table, &attr.Name, &attr.DataType)
		if err != nil {
			return nil, err
		}

		// skip tables that match excludePaths or does not match includePaths
		log.Tracef("checking if %s.%s.%s matches excludePaths %s\n", db, attr.Schema, attr.Table, excludePaths)
		if matchPathPatterns(db, attr.Schema, attr.Table, excludePaths) {
			continue
		}
		log.Tracef("checking if %s.%s.%s matches includePaths: %s\n", db, attr.Schema, attr.Table, includePaths)
		if !matchPathPatterns(db, attr.Schema, attr.Table, includePaths) {
			continue
		}

		// SchemaMetadata exists - add a table if necessary
		if schema, ok := repo.Schemas[attr.Schema]; ok {
			// TableMetadata exists - just append the attribute
			if table, ok := schema.Tables[attr.Table]; ok {
				table.Attributes = append(table.Attributes, &attr)
			} else { // First time seeing this table
				table := repository.NewTableMetadata(attr.Schema, attr.Table)
				table.Attributes = append(table.Attributes, &attr)
				schema.Tables[attr.Table] = table
			}
		} else { // SchemaMetadata doesn't exist - create it
			table := repository.NewTableMetadata(attr.Schema, attr.Table)
			table.Attributes = append(table.Attributes, &attr)
			schema := repository.NewSchemaMetadata(attr.Schema)
			schema.Tables[attr.Table] = table
			repo.Schemas[attr.Schema] = schema
		}
	}

	// Something broke while iterating the row set
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return repo, nil
}

func matchPathPatterns(database, schema, table string, patterns []glob.Glob) bool {
	for _, pattern := range patterns {
		if pattern.Match(fmt.Sprintf("%s.%s.%s", database, schema, table)) {
			return true
		}
	}
	return false
}
