package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/gobwas/glob"
)

const (
	genericIntrospectQuery = "SELECT " +
		"table_schema, " +
		"table_name, " +
		"column_name, " +
		"data_type " +
		"FROM " +
		"INFORMATION_SCHEMA.COLUMNS " +
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
	genericPingQuery           = "SELECT 1"
	genericSampleQueryTemplate = "SELECT %s FROM %s.%s LIMIT ? OFFSET ?"
)

// GenericRepository implements generic SQL functionalities that work for a
// subset of ANSI SQL compatible databases. Many Repository implementations may
// partially or fully delegate to this implementation. In that respect, it acts
// somewhat as a base implementation which can be used by SQL-compatible
// repositories. Note that while GenericRepository is an implementation of
// the Repository interface, GenericRepository is meant to be used as a building
// block for other Repository implementations, rather than as a standalone
// implementation. Specifically, the Repository.ListDatabases method is left
// un-implemented, since there is no standard way to list databases across
// different SQL database platforms. It does however provide the
// ListDatabasesWithQuery method, which dependent implementations can use to
// provide a custom query to list databases.
type GenericRepository struct {
	repoType string
	database string
	db       *sql.DB
}

var _ Repository = (*GenericRepository)(nil)

// NewGenericRepository is a constructor for the GenericRepository type. It
// opens a database handle for a given repoType and returns a pointer to a new
// GenericRepository instance. A connection may or may not be established
// depending on the underlying database type, as determined by the repoType
// parameter. The maxOpenConns parameter specifies the maximum number of open
// connections to the database. The repoIncludePaths and repoExcludePaths
// parameters are used to filter the tables and columns that are introspected by
// the repository.
func NewGenericRepository(repoType, database, connStr string, maxOpenConns uint) (
	*GenericRepository,
	error,
) {
	db, err := newDbHandle(repoType, connStr, maxOpenConns)
	if err != nil {
		return nil, fmt.Errorf("error retrieving DB handle for repo type %s: %w", repoType, err)
	}
	return &GenericRepository{
		repoType: repoType,
		database: database,
		db:       db,
	}, nil
}

// NewGenericRepositoryFromDB instantiate a new GenericRepository based on a
// given sql.DB handle.
func NewGenericRepositoryFromDB(repoType, database string, db *sql.DB) *GenericRepository {
	return &GenericRepository{
		repoType: repoType,
		database: database,
		db:       db,
	}
}

// ListDatabases is left unimplemented for GenericRepository, because there is
// no standard way to list databases across different SQL database platforms.
// See ListDatabasesWithQuery for a way to list databases using a custom query.
func (r *GenericRepository) ListDatabases(_ context.Context) ([]string, error) {
	return nil, errors.New("ListDatabases is not implemented")
}

// ListDatabasesWithQuery returns a list of the names of all databases on the
// server, as determined by the given query. The query is expected to return
// a row set containing a single column corresponding to the database name. If
// the query returns more than one column, an error will be returned.
func (r *GenericRepository) ListDatabasesWithQuery(
	ctx context.Context,
	query string,
	params ...any,
) ([]string, error) {
	log.Tracef("Query: %s", query)
	rows, err := r.db.QueryContext(ctx, query, params...)
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
func (r *GenericRepository) Introspect(
	ctx context.Context,
	params IntrospectParameters,
) (*Metadata, error) {
	return r.IntrospectWithQuery(ctx, genericIntrospectQuery, params)
}

// IntrospectWithQuery executes a query against the information_schema table in
// the database which returns a four-column (all varchar) row set (of N rows,
// depending on the number of tables in the database) in the form:
//
// table_schema, table_name, column_name, data_type
//
// This row set represents all the columns of all the tables in the repository.
// The row set is then parsed into an instance of Metadata and
// returned. Additionally, any errors which occur during the query execution or
// parsing process will be returned.
func (r *GenericRepository) IntrospectWithQuery(
	ctx context.Context,
	query string,
	params IntrospectParameters,
) (*Metadata, error) {
	log.Tracef("Query: %s", query)
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("error performing introspect query: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return newMetadataFromQueryResult(r.database, params.IncludePaths, params.ExcludePaths, rows)
}

// SampleTable samples the table referenced by the TableMetadata meta parameter
// by issuing a standard, ANSI-compatible SELECT query to the database. All
// attributes of the table are selected, and are quoted using double quotes. See
// Repository.SampleTable for more details.
func (r *GenericRepository) SampleTable(
	ctx context.Context,
	params SampleParameters,
) (Sample, error) {
	// ANSI SQL uses double-quotes to quote identifiers
	attrStr := params.Metadata.QuotedAttributeNamesString("\"")
	query := fmt.Sprintf(genericSampleQueryTemplate, attrStr, params.Metadata.Schema, params.Metadata.Name)
	return r.SampleTableWithQuery(ctx, query, params)
}

// SampleTableWithQuery calls SampleTable with a custom SQL query. Any
// placeholder parameters in the query should be passed via params.
func (r *GenericRepository) SampleTableWithQuery(
	ctx context.Context,
	query string,
	params SampleParameters,
) (Sample, error) {
	log.Tracef("Query: %s", query)
	rows, err := r.db.QueryContext(ctx, query, params.SampleSize, params.Offset)
	if err != nil {
		return Sample{},
			fmt.Errorf(
				"error sampling database %s, schema %s, table %s: %w",
				r.database,
				params.Metadata.Schema,
				params.Metadata.Name,
				err,
			)
	}
	defer func() { _ = rows.Close() }()
	sample := Sample{
		TablePath: []string{r.database, params.Metadata.Schema, params.Metadata.Name},
	}
	// Iterate the row set and append each row to the sample results.
	for rows.Next() {
		data, err := getCurrentRowAsMap(rows)
		if err != nil {
			return Sample{}, err
		}
		sample.Results = append(sample.Results, data)
	}
	if err := rows.Err(); err != nil {
		// Something broke while iterating the row set.
		return Sample{}, fmt.Errorf("error iterating sample data row set: %w", err)
	}
	if len(sample.Results) == 0 {
		return Sample{}, nil
	}
	return sample, nil
}

// Ping verifies the connection to the database used by this repository by
// executing a simple query. If the query fails, an error is returned.
func (r *GenericRepository) Ping(ctx context.Context) error {
	log.Tracef("Query: %s", genericPingQuery)
	rows, err := r.db.QueryContext(ctx, genericPingQuery)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	return nil
}

// GetDb is a getter for the repository's sql.DB handle.
func (r *GenericRepository) GetDb() *sql.DB {
	return r.db
}

// Close closes the database connection used by the repository.
func (r *GenericRepository) Close() error {
	return r.db.Close()
}

// newDbHandle opens a new database sql.DB handle for the given repoType and
// connection string. The maxOpenConns parameter specifies the maximum number of
// open connections to the database.
func newDbHandle(repoType, connStr string, maxOpenConns uint) (*sql.DB, error) {
	db, err := sql.Open(repoType, connStr)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(int(maxOpenConns))
	return db, nil
}

// getCurrentRowAsMap transforms the current row referenced by a sql.Rows row
// set into a map where the key is the column name and the value is the column
// value. It is effectively an alternative to the sql.Rows.Scan method, where it
// copies the value of the current row into a string/interface map. Note: just
// like Scan, because this only operates on the current row pointed to by the
// row set, this function does not iterate the row set forward. Therefore,
// sql.Rows.Next must be called first to iterate the row set forward, and the
// same error checking applies. The map returned represents the single,
// current-most row pointed to by the row set iterator.
func getCurrentRowAsMap(rows *sql.Rows) (map[string]any, error) {
	colNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	row := make(map[string]any, len(colNames))
	colValues := make([]any, len(colNames))
	colValPointers := make([]any, len(colNames))
	for i := range colValues {
		colValPointers[i] = &colValues[i]
	}
	// Scans the row into the set of column-value pointers
	if err := rows.Scan(colValPointers...); err != nil {
		return nil, err
	}
	for i, colName := range colNames {
		row[colName] = colValues[i]
	}
	return row, nil
}

// matchPathPatterns checks if the given database, schema, and table match any
// of the given glob patterns. It returns true if the database, schema, and
// table match any of the patterns, and false otherwise.
func matchPathPatterns(database, schema, table string, patterns []glob.Glob) bool {
	for _, pattern := range patterns {
		if pattern.Match(fmt.Sprintf("%s.%s.%s", database, schema, table)) {
			return true
		}
	}
	return false
}
