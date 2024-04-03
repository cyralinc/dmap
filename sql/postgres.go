package sql

import (
	"context"
	"fmt"
	"strings"

	// Postgresql DB driver
	_ "github.com/lib/pq"
)

const (
	RepoTypePostgres = "postgres"

	PostgresDatabaseQuery = `
SELECT 
	datname 
FROM
	pg_database
WHERE
	datistemplate = false
	AND datallowconn = true
	AND datname <> 'rdsadmin'
`
)

// PostgresRepository is a Repository implementation for Postgres databases.
type PostgresRepository struct {
	// The majority of the Repository functionality is delegated to
	// a generic SQL repository instance.
	generic *GenericRepository
}

// PostgresRepository implements Repository
var _ Repository = (*PostgresRepository)(nil)

// NewPostgresRepository creates a new PostgresRepository.
func NewPostgresRepository(cfg RepoConfig) (*PostgresRepository, error) {
	pgCfg, err := parsePostgresConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("error parsing postgres config: %w", err)
	}
	database := cfg.Database
	// Connect to the default database, if unspecified.
	if database == "" {
		database = "postgres"
	}
	connStr := fmt.Sprintf(
		"postgresql://%s:%s@%s:%d/%s%s",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		database,
		pgCfg.ConnOptsStr,
	)
	generic, err := NewGenericRepository(RepoTypePostgres, cfg.Database, connStr, cfg.MaxOpenConns)
	if err != nil {
		return nil, fmt.Errorf("could not instantiate generic sql repository: %w", err)
	}
	return &PostgresRepository{generic: generic}, nil
}

// ListDatabases returns a list of the names of all databases on the server by
// using a Postgres-specific database query. It delegates the actual work to
// GenericRepository.ListDatabasesWithQuery - see that method for more details.
func (r *PostgresRepository) ListDatabases(ctx context.Context) ([]string, error) {
	return r.generic.ListDatabasesWithQuery(ctx, PostgresDatabaseQuery)
}

// Introspect delegates introspection to GenericRepository. See
// Repository.Introspect and GenericRepository.IntrospectWithQuery for more
// details.
func (r *PostgresRepository) Introspect(ctx context.Context, params IntrospectParameters) (*Metadata, error) {
	return r.generic.Introspect(ctx, params)
}

// SampleTable delegates sampling to GenericRepository, using a
// Postgres-specific table sample query. See Repository.SampleTable and
// GenericRepository.SampleTableWithQuery for more details.
func (r *PostgresRepository) SampleTable(
	ctx context.Context,
	params SampleParameters,
) (Sample, error) {
	// Postgres uses double-quotes to quote identifiers
	attrStr := params.Metadata.QuotedAttributeNamesString("\"")
	// Postgres uses $x for placeholders
	query := fmt.Sprintf(
		"SELECT %s FROM %s.%s LIMIT $1 OFFSET $2",
		attrStr,
		params.Metadata.Schema,
		params.Metadata.Name,
	)
	return r.generic.SampleTableWithQuery(ctx, query, params)
}

// Ping delegates the ping to GenericRepository. See Repository.Ping and
// GenericRepository.Ping for more details.
func (r *PostgresRepository) Ping(ctx context.Context) error {
	return r.generic.Ping(ctx)
}

// Close delegates the close to GenericRepository. See Repository.Close and
// GenericRepository.Close for more details.
func (r *PostgresRepository) Close() error {
	return r.generic.Close()
}

// PostgresConfig contains Postgres-specific configuration parameters.
type PostgresConfig struct {
	// ConnOptsStr is a string containing Postgres-specific connection options.
	ConnOptsStr string
}

// parsePostgresConfig parses the Postgres-specific configuration parameters
// from the given  The Postgres connection options are built from the
// config and stored in the ConnOptsStr field of the returned Postgres
func parsePostgresConfig(cfg RepoConfig) (*PostgresConfig, error) {
	connOptsStr, err := buildConnOptsStr(cfg)
	if err != nil {
		return nil, fmt.Errorf("error building connection options string: %w", err)
	}
	return &PostgresConfig{ConnOptsStr: connOptsStr}, nil
}

// buildConnOptsStr parses the repo config to produce a string in the format
// "?option=value&option2=value2". Example:
//
//	buildConnOptsStr(RepoConfig{
//	    Advanced: map[string]any{
//	        "connection-string-args": []any{"sslmode=disable"},
//	    },
//	})
//
// returns ("?sslmode=disable", nil).
func buildConnOptsStr(cfg RepoConfig) (string, error) {
	connOptsMap, err := mapFromConnOpts(cfg)
	if err != nil {
		return "", fmt.Errorf("connection options: %w", err)
	}
	connOptsStr := ""
	for key, val := range connOptsMap {
		// Don't add if the value is empty, since that would make the
		// string malformed.
		if val != "" {
			if connOptsStr == "" {
				connOptsStr += fmt.Sprintf("%s=%s", key, val)
			} else {
				// Need & for subsequent options
				connOptsStr += fmt.Sprintf("&%s=%s", key, val)
			}
		}
	}
	// Only add ? if connection string is not empty
	if connOptsStr != "" {
		connOptsStr = "?" + connOptsStr
	}
	return connOptsStr, nil
}

// mapFromConnOpts builds a map from the list of connection options given. Each
// option has the format 'option=value'. An error is returned if the config is
// malformed.
func mapFromConnOpts(cfg RepoConfig) (map[string]string, error) {
	m := make(map[string]string)
	connOptsInterface, ok := cfg.Advanced[configConnOpts]
	if !ok {
		return nil, nil
	}
	connOpts, ok := connOptsInterface.([]any)
	if !ok {
		return nil, fmt.Errorf("'%s' is not a list", configConnOpts)
	}
	for _, optInterface := range connOpts {
		opt, ok := optInterface.(string)
		if !ok {
			return nil, fmt.Errorf("'%v' is not a string", optInterface)
		}
		splitOpt := strings.Split(opt, "=")
		if len(splitOpt) != 2 {
			return nil, fmt.Errorf(
				"malformed '%s'. "+
					"Please follow the format 'option=value'", configConnOpts,
			)
		}
		key := splitOpt[0]
		val := splitOpt[1]
		m[key] = val
	}
	return m, nil
}
