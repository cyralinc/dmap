package sql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/gobwas/glob"
	log "github.com/sirupsen/logrus"
)

// Metadata represents the structure of a SQL database. The traditional
// hierarchy is: Server (cluster) > Database > Schema (namespace) > Table. Some
// database systems do not have the concept of a "database" (e.g. MySQL). In
// those cases, the 'Database' field is expected to be an empty string. See:
// https://stackoverflow.com/a/17943883
type Metadata struct {
	RepoType string
	Database string
	Schemas  map[string]*SchemaMetadata
}

// SchemaMetadata represents the structure of a database schema. It contains a
// map of tables that belong to the schema. The key is the table name and the
// value is the table metadata for that table.
type SchemaMetadata struct {
	Name   string
	Tables map[string]*TableMetadata
}

// TableMetadata represents the structure of a database table. It contains a
// slice of attributes (i.e. columns) that belong to the table.
type TableMetadata struct {
	Schema     string
	Name       string
	Attributes []*AttributeMetadata
}

// AttributeNames returns a slice of attribute names for the table.
func (t *TableMetadata) AttributeNames() []string {
	attrNames := make([]string, 0, len(t.Attributes))
	for _, attr := range t.Attributes {
		attrNames = append(attrNames, attr.Name)
	}
	return attrNames
}

// QuotedAttributeNamesString returns a string of comma-separated attribute
// names for the table, with each name quoted using the given quote character.
func (t *TableMetadata) QuotedAttributeNamesString(quoteChar string) string {
	sep := quoteChar + "," + quoteChar
	commaSeparatedNames := strings.Join(t.AttributeNames(), sep)
	return quoteChar + commaSeparatedNames + quoteChar
}

// AttributeMetadata represents the structure of a database attribute (i.e.
// column). It contains the schema, table, name, and data type of the attribute.
type AttributeMetadata struct {
	Schema   string `field:"table_schema"`
	Table    string `field:"table_name"`
	Name     string `field:"column_name"`
	DataType string `field:"data_type"`
}

// NewMetadata creates a new Metadata object with the given repository type,
// repository name, and database name, with an empty map of schemas.
func NewMetadata(database string) *Metadata {
	return &Metadata{
		Database: database,
		Schemas:  make(map[string]*SchemaMetadata),
	}
}

// newMetadataFromQueryResult builds the repository metadata from the results
// of a query to the INFORMATION_SCHEMA columns view.
func newMetadataFromQueryResult(
	db string,
	includePaths, excludePaths []glob.Glob,
	rows *sql.Rows,
) (
	*Metadata,
	error,
) {
	repo := NewMetadata(db)
	for rows.Next() {
		var attr AttributeMetadata
		if err := rows.Scan(&attr.Schema, &attr.Table, &attr.Name, &attr.DataType); err != nil {
			return nil, fmt.Errorf("error scanning metadata query result row: %w", err)
		}
		// Skip tables that match excludePaths or does not match includePaths.
		log.Tracef("checking if %s.%s.%s matches excludePaths %s\n", db, attr.Schema, attr.Table, excludePaths)
		if matchPathPatterns(db, attr.Schema, attr.Table, excludePaths) {
			continue
		}
		log.Tracef("checking if %s.%s.%s matches includePaths: %s\n", db, attr.Schema, attr.Table, includePaths)
		if !matchPathPatterns(db, attr.Schema, attr.Table, includePaths) {
			continue
		}
		// SchemaMetadata exists - add a table if necessary.
		if schema, ok := repo.Schemas[attr.Schema]; ok {
			// TableMetadata exists - just append the attribute.
			if table, ok := schema.Tables[attr.Table]; ok {
				table.Attributes = append(table.Attributes, &attr)
			} else { // First time seeing this table.
				table := NewTableMetadata(attr.Schema, attr.Table)
				table.Attributes = append(table.Attributes, &attr)
				schema.Tables[attr.Table] = table
			}
		} else { // SchemaMetadata doesn't exist - create it.
			table := NewTableMetadata(attr.Schema, attr.Table)
			table.Attributes = append(table.Attributes, &attr)
			schema := NewSchemaMetadata(attr.Schema)
			schema.Tables[attr.Table] = table
			repo.Schemas[attr.Schema] = schema
		}
	}
	// Something broke while iterating the row set.
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating metadata query rows: %w", err)
	}
	return repo, nil
}

// NewSchemaMetadata creates a new SchemaMetadata object with the given schema
// name and an empty map of tables.
func NewSchemaMetadata(schemaName string) *SchemaMetadata {
	return &SchemaMetadata{
		Name:   schemaName,
		Tables: make(map[string]*TableMetadata),
	}
}

// NewTableMetadata creates a new TableMetadata object with the given schema and
// table name, and an empty slice of attributes.
func NewTableMetadata(schemaName, tableName string) *TableMetadata {
	return &TableMetadata{
		Schema:     schemaName,
		Name:       tableName,
		Attributes: []*AttributeMetadata{},
	}
}
