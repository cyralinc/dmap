package sql

import (
	"strings"
)

// Metadata represents the structure of a SQL database. The traditional
// hierarchy is: Server (cluster) > Database > Schema (namespace) > Table. Some
// database systems do not have the concept of a "database" (e.g. MySQL). In
// those cases, the 'Database' field is expected to be an empty string. See:
// https://stackoverflow.com/a/17943883
type Metadata struct {
	Name     string
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
func NewMetadata(repoType, repoName, database string) *Metadata {
	return &Metadata{
		Name:     repoName,
		RepoType: repoType,
		Database: database,
		Schemas:  make(map[string]*SchemaMetadata),
	}
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
