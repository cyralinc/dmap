package repository

import (
	"strings"
)

// Metadata represents the structure of a database repository catalog. The
// traditional hierarchy is: Server (cluster) > Database > Schema (namespace) >
// Table. Some database systems do not have the concept of a "database" (e.g.
// MySQL). In those cases, the 'Database' field is expected to be an empty
// string. See: https://stackoverflow.com/a/17943883
type Metadata struct {
	Name     string
	RepoType string
	Database string
	Schemas  map[string]*SchemaMetadata
}

// TODO: godoc -ccampo 2024-03-27
type SchemaMetadata struct {
	Name   string
	Tables map[string]*TableMetadata
}

// TODO: godoc -ccampo 2024-03-27
type TableMetadata struct {
	Schema     string
	Name       string
	Attributes []*AttributeMetadata
}

// TODO: godoc -ccampo 2024-03-27
func (t *TableMetadata) AttributeNames() []string {
	attrNames := make([]string, 0, len(t.Attributes))
	for _, attr := range t.Attributes {
		attrNames = append(attrNames, attr.Name)
	}
	return attrNames
}

// TODO: godoc -ccampo 2024-03-27
func (t *TableMetadata) QuotedAttributeNamesString(quoteChar string) string {
	sep := quoteChar + "," + quoteChar
	commaSeparatedNames := strings.Join(t.AttributeNames(), sep)
	return quoteChar + commaSeparatedNames + quoteChar
}

// TODO: godoc -ccampo 2024-03-27
type AttributeMetadata struct {
	Schema   string `field:"table_schema"`
	Table    string `field:"table_name"`
	Name     string `field:"column_name"`
	DataType string `field:"data_type"`
}

// TODO: godoc -ccampo 2024-03-27
func NewMetadata(repoType, repoName, database string) *Metadata {
	return &Metadata{
		Name:     repoName,
		RepoType: repoType,
		Database: database,
		Schemas:  make(map[string]*SchemaMetadata),
	}
}

// TODO: godoc -ccampo 2024-03-27
func NewSchemaMetadata(schemaName string) *SchemaMetadata {
	return &SchemaMetadata{
		Name:   schemaName,
		Tables: make(map[string]*TableMetadata),
	}
}

// TODO: godoc -ccampo 2024-03-27
func NewTableMetadata(schemaName, tableName string) *TableMetadata {
	return &TableMetadata{
		Schema:     schemaName,
		Name:       tableName,
		Attributes: []*AttributeMetadata{},
	}
}
