package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAttributeNames(t *testing.T) {
	table := TableMetadata{
		Schema: "schema",
		Name:   "table",
		Attributes: []*AttributeMetadata{
			{
				Schema:   "schema",
				Table:    "table",
				Name:     "name1",
				DataType: "varchar",
			},
			{
				Schema:   "schema",
				Table:    "table",
				Name:     "name2",
				DataType: "varchar",
			},
			{
				Schema:   "schema",
				Table:    "table",
				Name:     "name3",
				DataType: "varchar",
			},
		},
	}

	names := []string{"name1", "name2", "name3"}

	require.ElementsMatch(t, table.AttributeNames(), names)
}

func TestQuotedAttributeNamesString(t *testing.T) {
	table := TableMetadata{
		Schema: "schema",
		Name:   "table",
		Attributes: []*AttributeMetadata{
			{
				Schema:   "schema",
				Table:    "table",
				Name:     "name1",
				DataType: "varchar",
			},
			{
				Schema:   "schema",
				Table:    "table",
				Name:     "name2",
				DataType: "varchar",
			},
			{
				Schema:   "schema",
				Table:    "table",
				Name:     "name3",
				DataType: "varchar",
			},
		},
	}

	quoteChar := "`"
	expected := "`name1`,`name2`,`name3`"
	namesStr := table.QuotedAttributeNamesString(quoteChar)

	require.Equal(t, expected, namesStr)
}
