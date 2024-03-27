package repository

import (
	"fmt"
)

// SampleParameters contains all parameters necessary to sample a table.
type SampleParameters struct {
	SampleSize uint
	Offset     uint
}

// Sample represents a sample of a database table. The Metadata field contains
// metadata about the sample itself. The actual results of the sample, which
// are represented by a set of database rows, are contained in the Results
// field.
type Sample struct {
	Metadata SampleMetadata
	Results  []SampleResult
}

// SampleMetadata contains the metadata associated with a given sample, such as
// repo name, database name, table, schema, and query (if applicable). This can
// be used for diagnostic and informational purposes when analyzing a
// particular sample.
type SampleMetadata struct {
	Repo     string
	Database string
	Schema   string
	Table    string
}

// SampleResult stores the results from a single database sample. It is
// equivalent to a database row, where the map key is the column name and the
// map value is the column value.
type SampleResult map[string]any

// GetAttributeNamesAndValues splits a SampleResult map into two slices and
// returns them. The first slice contains all the keys of SampleResult,
// representing the table's attribute names, and the second slice is the map's
// corresponding values.
func (result SampleResult) GetAttributeNamesAndValues() ([]string, []string) {
	names := make([]string, 0, len(result))
	vals := make([]string, 0, len(result))
	for name, val := range result {
		names = append(names, name)
		var v string
		if b, ok := val.([]byte); ok {
			v = string(b)
		} else {
			v = fmt.Sprint(val)
		}
		vals = append(vals, v)
	}
	return names, vals
}
