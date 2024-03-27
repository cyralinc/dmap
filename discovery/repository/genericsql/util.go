package genericsql

import (
	"database/sql"
)

// GetCurrentRowAsMap transforms the current row referenced by a sql.Rows row
// set into a map where the key is the column name and the value is the column
// value. It is effectively an alternative to the sql.Rows.Scan method, where it
// copies the value of the current row into a string/interface map. Note: just
// like Scan, because this only operates on the current row pointed to by the
// row set, this function does not iterate the row set forward. Therefore,
// sql.Rows.Next must be called first to iterate the row set forward, and the
// same error checking applies. The map returned represents the single,
// current-most row pointed to by the row set iterator.
func GetCurrentRowAsMap(rows *sql.Rows) (map[string]any, error) {
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
