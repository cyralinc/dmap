package sql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSampleResult_GetAttributeNamesAndValues(t *testing.T) {
	tests := []struct {
		name                string
		result              SampleResult
		wantNames, wantVals []string
	}{
		{
			name: "string",
			result: SampleResult{
				"foo": "fooVal",
			},
			wantNames: []string{"foo"},
			wantVals:  []string{"fooVal"},
		},
		{
			name: "int",
			result: SampleResult{
				"foo": 123,
			},
			wantNames: []string{"foo"},
			wantVals:  []string{"123"},
		},
		{
			name: "float",
			result: SampleResult{
				"foo": 12.3,
			},
			wantNames: []string{"foo"},
			wantVals:  []string{"12.3"},
		},
		{
			name: "bytes",
			result: SampleResult{
				"foo": []byte("fooVal"),
			},
			wantNames: []string{"foo"},
			wantVals:  []string{"fooVal"},
		},
		{
			name: "bool",
			result: SampleResult{
				"foo": false,
			},
			wantNames: []string{"foo"},
			wantVals:  []string{"false"},
		},
		{
			name: "varied types",
			result: SampleResult{
				"foo":  "fooVal",
				"bar":  123,
				"baz":  12.3,
				"qux":  []byte("quxVal"),
				"quxx": true,
			},
			wantNames: []string{"foo", "bar", "baz", "qux", "quxx"},
			wantVals:  []string{"fooVal", "123", "12.3", "quxVal", "true"},
		},
	}
	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				gotNames, gotVals := tt.result.GetAttributeNamesAndValues()
				require.ElementsMatch(t, tt.wantNames, gotNames)
				require.ElementsMatch(t, tt.wantVals, gotVals)
			},
		)
	}
}
