package classification

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewLabelClassifier_Success(t *testing.T) {
	lbl, err := NewLabel("foo", "test label", "package foo\noutput = true")
	require.NoError(t, err)
	classifier, err := NewLabelClassifier(lbl)
	require.NoError(t, err)
	require.NotNil(t, classifier)
}

func TestLabelClassifier_Classify(t *testing.T) {
	tests := []struct {
		name       string
		classifier *LabelClassifier
		input      map[string]any
		want       Result
		wantError  require.ErrorAssertionFunc
	}{
		{
			name:       "nil input",
			classifier: &LabelClassifier{},
			input:      nil,
			want:       make(Result),
		},
		{
			name:       "empty input",
			classifier: &LabelClassifier{},
			input:      map[string]any{},
			want:       make(Result),
		},
		{
			name:       "success: single label, single attribute",
			classifier: newTestLabelClassifier(t, "AGE"),
			input:      map[string]any{"age": "42"},
			want: Result{
				"age": {
					"AGE": {},
				},
			},
		},
		{
			name:       "success: single label, multiple attributes",
			classifier: newTestLabelClassifier(t, "AGE"),
			input: map[string]any{
				"age": "42",
				"ccn": "4111111111111111",
			},
			want: Result{
				"age": {
					"AGE": {},
				},
			},
		},
		{
			name:       "success: multiple labels, single attribute",
			classifier: newTestLabelClassifier(t, "AGE", "CCN"),
			input:      map[string]any{"age": "42"},
			want: Result{
				"age": {
					"AGE": {},
				},
			},
		},
		{
			name:       "success: multiple labels, multiple attributes",
			classifier: newTestLabelClassifier(t, "AGE", "CCN"),
			input: map[string]any{
				"age": "42",
				"ccn": "4111111111111111",
			},
			want: Result{
				"age": {
					"AGE": {},
				},
				"ccn": {
					"CCN": {},
				},
			},
		},
		{
			name:       "success: multiple labels, multiple attributes, false positive",
			classifier: newTestLabelClassifier(t, "AGE", "CVV"),
			input: map[string]any{
				"age": "101",
				"cvv": "234",
			},
			want: Result{
				"age": {
					"AGE": {},
					"CVV": {},
				},
				"cvv": {
					"CVV": {},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				got, err := tt.classifier.Classify(context.Background(), tt.input)
				if tt.wantError == nil {
					tt.wantError = require.NoError
				}
				tt.wantError(t, err)
				requireResultEqual(t, tt.want, got)
			},
		)
	}
}

func requireResultEqual(t *testing.T, want, got Result) {
	require.Len(t, got, len(want))
	for k, v := range want {
		gotSet, ok := got[k]
		require.Truef(t, ok, "missing attribute %s", k)
		requireLabelSetEqual(t, v, gotSet)
	}
}

func requireLabelSetEqual(t *testing.T, want, got LabelSet) {
	require.Len(t, got, len(want))
	for k := range want {
		_, ok := got[k]
		require.Truef(t, ok, "missing label %s", k)
	}
}

func newTestLabelClassifier(t *testing.T, lblNames ...string) *LabelClassifier {
	lbls := make([]Label, len(lblNames))
	for i, lblName := range lblNames {
		lbls[i] = newTestLabel(t, lblName)
	}
	classifier, err := NewLabelClassifier(lbls...)
	require.NoError(t, err)
	return classifier
}

func newTestLabel(t *testing.T, lblName string) Label {
	fname := "labels/" + strings.ReplaceAll(strings.ToLower(lblName), " ", "_") + ".rego"
	fin, err := regoFs.ReadFile(fname)
	require.NoError(t, err)
	classifierCode := string(fin)
	lbl, err := NewLabel(lblName, "test label", classifierCode)
	require.NoError(t, err)
	return lbl
}
