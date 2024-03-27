package classification

import (
	"context"
	"strings"
	"testing"

	"github.com/open-policy-agent/opa/rego"
	"github.com/stretchr/testify/require"
)

func TestNewLabelClassifier_Success(t *testing.T) {
	classifier, err := NewLabelClassifier(
		LabelAndRule{
			Label: Label{
				Name: "foo",
			},
			ClassificationRule: rego.PreparedEvalQuery{},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, classifier)
}

func TestLabelClassifierClassify(t *testing.T) {
	tests := []struct {
		name       string
		classifier *LabelClassifier
		attrs      map[string]any
		want       map[string][]Label
		wantError  require.ErrorAssertionFunc
	}{
		{
			name:       "error nil attrs",
			classifier: &LabelClassifier{},
			attrs:      nil,
			wantError:  require.Error,
		},
		{
			name:       "error nil classifier",
			classifier: nil,
			attrs:      map[string]any{"test": "test"},
			wantError:  require.Error,
		},
		{
			name:       "error empty attributes",
			classifier: &LabelClassifier{},
			attrs:      map[string]any{},
			wantError:  require.Error,
		},
		{
			name:       "success: single label, single attribute",
			classifier: newTestLabelClassifier(t, "AGE"),
			attrs:      map[string]any{"age": "42"},
			want: map[string][]Label{
				"age": {{Name: "AGE"}},
			},
		},
		{
			name:       "success: single label, multiple attributes",
			classifier: newTestLabelClassifier(t, "AGE"),
			attrs: map[string]any{
				"age": "42",
				"ccn": "4111111111111111",
			},
			want: map[string][]Label{
				"age": {{Name: "AGE"}},
			},
		},
		{
			name:       "success: multiple labels, single attribute",
			classifier: newTestLabelClassifier(t, "AGE", "CCN"),
			attrs:      map[string]any{"age": "42"},
			want: map[string][]Label{
				"age": {{Name: "AGE"}},
			},
		},
		{
			name:       "success: multiple labels, multiple attributes",
			classifier: newTestLabelClassifier(t, "AGE", "CCN"),
			attrs: map[string]any{
				"age": "42",
				"ccn": "4111111111111111",
			},
			want: map[string][]Label{
				"age": {{Name: "AGE"}},
				"ccn": {{Name: "CCN"}},
			},
		},
		{
			name:       "success: multiple labels, multiple attributes, false positive",
			classifier: newTestLabelClassifier(t, "AGE", "CVV"),
			attrs: map[string]any{
				"age": "101",
				"cvv": "234",
			},
			want: map[string][]Label{
				"age": {{Name: "AGE"}, {Name: "CVV"}},
				"cvv": {{Name: "CVV"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				got, err := tt.classifier.Classify(context.Background(), tt.attrs)
				if tt.wantError == nil {
					tt.wantError = require.NoError
				}
				tt.wantError(t, err)
				require.Equal(t, tt.want, got)
			},
		)
	}
}

func TestGetEmbeddedLabels(t *testing.T) {
	got, err := GetEmbeddedLabels()
	require.NoError(t, err)
	require.NotEmpty(t, got)
}

func newTestLabel(t *testing.T, lblName string) LabelAndRule {
	fname := "rego/" + strings.ReplaceAll(strings.ToLower(lblName), " ", "_") + ".rego"
	fin, err := regoFs.ReadFile(fname)
	require.NoError(t, err)
	classifierCode := string(fin)
	rule, err := prepareClassificationRule(classifierCode)
	require.NoError(t, err)
	return LabelAndRule{
		Label: Label{
			Name: lblName,
		},
		ClassificationRule: rule,
	}
}

func newTestLabelClassifier(t *testing.T, lblNames ...string) *LabelClassifier {
	lbls := make([]LabelAndRule, len(lblNames))
	for i, lblName := range lblNames {
		lbls[i] = newTestLabel(t, lblName)
	}
	classifier, err := NewLabelClassifier(lbls...)
	require.NoError(t, err)
	return classifier
}
