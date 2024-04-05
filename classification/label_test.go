package classification

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// This isn't really a test as much as it is a validation of the embedded labels
// and their classification rules. The test will fail if the labels or their
// classification rules are invalid for any reason. It includes parsing the
// label Rego code, and validating that have their expected output. It serves
// mostly as a build-time a sanity check, which should hopefully avoid us
// releasing a build with broken embedded labels!
func TestGetPredefinedLabels_LabelsAreValid(t *testing.T) {
	// We want to read the labels ourselves in this test, so we can compare them
	// against the labels returned by GetPredefinedLabels.
	fname := "labels/labels.yaml"
	type yamlLabel struct {
		Label
		Rule string `yaml:"rule"`
	}
	yamlBytes, err := predefinedLabelsFs.ReadFile(fname)
	require.NoError(t, err)
	yamlLabels := make(map[string]yamlLabel)
	err = yaml.Unmarshal(yamlBytes, &yamlLabels)
	require.NoError(t, err)

	got, err := GetPredefinedLabels()
	require.NoError(t, err)
	require.Len(t, got, len(yamlLabels))

	// Validate the classification rules for each label by doing dummy
	// classification. We don't expect any results for an empty input - we
	// really just want to validate that the classification rules are valid and
	// there were no errors during the classification process.
	ctx := context.Background()
	classifier, err := NewLabelClassifier(ctx, got...)
	res, err := classifier.Classify(ctx, map[string]any{})
	require.NoError(t, err)
	require.Empty(t, res)
}
