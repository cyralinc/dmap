package classification

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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
		Label `yaml:",inline"`
		Rule  string `yaml:"rule"`
	}
	yamlBytes, err := predefinedLabelsFs.ReadFile(fname)
	require.NoError(t, err)
	yamlLabels := make(map[string]yamlLabel)
	err = yaml.Unmarshal(yamlBytes, &yamlLabels)
	require.NoError(t, err)
	for name, lbl := range yamlLabels {
		lbl.Name = name
		yamlLabels[name] = lbl
	}

	got, err := GetPredefinedLabels()
	require.NoError(t, err)
	require.Len(t, got, len(yamlLabels))
	for i, lbl := range got {
		want := yamlLabels[lbl.Name]
		require.Equal(t, want.Name, got[i].Name)
		require.Equal(t, want.Description, got[i].Description)
		require.ElementsMatch(t, want.Tags, got[i].Tags)
		// We don't care about the actual rule content here, just that it was
		// loaded. We'll validate the content below.
		require.NotNil(t, got[i].ClassificationRule)
	}

	// Validate the classification rules for each label by doing dummy
	// classification. We don't expect any results for an empty input - we
	// really just want to validate that the classification rules are valid and
	// there were no errors during the classification process.
	ctx := context.Background()
	classifier, err := NewLabelClassifier(ctx, got...)
	require.NoError(t, err)
	res, err := classifier.Classify(ctx, map[string]any{})
	require.NoError(t, err)
	require.Empty(t, res)
}

func TestGetCustomLabels_RelativeRulePath_SameDir(t *testing.T) {
	labelsDir := t.TempDir()

	// Create the labels YAML file.
	labelsYamlFile, err := os.CreateTemp(labelsDir, "labels.yaml")
	defer func() { _ = labelsYamlFile.Close() }()
	require.NoError(t, err)
	labelsYaml := `ADDRESS:
  description: Address
  rule: address.rego
  tags:
    - PII`
	err = os.WriteFile(labelsYamlFile.Name(), []byte(labelsYaml), os.FileMode(0755))
	require.NoError(t, err)

	// Create the rule rego file.
	ruleFile, err := os.Create(filepath.Join(labelsDir, "address.rego"))
	defer func() { _ = ruleFile.Close() }()
	require.NoError(t, err)
	err = os.WriteFile(ruleFile.Name(), []byte("package foo"), os.FileMode(0755))
	require.NoError(t, err)

	want := []Label{
		{
			Name:        "ADDRESS",
			Description: "Address",
			Tags:        []string{"PII"},
		},
	}
	got, err := GetCustomLabels(labelsYamlFile.Name())
	require.NoError(t, err)
	require.Len(t, got, len(want))
	for i := range got {
		require.Equal(t, want[i].Name, got[i].Name)
		require.Equal(t, want[i].Description, got[i].Description)
		require.ElementsMatch(t, want[i].Tags, got[i].Tags)
		// We don't care about the actual rule content, just that it was loaded.
		require.NotNil(t, got[i].ClassificationRule)
	}
}

func TestGetCustomLabels_RelativeRulePath_DifferentDir(t *testing.T) {
	labelsDir := t.TempDir()
	ruleDir := t.TempDir()
	relRulPath, err := filepath.Rel(labelsDir, ruleDir)
	require.NoError(t, err)

	// Create the labels YAML file.
	labelsYamlFile, err := os.CreateTemp(labelsDir, "labels.yaml")
	defer func() { _ = labelsYamlFile.Close() }()
	require.NoError(t, err)
	labelsYaml := fmt.Sprintf(
		`ADDRESS:
  description: Address
  rule: %s/address.rego
  tags:
    - PII`, relRulPath,
	)
	err = os.WriteFile(labelsYamlFile.Name(), []byte(labelsYaml), os.FileMode(0755))
	require.NoError(t, err)

	// Create the rule rego file.
	ruleFile, err := os.Create(filepath.Join(ruleDir, "address.rego"))
	defer func() { _ = ruleFile.Close() }()
	require.NoError(t, err)
	err = os.WriteFile(ruleFile.Name(), []byte("package foo"), os.FileMode(0755))
	require.NoError(t, err)

	want := []Label{
		{
			Name:        "ADDRESS",
			Description: "Address",
			Tags:        []string{"PII"},
		},
	}
	got, err := GetCustomLabels(labelsYamlFile.Name())
	require.NoError(t, err)
	require.Len(t, got, len(want))
	for i := range got {
		require.Equal(t, want[i].Name, got[i].Name)
		require.Equal(t, want[i].Description, got[i].Description)
		require.ElementsMatch(t, want[i].Tags, got[i].Tags)
		// We don't care about the actual rule content, just that it was loaded.
		require.NotNil(t, got[i].ClassificationRule)
	}
}

func TestGetCustomLabels_AbsoluteRulePath(t *testing.T) {
	labelsDir := t.TempDir()
	ruleDir := t.TempDir()

	// Create the labels YAML file.
	labelsYamlFile, err := os.CreateTemp(labelsDir, "labels.yaml")
	defer func() { _ = labelsYamlFile.Close() }()
	require.NoError(t, err)
	labelsYaml := fmt.Sprintf(
		`ADDRESS:
  description: Address
  rule: %s/address.rego
  tags:
    - PII`, ruleDir,
	)
	err = os.WriteFile(labelsYamlFile.Name(), []byte(labelsYaml), os.FileMode(0755))
	require.NoError(t, err)

	// Create the rule rego file.
	ruleFile, err := os.Create(filepath.Join(ruleDir, "address.rego"))
	defer func() { _ = ruleFile.Close() }()
	require.NoError(t, err)
	err = os.WriteFile(ruleFile.Name(), []byte("package foo"), os.FileMode(0755))
	require.NoError(t, err)

	want := []Label{
		{
			Name:        "ADDRESS",
			Description: "Address",
			Tags:        []string{"PII"},
		},
	}
	got, err := GetCustomLabels(labelsYamlFile.Name())
	require.NoError(t, err)
	require.Len(t, got, len(want))
	for i := range got {
		require.Equal(t, want[i].Name, got[i].Name)
		require.Equal(t, want[i].Description, got[i].Description)
		require.ElementsMatch(t, want[i].Tags, got[i].Tags)
		// We don't care about the actual rule content, just that it was loaded.
		require.NotNil(t, got[i].ClassificationRule)
	}
}
