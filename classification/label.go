package classification

import (
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/open-policy-agent/opa/ast"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

var (
	//go:embed labels/*.rego labels/labels.yaml
	predefinedLabelsFs embed.FS
)

// InvalidLabelsError is an error type that represents an error when one or
// more labels are invalid, e.g. they have invalid classification rules. The
// error contains a slice of errors that caused the error, which can be
// unwrapped to get the individual errors that caused the problems.
type InvalidLabelsError struct {
	errs []error
}

// Unwrap returns the errors that caused the InvalidLabelsError.
func (e InvalidLabelsError) Unwrap() []error {
	return e.errs
}

// Error returns a string representation of the InvalidLabelsError.
func (e InvalidLabelsError) Error() string {
	return errors.Join(e.errs...).Error()
}

// Label represents a data classification label.
type Label struct {
	// Name is the name of the label.
	Name string `yaml:"name" json:"name"`
	// Description is a brief description of the label.
	Description string `yaml:"description" json:"description"`
	// Tags are a list of arbitrary tags associated with the label.
	Tags []string `yaml:"tags" json:"tags"`
	// ClassificationRule is the compiled Rego classification rule used to
	// classify data.
	ClassificationRule *ast.Module `yaml:"-" json:"-"`
}

// NewLabel creates a new Label with the given name, description, classification
// rule, and tags. The classification rule is expected to be the raw Rego code
// that will be used to classify data. If the classification rule is invalid, an
// error is returned.
func NewLabel(name, description, classificationRule string, tags ...string) (Label, error) {
	rule, err := parseRego(classificationRule)
	if err != nil {
		return Label{}, fmt.Errorf("error preparing classification rule for label %s: %w", name, err)
	}
	return Label{
		Name:               name,
		Description:        description,
		Tags:               tags,
		ClassificationRule: rule,
	}, nil
}

// GetPredefinedLabels loads and returns the predefined embedded labels and
// their classification rules. The labels are read from the embedded labels.yaml
// file and the classification rules are read from the embedded Rego files. If
// there is an error reading or unmarshalling the labels file, it is returned.
// If there are errors reading or parsing a classification rules for labels, the
// errors are aggregated into an InvalidLabelsError and returned, along with
// the labels that were successfully read. Note that this should not return an
// error in reality, as the embedded labels should always be valid. If it does,
// it indicates a problem with the embedded labels!
func GetPredefinedLabels() ([]Label, error) {
	return getLabels("labels/labels.yaml", predefinedLabelsFs)
}

// GetCustomLabels loads and returns the labels and their classification rules
// defined in the given labels yaml file. The labels are read from the file
// along with their classification rule Rego files (defined in the yaml). If
// there is an error unmarshalling the labels file, it is returned. If there are
// errors reading or parsing a classification rules for labels, the errors are
// aggregated into an InvalidLabelsError and returned, along with the labels
// that were successfully read.
func GetCustomLabels(labelsYamlFname string) ([]Label, error) {
	path, err := filepath.Abs(labelsYamlFname)
	if err != nil {
		return nil, fmt.Errorf("error getting absolute path for labels yaml file %s: %w", labelsYamlFname, err)
	}
	labelFs := os.DirFS(filepath.Dir(path))
	return getLabels(filepath.Base(path), labelFs.(fs.ReadFileFS))
}

func getLabels(fname string, labelsFs fs.ReadFileFS) ([]Label, error) {
	// Read and parse the labels yaml file.
	yamlBytes, err := labelsFs.ReadFile(fname)
	if err != nil {
		return nil, fmt.Errorf("error reading label yaml file %s", fname)
	}
	type yamlLabel struct {
		Label
		Rule string `yaml:"rule"`
	}
	yamlLabels := make(map[string]yamlLabel)
	if err := yaml.Unmarshal(yamlBytes, &yamlLabels); err != nil {
		return nil, fmt.Errorf("error unmarshalling labels yaml: %w", err)
	}
	labels := make([]Label, 0, len(yamlLabels))
	// Read each label's classification rule.
	var errs []error
	for name, lbl := range yamlLabels {
		// We assume that the rule will be defined with a relative path in the
		// labels yaml file. If the rule is absolute, we create a new fs for
		// the rule's directory. This implies that embedded labels will not work
		// if the rule is absolute, but that should not be a problem because
		// they should always be relative.
		ruleFname := filepath.Join(filepath.Dir(fname), lbl.Rule)
		ruleFs := labelsFs
		if filepath.IsAbs(ruleFname) {
			ruleFs = os.DirFS(filepath.Dir(ruleFname)).(fs.ReadFileFS)
			ruleFname = filepath.Base(ruleFname)
		}
		rule, err := readLabelRule(ruleFname, ruleFs)
		if err != nil {
			errs = append(errs, fmt.Errorf("error reading classification rule for label %s: %w", name, err))
			continue
		}
		lbl.Name = name
		lbl.ClassificationRule = rule
		labels = append(labels, lbl.Label)
	}
	if len(errs) > 0 {
		return labels, InvalidLabelsError{errs}
	}
	return labels, nil
}

func readLabelRule(fname string, labelFs fs.ReadFileFS) (*ast.Module, error) {
	b, err := labelFs.ReadFile(fname)
	if err != nil {
		return nil, fmt.Errorf("error reading rego file %s: %w", fname, err)
	}
	rule, err := parseRego(string(b))
	if err != nil {
		return nil, fmt.Errorf("error parsing classification rule for file %s: %w", fname, err)
	}
	return rule, nil
}

func parseRego(code string) (*ast.Module, error) {
	log.Tracef("classifier module code: '%s'", code)
	module, err := ast.ParseModule("classifier", code)
	if err != nil {
		return nil, fmt.Errorf("error parsing rego code: %w", err)
	}
	return module, nil
}
