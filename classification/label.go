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
	return getLabels("labels/labels.yaml", true)
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
	return getLabels(path, false)
}

// getLabels reads the labels YAML file from the given path and returns the
// labels and their classification rules. If predefined is true, the labels are
// read from the embedded FS, otherwise they are read from the file system. If
// there is an error reading or unmarshalling the labels file, it is returned.
// If there are errors reading or parsing a classification rules for labels, the
// errors are aggregated into an InvalidLabelsError and returned, along with the
// labels that were successfully read.
func getLabels(path string, predefined bool) ([]Label, error) {
	var (
		labelsFs    fs.ReadFileFS
		labelsFname string
	)
	if predefined {
		labelsFs = predefinedLabelsFs
		labelsFname = path
	} else {
		labelsFs = os.DirFS(filepath.Dir(path)).(fs.ReadFileFS)
		labelsFname = filepath.Base(path)
	}
	// Read and parse the labels yaml file.
	yamlBytes, err := labelsFs.ReadFile(labelsFname)
	if err != nil {
		return nil, fmt.Errorf("error reading label yaml file %s", path)
	}
	type yamlLabel struct {
		Label `yaml:",inline"`
		Rule  string `yaml:"rule"`
	}
	yamlLabels := make(map[string]yamlLabel)
	if err := yaml.Unmarshal(yamlBytes, &yamlLabels); err != nil {
		return nil, fmt.Errorf("error unmarshalling labels yaml: %w", err)
	}
	labels := make([]Label, 0, len(yamlLabels))
	// Read each label's classification rule.
	var errs []error
	for name, lbl := range yamlLabels {
		// The rule file for this label is either an absolute path, a relative
		// path, or a predefined rule. We need to determine the fs to use to
		// read the rule file.
		var (
			ruleFs    fs.ReadFileFS
			ruleFname string
		)
		if predefined {
			// We're dealing with the predefined labels, therefore the rule FS
			// is same embedded FS as the labels YAML file. However, we need to
			// use the dir of the labels yaml file as the rule file root because
			// this is the root of the embedded fs - it's a bit of a quirk with
			// the embedded FS API.
			ruleFname = filepath.Join(filepath.Dir(path), lbl.Rule)
			ruleFs = labelsFs
		} else {
			ruleFname = filepath.Base(lbl.Rule)
			if filepath.IsAbs(lbl.Rule) {
				// The rule has an absolute path, so we need to create a new fs
				// for the rule's directory.
				ruleFs = os.DirFS(filepath.Dir(lbl.Rule)).(fs.ReadFileFS)
			} else {
				// The rule has a relative path, which is relative to the labels
				// YAML file (as opposed to the current directory).
				ruleFs = os.DirFS(
					filepath.Join(
						filepath.Dir(path),
						filepath.Dir(lbl.Rule),
					),
				).(fs.ReadFileFS)
			}
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
