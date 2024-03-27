package classification

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cyralinc/dmap/discovery/repository"
)

const (
	isDigitsRegex = "\\A\\d+\\z"
	digitsLabel   = "DIGITS"
	isARegex      = "\\A[Aa]\\z"
	isALabel      = "ISA"
	allCRegex     = "\\A[Cc]+\\z"
	allCLabel     = "ALLC"
)

var fooTable = ClassifiedTable{
	Repo:    "some_repo",
	Catalog: "testdb",
	Schema:  "testSchema",
	Table:   "Foo",
}

var noCatalogFooTable = ClassifiedTable{
	Repo:   "some_repo",
	Schema: "testSchema",
	Table:  "Foo",
}

func regexToClassifyCode(regex, label string) string {
	return fmt.Sprintf(
		"package classifier\noutput := \n"+
			"{k: v |\n  v := classify(k, input[k])\n}\n"+
			"classify(key, val) = \"%s\" {\n  re_match(`%s`, val)\n} "+
			"else = \"UNLABELED\" {\n  true\n}",
		label, regex,
	)
}

func multiRegexClassify(regexes, labels []string) string {
	return multiRegexClassifyWithPackage("classifier", regexes, labels)
}

func multiRegexClassifyWithPackage(pkg string, regexes, labels []string) string {
	prefix := "package " + pkg + "\noutput := \n" +
		"{k: v |\n v := classify(k, input[k])\n}\n" +
		"classify(key, val) = "
	middle := ""
	for i, r := range regexes {
		middle = fmt.Sprintf(
			"%s\"%s\" {\n re_match(`%s`, val)\n} else = ",
			middle, labels[i], r,
		)
	}
	return fmt.Sprintf("%s%s \"UNLABELED\" {\n true\n}", prefix, middle)
}

type regoClassifyTestCase struct {
	code            string
	table           *ClassifiedTable
	attributeNames  []string
	attributeValues []string
	expected        []Result
}

func TestRegoClassify(t *testing.T) {
	tests := []struct {
		name     string
		code     string
		table    *ClassifiedTable
		attrs    map[string]any
		expected []Result
	}{
		{
			name: "multi regex classify",
			code: multiRegexClassify(
				[]string{isDigitsRegex, isARegex, allCRegex},
				[]string{digitsLabel, isALabel, allCLabel},
			),
			table: &fooTable,
			attrs: map[string]any{
				"a": "fred",
				"b": "A",
				"c": "123",
				"d": "bob",
				"e": "0",
				"f": "cCc",
			},
			expected: []Result{
				{
					Table:           &fooTable,
					AttributeName:   "b",
					Classifications: []*Label{{Name: isALabel}},
				},
				{
					Table:           &fooTable,
					AttributeName:   "c",
					Classifications: []*Label{{Name: digitsLabel}},
				},
				{
					Table:           &fooTable,
					AttributeName:   "e",
					Classifications: []*Label{{Name: digitsLabel}},
				},
				{
					Table:           &fooTable,
					AttributeName:   "f",
					Classifications: []*Label{{Name: allCLabel}},
				},
			},
		},
		{
			name: "multi regex classify with custom package",
			code: multiRegexClassifyWithPackage(
				"foo_bar",
				[]string{isDigitsRegex, isARegex, allCRegex},
				[]string{digitsLabel, isALabel, allCLabel},
			),
			table: &fooTable,
			attrs: map[string]any{
				"a": "fred",
				"b": "A",
				"c": "123",
				"d": "bob",
				"e": "0",
				"f": "cCc",
			},
			expected: []Result{
				{
					Table:           &fooTable,
					AttributeName:   "b",
					Classifications: []*Label{{Name: isALabel}},
				},
				{
					Table:           &fooTable,
					AttributeName:   "c",
					Classifications: []*Label{{Name: digitsLabel}},
				},
				{
					Table:           &fooTable,
					AttributeName:   "e",
					Classifications: []*Label{{Name: digitsLabel}},
				},
				{
					Table:           &fooTable,
					AttributeName:   "f",
					Classifications: []*Label{{Name: allCLabel}},
				},
			},
		},
		{
			name:  "regex classifier",
			code:  regexToClassifyCode(isDigitsRegex, digitsLabel),
			table: &fooTable,
			attrs: map[string]any{
				"a": "123",
				"b": "0",
				"c": "fred",
				"d": "a",
				"e": "A",
			},
			expected: []Result{
				{
					Table:           &fooTable,
					AttributeName:   "a",
					Classifications: []*Label{{Name: digitsLabel}},
				},
				{
					Table:           &fooTable,
					AttributeName:   "b",
					Classifications: []*Label{{Name: digitsLabel}},
				},
			},
		},
		{
			name:  "regex classify - no catalog",
			code:  regexToClassifyCode(isDigitsRegex, digitsLabel),
			table: &noCatalogFooTable,
			attrs: map[string]any{
				"a": "123",
				"b": "0",
				"c": "fred",
				"d": "a",
				"e": "A",
			},
			expected: []Result{
				{
					Table:           &noCatalogFooTable,
					AttributeName:   "a",
					Classifications: []*Label{{Name: digitsLabel}},
				},
				{
					Table:           &noCatalogFooTable,
					AttributeName:   "b",
					Classifications: []*Label{{Name: digitsLabel}},
				},
			},
		},
		{
			name:  "regex classify - different attributes",
			code:  regexToClassifyCode(isARegex, isALabel),
			table: &fooTable,
			attrs: map[string]any{
				"a": "123",
				"b": "0",
				"c": "fred",
				"d": "a",
				"e": "A",
			},
			expected: []Result{
				{
					Table:           &fooTable,
					AttributeName:   "d",
					Classifications: []*Label{{Name: digitsLabel}},
				},
				{
					Table:           &fooTable,
					AttributeName:   "e",
					Classifications: []*Label{{Name: digitsLabel}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(
			tt.name, func(t *testing.T) {
				classifier, err := NewLabelClassifier(&Label{Name: "dummy", ClassificationRule: tt.code})
				if err != nil {
					t.Errorf("Error creating classifier: %s", err.Error())
				}
				results, err := classifier.Classify(
					context.Background(),
					tt.table,
					tt.attrs,
				)
				assert.NoError(t, err)
				if len(results) != len(tt.expected) {
					t.Errorf(
						"Expected %d classification results, but found %d",
						len(tt.expected), len(results),
					)
				}
				for i, r := range results {
					if tt.expected[i].Table.Repo != r.Table.Repo ||
						tt.expected[i].Table.Catalog != r.Table.Catalog ||
						tt.expected[i].Table.Schema != r.Table.Schema ||
						tt.expected[i].Table.Table != r.Table.Table {
						t.Errorf(
							"Expected classification results[%d] tables to match. "+
								"Found: '%+v', Expected: '%+v'",
							i, *r.Table, *tt.expected[i].Table,
						)
					}
					if tt.expected[i].AttributeName != r.AttributeName {
						t.Errorf(
							"Expected attribute names on classifiers[%d] to match. "+
								"Found: %s, Expected: %s",
							i, r.AttributeName, tt.expected[i].AttributeName,
						)
					}
					if len(tt.expected[i].Classifications) != len(r.Classifications) {
						t.Errorf(
							"Expected length of classifications[%d] to match. "+
								"Found: %d, Expected: %d",
							i, len(tt.expected[i].Classifications), len(r.Classifications),
						)
					}
					for j, cl := range r.Classifications {
						if cl.Name != tt.expected[i].Classifications[j].Name {
							t.Errorf(
								"Expected classification[%d][%d] == '%s'; "+
									"found: '%s'.",
								i, j, tt.expected[i].Classifications[j].Name, cl.Name,
							)
						}
					}
				}
			},
		)
	}
}

const (
	repoName    = "repoName"
	catalogName = "catalogName"
	schemaName  = "schema"
	tableName   = "table"
)

func Test_Classify_Rego_SingleSample(t *testing.T) {
	sample := repository.Sample{
		Metadata: repository.SampleMetadata{
			Repo:     repoName,
			Database: catalogName,
			Schema:   schemaName,
			Table:    tableName,
		},
		Results: []repository.SampleResult{
			{
				"age":             "52",
				"social_sec_num":  "512-23-4256",
				"credit_card_num": "4111111111111111",
			},
			{
				"age":             "4111111111111111",
				"social_sec_num":  "512-23-4258",
				"credit_card_num": "4111111111111111",
			},
		},
	}

	classifier := testRegoClassifier(t)

	actual, err := ClassifySamples(context.Background(), classifier, []repository.Sample{sample})
	require.NoError(t, err)

	table := &ClassifiedTable{
		Repo:    repoName,
		Catalog: catalogName,
		Schema:  schemaName,
		Table:   tableName,
	}

	expected := []Result{
		{
			Table:           table,
			AttributeName:   "age",
			Classifications: []*Label{{Name: "AGE"}, {Name: "CCN"}},
		},
		{
			Table:           table,
			AttributeName:   "credit_card_num",
			Classifications: []*Label{{Name: "CCN"}},
		},
	}

	assert.ElementsMatch(t, expected, actual)
}

func Test_Classify_Rego_MultipleSamples(t *testing.T) {
	metadata1 := repository.SampleMetadata{
		Repo:     repoName,
		Database: catalogName,
		Schema:   schemaName,
		Table:    tableName,
	}

	metadata2 := repository.SampleMetadata{
		Repo:     repoName,
		Database: catalogName,
		Schema:   schemaName + "2",
		Table:    tableName + "2",
	}

	samples := []repository.Sample{
		{
			Metadata: metadata1,
			Results: []repository.SampleResult{
				{
					"age":             "52",
					"social_sec_num":  "512-23-4256",
					"credit_card_num": "4111111111111111",
				},
			},
		},
		{
			Metadata: metadata1,
			Results: []repository.SampleResult{
				{
					"age":             "52",
					"social_sec_num":  "512-23-4256",
					"credit_card_num": "4111111111111112",
				},
			},
		},
		{
			Metadata: metadata2,
			Results: []repository.SampleResult{
				{
					"age":             "52",
					"name":            "Joe Smith",
					"social_sec_num":  "512-23-4256",
					"credit_card_num": "4111111111111112",
				},
				{
					"age":             "4111111111111113",
					"name":            "Joe Smith",
					"social_sec_num":  "512-23-4256",
					"credit_card_num": "4111111111111112",
				},
			},
		},
	}

	classifier := testRegoClassifier(t)

	actual, err := ClassifySamples(context.Background(), classifier, samples)
	require.NoError(t, err)

	table1 := &ClassifiedTable{
		Repo:    metadata1.Repo,
		Catalog: metadata1.Database,
		Schema:  metadata1.Schema,
		Table:   metadata1.Table,
	}

	table2 := &ClassifiedTable{
		Repo:    metadata2.Repo,
		Catalog: metadata2.Database,
		Schema:  metadata2.Schema,
		Table:   metadata2.Table,
	}

	expected := []Result{
		{
			Table:           table1,
			AttributeName:   "age",
			Classifications: []*Label{{Name: "AGE"}},
		},
		{
			Table:           table1,
			AttributeName:   "credit_card_num",
			Classifications: []*Label{{Name: "CCN"}},
		},
		{
			Table:           table2,
			AttributeName:   "age",
			Classifications: []*Label{{Name: "AGE"}, {Name: "CCN"}},
		},
		{
			Table:           table2,
			AttributeName:   "credit_card_num",
			Classifications: []*Label{{Name: "CCN"}},
		},
	}

	assert.ElementsMatch(t, expected, actual)
}

func testRegoClassifier(t *testing.T) Classifier {
	fin, err := os.ReadFile("./examples/example_policy_simple.rego")
	require.NoError(t, err)

	classifierCode := string(fin)

	classifier, err := NewLabelClassifier(&Label{Name: "dummy", ClassificationRule: classifierCode})
	require.NoError(t, err)

	return classifier
}
