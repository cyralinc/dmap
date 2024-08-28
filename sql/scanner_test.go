package sql

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/cyralinc/dmap/classification"
)

// TODO: tests for Scanner.Scan -ccampo 2024-04-05

func TestScanner_sampleDb_Success(t *testing.T) {
	ctx := context.Background()
	repo := NewMockRepository(t)
	meta := Metadata{
		Database: "database",
		Schemas: map[string]*SchemaMetadata{
			"schema1": {
				Name: "",
				Tables: map[string]*TableMetadata{
					"table1": {
						Schema: "schema1",
						Name:   "table1",
						Attributes: []*AttributeMetadata{
							{
								Schema:   "schema1",
								Table:    "table1",
								Name:     "name1",
								DataType: "varchar",
							},
							{
								Schema:   "schema1",
								Table:    "table1",
								Name:     "name2",
								DataType: "decimal",
							},
						},
					},
				},
			},
			"schema2": {
				Name: "",
				Tables: map[string]*TableMetadata{
					"table2": {
						Schema: "schema2",
						Name:   "table2",
						Attributes: []*AttributeMetadata{
							{
								Schema:   "schema2",
								Table:    "table2",
								Name:     "name3",
								DataType: "int",
							},
							{
								Schema:   "schema2",
								Table:    "table2",
								Name:     "name4",
								DataType: "timestamp",
							},
						},
					},
				},
			},
		},
	}
	table1Sample := Sample{
		TablePath: []string{"database", "schema1", "table1"},
		Results: []SampleResult{
			{
				"name1": "foo",
				"name2": "bar",
			},
			{
				"name1": "baz",
				"name2": "qux",
			},
		},
	}
	table2Sample := Sample{
		TablePath: []string{"database", "schema2", "table2"},
		Results: []SampleResult{
			{
				"name3": "foo1",
				"name4": "bar1",
			},
			{
				"name3": "baz1",
				"name4": "qux1",
			},
		},
	}

	repo.EXPECT().Introspect(ctx, mock.Anything).Return(&meta, nil)
	sampleParams1 := SampleParameters{
		Metadata: meta.Schemas["schema1"].Tables["table1"],
	}
	sampleParams2 := SampleParameters{
		Metadata: meta.Schemas["schema2"].Tables["table2"],
	}
	repo.EXPECT().SampleTable(ctx, sampleParams1).Return(table1Sample, nil)
	repo.EXPECT().SampleTable(ctx, sampleParams2).Return(table2Sample, nil)
	repo.EXPECT().Close().Return(nil)
	repoType := "mock"
	reg := NewRegistry()
	reg.MustRegister(
		repoType,
		func(ctx context.Context, cfg RepoConfig) (Repository, error) {
			return repo, nil
		},
	)
	s := Scanner{
		config: ScannerConfig{
			RepoType:   repoType,
			RepoConfig: RepoConfig{},
			Registry:   reg,
		},
	}
	samples, err := s.sampleDb(ctx, meta.Database)
	require.NoError(t, err)
	// Order is not important and is actually non-deterministic due to concurrency
	expected := []Sample{table1Sample, table2Sample}
	require.ElementsMatch(t, expected, samples)
}

func TestScanner_sampleDb_PartialError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	repo := NewMockRepository(t)
	meta := Metadata{
		Database: "database",
		Schemas: map[string]*SchemaMetadata{
			"schema1": {
				Name: "",
				Tables: map[string]*TableMetadata{
					"table1": {
						Schema: "schema1",
						Name:   "table1",
						Attributes: []*AttributeMetadata{
							{
								Schema:   "schema1",
								Table:    "table1",
								Name:     "name1",
								DataType: "varchar",
							},
							{
								Schema:   "schema1",
								Table:    "table1",
								Name:     "name2",
								DataType: "decimal",
							},
						},
					},
					"forbidden": {
						Schema: "schema1",
						Name:   "forbidden",
						Attributes: []*AttributeMetadata{
							{
								Schema:   "schema1",
								Table:    "forbidden",
								Name:     "name1",
								DataType: "varchar",
							},
							{
								Schema:   "schema1",
								Table:    "forbidden",
								Name:     "name2",
								DataType: "decimal",
							},
						},
					},
				},
			},
			"schema2": {
				Name: "",
				Tables: map[string]*TableMetadata{
					"table2": {
						Schema: "schema2",
						Name:   "table2",
						Attributes: []*AttributeMetadata{
							{
								Schema:   "schema2",
								Table:    "table2",
								Name:     "name3",
								DataType: "int",
							},
							{
								Schema:   "schema2",
								Table:    "table2",
								Name:     "name4",
								DataType: "timestamp",
							},
						},
					},
				},
			},
		},
	}
	table1Sample := Sample{
		TablePath: []string{"database", "schema1", "table1"},
		Results: []SampleResult{
			{
				"name1": "foo",
				"name2": "bar",
			},
			{
				"name1": "baz",
				"name2": "qux",
			},
		},
	}
	table2Sample := Sample{
		TablePath: []string{"database", "schema2", "table2"},
		Results: []SampleResult{
			{
				"name3": "foo1",
				"name4": "bar1",
			},
			{
				"name3": "baz1",
				"name4": "qux1",
			},
		},
	}

	repo.EXPECT().Introspect(ctx, mock.Anything).Return(&meta, nil)
	sampleParams1 := SampleParameters{
		Metadata: meta.Schemas["schema1"].Tables["table1"],
	}
	sampleParams2 := SampleParameters{
		Metadata: meta.Schemas["schema1"].Tables["forbidden"],
	}
	sampleParamsForbidden := SampleParameters{
		Metadata: meta.Schemas["schema2"].Tables["table2"],
	}
	repo.EXPECT().SampleTable(ctx, sampleParams1).Return(table1Sample, nil)
	errForbidden := errors.New("forbidden table")
	repo.EXPECT().SampleTable(ctx, sampleParamsForbidden).Return(Sample{}, errForbidden)
	repo.EXPECT().SampleTable(ctx, sampleParams2).Return(table2Sample, nil)
	repo.EXPECT().Close().Return(nil)
	repoType := "mock"
	reg := NewRegistry()
	reg.MustRegister(
		repoType,
		func(ctx context.Context, cfg RepoConfig) (Repository, error) {
			return repo, nil
		},
	)
	s := Scanner{
		config: ScannerConfig{
			RepoType:   repoType,
			RepoConfig: RepoConfig{},
			Registry:   reg,
		},
	}
	samples, err := s.sampleDb(ctx, meta.Database)
	require.ErrorIs(t, err, errForbidden)
	// Order is not important and is actually non-deterministic due to concurrency
	expected := []Sample{table1Sample, table2Sample}
	require.ElementsMatch(t, expected, samples)
}

func TestScanner_sampleAllDbs_Error(t *testing.T) {
	ctx := context.Background()
	listDbErr := errors.New("error listing databases")
	repo := NewMockRepository(t)
	repo.EXPECT().ListDatabases(ctx).Return(nil, listDbErr)
	repo.EXPECT().Close().Return(nil)
	repoType := "mock"
	reg := NewRegistry()
	reg.MustRegister(
		repoType,
		func(ctx context.Context, cfg RepoConfig) (Repository, error) {
			return repo, nil
		},
	)
	s := Scanner{
		config: ScannerConfig{
			RepoType:   repoType,
			RepoConfig: RepoConfig{},
			Registry:   reg,
		},
	}
	samples, err := s.sampleAllDbs(ctx)
	require.Nil(t, samples)
	require.ErrorIs(t, err, listDbErr)
}

func TestScanner_sampleAllDbs_Successful_TwoDatabases(t *testing.T) {
	ctx := context.Background()
	dbs := []string{"db1", "db2"}
	// Dummy metadata returned for each Introspect call
	meta := Metadata{
		Database: "db",
		Schemas: map[string]*SchemaMetadata{
			"schema": {
				Name: "schema",
				Tables: map[string]*TableMetadata{
					"table": {
						Schema: "schema",
						Name:   "table",
						Attributes: []*AttributeMetadata{
							{
								Schema:   "schema",
								Table:    "table",
								Name:     "attr",
								DataType: "string",
							},
						},
					},
				},
			},
		},
	}
	sample := Sample{
		TablePath: []string{"db", "schema", "table"},
		Results: []SampleResult{
			{
				"attr": "foo",
			},
		},
	}
	repo := NewMockRepository(t)
	repo.EXPECT().ListDatabases(mock.Anything).Return(dbs, nil)
	repo.EXPECT().Introspect(mock.Anything, mock.Anything).Return(&meta, nil)
	repo.EXPECT().SampleTable(mock.Anything, mock.Anything).Return(sample, nil)
	repo.EXPECT().Close().Return(nil)
	repoType := "mock"
	reg := NewRegistry()
	reg.MustRegister(
		repoType,
		func(ctx context.Context, cfg RepoConfig) (Repository, error) {
			return repo, nil
		},
	)
	s := Scanner{
		config: ScannerConfig{
			RepoType:   repoType,
			RepoConfig: RepoConfig{},
			Registry:   reg,
		},
	}
	samples, err := s.sampleAllDbs(ctx)
	require.NoError(t, err)
	// Two databases should be sampled, and our mock will return the sample for
	// each sample call. This really just asserts that we've sampled the correct
	// number of times.
	require.ElementsMatch(t, samples, []Sample{sample, sample})
}

func TestScanner_sampleAllDbs_IntrospectError(t *testing.T) {
	ctx := context.Background()
	dbs := []string{"db1", "db2"}
	introspectErr := errors.New("introspect error")
	repo := NewMockRepository(t)
	repo.EXPECT().ListDatabases(mock.Anything).Return(dbs, nil)
	repo.EXPECT().Introspect(mock.Anything, mock.Anything).Return(nil, introspectErr)
	repo.EXPECT().Close().Return(nil)
	repoType := "mock"
	reg := NewRegistry()
	reg.MustRegister(
		repoType,
		func(ctx context.Context, cfg RepoConfig) (Repository, error) {
			return repo, nil
		},
	)
	s := Scanner{
		config: ScannerConfig{
			RepoType:   repoType,
			RepoConfig: RepoConfig{},
			Registry:   reg,
		},
	}
	samples, err := s.sampleAllDbs(ctx)
	require.Empty(t, samples)
	require.NoError(t, err)
}

func TestScanner_sampleAllDbs_SampleError(t *testing.T) {
	ctx := context.Background()
	dbs := []string{"db1", "db2"}
	// Dummy metadata returned for each Introspect call
	meta := Metadata{
		Database: "db",
		Schemas: map[string]*SchemaMetadata{
			"schema": {
				Name: "schema",
				Tables: map[string]*TableMetadata{
					"table": {
						Schema: "schema",
						Name:   "table",
						Attributes: []*AttributeMetadata{
							{
								Schema:   "schema",
								Table:    "table",
								Name:     "attr",
								DataType: "string",
							},
						},
					},
				},
			},
		},
	}
	sampleErr := errors.New("sample error")
	repo := NewMockRepository(t)
	repo.EXPECT().ListDatabases(mock.Anything).Return(dbs, nil)
	repo.EXPECT().Introspect(mock.Anything, mock.Anything).Return(&meta, nil)
	repo.EXPECT().SampleTable(mock.Anything, mock.Anything).Return(Sample{}, sampleErr)
	repo.EXPECT().Close().Return(nil)
	repoType := "mock"
	reg := NewRegistry()
	reg.MustRegister(
		repoType,
		func(ctx context.Context, cfg RepoConfig) (Repository, error) {
			return repo, nil
		},
	)
	s := Scanner{
		config: ScannerConfig{
			RepoType:   repoType,
			RepoConfig: RepoConfig{},
			Registry:   reg,
		},
	}
	samples, err := s.sampleAllDbs(ctx)
	require.NoError(t, err)
	require.Empty(t, samples)
}

func TestScanner_sampleAllDbs_TwoDatabases_OneSampleError(t *testing.T) {
	ctx := context.Background()
	dbs := []string{"db1", "db2"}
	// Dummy metadata returned for each Introspect call
	meta := Metadata{
		Database: "db",
		Schemas: map[string]*SchemaMetadata{
			"schema": {
				Name: "schema",
				Tables: map[string]*TableMetadata{
					"table": {
						Schema: "schema",
						Name:   "table",
						Attributes: []*AttributeMetadata{
							{
								Schema:   "schema",
								Table:    "table",
								Name:     "attr",
								DataType: "string",
							},
						},
					},
				},
			},
		},
	}
	sample := Sample{
		TablePath: []string{"db", "schema", "table"},
		Results: []SampleResult{
			{
				"attr": "foo",
			},
		},
	}
	sampleErr := errors.New("sample error")
	repo := NewMockRepository(t)
	repo.EXPECT().ListDatabases(mock.Anything).Return(dbs, nil)
	repo.EXPECT().Introspect(mock.Anything, mock.Anything).Return(&meta, nil)
	repo.EXPECT().SampleTable(mock.Anything, mock.Anything).Return(sample, nil).Once()
	repo.EXPECT().SampleTable(mock.Anything, mock.Anything).Return(Sample{}, sampleErr).Once()
	repo.EXPECT().Close().Return(nil)
	repoType := "mock"
	reg := NewRegistry()
	reg.MustRegister(
		repoType,
		func(ctx context.Context, cfg RepoConfig) (Repository, error) {
			return repo, nil
		},
	)
	s := Scanner{
		config: ScannerConfig{
			RepoType:   repoType,
			RepoConfig: RepoConfig{},
			Registry:   reg,
		},
	}
	samples, err := s.sampleAllDbs(ctx)
	require.NoError(t, err)
	// Because of a single sample error, we expect only one database was
	// sampled.
	require.ElementsMatch(t, samples, []Sample{sample})
}

func TestScanner_classifySamples_SingleSample(t *testing.T) {
	ctx := context.Background()
	sample := Sample{
		TablePath: []string{"db", "schema", "table"},
		Results: []SampleResult{
			{
				"age":             "52",
				"social_sec_num":  "512-23-4258",
				"credit_card_num": "4111111111111111",
			},
			{
				"age":             "101",
				"social_sec_num":  "foobarbaz",
				"credit_card_num": "4111111111111111",
			},
		},
	}
	classifier := NewMockClassifier(t)
	// Need to explicitly convert it to a map because Mockery isn't smart enough
	// to infer the type.
	classifier.EXPECT().Classify(ctx, map[string]any(sample.Results[0])).Return(
		classification.Result{
			"age":             lblSet("AGE"),
			"social_sec_num":  lblSet("SSN"),
			"credit_card_num": lblSet("CCN"),
		},
		nil,
	)
	classifier.EXPECT().Classify(ctx, map[string]any(sample.Results[1])).Return(
		classification.Result{
			"age":             lblSet("AGE", "CVV"),
			"credit_card_num": lblSet("CCN"),
		},
		nil,
	)

	expected := []classification.Classification{
		{
			AttributePath: append(sample.TablePath, "age"),
			Labels:        lblSet("AGE", "CVV"),
		},
		{
			AttributePath: append(sample.TablePath, "social_sec_num"),
			Labels:        lblSet("SSN"),
		},
		{
			AttributePath: append(sample.TablePath, "credit_card_num"),
			Labels:        lblSet("CCN"),
		},
	}
	s := Scanner{classifier: classifier}
	actual, err := s.classifySamples(ctx, []Sample{sample})
	require.NoError(t, err)
	require.ElementsMatch(t, expected, actual)
}

func TestScanner_classifySamples_MultipleSamples(t *testing.T) {
	ctx := context.Background()
	samples := []Sample{
		{
			TablePath: []string{"db1", "schema1", "table1"},
			Results: []SampleResult{
				{
					"age":             "52",
					"social_sec_num":  "512-23-4258",
					"credit_card_num": "4111111111111111",
				},
				{
					"age":             "101",
					"social_sec_num":  "foobarbaz",
					"credit_card_num": "4111111111111111",
				},
			},
		},
		{
			TablePath: []string{"db2", "schema2", "table2"},
			Results: []SampleResult{
				{
					"fullname": "John Doe",
					"dob":      "2000-01-01",
					"random":   "foobarbaz",
				},
			},
		},
	}

	classifier := NewMockClassifier(t)
	// Need to explicitly convert it to a map because Mockery isn't smart enough
	// to infer the type.
	classifier.EXPECT().Classify(ctx, map[string]any(samples[0].Results[0])).Return(
		classification.Result{
			"age":             lblSet("AGE"),
			"social_sec_num":  lblSet("SSN"),
			"credit_card_num": lblSet("CCN"),
		},
		nil,
	)
	classifier.EXPECT().Classify(ctx, map[string]any(samples[0].Results[1])).Return(
		classification.Result{
			"age":             lblSet("AGE", "CVV"),
			"credit_card_num": lblSet("CCN"),
		},
		nil,
	)
	classifier.EXPECT().Classify(ctx, map[string]any(samples[1].Results[0])).Return(
		classification.Result{
			"fullname": lblSet("FULL_NAME"),
			"dob":      lblSet("DOB"),
		},
		nil,
	)

	expected := []classification.Classification{
		{
			AttributePath: append(samples[0].TablePath, "age"),
			Labels:        lblSet("AGE", "CVV"),
		},
		{
			AttributePath: append(samples[0].TablePath, "social_sec_num"),
			Labels:        lblSet("SSN"),
		},
		{
			AttributePath: append(samples[0].TablePath, "credit_card_num"),
			Labels:        lblSet("CCN"),
		},
		{
			AttributePath: append(samples[1].TablePath, "fullname"),
			Labels:        lblSet("FULL_NAME"),
		},
		{
			AttributePath: append(samples[1].TablePath, "dob"),
			Labels:        lblSet("DOB"),
		},
	}
	s := Scanner{classifier: classifier}
	actual, err := s.classifySamples(ctx, samples)
	require.NoError(t, err)
	require.ElementsMatch(t, expected, actual)
}

func lblSet(labels ...string) classification.LabelSet {
	set := make(classification.LabelSet)
	for _, label := range labels {
		set[label] = struct {
		}{}
	}
	return set
}
