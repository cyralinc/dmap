package sql

import (
	"context"
	"fmt"
	"maps"
	"strings"

	"github.com/cyralinc/dmap/classification"
	"github.com/cyralinc/dmap/scan"
)

// classifySamples uses the provided classifiers to classify the sample data
// passed via the "samples" parameter. It is mostly a helper function which
// loops through each repository.Sample, retrieves the attribute names and
// values of that sample, passes them to Classifier.Classify, and then
// aggregates the results. Please see the documentation for Classifier and its
// Classify method for more details. The returned slice represents all the
// unique classification results for a given sample set.
func classifySamples(
	ctx context.Context,
	samples []Sample,
	classifier classification.Classifier,
) ([]scan.Classification, error) {
	uniqueResults := make(map[string]scan.Classification)
	for _, sample := range samples {
		// Classify each sampled row and combine the results.
		for _, sampleResult := range sample.Results {
			res, err := classifier.Classify(ctx, sampleResult)
			if err != nil {
				return nil, fmt.Errorf("error classifying sample: %w", err)
			}
			for attr, labels := range res {
				attrPath := append(sample.TablePath, attr)
				key := pathKey(attrPath)
				result, ok := uniqueResults[key]
				if !ok {
					uniqueResults[key] = scan.Classification{
						AttributePath: attrPath,
						Labels:        labels,
					}
				} else {
					// Merge the labels from the new result into the existing result.
					maps.Copy(result.Labels, labels)
				}
			}
		}
	}
	// Convert the map of unique results to a slice.
	results := make([]scan.Classification, 0, len(uniqueResults))
	for _, result := range uniqueResults {
		results = append(results, result)
	}
	return results, nil
}

func pathKey(path []string) string {
	// U+2063 is an invisible separator. It is used here to ensure that the
	// pathKey is unique and does not conflict with any of the path elements.
	return strings.Join(path, "\u2063")
}
