package api

import (
	"context"
	"fmt"

	"github.com/go-resty/resty/v2"
	log "github.com/sirupsen/logrus"

	"github.com/cyralinc/dmap/scan"
)

const (
	dataLabelsPath      = "/v1/datalabels"
	classificationsPath = "/v1/classifications"
)

type RepoScan struct {
	RepoExternalId string `json:"repoExternalId"`
	Agent          string `json:"agent"`
}

// Classifications is the Dmap API representation of a set of data
// classifications.
type Classifications struct {
	RepoScanId      string           `json:"repoScanId"`
	Classifications []Classification `json:"classifications"`
}

// Labels is the Dmap API representation of a set of data labels.
type Labels struct {
	RepoScanId string  `json:"repoScanId"`
	Labels     []Label `json:"labels"`
}

// Classification is the Dmap API representation of a single data
// classification.
type Classification struct {
	Label         string   `json:"label"`
	AttributePath []string `json:"attributePath"`
}

// Label is the Dmap API representation of a single data label.
type Label struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Tags        []string `json:"tags"`
}

// RequestError is an error that occurs when an HTTP request to the Dmap API
// fails.
type RequestError struct {
	StatusCode int
	Status     string
	Body       string
}

// Error returns a string representation of the RequestError, and implements
// the error interface.
func (e RequestError) Error() string {
	return fmt.Sprintf("HTTP request error: status = %s, status code = %d, body = %s", e.Status, e.StatusCode, e.Body)
}

// DmapClient is a client for the Dmap HTTP API.
type DmapClient struct {
	client *resty.Client
}

// NewDmapClient creates a new DmapClient.
func NewDmapClient(baseURL, clientID, clientSecret string) *DmapClient {
	client := resty.New().
		SetBaseURL(baseURL).
		SetBasicAuth(clientID, clientSecret).
		SetLogger(log.StandardLogger())
	if log.GetLevel() == log.DebugLevel || log.GetLevel() == log.TraceLevel {
		client.SetDebug(true)
	}
	return &DmapClient{client: client}
}

// CreateRepoScan creates a new repository scan in the Dmap API with the given
// repository scan details. If an error occurs, it is returned. The ID of the
// created repository scan is returned if the request is successful.
func (c *DmapClient) CreateRepoScan(ctx context.Context, repoScan RepoScan) (string, error) {
	id := struct {
		ID string `json:"id"`
	}{}
	resp, err := c.client.R().
		SetContext(ctx).
		SetBody(repoScan).
		SetResult(&id).
		Post("/v1/reposcans")
	if err != nil || resp.IsError() {
		if err == nil {
			err = RequestError{
				StatusCode: resp.StatusCode(),
				Status:     resp.Status(),
				Body:       resp.String(),
			}
		}
		return "", fmt.Errorf("HTTP request error creating repo scan: %w", err)
	}
	return id.ID, nil
}

// UpsertLabels upserts the given labels to the Dmap API. All existing labels
// are replaced with the given labels. If an error occurs, it is returned.
func (c *DmapClient) UpsertLabels(ctx context.Context, labels Labels) error {
	resp, err := c.client.R().
		SetContext(ctx).
		SetBody(labels).
		Put(dataLabelsPath)
	if err != nil || resp.IsError() {
		if err == nil {
			err = RequestError{
				StatusCode: resp.StatusCode(),
				Status:     resp.Status(),
				Body:       resp.String(),
			}
		}
		return fmt.Errorf("HTTP request error upserting labels: %w", err)
	}
	return nil
}

// UpsertClassifications upserts the given classifications to the Dmap API for
// the given repository external ID. All existing classifications for the
// repository are replaced with the given classifications. If an error occurs,
// it is returned.
func (c *DmapClient) UpsertClassifications(
	ctx context.Context,
	repoExternalID string,
	classifications Classifications,
) error {
	resp, err := c.client.R().
		SetContext(ctx).
		SetBody(classifications).
		Put(classificationsPath)
	if err != nil || resp.IsError() {
		if err == nil {
			err = RequestError{
				StatusCode: resp.StatusCode(),
				Status:     resp.Status(),
				Body:       resp.String(),
			}
		}
		return fmt.Errorf(
			"HTTP request error upserting classifications for repo external ID %s: %w",
			repoExternalID,
			err,
		)
	}
	return nil
}

// PublishRepoScanResults publishes the given repository scan results (labels
// and classifications) to the Dmap API. If an error occurs, it is returned.
// Note that this operation is not atomic and may result in partial updates to
// the Dmap API if an error occurs.
func (c *DmapClient) PublishRepoScanResults(
	ctx context.Context,
	agent, repoExternalID string,
	results *scan.RepoScanResults,
) error {
	repoScanId, err := c.CreateRepoScan(ctx, RepoScan{Agent: agent, RepoExternalId: repoExternalID})
	if err != nil {
		return fmt.Errorf("error creating repo scan: %w", err)
	}
	labels := make([]Label, len(results.Labels))
	for i, label := range results.Labels {
		labels[i] = Label{
			Name:        label.Name,
			Description: label.Description,
			Tags:        label.Tags,
		}
	}
	if err := c.UpsertLabels(ctx, Labels{RepoScanId: repoScanId, Labels: labels}); err != nil {
		return fmt.Errorf("error upserting labels: %w", err)
	}
	classifications := make([]Classification, 0, len(results.Classifications))
	for _, classification := range results.Classifications {
		for label := range classification.Labels {
			classifications = append(
				classifications,
				Classification{
					Label:         label,
					AttributePath: classification.AttributePath,
				},
			)
		}
	}
	if err := c.UpsertClassifications(
		ctx,
		repoExternalID,
		Classifications{RepoScanId: repoScanId, Classifications: classifications},
	); err != nil {
		return fmt.Errorf("error upserting classifications for repo %s: %w", repoExternalID, err)
	}
	return nil
}
