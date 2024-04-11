package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cyralinc/dmap/classification"
	"github.com/cyralinc/dmap/scan"
)

func TestDmapClient_UpsertLabels_Success(t *testing.T) {
	clientID := "clientID"
	clientSecret := "clientSecret"
	lbls := Labels{
		Labels: []Label{
			{
				Name:        "lbl1",
				Description: "desc1",
				Tags:        []string{"tag1"},
			},
			{
				Name:        "lbl2",
				Description: "desc2",
				Tags:        []string{"tag2", "tag3"},
			},
		},
	}
	svr := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				requireBasicAuth(t, r, clientID, clientSecret)
				require.Equal(t, http.MethodPut, r.Method)
				require.Equal(t, dataLabelsPath, r.URL.Path)
				var l Labels
				err := json.NewDecoder(r.Body).Decode(&l)
				require.NoError(t, err)
				require.Equal(t, lbls, l)
			},
		),
	)
	defer svr.Close()
	c := NewDmapClient(svr.URL, clientID, clientSecret)
	err := c.UpsertLabels(context.Background(), lbls)
	require.NoError(t, err)
}

func TestDmapClient_UpsertLabels_ServerError(t *testing.T) {
	clientID := "clientID"
	clientSecret := "clientSecret"
	svr := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				requireBasicAuth(t, r, clientID, clientSecret)
				require.Equal(t, http.MethodPut, r.Method)
				require.Equal(t, dataLabelsPath, r.URL.Path)
				w.WriteHeader(http.StatusInternalServerError)
			},
		),
	)
	defer svr.Close()
	c := NewDmapClient(svr.URL, clientID, clientSecret)
	err := c.UpsertLabels(context.Background(), Labels{})
	var reqErr RequestError
	require.ErrorAs(t, err, &reqErr)
	expectedErr := RequestError{
		StatusCode: http.StatusInternalServerError,
		Status: fmt.Sprintf(
			"%d %s",
			http.StatusInternalServerError,
			http.StatusText(http.StatusInternalServerError),
		),
	}
	require.Equal(t, expectedErr, reqErr)
}

func TestDmapClient_UpsertLabels_Error(t *testing.T) {
	c := NewDmapClient("invalid", "clientID", "clientSecret")
	err := c.UpsertLabels(context.Background(), Labels{})
	require.Error(t, err)
}

func TestDmapClient_UpsertClassifications_Success(t *testing.T) {
	clientID := "clientID"
	clientSecret := "clientSecret"
	repoExternalID := "arn:aws:dynamodb:us-east-1:123456789012:table/test"
	classifications := Classifications{
		Classifications: []Classification{
			{
				Label:         "lbl1",
				AttributePath: []string{"schema1", "table1", "column1"},
			},
			{
				Label:         "lbl2",
				AttributePath: []string{"schema2", "table2", "column2"},
			},
		},
	}
	svr := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				requireBasicAuth(t, r, clientID, clientSecret)
				require.Equal(t, http.MethodPut, r.Method)
				path := strings.Replace(classificationsPath, "{repoExternalID}", url.QueryEscape(repoExternalID), 1)
				require.Equal(t, path, r.URL.Path)
				var c Classifications
				err := json.NewDecoder(r.Body).Decode(&c)
				require.NoError(t, err)
				require.Equal(t, classifications, c)
			},
		),
	)
	defer svr.Close()
	c := NewDmapClient(svr.URL, clientID, clientSecret)
	err := c.UpsertClassifications(context.Background(), repoExternalID, classifications)
	require.NoError(t, err)
}

func TestDmapClient_UpsertClassifications_ServerError(t *testing.T) {
	clientID := "clientID"
	clientSecret := "clientSecret"
	repoExternalID := "arn:aws:dynamodb:us-east-1:123456789012:table/test"
	svr := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				requireBasicAuth(t, r, clientID, clientSecret)
				require.Equal(t, http.MethodPut, r.Method)
				path := strings.Replace(classificationsPath, "{repoExternalID}", url.QueryEscape(repoExternalID), 1)
				require.Equal(t, path, r.URL.Path)
				w.WriteHeader(http.StatusInternalServerError)
			},
		),
	)
	defer svr.Close()
	c := NewDmapClient(svr.URL, clientID, clientSecret)
	err := c.UpsertClassifications(context.Background(), repoExternalID, Classifications{})
	var reqErr RequestError
	require.ErrorAs(t, err, &reqErr)
	expectedErr := RequestError{
		StatusCode: http.StatusInternalServerError,
		Status: fmt.Sprintf(
			"%d %s",
			http.StatusInternalServerError,
			http.StatusText(http.StatusInternalServerError),
		),
	}
	require.Equal(t, expectedErr, reqErr)
}

func TestDmapClient_UpsertClassifications_Error(t *testing.T) {
	c := NewDmapClient("invalid", "clientID", "clientSecret")
	err := c.UpsertClassifications(context.Background(), "repoExternalID", Classifications{})
	require.Error(t, err)
}

func TestDmapClientPublishRepoScanResults_Success(t *testing.T) {
	clientID := "clientID"
	clientSecret := "clientSecret"
	repoExternalID := "arn:aws:dynamodb:us-east-1:123456789012:table/test"
	results := scan.RepoScanResults{
		Labels: []classification.Label{
			{
				Name:        "lbl1",
				Description: "desc1",
				Tags:        []string{"tag1"},
			},
		},
		Classifications: []classification.Classification{
			{
				AttributePath: []string{"schema1", "table1", "column1"},
				Labels: classification.LabelSet{
					"lbl1": {},
				},
			},
		},
	}
	wantLbls := Labels{
		Labels: []Label{
			{
				Name:        results.Labels[0].Name,
				Description: results.Labels[0].Description,
				Tags:        results.Labels[0].Tags,
			},
		},
	}
	wantClassifications := Classifications{
		Classifications: []Classification{
			{
				Label:         "lbl1",
				AttributePath: results.Classifications[0].AttributePath,
			},
		},
	}
	svr := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				requireBasicAuth(t, r, clientID, clientSecret)
				require.Equal(t, http.MethodPut, r.Method)
				if r.URL.Path == dataLabelsPath {
					var l Labels
					err := json.NewDecoder(r.Body).Decode(&l)
					require.NoError(t, err)
					require.Equal(t, wantLbls, l)
				} else {
					path := strings.Replace(classificationsPath, "{repoExternalID}", url.QueryEscape(repoExternalID), 1)
					if r.URL.Path != path {
						t.Fatalf("unexpected path %s", r.URL.Path)
					}
					var c Classifications
					err := json.NewDecoder(r.Body).Decode(&c)
					require.NoError(t, err)
					require.Equal(t, wantClassifications, c)
				}
			},
		),
	)
	defer svr.Close()
	c := NewDmapClient(svr.URL, clientID, clientSecret)
	err := c.PublishRepoScanResults(context.Background(), repoExternalID, &results)
	require.NoError(t, err)
}

func TestDmapClientPublishRepoScanResults_Error(t *testing.T) {
	clientID := "clientID"
	clientSecret := "clientSecret"
	repoExternalID := "arn:aws:dynamodb:us-east-1:123456789012:table/test"
	c := NewDmapClient("", clientID, clientSecret)
	err := c.PublishRepoScanResults(context.Background(), repoExternalID, &scan.RepoScanResults{})
	require.Error(t, err)
}

func TestDmapClientPublishRepoScanResults_UpsertLabelsServerError(t *testing.T) {
	clientID := "clientID"
	clientSecret := "clientSecret"
	repoExternalID := "arn:aws:dynamodb:us-east-1:123456789012:table/test"
	results := scan.RepoScanResults{
		Labels: []classification.Label{
			{
				Name:        "lbl1",
				Description: "desc1",
				Tags:        []string{"tag1"},
			},
		},
		Classifications: []classification.Classification{
			{
				AttributePath: []string{"schema1", "table1", "column1"},
				Labels: classification.LabelSet{
					"lbl1": {},
				},
			},
		},
	}
	svr := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				requireBasicAuth(t, r, clientID, clientSecret)
				require.Equal(t, http.MethodPut, r.Method)
				if r.URL.Path == dataLabelsPath {
					w.WriteHeader(http.StatusInternalServerError)
				} else {
					t.Fatalf("unexpected path %s", r.URL.Path)
				}
			},
		),
	)
	defer svr.Close()
	c := NewDmapClient(svr.URL, clientID, clientSecret)
	err := c.PublishRepoScanResults(context.Background(), repoExternalID, &results)
	var reqErr RequestError
	require.ErrorAs(t, err, &reqErr)
	expectedErr := RequestError{
		StatusCode: http.StatusInternalServerError,
		Status: fmt.Sprintf(
			"%d %s",
			http.StatusInternalServerError,
			http.StatusText(http.StatusInternalServerError),
		),
	}
	require.Equal(t, expectedErr, reqErr)
}

func TestPublishRepoScanResults_UpsertClassificationsServerError(t *testing.T) {
	clientID := "clientID"
	clientSecret := "clientSecret"
	repoExternalID := "arn:aws:dynamodb:us-east-1:123456789012:table/test"
	results := scan.RepoScanResults{
		Labels: []classification.Label{
			{
				Name:        "lbl1",
				Description: "desc1",
				Tags:        []string{"tag1"},
			},
		},
		Classifications: []classification.Classification{
			{
				AttributePath: []string{"schema1", "table1", "column1"},
				Labels: classification.LabelSet{
					"lbl1": {},
				},
			},
		},
	}
	wantLbls := Labels{
		Labels: []Label{
			{
				Name:        results.Labels[0].Name,
				Description: results.Labels[0].Description,
				Tags:        results.Labels[0].Tags,
			},
		},
	}
	svr := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				requireBasicAuth(t, r, clientID, clientSecret)
				require.Equal(t, http.MethodPut, r.Method)
				if r.URL.Path == dataLabelsPath {
					var l Labels
					err := json.NewDecoder(r.Body).Decode(&l)
					require.NoError(t, err)
					require.Equal(t, wantLbls, l)
				} else {
					path := strings.Replace(classificationsPath, "{repoExternalID}", url.QueryEscape(repoExternalID), 1)
					if r.URL.Path != path {
						t.Fatalf("unexpected path %s", r.URL.Path)
					}
					w.WriteHeader(http.StatusInternalServerError)
				}
			},
		),
	)
	defer svr.Close()
	c := NewDmapClient(svr.URL, clientID, clientSecret)
	err := c.PublishRepoScanResults(context.Background(), repoExternalID, &results)
	var reqErr RequestError
	require.ErrorAs(t, err, &reqErr)
	expectedErr := RequestError{
		StatusCode: http.StatusInternalServerError,
		Status: fmt.Sprintf(
			"%d %s",
			http.StatusInternalServerError,
			http.StatusText(http.StatusInternalServerError),
		),
	}
	require.Equal(t, expectedErr, reqErr)
}

func requireBasicAuth(t *testing.T, r *http.Request, clientID, clientSecret string) {
	username, password, ok := r.BasicAuth()
	require.Truef(t, ok, "expected basic auth but got %s", r.Header.Get("Authorization"))
	require.Equal(t, clientID, username)
	require.Equal(t, clientSecret, password)
}
