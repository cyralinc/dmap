package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cyralinc/dmap/classification"
	"github.com/cyralinc/dmap/scan"
)

func TestDmapClient_CreateRepoScan_Success(t *testing.T) {
	clientID := "dummyClientId"
	clientSecret := "dummyClientSecret"
	agent := "dmap-test"
	repoExternalID := "arn:aws:dynamodb:us-east-1:123456789012:table/test"
	wantRepoScan := RepoScan{
		Agent:          agent,
		RepoExternalId: repoExternalID,
	}
	wantRepoScanId := "66284f0bf29b853e7db81bd4"
	svr := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				requireBasicAuth(t, r, clientID, clientSecret)
				require.Equal(t, http.MethodPost, r.Method)
				require.Equal(t, repoScanPath, r.URL.Path)
				var rs RepoScan
				err := json.NewDecoder(r.Body).Decode(&rs)
				require.NoError(t, err)
				require.Equal(t, wantRepoScan, rs)
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprintf(w, `{"id":"%s"}`, wantRepoScanId)
			},
		),
	)
	defer svr.Close()
	c := NewDmapClient(svr.URL, clientID, clientSecret)
	id, err := c.CreateRepoScan(context.Background(), wantRepoScan)
	require.NoError(t, err)
	require.Equal(t, wantRepoScanId, id)
}

func TestDmapClient_CreateRepoScan_ServerError(t *testing.T) {
	clientID := "dummyClientId"
	clientSecret := "dummyClientSecret"
	svr := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				requireBasicAuth(t, r, clientID, clientSecret)
				require.Equal(t, http.MethodPost, r.Method)
				require.Equal(t, repoScanPath, r.URL.Path)
				w.WriteHeader(http.StatusInternalServerError)
			},
		),
	)
	defer svr.Close()
	c := NewDmapClient(svr.URL, clientID, clientSecret)
	id, err := c.CreateRepoScan(context.Background(), RepoScan{})
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
	require.Empty(t, id)
}

func TestDmapClient_CreateRepoScan_Error(t *testing.T) {
	c := NewDmapClient("invalid", "dummyClientId", "dummyClientSecret")
	id, err := c.CreateRepoScan(context.Background(), RepoScan{})
	require.Error(t, err)
	require.Empty(t, id)
}

func TestDmapClient_UpsertLabels_Success(t *testing.T) {
	clientID := "dummyClientId"
	clientSecret := "dummyClientSecret"
	repoScanId := "66284f0bf29b853e7db81bd4"
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
				path := strings.Replace(labelsPath, "{"+repoScanIdKey+"}", repoScanId, 1)
				require.Equal(t, path, r.URL.Path)
				var l Labels
				err := json.NewDecoder(r.Body).Decode(&l)
				require.NoError(t, err)
				require.Equal(t, lbls, l)
			},
		),
	)
	defer svr.Close()
	c := NewDmapClient(svr.URL, clientID, clientSecret)
	err := c.UpsertLabels(context.Background(), repoScanId, lbls)
	require.NoError(t, err)
}

func TestDmapClient_UpsertLabels_ServerError(t *testing.T) {
	clientID := "dummyClientId"
	clientSecret := "dummyClientSecret"
	repoScanId := "66284f0bf29b853e7db81bd4"
	svr := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				requireBasicAuth(t, r, clientID, clientSecret)
				require.Equal(t, http.MethodPut, r.Method)
				path := strings.Replace(labelsPath, "{"+repoScanIdKey+"}", repoScanId, 1)
				require.Equal(t, path, r.URL.Path)
				w.WriteHeader(http.StatusInternalServerError)
			},
		),
	)
	defer svr.Close()
	c := NewDmapClient(svr.URL, clientID, clientSecret)
	err := c.UpsertLabels(context.Background(), repoScanId, Labels{})
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
	repoScanId := "66284f0bf29b853e7db81bd4"
	c := NewDmapClient("invalid", "dummyClientId", "dummyClientSecret")
	err := c.UpsertLabels(context.Background(), repoScanId, Labels{})
	require.Error(t, err)
}

func TestDmapClient_UpsertClassifications_Success(t *testing.T) {
	clientID := "dummyClientId"
	clientSecret := "dummyClientSecret"
	repoScanId := "66284f0bf29b853e7db81bd4"
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
				path := strings.Replace(classificationsPath, "{"+repoScanIdKey+"}", repoScanId, 1)
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
	err := c.UpsertClassifications(context.Background(), repoScanId, classifications)
	require.NoError(t, err)
}

func TestDmapClient_UpsertClassifications_ServerError(t *testing.T) {
	clientID := "dummyClientId"
	clientSecret := "dummyClientSecret"
	repoScanId := "66284f0bf29b853e7db81bd4"
	svr := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				requireBasicAuth(t, r, clientID, clientSecret)
				require.Equal(t, http.MethodPut, r.Method)
				path := strings.Replace(classificationsPath, "{"+repoScanIdKey+"}", repoScanId, 1)
				require.Equal(t, path, r.URL.Path)
				w.WriteHeader(http.StatusInternalServerError)
			},
		),
	)
	defer svr.Close()
	c := NewDmapClient(svr.URL, clientID, clientSecret)
	err := c.UpsertClassifications(context.Background(), repoScanId, Classifications{})
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
	repoScanId := "66284f0bf29b853e7db81bd4"
	c := NewDmapClient("invalid", "dummyClientId", "dummyClientSecret")
	err := c.UpsertClassifications(context.Background(), repoScanId, Classifications{})
	require.Error(t, err)
}

func TestDmapClientPublishRepoScanResults_Success(t *testing.T) {
	clientID := "dummyClientId"
	clientSecret := "dummyClientSecret"
	agent := "dmap-test"
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
	wantRepoScanId := "66284f0bf29b853e7db81bd4"
	wantRepoScan := RepoScan{
		Agent:          agent,
		RepoExternalId: repoExternalID,
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
				if r.URL.Path == repoScanPath {
					require.Equal(t, http.MethodPost, r.Method)
					var rs RepoScan
					err := json.NewDecoder(r.Body).Decode(&rs)
					require.NoError(t, err)
					require.Equal(t, wantRepoScan, rs)
					w.Header().Set("Content-Type", "application/json")
					_, _ = fmt.Fprintf(w, `{"id":"%s"}`, wantRepoScanId)
				} else {
					require.Equal(t, http.MethodPut, r.Method)
					if strings.HasSuffix(r.URL.Path, "/labels") {
						path := strings.Replace(labelsPath, "{"+repoScanIdKey+"}", wantRepoScanId, 1)
						require.Equal(t, path, r.URL.Path)
						var l Labels
						err := json.NewDecoder(r.Body).Decode(&l)
						require.NoError(t, err)
						require.Equal(t, wantLbls, l)
					} else if strings.HasSuffix(r.URL.Path, "/classifications") {
						path := strings.Replace(classificationsPath, "{"+repoScanIdKey+"}", wantRepoScanId, 1)
						require.Equal(t, path, r.URL.Path)
						var c Classifications
						err := json.NewDecoder(r.Body).Decode(&c)
						require.NoError(t, err)
						require.Equal(t, wantClassifications, c)
					} else {
						t.Fatalf("unexpected path %s", r.URL.Path)
					}
				}
			},
		),
	)
	defer svr.Close()
	c := NewDmapClient(svr.URL, clientID, clientSecret)
	err := c.PublishRepoScanResults(context.Background(), agent, repoExternalID, &results)
	require.NoError(t, err)
}

func TestDmapClientPublishRepoScanResults_Error(t *testing.T) {
	clientID := "dummyClientId"
	clientSecret := "dummyClientSecret"
	agent := "dmap-test"
	repoExternalID := "arn:aws:dynamodb:us-east-1:123456789012:table/test"
	c := NewDmapClient("", clientID, clientSecret)
	err := c.PublishRepoScanResults(context.Background(), agent, repoExternalID, &scan.RepoScanResults{})
	require.Error(t, err)
}

func TestDmapClientPublishRepoScanResults_UpsertLabelsServerError(t *testing.T) {
	clientID := "dummyClientId"
	clientSecret := "dummyClientSecret"
	agent := "dmap-test"
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
	wantRepoScanId := "66284f0bf29b853e7db81bd4"
	wantRepoScan := RepoScan{
		Agent:          agent,
		RepoExternalId: repoExternalID,
	}
	svr := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				requireBasicAuth(t, r, clientID, clientSecret)
				if r.URL.Path == repoScanPath {
					require.Equal(t, http.MethodPost, r.Method)
					var rs RepoScan
					err := json.NewDecoder(r.Body).Decode(&rs)
					require.NoError(t, err)
					require.Equal(t, wantRepoScan, rs)
					w.Header().Set("Content-Type", "application/json")
					_, _ = fmt.Fprintf(w, `{"id":"%s"}`, wantRepoScanId)
				} else {
					require.Equal(t, http.MethodPut, r.Method)
					if strings.HasSuffix(r.URL.Path, "/labels") {
						w.WriteHeader(http.StatusInternalServerError)
					} else {
						t.Fatalf("unexpected path %s", r.URL.Path)
					}
				}
			},
		),
	)
	defer svr.Close()
	c := NewDmapClient(svr.URL, clientID, clientSecret)
	err := c.PublishRepoScanResults(context.Background(), agent, repoExternalID, &results)
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
	clientID := "dummyClientId"
	clientSecret := "dummyClientSecret"
	agent := "dmap-test"
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
	wantRepoScanId := "66284f0bf29b853e7db81bd4"
	wantRepoScan := RepoScan{
		Agent:          agent,
		RepoExternalId: repoExternalID,
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
				if r.URL.Path == repoScanPath {
					require.Equal(t, http.MethodPost, r.Method)
					var rs RepoScan
					err := json.NewDecoder(r.Body).Decode(&rs)
					require.NoError(t, err)
					require.Equal(t, wantRepoScan, rs)
					w.Header().Set("Content-Type", "application/json")
					_, _ = fmt.Fprintf(w, `{"id":"%s"}`, wantRepoScanId)
				} else {
					require.Equal(t, http.MethodPut, r.Method)
					if strings.HasSuffix(r.URL.Path, "/labels") {
						path := strings.Replace(labelsPath, "{"+repoScanIdKey+"}", wantRepoScanId, 1)
						require.Equal(t, path, r.URL.Path)
						var l Labels
						err := json.NewDecoder(r.Body).Decode(&l)
						require.NoError(t, err)
						require.Equal(t, wantLbls, l)
					} else if strings.HasSuffix(r.URL.Path, "/classifications") {
						w.WriteHeader(http.StatusInternalServerError)
					} else {
						t.Fatalf("unexpected path %s", r.URL.Path)
					}
				}
			},
		),
	)
	defer svr.Close()
	c := NewDmapClient(svr.URL, clientID, clientSecret)
	err := c.PublishRepoScanResults(context.Background(), agent, repoExternalID, &results)
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
