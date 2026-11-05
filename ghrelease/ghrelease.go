// Package ghrelease queries GitHub for a repository's releases: the latest one,
// for self-update flows, and one by exact tag, for the API download route
// artifact resolution takes when the release download host fails. Resolution
// never consults the latest release, so a descriptor fetch can never chase a
// moving tag.
package ghrelease

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
)

const defaultBaseURL = "https://api.github.com"

// Asset is one downloadable release asset. URL is its browser download URL and
// APIURL the API endpoint that serves its bytes to an
// "Accept: application/octet-stream" request.
type Asset struct {
	Name   string
	URL    string
	APIURL string
	Size   int64
}

// Release is a repository release: its tag and downloadable assets.
type Release struct {
	Tag    string
	Assets []Asset
}

// Client queries the GitHub releases API. The zero value queries the public API
// unauthenticated; set Token to authenticate and BaseURL to point at a test
// server.
type Client struct {
	HTTPClient *http.Client
	BaseURL    string
	Token      string
}

func (c Client) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return http.DefaultClient
}

func (c Client) baseURL() string {
	if c.BaseURL != "" {
		return c.BaseURL
	}
	return defaultBaseURL
}

// Latest returns the repository's latest published release. repo is "owner/name".
func (c Client) Latest(ctx context.Context, repo string) (Release, error) {
	return c.release(ctx, repo, "latest", "latest release")
}

// Tagged returns the repository's release at tag. repo is "owner/name".
func (c Client) Tagged(ctx context.Context, repo, tag string) (Release, error) {
	if tag == "" {
		return Release{}, errors.New("ghrelease: empty tag")
	}
	return c.release(ctx, repo, "tags/"+url.PathEscape(tag), "release "+tag)
}

func (c Client) release(ctx context.Context, repo, path, what string) (Release, error) {
	if repo == "" {
		return Release{}, errors.New("ghrelease: empty repository")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/repos/%s/releases/%s", c.baseURL(), repo, path), nil)
	if err != nil {
		return Release{}, fmt.Errorf("ghrelease: build request: %w", err)
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if c.Token != "" {
		request.Header.Set("Authorization", "Bearer "+c.Token)
	}
	response, err := c.httpClient().Do(request)
	if err != nil {
		return Release{}, fmt.Errorf("ghrelease: get %s for %q: %w", what, repo, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return Release{}, fmt.Errorf("ghrelease: %s for %q: status %s", what, repo, response.Status)
	}
	var payload struct {
		TagName string `json:"tag_name"`
		Assets  []struct {
			Name   string `json:"name"`
			URL    string `json:"browser_download_url"`
			APIURL string `json:"url"`
			Size   int64  `json:"size"`
		} `json:"assets"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return Release{}, fmt.Errorf("ghrelease: decode %s for %q: %w", what, repo, err)
	}
	assets := make([]Asset, len(payload.Assets))
	for i, a := range payload.Assets {
		assets[i] = Asset{Name: a.Name, URL: a.URL, APIURL: a.APIURL, Size: a.Size}
	}
	return Release{Tag: payload.TagName, Assets: assets}, nil
}

// Latest returns the repository's latest published release using the default
// client, authenticating with the ambient GITHUB_TOKEN when it is set.
func Latest(ctx context.Context, repo string) (Release, error) {
	return Client{Token: os.Getenv("GITHUB_TOKEN")}.Latest(ctx, repo)
}
