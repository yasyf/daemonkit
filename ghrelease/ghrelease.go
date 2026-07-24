// Package ghrelease queries GitHub for a repository's latest published release.
// It exists only for self-update flows; version-exact artifact resolution never
// consults it, so a descriptor fetch can never chase a moving latest tag.
package ghrelease

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
)

const defaultBaseURL = "https://api.github.com"

// Asset is one downloadable release asset.
type Asset struct {
	Name string
	URL  string
	Size int64
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
	if repo == "" {
		return Release{}, errors.New("ghrelease: empty repository")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/repos/%s/releases/latest", c.baseURL(), repo), nil)
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
		return Release{}, fmt.Errorf("ghrelease: get latest release for %q: %w", repo, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return Release{}, fmt.Errorf("ghrelease: latest release for %q: status %s", repo, response.Status)
	}
	var payload struct {
		TagName string `json:"tag_name"`
		Assets  []struct {
			Name string `json:"name"`
			URL  string `json:"browser_download_url"`
			Size int64  `json:"size"`
		} `json:"assets"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return Release{}, fmt.Errorf("ghrelease: decode latest release for %q: %w", repo, err)
	}
	assets := make([]Asset, len(payload.Assets))
	for i, a := range payload.Assets {
		assets[i] = Asset{Name: a.Name, URL: a.URL, Size: a.Size}
	}
	return Release{Tag: payload.TagName, Assets: assets}, nil
}

// Latest returns the repository's latest published release using the default
// client, authenticating with the ambient GITHUB_TOKEN when it is set.
func Latest(ctx context.Context, repo string) (Release, error) {
	return Client{Token: os.Getenv("GITHUB_TOKEN")}.Latest(ctx, repo)
}
