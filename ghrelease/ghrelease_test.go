package ghrelease

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLatestReturnsTagAndAssets(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/yasyf/cc-review/releases/latest" {
			t.Errorf("path = %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Errorf("Authorization = %q, want Bearer secret", got)
		}
		_, _ = w.Write([]byte(`{"tag_name":"v0.21.3","assets":[{"name":"cc-review_darwin_arm64.tar.gz","browser_download_url":"https://example.test/dl","size":1234}]}`))
	}))
	defer server.Close()

	release, err := (Client{BaseURL: server.URL, Token: "secret"}).Latest(context.Background(), "yasyf/cc-review")
	if err != nil {
		t.Fatalf("Latest() = %v", err)
	}
	if release.Tag != "v0.21.3" {
		t.Errorf("Tag = %q, want v0.21.3", release.Tag)
	}
	if len(release.Assets) != 1 {
		t.Fatalf("assets = %d, want 1", len(release.Assets))
	}
	want := Asset{Name: "cc-review_darwin_arm64.tar.gz", URL: "https://example.test/dl", Size: 1234}
	if release.Assets[0] != want {
		t.Errorf("asset = %+v, want %+v", release.Assets[0], want)
	}
}

func TestLatestUnauthenticatedSendsNoAuthHeader(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, ok := r.Header["Authorization"]; ok {
			t.Errorf("unexpected Authorization header %q", r.Header.Get("Authorization"))
		}
		_, _ = w.Write([]byte(`{"tag_name":"v1.0.0","assets":[]}`))
	}))
	defer server.Close()

	release, err := (Client{BaseURL: server.URL}).Latest(context.Background(), "owner/name")
	if err != nil {
		t.Fatalf("Latest() = %v", err)
	}
	if release.Tag != "v1.0.0" || len(release.Assets) != 0 {
		t.Fatalf("release = %+v, want tag v1.0.0 with no assets", release)
	}
}

func TestLatestNonOKStatusErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	if _, err := (Client{BaseURL: server.URL}).Latest(context.Background(), "owner/name"); err == nil {
		t.Fatal("Latest() = nil, want error on 404")
	}
}

func TestLatestEmptyRepoErrors(t *testing.T) {
	if _, err := (Client{}).Latest(context.Background(), ""); err == nil {
		t.Fatal("Latest(\"\") = nil, want error")
	}
}
