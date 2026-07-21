package templates

import (
	"os"
	"strings"
	"testing"
)

func TestReleaseTemplateOwnsStableCaskPublication(t *testing.T) {
	payload, err := os.ReadFile("release.yml.tmpl")
	if err != nil {
		t.Fatal(err)
	}
	workflow := string(payload)
	const ref = "f45550932b0c8a42eb04e9ab0e5de8f82ad78b6a"

	required := []string{
		"release-app.yml@" + ref,
		"asset_name: __CASK_TOKEN__",
		"needs.version.outputs.stable == 'true'",
		"needs.release.outputs.changed == 'true'",
		"__ASSET_URL__=${{ needs.release.outputs.asset_url }}",
		"__SHA_APP__=${{ needs.release.outputs.sha256 }}",
		"Formula/${CASK_TOKEN}.rb already exists",
		"older than registered cask version",
		"cask template must install the full .app",
		"cask template must not install this application as a bare binary",
		"render-formula@" + ref,
	}
	for _, value := range required {
		if !strings.Contains(workflow, value) {
			t.Fatalf("release template missing %q", value)
		}
	}
	for _, forbidden := range []string{"cask_token:", "cask_template_path:", "release-app.yml@v1"} {
		if strings.Contains(workflow, forbidden) {
			t.Fatalf("release template retains deleted contract %q", forbidden)
		}
	}
	if strings.Count(workflow, "actions/publish@"+ref) != 1 {
		t.Fatal("release template must publish the cask exactly once")
	}
}
