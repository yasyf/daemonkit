package templates

import (
	"os"
	"strings"
	"testing"
)

func TestReleaseTemplateOwnsAtomicReleaseAndStableCaskPublication(t *testing.T) {
	payload, err := os.ReadFile("release.yml.tmpl")
	if err != nil {
		t.Fatal(err)
	}
	workflow := string(payload)
	const (
		workflowRef           = "83ee384b1d4fe25a8e4aa7258bb76d55e1593735"
		stageDraftActionRef   = "e4c3108e693681df1a3c666bae80e890bc44cf3e"
		publishDraftActionRef = "54e3e194bda69896894a82c17fcdb2822beefab5"
		renderFormulaRef      = "19c3d5013032ad9c88f9a8f1170d1f366c19b8d9"
		publishTapRef         = "9ca67392d45d66b6ae01e262383c8f3138d56f5e"
	)

	required := []string{
		"release-app.yml@" + workflowRef,
		"name: ${{ needs.release.outputs.artifact_name }}",
		"actions/download-artifact@v8",
		"asset_name: __CASK_TOKEN__",
		"needs.version.outputs.stable == 'true'",
		"needs.release.outputs.changed == 'true'",
		"stage-draft-release@" + stageDraftActionRef,
		"publish-draft-release@" + publishDraftActionRef,
		"release-id: ${{ steps.draft.outputs['release-id'] }}",
		"manifest: ${{ runner.temp }}/app-release-assets",
		"make-latest: ${{ needs.version.outputs.stable == 'true' }}",
		"ruby -c \"$cask\"",
		"public release asset SHA differs from the verified artifact",
		"__ASSET_URL__=${{ needs.release.outputs.asset_url }}",
		"__SHA_APP__=${{ needs.release.outputs.sha256 }}",
		"Formula/${CASK_TOKEN}.rb already exists",
		"older than registered cask version",
		"cask template must install the full .app",
		"cask template must not install this application as a bare binary",
		"render-formula@" + renderFormulaRef,
		"actions/publish@" + publishTapRef,
		"cmp tap-staging/Casks/__CASK_TOKEN__.rb \"$published\"",
	}
	for _, value := range required {
		if !strings.Contains(workflow, value) {
			t.Fatalf("release template missing %q", value)
		}
	}
	for _, forbidden := range []string{
		"cask_token:", "cask_template_path:", "release-app.yml@v1", "softprops/action-gh-release",
		"gh release create", "gh release edit",
		"f45550932b0c8a42eb04e9ab0e5de8f82ad78b6a", "e55053b4d8f50a915eb518afb89e6f7cb8c9993f",
		"1666a5363ad6f2ed7ac0be901702e523cc1fba66",
		"release-app.yml@19c3d5013032ad9c88f9a8f1170d1f366c19b8d9",
		"release-app.yml@54e3e194bda69896894a82c17fcdb2822beefab5",
		"release-app.yml@8f422c652d836c40f9cc5a9d893d4120b26bc681",
		"release-app.yml@0f472e87dac4a05f0d275c2b3f8c69adb20929d0",
		"stage-draft-release@54e3e194bda69896894a82c17fcdb2822beefab5",
		"actions/publish@19c3d5013032ad9c88f9a8f1170d1f366c19b8d9",
	} {
		if strings.Contains(workflow, forbidden) {
			t.Fatalf("release template retains deleted contract %q", forbidden)
		}
	}
	if strings.Count(workflow, "stage-draft-release@"+stageDraftActionRef) != 1 ||
		strings.Count(workflow, "publish-draft-release@"+publishDraftActionRef) != 1 {
		t.Fatal("release template must stage and publish exactly one caller-owned draft")
	}
	if strings.Count(workflow, "actions/publish@"+publishTapRef) != 1 {
		t.Fatal("release template must publish the cask exactly once")
	}
	ordered := []string{
		"actions/download-artifact@v8",
		"stage-draft-release@" + stageDraftActionRef,
		"name: Render the cask",
		"name: Test the rendered cask locally",
		"publish-draft-release@" + publishDraftActionRef,
		"name: Verify the public release asset",
		"actions/publish@" + publishTapRef,
		"name: Verify the published cask and asset",
	}
	previous := -1
	for _, marker := range ordered {
		position := strings.Index(workflow, marker)
		if position <= previous {
			t.Fatalf("release ordering marker %q is missing or out of order", marker)
		}
		previous = position
	}
}
