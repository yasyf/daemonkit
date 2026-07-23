package templates

import (
	"errors"
	"os"
	"regexp"
	"strings"
	"testing"
)

var caskPlaceholder = regexp.MustCompile(`__[A-Z0-9_]+__`)

func TestCaskTemplateRequiresExactStopHook(t *testing.T) {
	rendered, err := renderCaskTemplate("--stop-and-uninstall-service")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(rendered, `args: ["--stop-and-uninstall-service"]`) != 2 {
		t.Fatal("exact stop hook must gate both upgrade and uninstall")
	}
	if strings.Count(rendered, "must_succeed: true") < 2 {
		t.Fatal("exact stop hook must fail closed")
	}
}

func TestCaskTemplateUsesAuthoritativeAssetURL(t *testing.T) {
	rendered, err := renderCaskTemplate("--stop-and-uninstall-service")
	if err != nil {
		t.Fatal(err)
	}
	const assetURL = "https://github.com/example/example-helper/releases/download/v1.2.3/example-helper-v1.2.3-darwin.zip"
	if !strings.Contains(rendered, `url "`+assetURL+`"`) {
		t.Fatal("cask does not use the authoritative release asset URL")
	}
	if strings.Contains(rendered, `releases/download/v#{version}`) {
		t.Fatal("cask reconstructs the release asset URL")
	}
}

func TestCaskTemplateUsesMeaningfulUserApplicationPath(t *testing.T) {
	rendered, err := renderCaskTemplate("--stop-and-uninstall-service")
	if err != nil {
		t.Fatal(err)
	}
	const installedApp = `#{Dir.home}/Applications/Example Helper.app`
	if !strings.Contains(rendered, `app "Example Helper.app", target: "`+installedApp+`"`) {
		t.Fatal("cask does not install the product helper at its stable user application path")
	}
	if got := strings.Count(rendered, installedApp); got != 4 {
		t.Fatalf("stable user application path appears %d times, want app plus every lifecycle hook", got)
	}
	if strings.Contains(rendered, "#{appdir}") {
		t.Fatal("cask still relies on Homebrew's system application directory")
	}
	for _, required := range []string{
		"Do not use this template for FuseKit consumer runtimes",
		"embeds holder.Runtime",
		"$HOME/Applications/<MeaningfulProduct>.app",
		"its CLI reconciles that app",
		"There is no generic FuseKit application or cask",
	} {
		if !strings.Contains(rendered, required) {
			t.Fatalf("cask omits consumer-runtime contract %q", required)
		}
	}
	for _, retired := range []string{"fusekit-holder", "FuseKitHolder.app"} {
		if strings.Contains(rendered, retired) {
			t.Fatalf("cask still cites retired holder artifact %q", retired)
		}
	}
}

func TestCaskTemplateGenerationFailsWithoutExactStopHook(t *testing.T) {
	if _, err := renderCaskTemplate(""); !errors.Is(err, errMissingStopHook) {
		t.Fatalf("render without stop hook = %v, want %v", err, errMissingStopHook)
	}
}

func TestCaskTemplateRejectsNameBasedProcessControl(t *testing.T) {
	rendered, err := renderCaskTemplate("--stop-and-uninstall-service")
	if err != nil {
		t.Fatal(err)
	}
	lower := strings.ToLower(rendered)
	for _, forbidden := range []string{"pkill", "pgrep", "killall", "osascript", "__proc_name__", "uninstall quit:"} {
		if strings.Contains(lower, forbidden) {
			t.Fatalf("cask contains forbidden process control %q", forbidden)
		}
	}
}

var errMissingStopHook = errors.New("daemonkit: exact stop and uninstall hook is required")

func renderCaskTemplate(stopHook string) (string, error) {
	if strings.TrimSpace(stopHook) == "" {
		return "", errMissingStopHook
	}
	payload, err := os.ReadFile("cask.rb.tmpl")
	if err != nil {
		return "", err
	}
	values := map[string]string{
		"__APP_NAME__": "Example Helper", "__GH_OWNER__": "example", "__CASK_TOKEN__": "example-helper",
		"__VERSION__": "1.2.3", "__SHA_APP__": strings.Repeat("a", 64), "__DISPLAY_NAME__": "Example Helper",
		"__DESC__": "Example", "__GH_REPO__": "example-helper", "__MACOS_MIN__": "sequoia",
		"__BINARY_NAME__": "Example Helper", "__STOP_UNINSTALL_ARG__": stopHook,
		"__BUNDLE_ID__": "com.example.helper", "__LAUNCHAGENT_LABEL__": "com.example.helper.agent",
		"__ASSET_URL__": "https://github.com/example/example-helper/releases/download/v1.2.3/example-helper-v1.2.3-darwin.zip",
	}
	rendered := string(payload)
	for token, value := range values {
		rendered = strings.ReplaceAll(rendered, token, value)
	}
	if token := caskPlaceholder.FindString(rendered); token != "" {
		return "", errors.New("daemonkit: unrendered cask placeholder " + token)
	}
	return rendered, nil
}
