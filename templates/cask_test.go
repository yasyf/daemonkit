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

func TestCaskTemplateHandlesMissingBinaryHusks(t *testing.T) {
	rendered, err := renderCaskTemplate("--stop-and-uninstall-service")
	if err != nil {
		t.Fatal(err)
	}
	preflightStart := strings.Index(rendered, "\n  preflight do")
	postflightStart := strings.Index(rendered, "\n  postflight do")
	uninstallPreflightStart := strings.Index(rendered, "\n  uninstall_preflight do")
	zapStart := strings.Index(rendered, "\n  zap trash:")
	if preflightStart < 0 || postflightStart < 0 || uninstallPreflightStart < 0 || zapStart < 0 {
		t.Fatal("cask is missing an expected stanza")
	}
	preflight := rendered[preflightStart:postflightStart]
	uninstallPreflight := rendered[uninstallPreflightStart:zapStart]
	guarded := regexp.MustCompile(`(?s)if File\.executable\?\(installed_binary\)(.*?)\n\s*(?:elsif|end)\b`)
	for name, stanza := range map[string]string{
		"preflight":           preflight,
		"uninstall_preflight": uninstallPreflight,
	} {
		match := guarded.FindStringSubmatch(stanza)
		if match == nil {
			t.Fatalf("%s does not guard the exact stop hook with File.executable?", name)
		}
		if !strings.Contains(match[1], `args: ["--stop-and-uninstall-service"]`) {
			t.Fatalf("%s stop hook is not inside the File.executable? guard", name)
		}
		if !strings.Contains(match[1], "must_succeed: true") {
			t.Fatalf("%s guarded stop hook does not fail closed", name)
		}
	}
	if !strings.Contains(preflight, "elsif Dir.exist?(installed_app)") ||
		!strings.Contains(preflight, "FileUtils.rm_r(installed_app)") {
		t.Fatal("preflight does not remove a binary-less app husk")
	}
	if strings.Count(rendered, "FileUtils.rm_r(installed_app)") != 1 ||
		strings.Contains(uninstallPreflight, "FileUtils.rm_r") {
		t.Fatal("app husk removal must exist in preflight only")
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
