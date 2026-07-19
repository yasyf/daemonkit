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
