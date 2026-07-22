package service

import (
	"strings"
	"testing"
)

func TestAgentPlistRendersWatchPathsAndCalendar(t *testing.T) {
	agent := testAgent(t)
	agent.RestartPolicy = NoRestart
	agent.WatchPaths = []string{"/Users/x/.local/bin/claude", "/Users/x/a<b"}
	agent.StartCalendarInterval = []CalendarInterval{Daily(3, 30)}
	body, err := agent.Plist()
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, want := range []string{
		"<key>WatchPaths</key>",
		"<string>/Users/x/.local/bin/claude</string>",
		"/Users/x/a&lt;b",
		"<key>StartCalendarInterval</key>",
		"<key>Hour</key>",
		"<integer>3</integer>",
		"<key>Minute</key>",
		"<integer>30</integer>",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("rendered plist missing %q\n%s", want, text)
		}
	}
	if strings.Contains(text, "a<b</string>") {
		t.Fatalf("watch path not escaped\n%s", text)
	}
}

func TestAgentPlistOmitsWatchAndCalendarWhenEmpty(t *testing.T) {
	agent := testAgent(t)
	body, err := agent.Plist()
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, unwanted := range []string{"WatchPaths", "StartCalendarInterval"} {
		if strings.Contains(text, unwanted) {
			t.Errorf("empty agent rendered %q\n%s", unwanted, text)
		}
	}
}

func TestAgentPlistRejectsRelativeWatchPath(t *testing.T) {
	agent := testAgent(t)
	agent.WatchPaths = []string{"relative/path"}
	if _, err := agent.Plist(); err == nil {
		t.Fatal("expected error for relative watch path")
	}
}

func TestCalendarIntervalRejectsOutOfRange(t *testing.T) {
	agent := testAgent(t)
	agent.StartCalendarInterval = []CalendarInterval{Daily(25, 0)}
	if _, err := agent.Plist(); err == nil {
		t.Fatal("expected error for hour=25")
	}
}
