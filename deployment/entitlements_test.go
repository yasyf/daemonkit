package deployment

import "testing"

func TestDigestEntitlementsIsOrderIndependentAndComplete(t *testing.T) {
	first := []byte(`<?xml version="1.0"?><plist version="1.0"><dict><key>enabled</key><true/><key>groups</key><array><string>one</string><string>two</string></array></dict></plist>`)
	second := []byte(`<?xml version="1.0"?><plist version="1.0"><dict><key>groups</key><array><string>one</string><string>two</string></array><key>enabled</key><true/></dict></plist>`)
	changed := []byte(`<?xml version="1.0"?><plist version="1.0"><dict><key>enabled</key><true/><key>groups</key><array><string>two</string><string>one</string></array></dict></plist>`)
	firstDigest, err := DigestEntitlements(first)
	if err != nil {
		t.Fatal(err)
	}
	secondDigest, err := DigestEntitlements(second)
	if err != nil {
		t.Fatal(err)
	}
	changedDigest, err := DigestEntitlements(changed)
	if err != nil {
		t.Fatal(err)
	}
	if firstDigest != secondDigest {
		t.Fatalf("dictionary order changed digest: %s != %s", firstDigest, secondDigest)
	}
	if firstDigest == changedDigest {
		t.Fatal("array order was not sealed")
	}
}

func TestDecodeEntitlementsOutputIgnoresCodesignDiagnostics(t *testing.T) {
	payload := []byte("Executable=/Applications/Helper.app/Contents/MacOS/Helper\n" +
		`<?xml version="1.0"?><plist version="1.0"><dict><key>enabled</key><true/></dict></plist>`)
	want, err := DigestEntitlements(payload[len("Executable=/Applications/Helper.app/Contents/MacOS/Helper\n"):])
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeEntitlementsOutput(payload)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("digest = %s, want %s", got, want)
	}
}
