package deploy

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"syscall"
	"testing"
)

// legacyActivation is one activation receipt in the shape v0.20.10 sealed
// (deployment/state.go at that tag): an identity this package renamed, a schema
// constant renamed with it, and operation_id, config_fingerprint,
// consumer_build, policy_digest, phase, and plan — six fields no type here
// declares. readRecord refuses an unknown field, so this is exactly the payload
// every installed machine holds and a cut-era binary cannot decode.
const legacyActivation = `{"identity":"daemonkit.deployment.activation.v1","schema":1,` +
	`"operation_id":"3f1c0b7a5e2d48c9a06b1f83d47e59a2c8b13d6f0a25e74981c3b6a0f5d2e847",` +
	`"config_fingerprint":"9b7c1d2e3f405162738495a6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f80",` +
	`"consumer_build":"0.20.10",` +
	`"policy_digest":"2c4e6a8093b5d7f10213243546576879a0b1c2d3e4f5061728394a5b6c7d8e9f",` +
	`"phase":"active",` +
	`"generation":{"path":"/Applications/Example.app","version":"1.0",` +
	`"team_id":"ABCDE12345","signing_identifier":"com.example.daemonkit.test",` +
	`"designated_requirement":"anchor apple generic and identifier \"com.example.daemonkit.test\"` +
	` and certificate leaf[subject.OU] = \"ABCDE12345\"",` +
	`"cdhash":"6a1f0b93c25d478e0f31a6b58c02d94e7f13a5b6",` +
	`"entitlements_digest":"41d0e2b78f635c9a1204e8b3d76f5a09c1e82b47d0369af5182c7b04e6d93f18",` +
	`"bundle_digest":"b3e07f21a94c8d5062f17b3e8a04d95c6172e8b3f405a917c26d0e3b48f7a501",` +
	`"file_id":{"device":"16777232","inode":"8419043"}},` +
	`"plan":{"agents":[{"Label":"com.example.daemonkit.test",` +
	`"Program":"/Applications/Example.app/Contents/MacOS/example","Args":null,` +
	`"LogPath":"/Users/example/Library/Logs/example.log","Env":null,` +
	`"AssociatedBundleIdentifiers":null,"RestartPolicy":2,"StartInterval":0,` +
	`"WatchPaths":null,"StartCalendarInterval":null,"ProcessType":0,` +
	`"LimitLoadToSessionType":0}],` +
	`"digest":"7c05e91b3a2d64f8091e5c7b3d02a648f91b5c7d3e0a2648b91d5f7c30a26e48"},` +
	`"readiness":{"runtime_build":"0.20.10",` +
	`"process_generation":"4d81f0a63b27c95e18d40b7a3f62c951",` +
	`"resource_digest":"08a3f61c7d29e40b5382a71f6c04d9e83b15a72f60c4d918e3b07a52f64c8d19"}}` + "\n"

// plantLegacyDeployment stands up what a v0.20.10 install left on disk: the
// receipts, the lock, the bbolt service store, and a prior generation some
// earlier crash stranded, all under the metadata directory this package renamed
// away from.
func plantLegacyDeployment(t *testing.T, root, name string) string {
	t.Helper()
	tree := filepath.Join(root, legacyMetadataDir, name)
	if err := os.MkdirAll(filepath.Join(tree, "prior.app", "Contents", "MacOS"), 0o700); err != nil {
		t.Fatal(err)
	}
	for rel, body := range map[string]string{
		"activation.json":                  legacyActivation,
		"deployment.lock":                  "",
		"services.db":                      "\x00\x00\x00\x00bbolt page",
		"prior.app/Contents/MacOS/example": "prior",
	} {
		if err := os.WriteFile(filepath.Join(tree, filepath.FromSlash(rel)), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return tree
}

// TestEnsureMetadataArchivesTheLegacyTree holds the rename to its whole reason.
// Every record a v0.20.10 install sealed is undecodable here, so the tree is
// moved aside whole — evidence, not garbage — and the deployment comes up at
// the new path as a first install rather than failing its first read.
func TestEnsureMetadataArchivesTheLegacyTree(t *testing.T) {
	f := newFixture(t)
	legacy := plantLegacyDeployment(t, f.root, "Example")

	var activation activationRecord
	if err := readRecord(filepath.Join(legacy, "activation.json"), &activation); !errors.Is(err, ErrState) {
		t.Fatalf("readRecord on a v0.20.10 activation = %v, want %v; this fixture proves nothing", err, ErrState)
	}

	if err := f.deploy.layout.ensureMetadata(); err != nil {
		t.Fatalf("ensureMetadata: %v", err)
	}

	if fileExists(legacy) {
		t.Fatal("the legacy tree is still at the path a cut-era read chokes on")
	}
	archived := legacy + ".bak"
	body, err := os.ReadFile(filepath.Join(archived, "activation.json"))
	if err != nil {
		t.Fatalf("read the archived activation: %v", err)
	}
	if string(body) != legacyActivation {
		t.Fatalf("archived activation = %q, want the planted bytes unread and unaltered", body)
	}
	if !fileExists(filepath.Join(archived, "prior.app", "Contents", "MacOS", "example")) {
		t.Fatal("the archive lost the prior generation the legacy tree stranded")
	}

	entries, err := os.ReadDir(f.deploy.layout.metadata)
	if err != nil {
		t.Fatalf("read the new metadata directory: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("the new metadata directory came up holding %v, want empty", entries)
	}
	if _, err := f.deploy.Install(f.ctx(), f.candidate("Source", "1.0", "one")); err != nil {
		t.Fatalf("Install over an archived legacy tree: %v", err)
	}
	f.wantCanonical("one")
}

// TestArchiveLegacyRotatesPastAPriorArchive holds the archive to the one path a
// downgrade takes. A pre-cut binary run against a state directory this package
// already archived recreates the legacy tree, and the next open here meets its
// own prior archive — nothing on either side of that downgrade ever removes one.
// Refusing there fails ensureMetadata, and with it every operation the
// deployment will ever be asked to do, forever. The name rotates instead, and
// no era's evidence is overwritten by the era that follows it.
func TestArchiveLegacyRotatesPastAPriorArchive(t *testing.T) {
	f := newFixture(t)
	archives := []string{
		f.deploy.layout.legacy + ".bak",
		f.deploy.layout.legacy + ".bak.2",
		f.deploy.layout.legacy + ".bak.3",
	}
	for era := range archives {
		legacy := plantLegacyDeployment(t, f.root, "Example")
		if err := os.WriteFile(filepath.Join(legacy, "era"), []byte(strconv.Itoa(era)), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := f.deploy.layout.ensureMetadata(); err != nil {
			t.Fatalf("era %d: ensureMetadata: %v", era, err)
		}
		if fileExists(legacy) {
			t.Fatalf("era %d: the legacy tree is still at the path a cut-era read chokes on", era)
		}
	}

	for era, archive := range archives {
		body, err := os.ReadFile(filepath.Join(archive, "era"))
		if err != nil {
			t.Fatalf("read the archived era: %v", err)
		}
		if string(body) != strconv.Itoa(era) {
			t.Fatalf("%s holds era %s, want %d: one archive overwrote another", archive, body, era)
		}
		if !fileExists(filepath.Join(archive, "prior.app", "Contents", "MacOS", "example")) {
			t.Fatalf("%s lost the prior generation its legacy tree stranded", archive)
		}
	}

	if _, err := f.deploy.Install(f.ctx(), f.candidate("Source", "1.0", "one")); err != nil {
		t.Fatalf("Install over three archived legacy trees: %v", err)
	}
	f.wantCanonical("one")
}

// TestArchiveLegacyIsOneShotUnderConcurrentOpeners holds the archive to exactly
// one rename. Openers race here — ensureMetadata runs before the deployment
// lock exists to be held — and a second archive of the same tree would strand
// half the evidence under a name nothing ever looks for. Rotation does not
// weaken that: the loser of the race finds the source gone, not a free name.
func TestArchiveLegacyIsOneShotUnderConcurrentOpeners(t *testing.T) {
	tests := []struct {
		name  string
		prior []string
		want  []string
	}{
		{"the first archive", nil, []string{"Example.bak"}},
		{"a prior archive stands", []string{"Example.bak"}, []string{"Example.bak", "Example.bak.2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			for _, name := range tt.prior {
				plantLegacyDeployment(t, f.root, name)
			}
			plantLegacyDeployment(t, f.root, "Example")

			errs := make([]error, 8)
			var wg sync.WaitGroup
			wg.Add(len(errs))
			for i := range errs {
				go func() {
					defer wg.Done()
					errs[i] = f.deploy.layout.ensureMetadata()
				}()
			}
			wg.Wait()
			for i, err := range errs {
				if err != nil {
					t.Fatalf("opener %d: ensureMetadata: %v", i, err)
				}
			}

			entries, err := os.ReadDir(filepath.Join(f.root, legacyMetadataDir))
			if err != nil {
				t.Fatal(err)
			}
			archives := make([]string, 0, len(entries))
			for _, entry := range entries {
				archives = append(archives, entry.Name())
			}
			if !slices.Equal(archives, tt.want) {
				t.Fatalf("the racing openers left %v, want exactly %v", archives, tt.want)
			}
		})
	}
}

// TestArchiveLegacyLeavesTheLoopOnAnUnrotatableFailure holds rotation to the
// one failure a later name can answer. Every other failure has to leave the
// loop: a non-directory at the archive path never becomes free, so retrying it
// spins ensureMetadata forever instead of surfacing the broken state.
func TestArchiveLegacyLeavesTheLoopOnAnUnrotatableFailure(t *testing.T) {
	f := newFixture(t)
	plantLegacyDeployment(t, f.root, "Example")
	if err := os.WriteFile(f.deploy.layout.legacy+".bak", nil, 0o600); err != nil {
		t.Fatal(err)
	}

	if err := f.deploy.layout.ensureMetadata(); !errors.Is(err, syscall.ENOTDIR) {
		t.Fatalf("ensureMetadata over a file at the archive path = %v, want ENOTDIR", err)
	}
}

// TestArchiveLegacyLeavesASiblingProductAlone holds the archive to the one
// deployment doing it. Every product installed in the same directory shares the
// legacy metadata directory, and a sibling an older binary still manages opens
// its deployment lock by path: move that tree and the next pre-cut process
// creates a second lock file for an app another process already holds.
func TestArchiveLegacyLeavesASiblingProductAlone(t *testing.T) {
	f := newFixture(t)
	sibling := plantLegacyDeployment(t, f.root, "Sibling")
	plantLegacyDeployment(t, f.root, "Example")

	if err := f.deploy.layout.ensureMetadata(); err != nil {
		t.Fatalf("ensureMetadata: %v", err)
	}

	if !fileExists(filepath.Join(sibling, "deployment.lock")) {
		t.Fatal("opening one deployment archived another product's live legacy tree")
	}
	if fileExists(sibling + ".bak") {
		t.Fatal("opening one deployment archived another product's tree under a .bak nothing looks for")
	}
}
