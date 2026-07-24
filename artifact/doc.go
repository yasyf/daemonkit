// Package artifact resolves a version-exact executable from a declarative
// descriptor, for the cc-family's one central "give me the binary that matches
// my version" primitive.
//
// A descriptor is an executable file — a "#!/usr/bin/env binrun" shebang line
// followed by a JSON body in the dotslash dialect — carrying schema 1, a name, a
// kind, a version source, and the per-platform facts needed to materialize the
// artifact. The runner is generic and rarely changes; every fast-moving version
// fact lives in the descriptor shipped beside the thing that pins it.
//
// # Kinds
//
// ReleaseBinary resolves a github-release asset pinned by exact tag and sha256.
// It downloads with hash-while-streaming, verifies size and digest before any
// rename, and stores the result in a content-addressed cache under
// Store.CacheDir as cache/<first-two-hex>/<digest>/<path>; the verified rename is
// the receipt, and a sibling meta.json records provenance for gc. A second
// resolution is a stat-and-return cache hit.
//
// PythonTool materializes a PyPI distribution into a version-addressed store
// under Store.ToolsDir via "uv tool install <dist>==<version>" with a redirected
// UV_TOOL_DIR and UV_TOOL_BIN_DIR, then returns the real console-script
// entrypoint inside the environment. uv enforces PyPI hashes, so no descriptor
// digest is carried. A second resolution returns the existing environment
// offline.
//
// SignedApp delegates to the deployment package. For a caller-managed directory
// with a WithSignedAppDeploy base config it publishes the exact signed app
// through deployment.Controller (this package never reimplements that workflow).
// For a TCC-bound install such as /Applications it is attest-only: it verifies
// the app exists and — for a static descriptor — that its version matches, and
// otherwise returns a ManualUpgradeError the caller renders as a
// "brew upgrade --cask" handoff.
//
// # Version source and the supply-chain rule
//
// A VersionSource is either a baked Static version (rendered at descriptor build
// time, with full commit-time platform digests) or a dynamic Command whose JSON
// stdout carries the version under JSONField (host authority; no baked digest).
// A dynamic version is valid only for PythonTool and SignedApp, where an
// independent integrity gate exists — uv's PyPI hashes and codesign's designated
// requirement respectively. A dynamic ReleaseBinary is rejected by Validate with
// ErrDynamicIntegrity, because no such gate exists for a bare downloaded binary.
//
// # Resolution is exact
//
// Resolve and Fetch pin the exact descriptor version and never consult a
// repository's latest release. Self-update flows compose ghrelease.Latest with a
// fetch and a descriptor flip; that path lives in the consumer, never inside
// Resolve. Per-artifact materialization is serialized by a proc file lock held
// for the whole download-verify-rename or uv-install, so concurrent callers on
// one host converge on a single copy.
//
// # Exit codes
//
// Every function returns an error; the package never calls os.Exit. A runner
// maps any error to exit 1, keeping exit 2 reserved for a real hook verdict.
package artifact
