## Swift Style

Swift 6 language mode. The Swift half is an SPM **library** (`Sources/DaemonKit/`,
manifest at the repo root — never tidy it into `Sources/`); build with
`swift build`, test with `swift test`. Full rules: `Sources/STYLEGUIDE.md`.

**Doc comments on the public API only.** Public types and functions carry a `///`
summary; internals get none. No other comments except TODOs, non-obvious
workarounds, or disabled code.

**Typed errors, thrown.** Failures are `Error`-conforming enums thrown up the
stack — no sentinel returns, no `fatalError` for recoverable conditions.

**XcodeBuildMCP.** If using XcodeBuildMCP, use the installed `xcodebuildmcp-cli`
skill before calling XcodeBuildMCP tools.
