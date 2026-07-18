# Templates

Render-time sources for a daemonkit signed helper app: an XcodeGen project, its
entitlements, the per-build version generator, and the Homebrew cask. Nothing
here is consumed at Go build time — repo-bootstrap's daemonkit layer substitutes
the `__PLACEHOLDER__` tokens once at scaffold time, and the release workflow
fills the per-release values (`__VERSION__`, `__SHA_APP__`) in the cask.

## What's here

| File | Renders to | Contract |
|---|---|---|
| `project.yml.tmpl` | the consumer's `project.yml` | Host app + WidgetKit appex + hostless test bundle. Version keys are `$(MARKETING_VERSION)` / `$(CURRENT_PROJECT_VERSION)` references, never literals; signing defaults to Manual with the ad-hoc identity so unsigned local builds work, and release CI overrides identity, team, and profile on the command line. |
| `gen-version-xcconfig.sh` | `Version.xcconfig` (gitignored) | Run BEFORE `xcodegen generate`. Stamps the marketing version plus an epoch-nanos build number, unique and monotonic per build — no checked-in build number, ever. |
| `entitlements/app.entitlements.tmpl` | `<App>.entitlements` | Non-sandboxed host app. The App Group is its only entitlement — the profile-authorized claim that makes the first group-container access a silent TCC grant. |
| `entitlements/widget.entitlements.tmpl` | `<App>Widget.entitlements` | Sandboxed WidgetKit appex (macOS requires it): App Group plus a home-relative read-only temporary exception into the app's dotdir. |
| `entitlements/appex-shared.entitlements.tmpl` | rendered on demand | File-Provider-style shared-container appex: App Group plus a home-relative read-write exception, resolved against the real home (`getpwuid`, not the container's `NSHomeDirectory`). Not wired into the default project. |
| `cask.rb.tmpl` | the tap's cask | Full `.app` bundle (never a bare `binary` — only a bundle staples and keeps bundle-keyed TCC identity). Postflight strips quarantine and restarts the resident process; uninstall boots the supervisor out before quitting the app. |

## Render order

1. `gen-version-xcconfig.sh <marketing-version>` — writes `Version.xcconfig`.
2. `xcodegen generate` — emits `<App>.xcodeproj` and the `Generated/` Info.plists.
3. `xcodebuild` — Release builds sign inward-out per target with the named
   provisioning profiles; the reusable `release-app.yml` workflow in
   yasyf/homebrew-tap drives this and asserts the final entitlements.

Two invariants carry the whole design. Version values flow xcconfig, then build
settings, then `$(…)` plist references — setting `MARKETING_VERSION` or
`CURRENT_PROJECT_VERSION` in any `settings:` block silently overrides the
generated file, and WidgetKit's chronod caches widget metadata keyed by the
bundle version it would mask. And entitlement identity is byte-literal: every
target sharing state carries the identical `$(TeamIdentifierPrefix)<group>`
string, which release CI diffs across targets.
