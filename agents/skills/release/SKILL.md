---
name: release
description: Release shibuya-redis-adapter to Hackage following PVP
argument-hint: "[major|minor|patch]"
disable-model-invocation: true
allowed-tools: Read, Bash, Edit, Glob, Grep, Write, AskUserQuestion
---

# shibuya-redis-adapter Release Skill

Release the single package in this repository, **shibuya-redis-adapter**, to
Hackage. Follow the Haskell PVP.

## Versioning Strategy

- There is one package, so there is one version: the `version:` field of
  `shibuya-redis-adapter.cabal` at the repository root.
- An annotated git tag `v<version>` (e.g. `v0.1.0.0`) marks each release. A
  GitHub release is created for that tag.
- PVP version format `A.B.C.D`:
  - `A.B` is the **major** version. Bump it for breaking API changes: removed or
    renamed exports from `Shibuya.Adapter.Redis`, changed types, changed
    semantics (delivery, ack/retry, dead-letter behavior), or a raised lower
    bound that forces users onto a new major `shibuya-core`.
  - `C` is the **minor** version. Bump it for backwards-compatible API
    additions: new exports, new modules, new instances, new config fields that
    have defaults.
  - `D` is the **patch** version. Bump it for bug fixes, docs, internal-only
    changes (the modules under `other-modules`), and performance work.

## Package

| Package | Directory | Published |
|---------|-----------|-----------|
| shibuya-redis-adapter | `.` (repo root) | yes, the whole package |

All of the package's components ship in the sdist:
- library (`src/`, public module `Shibuya.Adapter.Redis`)
- test-suites `runner-fixture`, `reader-integration`, `group-integration`,
  `release-integration` (`test/`). Each one starts its own `redis-server`,
  using `$REDIS_SERVER` if set and `redis-server` from `PATH` otherwise.
- executables `reader-example`, `worker-example` (`examples/`)

Nothing else in this repository is published. `docs/`, `mori/`, `nix/` and
`agents/` are project infrastructure. `docs/compatibility.md` and
`docs/initial-spec.md` are only included in the sdist as `extra-source-files`.

The upstream dependencies that matter at release time are `shibuya-core`
(currently `^>=0.13.0.1`) and `hedis` (`^>=0.16.4`). Both come from Hackage,
and `flake.module.nix` pins each one with `callHackageDirect`.

## Arguments

`$ARGUMENTS` is optional:
- `major`, `minor`, or `patch` sets the bump level.
- If it is omitted, work out the bump level from the changes (step 2).

## Steps

### 0. First-release prerequisites (check every time)

`cabal check` and Hackage reject packages that lack metadata. Before the first
release, the cabal file had none of the following. Make sure each one exists
and is correct, and add whatever is missing (ask the user for the values):

- `license: BSD-3-Clause` (or whatever the user picks), `license-file: LICENSE`,
  and a `LICENSE` file at the repo root
- `author`, `maintainer`, `copyright`
- `description` (a paragraph) and `category` (e.g. `Concurrency, Streaming`)
- `homepage: https://github.com/shinzui/shibuya-redis-adapter` and
  `bug-reports: https://github.com/shinzui/shibuya-redis-adapter/issues`
- a `source-repository head` stanza (`type: git`,
  `location: https://github.com/shinzui/shibuya-redis-adapter.git`)
- `tested-with: GHC ==9.12.4`
- `extra-doc-files: CHANGELOG.md` (with the file itself, see step 3)
- upper bounds on every `build-depends` entry. Hackage only warns about
  missing ones, but PVP expects them. Bound at least `bytestring`, `text`,
  `async`, `containers`, `effectful-core`, `entropy`, `stm`, and
  `streamly-core` in the library and `shared` stanzas. Use bounds that
  include the versions the current dev shell resolves to
  (`cabal build all --dry-run` or `dist-newstyle/cache/plan.json`).

If you change any of this, show the user the diff in step 3 together with the
version and changelog changes.

### 1. Determine what changed since the last release

- Read the current version from `shibuya-redis-adapter.cabal`.
- Find the latest tag with `git tag --list 'v*' --sort=-v:refname | head -1`.
- If no tag exists, this is the **first release**. The commit range is the
  whole history (`git log --oneline`).
- Otherwise run `git log --oneline <last-tag>..HEAD`. If it prints nothing,
  tell the user there is nothing to release and stop.
- Run `git diff --stat <last-tag>..HEAD -- src test examples shibuya-redis-adapter.cabal`
  to see which changes affect the package. Changes only under `docs/`, `mori/`,
  `nix/`, or `agents/` don't justify a Hackage release on their own. Say so
  if that's all there is.

Present a summary:
- current version
- last release tag (or "none, first release")
- number of commits since the last release
- whether `src/` (the public API) changed, or only tests, examples, or docs

### 2. Determine the next version using PVP

- **First release:** if no `v*` tag exists, use the cabal version as it
  is (currently `0.1.0.0`) unless `$ARGUMENTS` says otherwise.
- If `$ARGUMENTS` is `major`, `minor`, or `patch`, use that level.
- Otherwise read the commits (they follow Conventional Commits) and the diff
  of `src/Shibuya/Adapter/Redis.hs` (the only exposed module):
  - `!` / `BREAKING CHANGE:`, removed or renamed exports, changed types → **major**
  - `feat:` that adds exports or capabilities → **minor**
  - `fix:`, `perf:`, `refactor:`, `docs:`, `test:`, `chore:` → **patch**
- Also check whether a newer `shibuya-core` is on Hackage
  (`cabal list --simple-output shibuya-core` or
  https://hackage.haskell.org/package/shibuya-core). If the user wants to
  move to it, update the `^>=` bound in the `shared` stanza **and** the
  `callHackageDirect` pin (version and sha256) in `flake.module.nix`. Requiring
  a new major `shibuya-core` is a major bump for this package if its types show
  up in this package's API.
- Present the proposed version and your reasoning, and ask the user to confirm
  before going on.

Increment:
- **major**: increment `B`, reset `C` and `D` to 0 (`0.1.2.3` → `0.2.0.0`)
- **minor**: increment `C`, reset `D` to 0 (`0.1.2.3` → `0.1.3.0`)
- **patch**: increment `D` (`0.1.2.3` → `0.1.2.4`)

### 3. Update version and changelog

- Set `version:` in `shibuya-redis-adapter.cabal` to the new version.
- Update `CHANGELOG.md` at the repo root. If it doesn't exist, create it with a
  `# Changelog` header and an `## Unreleased` section. Make sure it is listed
  under `extra-doc-files`.
  - Add a `## <version> — YYYY-MM-DD` section (today's date) above the
    previous entries.
  - Move anything under `## Unreleased` into the new section, and leave an
    empty `## Unreleased` above it.
  - Summarize the commits since the last release, grouped as **Breaking
    Changes** (major only), **New Features**, **Bug Fixes**, and **Other
    Changes**. Leave out empty groups. Describe what users of the
    package will notice, not the internal commit noise.

Show the user ALL changes (metadata, version, bounds, pins, changelog) and get
approval before going on.

### 4. Verify (all gates are mandatory)

Run these inside the dev shell (`direnv` loads it; otherwise prefix with
`nix develop -c`):

1. `nix fmt`: treefmt (fourmolu, cabal-gild, nixpkgs-fmt). Afterwards, check
   `git status` for files it reformatted.
2. `cabal build all`: library, tests, and examples.
3. `cabal test all`: the four Redis integration suites. Each one starts its own
   `redis-server`, so no shared Redis needs to be running.
4. `cabal check`: there must be no errors. Fix any warnings or explain to the
   user why they are acceptable.
5. `git add` any new files (`CHANGELOG.md`, `LICENSE`, …). Nix only sees files
   in the git tree.
6. `nix flake check`: the treefmt and pre-commit checks.
7. `nix build`: builds `packages.default`, which runs the test-suites in the
   sandbox with `redis` as a test tool (see `flake.module.nix`).

If any gate fails, stop, report it, and fix it before going on. Don't
work around a gate.

### 5. Commit, tag, and push

- Stage the cabal file, `CHANGELOG.md`, and any other files changed in steps 0–4
  (`LICENSE`, `flake.module.nix`, `flake.lock`, formatting fixes).
- Commit with Conventional Commits: `chore(release): <new-version>`. In the
  body, summarize the release and why this bump level was chosen.
- Create an annotated tag:
  `git tag -a v<version> -m "Release shibuya-redis-adapter <version>"`
- Push: `git push && git push origin v<version>`

### 6. Publish to Hackage

From the repo root:

1. `cabal check` (again, on the committed tree).
2. `cabal sdist`. It writes `dist-newstyle/sdist/shibuya-redis-adapter-<version>.tar.gz`.
3. Optional but recommended: run
   `cabal upload dist-newstyle/sdist/shibuya-redis-adapter-<version>.tar.gz`
   (without `--publish`) to upload a **candidate**. Check the candidate page
   on Hackage, then go on.
4. `cabal upload --publish dist-newstyle/sdist/shibuya-redis-adapter-<version>.tar.gz`
5. `cabal haddock --haddock-for-hackage --haddock-hyperlink-source --haddock-quickjump`
6. `cabal upload --publish --documentation dist-newstyle/shibuya-redis-adapter-<version>-docs.tar.gz`
7. Report https://hackage.haskell.org/package/shibuya-redis-adapter-<version>

A Hackage publish can't be undone. Before step 4, ask the user for one
final confirmation.

### 7. Create the GitHub release

```bash
gh release create v<version> --title "v<version>" --notes "$(cat <<'EOF'
## Hackage

https://hackage.haskell.org/package/shibuya-redis-adapter-<version>

## What's Changed

<the CHANGELOG.md section for this version>
EOF
)"
```

Report the GitHub release URL.

## Important

- Always ask the user to confirm the version bump, metadata, and changelog
  before committing, and confirm again before the irreversible
  `cabal upload --publish`.
- Never skip a gate: `nix fmt`, `cabal build all`, `cabal test all`,
  `cabal check`, `nix flake check`, `nix build`.
- If any step fails, stop and report the error instead of continuing.
- If the sdist upload fails, don't upload docs, and don't create the GitHub
  release. If the docs upload fails after the sdist was published, retry the
  docs upload only, because the version can't be republished.
- Keep the `shibuya-core` / `hedis` cabal bounds and the `flake.module.nix`
  `callHackageDirect` pins in sync.
- Create the commit and tag only AFTER the user approves all changes.
- Don't create a branch. Release from `master`.
