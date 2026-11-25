---
id: 9
slug: package-and-qualify-the-0-1-0-0-release
title: "Package and qualify the 0.1.0.0 release"
kind: exec-plan
created_at: 2026-11-25T13:50:20Z
intention: "intention_01m89fh99nex0r6hchenpa2c1b"
master_plan: "docs/masterplans/2-harden-the-redis-adapter-for-a-bug-free-first-release.md"
provenance:
  created_by:
    model: "claude-fable-5-2"
    harness: "claude-code"
    at: 2026-11-25T13:50:20Z
---

# Package and qualify the 0.1.0.0 release

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.
If durable project context changes, update or create ADRs in docs/adr/ in the same change.
Record friction met while implementing in the friction-log bundle as it occurs (see FRICTION.md).


## Purpose / Big Picture


This plan makes the package acceptable to Hackage and produces the evidence that version 0.1.0.0 works. After it, `cabal check` reports no errors, dependency ranges follow the Shibuya project's rules, example programs are not forced on downstream users, the user documentation matches the code, and every test suite has passed on both supported Redis versions from a clean source archive. It does not upload or tag anything; the owner does that afterwards.


## Progress


- [ ] M1. `cabal check` passes: license and `LICENSE` file, changelog, metadata, source repository, and upper bounds on every dependency including the required `effectful-core` range.
- [ ] M2. The two example programs build only when a Cabal flag is on; the README's commands still work in the development shell.
- [ ] M3. `README.md` and `docs/compatibility.md` describe the final behavior, permissions and telemetry.
- [ ] M4. All suites pass on Redis 7.2.17 and 8.10.1; the unpacked source archive builds and its Haddock generates; `nix flake check` passes; results are recorded in `docs/compatibility.md`.


## Surprises & Discoveries


(None yet.)


## Decision Log


- Decision: Put the examples behind a manual Cabal flag instead of a separate package.
  Rationale: Two small programs do not justify a second package and release. A flag keeps them out of downstream builds, which is the actual problem.
  Date: 2026-11-25

- Decision: Do not upload to Hackage or create a tag in this plan.
  Rationale: Publication cannot be undone and is the owner's call. The repository's `agents/skills/release` skill covers it.
  Date: 2026-11-25


## Outcomes & Retrospective


(To be filled during and after implementation.)


## Context and Orientation


This repository is a Haskell library, `shibuya-redis-adapter`, version 0.1.0.0, not yet published. Its package description is `shibuya-redis-adapter.cabal`. Running `cabal check` today reports one error, "The 'license' field is missing or is NONE", which means Hackage would reject the upload, plus warnings: no `source-repository`, `category`, `maintainer` or `description`, and no upper bounds on `bytestring`, `text`, `async`, `containers`, `effectful-core`, `entropy`, `stm`, `streamly-core`, `directory`, `filepath` and `temporary`. There is no `LICENSE` or `CHANGELOG.md`.

The Cabal file defines a library, four integration test suites (`runner-fixture`, `reader-integration`, `group-integration`, `release-integration`; earlier plans in this initiative may have added `telemetry-integration`) and two executables, `reader-example` and `worker-example`, built from `examples/`. Executables in a library package are built for anyone who runs `cabal install` on it and add their dependencies to its build plan.

Rules inherited from the Shibuya project (`mori://shinzui/shibuya`; artifact URIs pending):

- `docs/adr/0005-exclude-regressed-effectful-core-releases-in-every-package.md` requires every component of every Shibuya package to declare exactly `effectful-core (>=2.6.1 && <2.7) || (>=2.7.1.1 && <2.8)`.
- The sibling adapters are the template for metadata. See `shibuya-kafka-adapter/shibuya-kafka-adapter.cabal` in `mori://shinzui/shibuya-kafka-adapter` and `shibuya-pgmq-adapter/shibuya-pgmq-adapter.cabal` in `mori://shinzui/shibuya-pgmq-adapter`: MIT license with a `LICENSE` file, `author`, `maintainer`, `copyright`, `homepage`, `bug-reports`, `category`, `tested-with`, `extra-doc-files: CHANGELOG.md README.md`, a `source-repository head` stanza, and caret bounds on every dependency.

The development environment is a Nix flake. `nix develop` provides GHC 9.12.4, Cabal and Redis 8.10.1. `flake.module.nix` pins `shibuya-core` 0.13.0.1 and `hedis` 0.16.4 from Hackage. The test fixture (`test/Fixture.hs`) starts `redis-server` from the `PATH`, or the executable named by the `REDIS_SERVER` environment variable. `docs/compatibility.md` records the qualified versions and build IDs, a scenario table, the Redis permissions each mode needs, and runtime and recovery behavior. `README.md` has the quick start and an integration overview. The process-compose file starts a local Redis for the examples.

No local ADR governs packaging. This plan is a child of [the hardening MasterPlan](../masterplans/2-harden-the-redis-adapter-for-a-bug-free-first-release.md) and must start only after [plan 8](8-narrow-name-and-document-the-public-api.md) is Complete, because its evidence must describe the final code. By then, plans 5 to 8 will have changed retry behavior, added `SCRIPT LOAD` and `EVALSHA` to the worker's required commands, added tracing, and split out an `Internal` module.


## Plan of Work


**M1, package description.** Confirm the license with the owner if anything suggests it should differ from the sibling adapters' MIT; otherwise add `license: MIT`, `license-file: LICENSE` and the `LICENSE` text matching the siblings. Add `description`, `author`, `maintainer`, `copyright`, `homepage`, `bug-reports` (the repository is `shinzui/shibuya-redis-adapter` on GitHub, per `mori.dhall`), `category: Concurrency, Database`, `tested-with: ghc ==9.12.4` and a `source-repository head` stanza. Create `CHANGELOG.md` with a 0.1.0.0 entry describing the two delivery modes, dead-lettering, tracing and the qualified versions; list it and `README.md` under `extra-doc-files` and keep the `docs/` files under `extra-source-files`. Give every dependency in every component a lower and upper bound. For each, look up the latest release on Hackage and the version the development shell resolves (`cabal freeze` in the shell shows it), and choose a caret range that includes the tested version. Use the exact `effectful-core` range above in every component. Tighten `base` to the range tested. Update `mori.dhall`'s dependency list if dependencies were added by earlier plans.

**M2, examples.** Add a flag:

```cabal
flag examples
  description: Build the reader and worker example programs.
  default: False
  manual: True
```

Set `buildable: False` on both executables unless the flag is on. In `cabal.project`, enable the flag for local development so `cabal run reader-example` in the README keeps working, and make sure the Nix build still compiles the examples (pass the flag in `flake.module.nix` if needed) so they cannot rot.

**M3, documentation.** Read `README.md` and `docs/compatibility.md` against the code and fix every statement that is no longer true. In particular: a retried group entry is redelivered after its delay, including to the same worker; retry exhaustion dead-letters when a destination is configured and stops the processor otherwise; the worker permissions include `SCRIPT LOAD` and `EVALSHA`; telemetry is configured per mode with the stated defaults and producers write `traceparent` as a stream field; the qualified dependency versions. Update the `mori.dhall` lifecycle only if the owner asks.

**M4, qualification.** Run the whole matrix and record the results. The development shell provides Redis 8.10.1. For 7.2.17, obtain a `redis-server` binary of that version and pass its absolute path in `REDIS_SERVER`; the earlier qualification did not record how its binary was obtained, so record the method this time in `docs/compatibility.md`. Capture each server's version, source revision and build ID from `redis-server --version` and update the table. Then build from the source archive in a scratch directory to prove the archive is complete.


## Concrete Steps


From the repository root:

```bash
nix develop -c cabal check
```

Expected at the end of M1:

```text
No errors or warnings could be found in the package.
```

Qualification:

```bash
nix develop -c cabal build all
nix develop -c cabal test all --test-show-details=direct
nix develop -c env REDIS_SERVER=/absolute/path/to/redis-7.2.17/bin/redis-server cabal test all --test-show-details=direct
nix develop -c cabal haddock shibuya-redis-adapter
nix flake check
```

Source archive:

```bash
nix develop -c cabal sdist
mkdir -p .dev/sdist-check && tar -xzf dist-newstyle/sdist/shibuya-redis-adapter-0.1.0.0.tar.gz -C .dev/sdist-check
cd .dev/sdist-check/shibuya-redis-adapter-0.1.0.0 && nix develop ../../.. -c cabal build all -fexamples
```

Examples, with the local Redis started as the README describes (`process-compose -U up -t=false` in another terminal):

```bash
stream="shibuya-demo-$(date +%s)"
redis-cli -s "$REDIS_SOCKET" XADD "$stream" 1-0 item first
cabal run reader-example -- "$REDIS_SOCKET" "$stream" ".dev/${stream}.cursor" 1
cabal run worker-example -- "$REDIS_SOCKET" "$stream" thumbnails earliest 1
```

Each prints `1-0` followed by the message ID and exits zero.


## Validation and Acceptance


Accepted when: `cabal check` reports no errors or warnings; a default build of the package (flag off) does not build the examples and a build with `-fexamples` does; every suite passes on both Redis versions; the unpacked archive builds and its Haddock generates; `nix flake check` passes; both examples run; and `docs/compatibility.md` shows the dated results, versions and build IDs. If any suite fails on either server, the failure is a defect to fix under the plan that owns that behavior, not something to waive here.


## Idempotence and Recovery


All commands can be rerun. The archive check extracts into `.dev/`, which is ignored by Git; delete `.dev/sdist-check` to repeat it. Nothing in this plan publishes, pushes or tags.


## Interfaces and Dependencies


Final dependency ranges for the library, tests and examples are defined here; `effectful-core` uses the cohort range from the Shibuya ADR named above. The package gains the `examples` flag. After this plan is Complete the owner can run the release skill at `agents/skills/release/SKILL.md` to tag and upload.
