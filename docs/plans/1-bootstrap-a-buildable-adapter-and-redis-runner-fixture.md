---
id: 1
slug: bootstrap-a-buildable-adapter-and-redis-runner-fixture
title: "Bootstrap a buildable adapter and Redis runner fixture"
kind: exec-plan
created_at: 2026-11-25T05:44:26Z
master_plan: "docs/masterplans/1-build-the-shibuya-redis-streams-adapter.md"
provenance:
  created_by:
    model: "gpt-6.2-astra"
    harness: "codex-cli"
    at: 2026-11-25T05:44:26Z
  revisions:
    - model: "gpt-6.2-astra"
      harness: "codex-cli"
      at: 2026-11-25T05:54:57Z
      mode: "implement"
      note: "Implement the package, connection lifecycle and runner fixture."
---

# Bootstrap a buildable adapter and Redis runner fixture

This ExecPlan is a living document. The sections Progress, Surprises & Discoveries,
Decision Log, and Outcomes & Retrospective must be kept up to date as work proceeds.
If durable project context changes, update or create ADRs in docs/adr/ in the same change.
Record friction met while implementing in the friction-log bundle as it occurs (see FRICTION.md).


## Purpose / Big Picture


A contributor can enter a reproducible shell, start an isolated Redis server and run a Shibuya handler that observes the exact bytes appended to a stream. Both delivery modes will use the same envelope and resource lifetime.


## Progress


- [x] 2026-11-24 — The generated shell built the library with GHC 9.12.4 and started Redis 8.10.1 on a private Unix socket.
- [x] 2026-11-24 — The runner-fixture suite passed against released shibuya-core 0.13.0.1 and hedis 0.16.4. It verified binary field fidelity, source identity, duplicate-safe rejection after an interrupted finalizer response, configured telemetry, partial acquisition cleanup, poll cancellation and command availability through drain.


## Surprises & Discoveries


The generated flake’s bare package set selects shibuya-core 0.3.0.0. Cabal resolves the released contract directly. The Nix build needed an unmanaged overlay for the same Shibuya and hedis releases and the newer resource-pool dependency. On case-insensitive Git worktrees the generated redis directory exclusion also matches the Redis Haskell module directory. An explicit include keeps its sources in the flake.





## Decision Log


2026-11-24 — Give blocking reads a dedicated pool even when a caller supplies the command pool. Keep commands alive until the runner has drained. Encode binary identity components with lengths and scope the rejection hash by logical reader or group. See the [lifetime decision](../adr/0002-keep-command-connections-through-runner-drain.md) and [storage decision](../adr/0001-stream-identity-and-rejection-records.md).





## Outcomes & Retrospective


The library and runner fixture build from the repository root. The fixture waits for Redis to report a blocked client before cancelling the poll, and holds a finalizer until intake closes. Hiding the reply to the first rejection write makes Shibuya invoke the finalizer again. Redis retains one record. The public reader and group constructors belong to subsequent plans. [FRIC-1](../friction-log/1-default-nix-package-set-selects-an-incompatible-shibuya.md) was resolved through the project overlay. Root Nix builds run the adapter suite. They omit upstream Shibuya and hedis test suites, which require a different QuickCheck range and multi-server fixtures respectively.





## Context and Orientation


The repository starts with Seihou planning tools and [the initial specification](../initial-spec.md). There is no Cabal package or flake yet. Resolve the module with `mori://shinzui/seihou-modules/seihou/modules/nix-haskell-flake`. It supplies a managed flake, lock, shell tools and optional Redis process. Its unmanaged flake.module.nix is the place for local extensions. Use the released contracts behind `mori://shinzui/shibuya/packages/shibuya-core`, `mori://informatikr/hedis/packages/hedis` and `mori://shinzui/shibuya/docs/architecture`. The runner owns handler scheduling, admission and repeated finalization on transient exceptions. The adapter supplies a Streamly stream in Eff, an AckHandle per delivery, shutdown and AdapterTelemetry. A superseded item can skip both handler and finalizer. Release resources for it even when neither runs.

Mori indexes the architecture guide but has no decision bundle for `mori://shinzui/shibuya`. Artifact-level decision URIs are pending. In that project, the relevant records are `docs/adr/0003-make-processor-termination-and-shutdown-outcomes-explicit.md`, `docs/adr/0010-pause-processors-at-the-adapter-source-and-report-pause-as-operator-intent.md`, `docs/adr/0012-keep-telemetry-ownership-and-disabled-context-contracts-explicit.md` and `docs/adr/0013-let-adapters-supersede-a-delivery-before-its-handler-runs.md`. They distinguish deliberate halt from failed finalization, gate intake before source pulls, preserve caller telemetry ownership and give the adapter a prompt, side-effect-free admission check. There are no local ADRs at creation.


## Plan of Work


First generate the shell, root shibuya-redis-adapter.cabal and cabal.project. Use the module's root package support. Follow `mori://shinzui/haskell-jitsurei/docs/core-standards` and `mori://shinzui/haskell-jitsurei/docs/core-record-patterns`: a common GHC2024 stanza on GHC 9.12 or newer, DeriveAnyClass, DuplicateRecordFields, OverloadedLabels and OverloadedStrings, explicit deriving strategies, postpositive qualified imports and generic-lens labels on public records. Before setting any dependency bound, compare the current Hackage versions and revisions with upstream release tags and compile the released source. Record compiler, solver choices and commands in the fixture evidence.

Next build src/Shibuya/Adapter/Redis.hs and internal Types, Connection and DeadLetter modules under src/Shibuya/Adapter/Redis/. Define a raw entry with binary key, numeric millisecond/sequence ID and an ordered list of binary pairs. Encode a versioned, length-prefixed tuple of namespace, source generation when supplied, stream and entry ID as printable MessageId. Omit unknown producer time and attempt. An opt-in decoder/header mapping must keep the original entry for failure routing. Supply a common rejection primitive keyed by reader-or-group tag, logical identity, source generation, stream and entry. A Redis hash field per rejection can hold the record itself: a single HSETNX makes repeat writes converge; expose the destination representation and its retention contract. Provide the equivalent operation to the group's fenced script. Store separate reason code and bounded detail.

Make acquisition exception-safe. Reserve separate blocking-read and command capacity, with distinct owned or borrowed handles and validation of usable limits. Bound command waits, polling, intake and reconnect backoff. Expose validated configuration or processor construction helpers that ensure runner settings and mode guarantees agree. Disabling telemetry leaves caller resources alone. Keep a bounded local event vocabulary and a test observer for the later mode counters. In test/Fixture.hs and test/Main.hs, build a private one-entry source via hedis, mkIngested and an observable AckHandle. Exercise under runApp. Adapter shutdown is called before the runner finishes draining. Provide a bracket around the whole app lifetime so command resources outlive admitted callbacks and are closed even after forced cancellation. Exercise duplicate shutdown and partial acquisition.


## Concrete Steps


From the repository root inspect the installed module variables, preview and then apply the same configuration. Persist the chosen values alongside the existing .seihou/config.dhall keys. Answer unused service prompts with false. Stage newly generated flake inputs for local Git-backed Nix evaluation.

```bash
seihou vars nix-haskell-flake
seihou run nix-haskell-flake --var project.name=shibuya-redis-adapter --var 'project.description=Redis Streams adapter for Shibuya' --var nix.redis=true --var nix.process-compose=true --var nix.postgresql=false --dry-run
seihou run nix-haskell-flake --var project.name=shibuya-redis-adapter --var 'project.description=Redis Streams adapter for Shibuya' --var nix.redis=true --var nix.process-compose=true --var nix.postgresql=false
nix develop -c cabal build all
nix develop -c cabal test runner-fixture --test-show-details=direct
```

Have the test fixture spawn redis-server on a fresh short temporary socket, wait for PING and bracket its process. Keep the generated process-compose Redis service available for interactive examples.


## Validation and Acceptance


Observe PONG, then one runner delivery with duplicate field names, non-UTF-8 bytes and the expected MessageId. Check distinct tuples cannot collide at component boundaries. Re-run a rejection after hiding the first successful reply and inspect one record. Cancel a blocked poll, drain an admitted finalizer and use the borrowed pool for a command after shutdown. Accept only with bounded completion and clean fixture exit.


## Idempotence and Recovery


Preview regeneration against the existing Seihou manifest and preserve local files on conflicts. Use fresh private servers for tests. Repeated shutdown first closes intake, then the owning bracket releases pools once. Surface malformed replies and access failures separately from normal blocking timeouts.


## Interfaces and Dependencies


No prerequisite. The downstream contracts are the Redis entry/envelope conversion, rejection storage format, connection bracket, adapter observer and a real-runner fixture. Avoid a public fixture adapter. Each public delivery constructor is added by its owning mode.
