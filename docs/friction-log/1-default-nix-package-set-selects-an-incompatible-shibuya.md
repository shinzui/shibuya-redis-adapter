---
type: Friction Entry
title: "Default Nix package set selects an incompatible Shibuya"
description: "The generated root package selected a broken Shibuya release, requiring dependency wiring for the production build."
generated:
  by: "codex-cli/gpt-6.2-astra"
  at: "2026-11-25T05:54:36Z"
frictionId: FRIC-1
status: "resolved"
impact: detour
kind: tooling
origin: "mori://shinzui/shibuya-redis-adapter"
affects: "mori://shinzui/seihou-modules/seihou/modules/nix-haskell-flake"
plan: "docs/plans/1-bootstrap-a-buildable-adapter-and-redis-runner-fixture.md"
workaround: "Use the project extension point to compose the shared Haskell package registry."
resolution: "The unmanaged project overlay selects the released Shibuya and hedis libraries and the resource-pool version required by hedis. The root derivation runs the adapter fixture."
amended:
  - by: "codex-cli/gpt-6.2-astra"
    at: "2026-11-25T05:58:20Z"
    change: "Root package builds using the managed flake and a project-specific dependency overlay."
---
# Default Nix package set selects an incompatible Shibuya

## Attempted

Evaluate the root package with nix build --dry-run.

## Encountered

Nixpkgs selected shibuya-core-0.3.0.0 while Cabal built the required 0.13.0.1.

## Workaround

Use the project extension point to compose the shared Haskell package registry.
