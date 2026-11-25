---
okf_version: "0.2"
---

# Files

- [profile.dhall](profile.dhall)

# Friction Entry

- [Default Nix package set selects an incompatible Shibuya](1-default-nix-package-set-selects-an-incompatible-shibuya.md) - The generated root package selected a broken Shibuya release, requiring dependency wiring for the production build.
- [Haskell OTLP exporter appends the signal path to the trace endpoint](2-haskell-otlp-exporter-appends-the-signal-path-to-the-trace-endpoint.md) - A full per-signal OTLP URL produced no traces in Jaeger until the URL was changed to a base prefix.
