# Bundle Update Log

## 2026-11-25

* **Update**: FRIC-2 accepted: Document the working endpoint and validate exported traces through the global Jaeger API.


* **Addition**: FRIC-2 records detour interface friction with mori://iand675/hs-opentelemetry/packages/hs-opentelemetry-exporter-otlp: Haskell OTLP exporter appends the signal path to the trace endpoint


* **Update**: FRIC-1 resolved: Root package builds using the managed flake and a project-specific dependency overlay.


* **Addition**: FRIC-1 records detour tooling friction with mori://shinzui/seihou-modules/seihou/modules/nix-haskell-flake: Default Nix package set selects an incompatible Shibuya

* **Adoption**: Adopt the `friction-log` profile (`coordination.frictionLog`) from https://raw.githubusercontent.com/shinzui/okf-profiles/v0.23.0/package.dhall.
