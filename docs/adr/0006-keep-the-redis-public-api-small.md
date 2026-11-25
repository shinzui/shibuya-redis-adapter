# Keep the Redis public API small

Accepted 2026-11-25.

`Shibuya.Adapter.Redis` is the application API. It exposes configuration, runtime and processor construction, Redis entries and identity, observability, and the rejection encoder that applications can use when managing a dead-letter hash. Package versioning covers this facade.

`Shibuya.Adapter.Redis.Internal` exposes implementation helpers used by tests and advanced integrations. Its exports can change in any release. Redis command and polling plumbing, envelope conversion, the Lua body and attempt key framing live behind that boundary.

A runtime reserves one processor attachment after option validation and before initialization touches Redis or checkpoint state. Failed initialization releases the reservation. The successful attachment lasts through the bracket because the reader connection and intake flag belong to that processor. The command connection lifetime still follows [ADR 2](0002-keep-command-connections-through-runner-drain.md).
