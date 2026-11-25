# Keep Redis command connections available during runner drain

Accepted 2026-11-24.

Shibuya calls adapter shutdown before waiting for admitted handlers and finalizers. A blocking stream read and a finalizer can therefore overlap with shutdown. Give each runtime a dedicated read pool with one connection and an independent command pool. The command pool can be borrowed from the application for the same Redis primary and database. Its owner must reserve command capacity and keep it available for the lifetime of the runner.

Bracket the entire application run, including its graceful or forced shutdown, with withRedisRuntime. stopIntake wakes outstanding polls and prevents subsequent polls. It can be called repeatedly. Existing finalizers may continue to run commands until the application bracket returns. Release adapter-owned pools on normal exit, failure or cancellation. The application owns the lifetime of borrowed pools and of telemetry resources.
