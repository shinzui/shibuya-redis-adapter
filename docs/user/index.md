---
okf_version: "0.2"
---

# Files

- [profile.dhall](profile.dhall)

# Guide

- [Resume an ordered projection](resume-a-projection.md) - Persist a reader checkpoint and decide how to handle gaps after a restart or snapshot.
- [Run a recoverable worker pool](run-a-worker-pool.md) - Configure group position, concurrency and lease renewal for competing workers.
- [Connect a Redis stream to a processor](start-a-processor.md) - Choose a delivery mode and run its processor for the lifetime of a Redis runtime.

# Reference

- [Delivery outcomes and trace context](delivery-outcomes.md) - Look up progress, dead-letter and tracing behavior for a reader or worker delivery.
