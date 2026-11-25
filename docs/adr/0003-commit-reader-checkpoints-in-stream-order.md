# Commit reader checkpoints in stream order

Accepted 2026-11-25.

Each reader has its own completed checkpoint. Its bounded read buffer can hold later entries while the source waits for the head to finish. A retry delivers the same head with its local attempt incremented. A configured dead-letter hash receives a record before the checkpoint moves. Replaying a rejection after a failed save reuses the same hash field.

The application owns checkpoint persistence and uses the reader name and complete source identity as its storage scope. It supplies a single writer or a conditional store that accepts an already committed identical value after an uncertain reply. The checkpoint carries the cursor and optional producer continuity evidence together. Live startup resolves the tail once and saves that concrete boundary before intake.

The reader detects cursor rollback using the last-generated ID Redis reports. Applications that need to detect missing entries or a new generation with larger IDs should validate producer evidence. A gap report carries its source, checkpoint and evidence. Stop and resnapshot both end the processor and preserve the checkpoint for application recovery. Explicit best effort can reset a rolled-back cursor or accept a discontinuous successor.
