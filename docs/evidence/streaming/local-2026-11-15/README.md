# Local streaming conformance — November 15, 2026

Actual local Python, SQLite and Apache Beam execution with synthetic USD inputs. This record is not Pub/Sub, Dataflow or native streaming BigQuery acceptance.

- **16 source events, 17 ingest attempts, 16 canonical business events.**
- Independent source reconstruction: **USD 200,600 signed commitments** and **USD 60,180 payments**, kept separate.
- Executed pre-commit rollback, post-commit acknowledgment loss, exact retry, conflicting request rejection and two concurrent approval writers.
- A later payment correction survives replay of older revisions. Original source segments and the corrected-source archive remain separate.
- **66 streaming tests and 195 unchanged application regression tests passed.** The Beam tests execute event-time panes, event/idle timers and exact before/at/after horizon boundaries.

[Execution record](verification.json) · [Regression observations](regression-results.json) · [Original sealed manifest](source/manifest.json) · [Corrected sealed manifest](corrected-source/manifest.json). Segment files are stored alongside each manifest. Databases, credentials and raw runtime logs are excluded.

The execution record identifies its Git base and exact executed module hashes; it does not label uncommitted working-tree bytes as that base commit. The later repository commit contains these same source bytes. The native lane remains unexecuted.
