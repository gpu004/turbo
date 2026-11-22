# Architecture Decision Records

Non-obvious architectural and compatibility decisions extracted from code comments live here.

| ADR | Decision |
| --- | --- |
| [0001](0001-r2-commit-pacing-and-retry.md) | R2 commit pacing, retryable error codes, lease floor, and ambiguous write recovery |
| [0002](0002-group-commit-batching-and-capacity-shedding.md) | Group-commit batching, isolate memory backpressure, and capacity shedding |
| [0003](0003-stored-queue-file-compatibility.md) | Stored `QueueFile` format and rolling-upgrade compatibility |
| [0004](0004-rpc-boundary-and-lease-redaction.md) | Worker-to-Durable-Object RPC boundary and type-level lease token redaction |
| [0005](0005-oxlint-global-detection.md) | Lexical-scope global detection in custom Oxlint rules |
