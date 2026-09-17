# Task logs

The task details, history and host links for `go-process` tasks open retained
stdout/stderr in the scheduler UI. The viewer supports separate streams, 64 KiB
pages, refresh, restart from byte zero, empty output and unavailable-log errors.
It renders output as literal text. It does not require a Thermos observer.

The read API is:

```text
GET /tasklogs/{taskId}/{stdout|stderr}?offset=0&limit=65536
```

Responses contain `taskId`, `stream`, `offset`, `nextOffset`, `hasMore`,
`truncated`, `complete` and `data`. Offsets count source bytes. `hasMore` describes
remaining retained bytes; `complete` means the process is terminal and cleanup
has completed. `truncated` reports dropped bytes confirmed when execution ends.
A running stream may already have reached its cap before that diagnostic is final.
Refresh checks the current page; Next follows the returned offset. The browser
keeps one page in memory. Text pages preserve UTF-8 boundaries when possible;
invalid UTF-8, deliberately unaligned offsets and byte limits too small for a
character use replacement characters. This is a text viewer, not binary download.

Each stream retains the beginning of its output, up to the agent's `--log-bytes`
limit (default 1 MiB; configured range 1 KiB–16 MiB). The endpoint accepts a page
limit of 1–65536 and offsets from zero through 16 MiB. Completed logs remain
available across daemon and scheduler restarts while their files and enrolled
journal are retained. There is no age-based log deletion in this slice. Missing
or never-created logs return 404; unavailable agents return 503.

The scheduler resolves the durable Run command to its enrolled node and immutable
attempt identity, releases the storage transaction, then requests a bounded page
over mutual TLS with current session/epoch authority. Callers cannot supply an
agent address or filesystem path. The agent rejects links and nonregular log
files. Both endpoints use bounded concurrency and disable response caching.

The scheduler endpoint uses the existing read-API access policy. The current Pi
lab is a trusted network deployment; this feature does not introduce per-job log
permissions or workload isolation. Keep the same runtime work root when using an
offline compacted journal so retained log paths remain available.
