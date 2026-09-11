# JAVA-10: measured concurrency and HTTP decision

JAVA-10 is complete as a design and measurement decision. Retain the serialized
controller, bounded two-worker HTTP executor and current HTTPS transport for the
existing two-agent MVP. No concurrent or alternative-transport prototype was
adopted. The study exposes slow-agent latency and burst failures; it does not
establish an adoption benefit for an unimplemented candidate or qualify a higher
operator load. That is a deliberate limit of this decision, not a claim that
concurrency cannot help.

[JAVA-08](JAVA08_STATUS.md) first qualified lifecycle ownership at source
`e446dda4284ef5b99bf4f5843bfcfc269634b81f`. The
[evidence ledger](java08-evidence.json) records all three JVM profile gates, the
six normal and six load trials, raw outcomes, aggregate verification and cleanup.

## Experiment and limits

Both variants use the pinned Java 25 profile. JAVA-07 source is
`c2e9061ba4cc23f629e3ff0c26a98a7a9e17c0a8`; JAVA-08 uses the qualified source above.
Run order was three JAVA-07 normal trials, three JAVA-08 normal trials, three
JAVA-07 load trials, then three JAVA-08 load trials. Every trial had a fresh
owned scheduler/two-agent/proxy cluster. Workload limits and instrumentation were
identical within each comparison: one CPU and 128 PIDs per scheduler/agent
container, no Docker memory cap, and scheduler flags `-Xms32m -Xmx192m
-XX:ActiveProcessorCount=2`. Normal trials also used the same GC/NMT diagnostics.
There were no simultaneous builds or qualification runs during measurement.

The load harness warms eight state reads, then measures eight quiet reads,
pauses the entire owned agent-a container and its workloads, measures eight
paused reads, completes a batch on agent-b, and sends a synchronized 24-client
state-read burst. It unpauses the same container with unchanged start time,
verifies the original two physical service attempts recover, stops them and
checks all three attempts are terminal, cleaned and unreserved. All six trials
passed those conditions; all 24 burst outcomes per trial were collected without
hitting the ten-second collection deadline. That deadline does not bound Python
executor shutdown. The pause is not a daemon-only stall.

These are ordered exploratory samples on one Pi, with three trials per variant,
not randomized/interleaved experiments, confidence intervals or a production
SLO. Warmup, JIT/GC, CPU throttling and host/cache state can affect the data;
the study does not isolate their contributions. Resource readings are four
labelled samples per load trial and eight per normal trial, not continuous
measurements or peaks during admission. p95 uses the nearest-rank method; for
three observations it is simply the maximum. Latencies include client and
network overhead. The burst includes TCP/TLS setup, dispatch and queueing, so
its failures cannot be attributed solely to the request executor.

## Normal workload

Values are median / p95. Resource figures are scheduler process samples.

| Metric | Samples JAVA-07 / JAVA-08 | JAVA-07 | JAVA-08 |
| --- | --- | --- | --- |
| Cluster startup (ms) | 3 / 3 | 7078.4 / 8106.3 | 7125.5 / 7185.0 |
| State GET (ms) | 90 / 90 | 25.7 / 98.4 | 26.8 / 104.7 |
| Batch submit acknowledgement (ms) | 18 / 18 | 73.2 / 117.5 | 75.5 / 121.8 |
| Batch submit to observed completion (ms) | 18 / 18 | 690.4 / 809.5 | 698.7 / 879.6 |
| Service submit to ready (ms) | 3 / 3 | 1863.2 / 1877.1 | 1777.3 / 1798.1 |
| Scheduler crash to recovered (ms) | 3 / 3 | 3529.8 / 3563.7 | 3536.6 / 3629.8 |
| Service stop to cleaned (ms) | 3 / 3 | 734.3 / 768.4 | 783.5 / 883.4 |
| Backup acknowledgement (ms) | 3 / 3 | 44.3 / 45.0 | 47.6 / 64.8 |
| GC pause (ms) | 43 / 42 | 3.4 / 7.6 | 3.6 / 65.7 |
| RSS (KiB) | 24 / 24 | 125568.0 / 128048.0 | 122408.0 / 126416.0 |
| Anonymous RSS (KiB) | 24 / 24 | 100928.0 / 103360.0 | 97648.0 / 101648.0 |
| Threads | 24 / 24 | 27.0 / 27.0 | 27.0 / 27.0 |
| File descriptors | 24 / 24 | 23.0 / 24.0 | 23.0 / 26.0 |

Each variant completed 18 sequential batches. Dividing those completions by the
sum of their submit-to-observed-completion windows gives 1.487
versus 1.442 batches/second. This completion-rate proxy excludes
between-batch reads, resource sampling and the rest of the trial; sustained
throughput and maximum capacity were not measured.

Ordinary request, batch and recovery medians were close. GC pause p95 was higher
in JAVA-08, and several other tails varied. There is no claim of a speedup,
performance neutrality, or a demonstrated causal GC regression from this small
ordered sample. Lifecycle hardening is justified by failure/ownership behavior,
not these timings.

## Paused agent and operator burst

The latency columns include **successful requests only**, in milliseconds.
Errors retain their full denominators and separate timings in the ledger.

| Phase / variant | Successful / total | Failures | Median | p95 | Maximum |
| --- | --- | --- | --- | --- | --- |
| Quiet / JAVA-07 | 24 / 24 | 0 | 26.9 | 79.9 | 149.0 |
| Quiet / JAVA-08 | 24 / 24 | 0 | 27.5 | 95.0 | 172.0 |
| Paused / JAVA-07 | 24 / 24 | 0 | 28.2 | 1536.2 | 1542.3 |
| Paused / JAVA-08 | 24 / 24 | 0 | 26.4 | 1225.1 | 1606.6 |
| Burst / JAVA-07 | 56 / 72 | 16 | 1843.4 | 3626.7 | 3636.7 |
| Burst / JAVA-08 | 54 / 72 | 18 | 1848.5 | 3599.5 | 3624.1 |

Every failed burst outcome was `builtins.ConnectionResetError`: 16/72 for
JAVA-07 and 18/72 for JAVA-08. Per-trial successes were 18, 18, 20 and 18, 18, 18
respectively. Failure timing median / p95 / maximum was 23.5 / 94.1 / 94.1 ms
for JAVA-07 and 27.0 / 43.6 / 43.6 ms for JAVA-08. Mixing those quick failures
into successful-request latency would hide the approximately 1.85-second median
for requests that did complete. All trial indexes/outcomes are retained; none
was silently retried or removed. Burst start-offset maxima were 7.2 ms and
18.0 ms, documenting the actual release spread.

| Load resource samples | JAVA-07 median / maximum | JAVA-08 median / maximum |
| --- | --- | --- |
| RSS (KiB) | 119168 / 124400 | 121184 / 123600 |
| Anonymous RSS (KiB) | 94480 / 99680 | 96424 / 98848 |
| Threads | 27 / 27 | 27 / 27 |
| File descriptors | 22 / 25 | 22 / 25 |

There are 12 samples per variant: before pause, after pause, while paused after
the burst, and after resume. Stable sampled threads/FDs do not prove a bound on
transient sockets during the burst. The executor bounds submitted work at two
workers plus a 16-entry queue, but that is not an application admission protocol
that reliably returns HTTP 429/503 to excess clients.

## Source interpretation

[`NativeEngine.tick`](../../scheduler/native/src/main/java/org/apache/aurora/nativescheduler/NativeEngine.java#L111)
holds the engine monitor while polling each configured node, placing work and
delivering commands. Submit, stop and state use the same monitor.
[`poll`](../../scheduler/native/src/main/java/org/apache/aurora/nativescheduler/NativeEngine.java#L126)
reduces each page in a transaction and acknowledges only its committed cursor.
The [transport](../../scheduler/native/src/main/java/org/apache/aurora/nativescheduler/NativeSchedulerMain.java#L130)
uses direct proxy policy, mTLS/hostname verification, redirect refusal, a
1,000 ms connection-establishment timeout and 1,500 ms read inactivity timeout.
Those limits are not an absolute whole-exchange deadline.

That source behavior is consistent with a paused agent delaying operator reads
while the healthy agent still makes progress. It is an interpretation supported
by source and timings, not a measurement of monitor contention. More HTTP or
virtual threads alone would still wait on that monitor. The current study does
not justify expanding the supported operating profile; reliably serving 24
concurrent operators under this fault would need more work. Retain the simpler
ordering model for the present MVP and use the measured limit to guide a future
workload-driven prototype.

## Decision boundary and future bounded candidate

This is a decision about adopting a change for the current two-agent MVP.
The experiment compares the serialized JAVA-07 and JAVA-08 runtimes; it does
not compare a concurrent implementation. The slow-agent case can reveal a
latency limitation without establishing that an unimplemented candidate is
faster, safer, or worth its complexity. Raising the supported node count or
operator request rate needs its own operating target and qualification.

If that need is established, the first candidate should have at most two agent
exchanges in flight globally, one per agent, and one bounded polling cycle.
Capture epoch, session, cursor and generation on the owner thread; fetch bounded
immutable results outside SQL; return results to the owner for reduction in
configured node order. Never pass `Tx`, a live JDBC connection, mutable engine
collections or a mutable response tree between workers. Retain four-page limits
and a bound on retained response bytes; queue capacity is an admission limit,
not a substitute for a byte budget.

Each page must commit before its cursor is acknowledged, and one agent's next
page/ACK must not overtake its earlier work. Placement uses the ordered completed
poll cycle, including reachability and unknown reservations. Delivery remains
per-agent ordered. A stop invalidates the relevant generation,
removes unsent stale work and durably records cancellation. Recheck pending work
on the owner before dispatch. Cancellation of a future does not prove that an
in-flight Run never reached an agent: keep uncertain allocations reserved, retain
stable command identities, and use the existing durable Stop/replay protocol.
Late results must neither resurrect desired state nor acknowledge a different
or superseded operation. Releasing the engine monitor around I/O changes these
interleavings and requires explicit tests.

Use explicit admission rejection, an absolute operation deadline in addition to
connect/read inactivity limits, cancellation of queued and active work, and
bounded shutdown owned by the daemon. Store closure remains last after all
workers and transport callbacks have stopped; timeout retains ownership.
Virtual threads can be evaluated under those same limits. They do not remove
the synchronized engine bottleneck or justify unbounded task admission. A
bounded platform-worker prototype would provide a comparison before selecting
an executor. Preview structured concurrency and scoped transaction replacement
remain outside this work.

Evaluate `HttpClient` in a separate experiment. Preserve direct proxy policy,
client certificate/trust material, hostname verification, redirect refusal,
canonical request bytes, authority headers, the existing HTTP protocol behavior,
and inclusive body limits before parsing. Define body-stall and whole-exchange
deadlines, cancel/close behavior, and handling of uncertain sends. A response
handler that buffers an unbounded body or a timeout that only covers headers
would not meet the contract. Connection reuse and additional client threads/FDs
must be measured rather than assumed beneficial. Existing mTLS/auth and stalled
body tests must run through the candidate transport; key-store loading alone is
not equivalent to a handshake test.

Reopen the decision when a representative sustained workload exceeds an agreed
operator error/latency budget, controller lag prevents required healthy-agent
progress, or the supported node count grows. First instrument queue delay,
monitor wait and per-agent exchange timing to separate causes. Then compare
baseline, bounded platform workers, bounded virtual threads and (separately)
HTTP transport using the same resource limits and an interleaved trial order.
Require deterministic placement, per-agent command/ACK order, transaction
isolation, commit-failure/no-ACK cases, late response/stop races, blocked-I/O
shutdown, retained reservations, the three-profile physical gate and cleanup.
The current study does not claim those tests validated a concurrency prototype.

## Reproduction and completion

Use a fresh output root for each invocation; the harness checks bundle/source
identity and owns every container it pauses. For example, with the recorded
local bundle and matching source checkout:

```sh
build-support/native/native-load-study \
  --label java08 \
  --bundle .cache/java08-java25-bundle \
  --source-tree .cache/java08-fresh-source \
  --output .pi-lab/java10-another-java08
```

Use `native-benchmark` with the same bundle/source pair and a distinct output
root for the normal workload. The [build guide](../../build-support/native/README.md)
provides fresh-bundle instructions. Historical comparisons require the matching
historical bundle and source, not retargeting an old fixture to current code.

All 15 accepted qualification/benchmark/load runs were cleaned up, and final
verification preserved the original MVP service/container identities and Home
Assistant health. JAVA-09's [SQL class decision](JAVA09_SQL_RECORD_DECISION.md)
and this conditional no-prototype decision complete the remaining Java audit
tasks. SUPERVISE-01 remains the next execution-capability task in the broader
roadmap. Java 25 remains the default.
