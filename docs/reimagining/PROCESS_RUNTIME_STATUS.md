# Slice 1 continuation: real agent process execution

Implemented on `codex/standalone-foundations`, following the
[durable core increment](DURABLE_CORE_STATUS.md). Fork and upstream master were
refreshed on 2026-09-10 and still match
`11ebaeeb071cb182c388a40755e84f60dda32260`.

The Go agent now executes trusted preinstalled batch and service processes.
Native-host and ARM64 Docker scenarios verify real workload evidence, admission
replay, readiness, stop, cleanup and bounded logs. The Docker lane runs one agent
with workloads; scheduler communication and the integrated two-agent cluster
remain subsequent work.

## Implemented behavior

- Admission, immutable command results and reservations remain durable. Execution
  separately commits one-shot launch intent, helper PID/start identity and release
  intent before opening the inherited launch gate. Stop admission shares that
  gate's lock. Replayed accepted commands do not launch again. Pure Stop tombstones report
  cleanup once; late Stop/rejected Run commands preserve terminal observations.
- The helper executes explicit argv/environment as the agent user in a private
  attempt directory. It creates a new session, preserves parent-death signaling,
  uses no-new-privileges, and rejects set-ID/file-capability executables. PID-safe
  signaling and ownership checks avoid a raw-PID fallback.
- Outcomes come from actual process waiting. TCP readiness requires ownership of
  the exact assigned listening socket and a successful probe. Probes are
  cancellable and capped at 250 ms; application HTTP readiness remains separate.
- Stop clears readiness, retains its original durable deadline on replay, and
  escalates TERM to KILL. Capacity releases only after verified cleanup. Uncertain
  or surviving leaderless descendants remain reserved. This trusted lane does
  not contain arbitrary processes that detach from their session.
- Each stdout/stderr stream retains a configured prefix (1 KiB..16 MiB), drains
  excess output, and reports dropped bytes. There is no remote log API or rotation.
- `serve-local` provides bounded JSONL control through operator-owned stdin/stdout.
  EOF, signals, stalled output and explicit shutdown trigger cleanup. Shutdown
  success follows runtime and store closure; it is not a network authentication API.

Runtime activation atomically migrates a journal from format 1 to format 2 and
binds observed boot, PID namespace, network namespace and namespace-init identity.
The old admission-only binary refuses format 2. Same-scope daemon recovery never
relaunches consumed intent: it cleans verified processes and reports Lost without
inventing an exit code. Changed kernel scope refuses startup before signaling
anything. Container-loss recovery therefore still requires explicit fencing and
enrollment integration; retained volumes alone do not authorize old PID adoption.

## Executed checks

| Check | Result |
| --- | --- |
| Agent/CLI/protocol | 37 top-level Go test entries pass, including subprocess helper entries; vet passes. Runtime scenarios include five crash boundaries, failed intent commit, Stop/gate serialization, explicit environment, owned-socket readiness, PID/namespace mismatch and leaderless-descendant uncertainty. |
| Native fixture | Seven top-level entries pass, including its helper; vet passes. Checks cover HTTP 503-to-200 readiness, physical probe identity, TERM/INT behavior, exact bind conflict/rebinding, exit code and private evidence-file handling. |
| Lab failure regressions | 45 Python tests pass across existing lab scaffolding, durable-core containers, native process harness and process-container ownership/failure paths. |
| Native process harness | Nine physical cases pass: successful batch/replay, exit 7, missing executable, graceful service stop, forced stop, log truncation, external port conflict, agent reservation conflict and daemon SIGKILL/recovery without relaunch. |
| Container process harness | Four physical cases pass: batch/replay, graceful stop, forced stop and bounded logs. The fixture records TERM receipt and successfully rebinds the exact port after Stop. Both log-stream caps are checked. |

The orchestrator reviewed delegated code and reran checks independently. Luna
provided fixture/harness scaffolding; Astra medium implemented the runtime and
hardened the native harness. Independent review also hardened the container
runner's creation-ID ownership checks, partial-startup cleanup and failure evidence.

Final local evidence:

- `.pi-tools/runtime-parent-tests.jsonl`
- `.pi-tools/runtime-fixture-parent-tests.jsonl`
- `.pi-tools/runtime-lab-parent-tests.log`
- `.pi-lab/process-runtime-acceptance/result.json`
- `.pi-lab/process-container-acceptance/result.json`

Both physical harnesses rebuild current source offline with the pinned checkout
toolchain and record source/binary hashes. The Docker lane requires the existing
digest-pinned ARM64 base, verifies the created container's identity/configuration
and bind roots, and removes only that container. State, logs and evidence remain.
It uses no host-network publication, Docker socket mount, privileged mode or
unconfined seccomp. No hard-memory enforcement is advertised. Kernel/page-size
qualification is on this Pi's aarch64, 16 KiB-page environment.

## Reproduce

Use fresh absolute directories for each harness invocation:

```sh
build-support/lab/process-smoke --run-root "$PWD/.pi-lab/process-review-new"
sg docker -c 'build-support/lab/process-container-smoke --run-root "$PWD/.pi-lab/process-container-review-new"'
python3 -m unittest discover -s build-support/lab/tests -v
```

The [agent README](../../agent/README.md) documents the public local API, build
commands, store migration and runtime limits. The [lab README](../../build-support/lab/README.md)
documents prerequisites and qualification lanes. Go's race detector remains
unavailable on this host's VMA layout; ordinary concurrent tests pass. These
changes do not rerun or establish the full legacy Java/frontend suite.

## Next integration gates

1. Add authenticated enrollment/transport and real session/runtime identity
   handoff, including a fenced recovery policy for container-incarnation changes.
2. Assemble the native Java scheduler around the protocol validator and SQL
   store; enable minimal durable submit/read/stop and placement across two agents.
3. Connect committed dispatch, observation reduction, contiguous ACKs and complete
   reconnect inventory. Preserve desired membership without resurrecting completed
   batches or cancelled services.
4. Build the actual scheduler/two-agent/proxy lab and exercise partitions, restart,
   stop/reconnect and isolated backup restore. Then remove Mesos runtime
   dependencies and modernize Java against that standalone regression baseline.
