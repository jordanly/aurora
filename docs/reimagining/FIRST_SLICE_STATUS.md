# First implementation slice: standalone foundations

Recorded 2026-09-10 on the Raspberry Pi. The fork and local `master` were
fast-forwarded to upstream commit `11ebaeeb071cb182c388a40755e84f60dda32260`.
The fork's remote ref was verified after pushing. Implementation lives on
`codex/standalone-foundations`; implementation changes have not been pushed to
`master`.

This increment makes the scheduler testable, introduces executable native
protocol fixtures, and prepares isolated lab runs. The integrated scheduler and
two-agent demonstration remains the later **SLICE-01** acceptance gate in the
[ordered backlog](IMPLEMENTATION_BACKLOG.md).

| Work | Delivered and verified | Remaining gate |
| --- | --- | --- |
| BUILD-01 | Checkout-local ARM64 Java 8, Gradle 4.10.2 and compiler-only Thrift 0.10.0; verified archive checksums; explicit Thrift override; Python 3 wrapper fix; generated API and scheduler compile. | Python 2 comparison is source-derived rather than executed under Python 2. Native Mesos/JNI qualification is outside this lane. |
| BUILD-02, bounded | Separate focused scheduler test task, two-worker/fork caps, frontend retained for run/JAR/full tests, original coverage thresholds retained. | Full suite, frontend execution, distribution packaging and wider integration coverage are unexecuted. |
| CONTRACT-01, bounded | Strict alpha schemas; separate Job templates and resolved Run assignments; JobKey; lossless counters; authority-independent command hashes; Java/Go canonical encoding parity. | Real Java/Go schema/admission validators, authenticated transport, durable reducers, inventory assembly and committed ACK behavior are unimplemented. |
| LAB-01, partial | Preflight and Docker daemon access; marked private run roots; explicit project names; source-hash-bound deterministic Compose rendering; inspection and refusal-only destruction skeleton. | Images/toolchain manifests for images, real configs/certificates, launch, cleanup and native runtime smoke tests. |

The native profile deliberately accepts one trusted batch instance or zero to two
service instances, with one preinstalled process per instance. Job templates can
reference named ports; Run assignments carry exact resolved sockets and argv.
Unsupported execution capabilities reject in the fixture validator. The limits
describe this alpha fixture profile, not permanent Aurora product limits.

## Executed checks

All checks below ran on `aarch64`, kernel `6.18.39+rpt-rpi-2712`, with 16,384-byte
pages. The orchestrator reviewed delegated changes and independently repeated the
Java, protocol and lab checks after corrections.

| Check | Result |
| --- | --- |
| Python wrapper regressions | **8 passed**, including equality/hash/string field preservation, unions, metadata and deterministic API goldens. |
| Gradle build | Generated API, commons, scheduler main and scheduler test sources compiled. Generated and scheduler classes use Java 8 bytecode. |
| Scheduler state machine | **21 TaskStateMachineTest tests passed**, zero failures/errors/skips. The independent rerun executed the focused test task successfully. |
| Clean regeneration | All **240 generated Java/resource files** reproduced byte-identical hashes after deleting their owned generation directories. |
| Build task graphs | Focused checks exclude frontend and coverage tasks. `run jar test` retains webpack ordering and the original JaCoCo gates. An incorrect explicitly selected compiler is rejected. |
| Protocol structural/semantic corpus | **13 valid and 27 invalid fixtures**, **13 canonical/hash goldens**; template resolution and refreshed-authority invariants pass. |
| Cross-language canonical profile | Java 8 and Go each pass **13 encoding/hash vectors** and **12 parser rejection vectors**; Python rejects the same 12 parser/profile negatives. |
| Lab harness | **7 tests passed**, including default IDs, deterministic rendering, malformed ownership records, symlink refusal, missing/denied Docker and the actual Compose parser. |
| Compose topology | Default five-service and optional six-service configurations parse. Project identity, literal paths, declared network separation, bind roots, read-only defaults and loopback publication are checked. These are configuration checks, not runtime isolation proof. |

The temporary Java toolchain retains legacy application dependency versions.
Go 1.27.1 runs from a verified checkout-local ARM64 archive. Download versions,
checksums, comparison limits and reproduction commands are recorded in the
[build README](../../build-support/java/README.md) and
[protocol README](../../protocol/native-v1alpha1/README.md).
No global Java/Go install or host package change was required.

Review caught defects before accepting the increment: the Python 3 generator
exhausted reused fields; lab default timestamps and project naming needed fixes;
symlink outputs required refusal; templates needed separation from assigned ports;
and the canonical adapters disagreed on malformed Unicode escapes, object keys
and negative zero. Relevant regression vectors now exercise those cases.

Luna handled the lab scaffold. Astra at medium reasoning handled build recovery
and protocol contracts, then reviewed each other's work. Parent review included
source inspection, independent execution and actual Compose parsing.

## Reproduce from the checkout

```sh
# Once: download verified archives and build the temporary Thrift compiler.
build-support/java/bootstrap.sh

# Build and selected scheduler behavior, with local tools/caches.
build-support/java/gradle-local :api:testThriftWrapperGenerator :api:classes \
  focusedTest --tests '*TaskStateMachineTest'

# Python protocol checks require conformance/requirements.txt; the protocol
# README supplies the independent Java and Go adapter commands.
python3 protocol/native-v1alpha1/conformance/check.py
python3 -m unittest discover -s build-support/lab/tests -v

# Host facts; requires a session with the docker group active.
build-support/lab/labctl preflight --require-docker

# Preparation only. Pick a fresh run ID for each initialization.
build-support/lab/labctl init --run-id first-review
build-support/lab/labctl render "$PWD/.pi-lab/first-review"
docker compose -f "$PWD/.pi-lab/first-review/generated/compose.yaml" config --quiet
build-support/lab/labctl inspect "$PWD/.pi-lab/first-review"
```

`.pi-tools/` and `.pi-lab/` are ignored. The local Java evidence includes
`.pi-tools/parent-focused-review.log`, `.pi-tools/gradle-focused-test.log`,
`.pi-tools/gradle-regeneration.log` and
`dist/test-results/focusedTest/TEST-org.apache.aurora.scheduler.state.TaskStateMachineTest.xml`.
Those disposable outputs are not required source artifacts.

Compose parsing requires only the installed client/plugin; it does not establish
daemon access or launch a service. Literal dollar signs are escaped in rendered
paths; the parser check accounts for Compose re-escaping them on output.
[Compose interpolation](https://docs.docker.com/reference/compose-file/interpolation/),
[Compose 5.5.1 config serialization](https://github.com/docker/compose/blob/v5.5.1/cmd/compose/config.go#L180)

## Current host status and next increment

Docker access was initially denied because `jordanly` lacked the socket's
`docker` group. On 2026-09-10, the user explicitly authorized adding that
membership. The socket remains mode 0660 with owner/group root:docker.
`docker info` and `labctl preflight --require-docker` now pass as `jordanly`
through `sg docker -c '...'`, outside the tool sandbox. Verified facts are
server 29.8.0, architecture aarch64, Debian 13, systemd cgroup driver and cgroup
v2; Compose is 5.5.1. Existing processes do not inherit new supplementary groups:
use a fresh login/session or `sg docker` for commands in the current session.
No service, socket-permission or boot change was made. The root cgroup v2
controllers still omit memory; hard memory enforcement is explicitly unverified
and must not be advertised. Container execution remains an unexecuted gate.

The next code increment is:

1. Finish CONTRACT-01 with shared Java/Go structural and semantic validation,
   command outcome fixtures and explicit reducer transition tests.
2. Start AGENT-01: choose one local store through a small replay/corruption spike;
   implement durable admission, reservations, stop tombstones and observation
   outbox against a fake scheduler, before any real process launch.
3. Start CORE-01 and the native SQL transaction boundary in parallel, using the
   recovered Java tests. Preserve job desired membership separately from attempts
   and make nested write failures mark the transaction rollback-only.
4. Complete ACCESS-01 and LAB-02 as agent/runtime interfaces stabilize. Build
   native images using the now-verified Docker access, then
   qualify real ARM64 execution and the two-agent failure scenarios.

The ordering remains **Go agent and Mesos replacement → working durable lab →
Mesos dependency cleanup → full Java modernization**. This increment creates no
native scheduler/agent runtime, live cluster, production authentication, memory
enforcement or completed migration claim.
