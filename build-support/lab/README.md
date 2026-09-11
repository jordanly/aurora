# Aurora container lab support

`labctl` provides bounded scaffolding for the Raspberry Pi container lab. It does not start services, build images, generate certificates, or change host configuration.

```sh
build-support/lab/labctl preflight
build-support/lab/labctl init --root "$PWD/.pi-lab" --run-id review
build-support/lab/labctl render "$PWD/.pi-lab/review"
build-support/lab/labctl inspect "$PWD/.pi-lab/review"
```

`init` creates a private, marked run root and records the blueprint hash, selected UID/GID, and project name. Generated files explicitly say `unconfigured`; they are scaffolding and contain no credentials. `render` emits an absolute, run-scoped Compose file with an explicit project name after checking the source hash and ownership markers.

`preflight` reports host page size, Docker and Compose client/server availability, cgroup controllers, `memoryControllerAvailable`, and the conservative `hardMemoryEnforcementVerified` field (always false until a later verified enforcement probe). The CLI targets Python 3.8 or newer.

`destroy --confirm` is intentionally unavailable in this slice. It refuses when Docker state cannot be queried and otherwise exits with an unimplemented status without removing anything. `up`, `build`, and scenario execution are not implemented and must not be treated as successful.

## Maintained Java 25+ checks

Use `./gradlew check` from the repository root for the scheduler/protocol and
SQL tests. Use [native-build and native-qualify](../native/README.md) for the
packaged scheduler/two-agent Docker gate, including crash recovery, isolated
snapshot restore and the ten-minute mixed workload. Execution and policy checks
exercise the opt-in supervisor/task and scheduler policy capabilities.

The earlier `native-smoke` durable-core builder is retired. Its command now
refuses before creating a run; its Java 8 compilation, Dockerfile, Compose input
and obsolete tool manifest have been removed. Historical implementation and
receipts remain available at commit `61ee74f30`. Shared process, path, hashing,
redaction and durable-evidence utilities now live in `lab_common.py`.

New cluster labs require a verified Java 25+ bundle, including its certificate
helper and keytool. They use OpenSSL's default PKCS12 algorithms. Existing lab
status, fault and cleanup operations use their recorded artifacts and do not
need the retired Java 8 bootstrap or regenerate certificates.

The current container base is pinned in `build-support/native/toolchains.json`.
Acquire it explicitly if absent from Docker's image store:

```sh
docker pull --platform linux/arm64 \
  'debian:bookworm-slim@sha256:6bd27d44e6c32a66bbd72d7cb2b76a8ae3497ec2e5274a81abd1b37f6013fa1f'
python3 -m unittest discover -s build-support/lab/tests -v
```

These unit tests verify current utility and ownership/error boundaries. Physical
Docker/process checks remain separate evidence. The tools clean only their own
verified resources and preserve run roots, artifacts and failure evidence.

## Real process runtime lanes

`process-smoke` now drives a local Go agent through its operator-owned JSONL
control interface. Nine physical cases qualify batch outcomes/replay, service
readiness, graceful/forced stop, log bounds, exact port conflicts and daemon
crash recovery. `process-container-smoke` independently qualifies four cases in
one ARM64 agent container using the existing pinned Debian base. Its service
checks record received TERM and rebind the exact socket after cleanup; no host
PID is inferred from container PID values. Neither lane is a scheduler cluster.

```sh
build-support/lab/process-smoke --run-root "$PWD/.pi-lab/process-new"
sg docker -c 'build-support/lab/process-container-smoke --run-root "$PWD/.pi-lab/container-process-new"'
```

Both require a new absolute private root and rebuild current source with cached
Go modules. Acquire the pinned base above before the container lane. It uses
Docker's creation-ID file, validates ownership/isolation/mounts before removing
its container, and preserves unknown resources. The container has a read-only
view of artifacts/configuration/evidence and writes only its explicit state,
work and fixture-evidence mounts. It cannot write the host's result file.
Failures, partial case evidence and cleanup outcomes are retained in `result.json`.
Run roots, binaries, journals and logs are never automatically deleted.

The process lanes add 24 failure/ownership tests to the preceding 21 lab tests.
See [the runtime gate ledger](../../docs/reimagining/PROCESS_RUNTIME_STATUS.md)
for executed results, remaining recovery limits and the next integration work.


## Integrated native cluster

`clusterctl` runs a verified native image bundle with one Java scheduler, two Go process agents and
transparent TLS fault proxies on separate internal Docker bridges. `cluster-check`
executes physical workload, restart, partition, cancellation and isolated restore
scenarios. These are the integrated successors to the focused smoke lanes above.

```sh
sg docker -c 'build-support/lab/clusterctl --run-root "$PWD/.pi-lab/cluster-new" up --bundle "$PWD/.cache/aurora-native/bundle"'
sg docker -c 'build-support/lab/cluster-check --run-root "$PWD/.pi-lab/cluster-new" --repeats 3 --mixed-seconds 600'
sg docker -c 'build-support/lab/clusterctl --run-root "$PWD/.pi-lab/cluster-new" demo'
build-support/lab/clusterctl --run-root "$PWD/.pi-lab/cluster-new" status
sg docker -c 'build-support/lab/clusterctl --run-root "$PWD/.pi-lab/cluster-new" down'
```

The image-backed lane accepts an absolute directory containing the native build's
`bundle.json`:

```sh
sg docker -c 'build-support/lab/clusterctl --run-root "$PWD/.pi-lab/cluster-images" up --bundle "$PWD/.cache/aurora-native/bundle"'
sg docker -c 'build-support/lab/cluster-check --run-root "$PWD/.pi-lab/cluster-images" --repeats 3 --mixed-seconds 600'
```

Use the actual bundle output directory from the native build. The launcher checks
its source hashes against this checkout, verifies artifact and host JRE hashes,
and checks all five local Linux ARM64 image IDs. It runs `scheduler-lab`,
`agent-lab`, and `tools` by immutable image ID. These lab images include the
keeper and workload fixture needed for fault acceptance. Runtime containers and
the isolated restore bind only configuration, certificates, state, work, controls,
and evidence; executable files, JREs, and libraries come from the images.
Certificate creation uses the bundle's verified host helper and keytool plus host
OpenSSL. The bundle must remain available and unchanged through acceptance;
evidence records its manifest hash and exact image IDs. New labs require `up --bundle`; omission fails before creating a run root.
Existing Java 8 labs retain status, fault and cleanup support. Java 25 bundles
enable classpath JNI explicitly and deny other native access. Use a fresh run root.


The operator client verifies mutual TLS at the scheduler's private bridge address.
No host port is published. The Pi's Docker session may require `sg docker`; HTTPS
status/submit/stop can run as the same UID without Docker access. Each lab retains
its creation GID for container ownership across caller group changes.

Use Python 3.9 or newer for the integrated runner. Certificate/private key material
stays in the private run directory and is mounted only into the relevant role.
The seven-day lab CA is recreated with each new lab. Agent container recreation
with an old runtime journal remains unsupported; keepers exercise daemon-only
restart in the unchanged namespace. `down` preserves state and evidence, verifies
creation IDs before removing resources and never touches unrelated Docker images
or containers. See [the cluster guide](../../docs/reimagining/CLUSTER_MVP.md) for
commands, durable semantics, profile limits and qualification details.
