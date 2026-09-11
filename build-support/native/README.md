# Native runtime packaging (Java 25)

This is the supported packaging and qualification lane for the native cluster MVP.
It builds a Java scheduler with SQLite and a Go agent with bbolt. The profile
uses Temurin 25.0.4.1+1, separately pinned JDK/JRE artifacts, and Gradle 9.7.1.
[JAVA-02](../../docs/reimagining/JAVA02_STATUS.md) records the current dependency
update and its qualification status. [JAVA-01](../../docs/reimagining/JAVA01_STATUS.md)
preserves the preceding Java 25 baseline; CUT-01 preserves the Java 8 baseline.

The current pinned profile is **Linux ARM64**, including this Pi's 16 KiB page
kernel. The host needs Python 3.9+ (the Pi uses 3.13), OpenSSL, Git, Docker with
BuildKit and access to the Docker daemon as an ordinary user. No system Java,
Go, Gradle, Thrift, Mesos, Pants, Python worker environment or frontend build is
required. Network access is needed for the initial pinned tool archives, Maven
artifacts, Go modules and Debian base image. Docker image builds themselves run
without networking.

From a fresh checkout, using new absolute output paths:

```sh
build-support/native/native-build \
  --output /absolute/path/native-bundle \
  --cache /absolute/path/native-cache
build-support/native/native-qualify \
  --bundle /absolute/path/native-bundle \
  --run-root /absolute/path/native-qualification
```

On this Pi, if the current login has not inherited its Docker group membership,
run those commands through `sg docker -c 'COMMAND'`. The builder must run as the
ordinary user. It never changes host groups, daemon settings or boot settings.
The cache must be owned by that user and not writable by other users/groups.

`--seed-archives /absolute/path/downloads` optionally copies checksum-verified
archives into a new cache. It does not reuse compiled outputs or unverified
extracted toolchains. `--offline` requires the archive, Maven, Go and base-image
caches to be warm and suppresses network dependency retrieval. Every build
extracts the verified tool archives into its own build directory and rebuilds
all application binaries. Output directories must be new; a failed build keeps
its logs and ownership records. Use another output directory for a retry.

## Artifacts and gates

`toolchains.json` pins the Debian base by digest, Temurin JDK/JRE and Gradle/Go archives by SHA-256,
and the Go runtime module graph. `go.sum` supplies module content checksums.
The Java gate pins all seven external runtime JARs by SHA-256 and permits exactly
two application JARs. It parses actual class identities and references, rejects
legacy Aurora/Mesos/ZooKeeper/worker classes and resources, and excludes the SQL
qualification CLI from production. SQLite's bundled JNI is explicitly allowed.

The builder runs Java tests, Go tests/vet/module verification, Python harness and
packaging regressions, and checks the installed runtime. It records both Gradle
runtime dependency graphs in `reports/java-dependencies.log`; the separate exact
JAR hashes remain the acceptance gate. Five local images result:

| Bundle role | Contents / use |
| --- | --- |
| `scheduler` | JRE, nine runtime JARs, scheduler launcher |
| `agent` | Static Go agent |
| `scheduler-lab` | Scheduler plus keeper and workload fixtures |
| `agent-lab` | Agent plus keeper and workload fixtures |
| `tools` | Static TLS proxy/certificate/keeper helper |

Production images use numeric UID/GID `1000:1000`. Lab containers override the
GID with the recorded creator GID so private host state remains accessible.
Production images contain no build toolchains or lab fixtures. Derived lab
images are separately labeled. Image tags are unique per build, image IDs are
recorded, and nothing is pushed or pruned.

The gate exports each image from an ownership-checked temporary container. It
compares every Aurora file with the staged payload and every other nondirectory
entry with an export of the pinned Debian base. Only Docker's generated
`/etc/hosts`, `/etc/hostname` and `/etc/resolv.conf` regular files may differ.
Consequently, adding a renamed foreign JAR/binary outside the runtime also fails.
Normal base filesystem links are preserved; links inside the Aurora payload are
rejected. Temporary probe containers are removed only after matching their
immutable IDs, image IDs and ownership labels.

The real production entrypoints are exercised separately: agent `version` and
scheduler `--inspect-only`, including loading SQLite JNI and opening a fresh
store under the image's JRE. Qualification then uses the same payloads in the lab
images, with no executable/JRE/JAR bind mounts.

`bundle.json` records immutable image IDs, source hashes, payload hashes, tool
pins, the source commit/worktree status and build-report hashes. Host certificate
creation uses the verified helper and keytool shipped inside the bundle. A
bundle is accepted only by a checkout with matching build/runtime source hashes;
keep that source checkout alongside it. Images are local to the Docker daemon;
this slice does not publish a registry release or claim bit-for-bit image ID
reproducibility across builds.

## Qualification and operation

`native-qualify` creates a separate scheduler/two-agent cluster, runs all three
recovery/backup-restore rounds and a 600-second mixed service/batch workload, and
removes only that run's recorded containers/networks. State, logs, certificates
and evidence remain in its private run directory. `qualification.json` succeeds
only if the cluster checks and cleanup both pass. The detailed case evidence is
in `evidence/*-result.json`. A failed or interrupted run preserves uncertain
ownership for inspection; use the documented `clusterctl down` operation to retry
cleanup once the recorded identities can be verified.

To keep a cluster running after building:

```sh
build-support/lab/clusterctl --run-root /absolute/path/demo-cluster up \
  --bundle /absolute/path/native-bundle
build-support/lab/clusterctl --run-root /absolute/path/demo-cluster demo
build-support/lab/clusterctl --run-root /absolute/path/demo-cluster status
build-support/lab/clusterctl --run-root /absolute/path/demo-cluster down
```

Configuration and certificates are generated per run. The API is on a private
Docker bridge address reported by `up`; mTLS verifies the `scheduler` DNS name.
There are no published host ports. Config, TLS material, state, work, evidence and
keeper control are the only bind mounts. Three private networks isolate each
agent from the scheduler through its fault-injection proxy; restores use a fourth
isolated network. The keeper permits daemon crash/restart in the same agent
container namespace. Container replacement/adoption is a separate future feature.
See [the lab guide](../lab/README.md) for faults, job submission and evidence.

For direct production use, pass the scheduler launcher its `--config`, `--state`,
`--listen`, `--tls-keystore`, `--tls-truststore` and `--tls-password-file` paths.
Mount private writable state and an executable temporary directory at
`/run/aurora-native` for SQLite JNI. Agent `serve` takes enrollment/config, state,
work-root, network and TLS paths as documented in [the agent guide](../../agent/README.md).
Neither image contains credentials. Protect their private stores and certificates;
the lab demonstrates the required read-only root filesystem, dropped capabilities,
nonroot execution and limited temporary filesystems.

## Scope and follow-on work

The legacy root build, Mesos implementation and Python workers remain in the
repository for reference, isolated from this native source set and packaging
lane. CUT-01 enforces their absence from native distributions; it does not claim
a repository-wide deletion or a production security audit. The cluster is a
single scheduler with static enrollment, bounded history and no HA or rolling
job-update API. This Pi has no memory cgroup enabled; admission accounting is
not hard kernel memory isolation. See [MVP boundaries](../../docs/reimagining/CLUSTER_MVP.md).

JAVA-01 requires `--release 25`, exact class-file major version 69, two builds
in distinct output directories, and `--warning-mode=fail`. Own JARs, tar/zip
archives, and installed trees must be byte-for-byte identical. The runtime
keeps the Java compiler API/JVM JIT but excludes `javac` and source compiler
tools. SQLite JNI uses `--enable-native-access=ALL-UNNAMED`,
`--illegal-native-access=deny`, and executable `/run/aurora-native`.

Fresh labs require `clusterctl up --bundle`; existing no-bundle lab operations
remain supported unchanged. New native builds do not use the legacy `build-support/java/gradle-local` wrapper.
The standalone `native-jvm-compat` operation checks private copies through
Java 8 → 25 → 8 by default. For dependency changes on Java 25, pass both major
versions explicitly and enable bidirectional fixture writes:

```sh
build-support/native/native-jvm-compat \
  --old-bundle /absolute/path/previous-bundle \
  --new-bundle /absolute/path/native-bundle \
  --old-java-major 25 --new-java-major 25 --dependency-writes \
  --snapshot /absolute/path/previous-lab/scheduler/state/backups/SNAPSHOT \
  --config /absolute/path/previous-lab/config/scheduler.json \
  --output /absolute/path/new-compatibility-result \
  --javac /absolute/path/native-bundle/build/tools/java/jdk-25.0.4.1+1/bin/javac
```

Supply a standalone populated backup directory containing only `scheduler.db`
and its original lab config. The original database, config, TLS files and bundle
artifacts are verified unchanged. The optional write scenario exercises private
synthetic Job/Run/Stop records, replay, conflicts, transaction rollback, snapshot
reopen and old/new dependency writes; it never dispatches or launches workloads.
Actual TLS handshakes and workload execution are covered by `native-qualify`.

The runtime remains seven external JARs: Jackson core/databind 2.22.2 and
annotations 2.22, networknt 2.0.7, SLF4J API/NOP 2.0.19, SQLite JDBC 3.53.4.0.
The Jackson BOM aligns its family; YAML, date-time and optional alternate regex
engines remain outside this JSON-only profile. Broader launcher/HTTP qualification
continues in JAVA-03; JMH, Thrift and legacy-root work are outside this profile.
