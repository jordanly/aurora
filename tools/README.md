# Aurora development tools

This Go module contains development/build tooling, not a scheduler implementation.
It uses the existing JSON toolchain manifests and does not require Python.

From any working directory, invoke the repository's `build-support/bootstrap-go`:

```
build-support/bootstrap-go gradle test --offline
build-support/bootstrap-go ui ci --offline
build-support/bootstrap-go thrift --offline
build-support/bootstrap-go agents --offline --check --output /absolute/output
build-support/bootstrap-go entities schema.json java-output resources-output
```

`gradle` runs the original root graph with pinned Java 25 and Gradle. `ui` calls
the real npm JavaScript entrypoint using pinned Node. `thrift` builds only the
pinned 0.10.0 compiler with `--disable-plugin`, retaining checksum/recipe/binary
receipts. `agents` builds the Go agent and cluster helper with their existing
JSON provenance fields. `entities` generates the retained Java Thrift wrappers.
Arguments are passed as argument arrays, never interpreted by a shell.

The initial shell stage supports the existing Linux/ARM64 pin in `go-tools.json`.
Its fixed literals are checked against the JSON manifest by tests. It verifies
the complete Go archive SHA256 before using system tar, extracts a fresh private
SDK for each invocation, and never accepts a preinstalled executable based on a
version string. It requires standard Linux core tools, `flock`, `tar`, and `curl`
when a pinned archive is not already available. Node's `.tar.xz` additionally uses
host `xz` only as a streaming decompressor; Go validates the archive entries.
Thrift source builds require the existing host `make` and `g++` toolchain.

`--offline` or `--offline=true` anywhere in the command prevents initial SDK
downloads, as does setting either `AURORA_INPLACE_OFFLINE=1` or
`AURORA_INPLACE_GO_OFFLINE=1`. These initial SDK checks do not change each
downstream command's own offline flag and environment policy. Individual
commands also disable tool downloads and, for agents, Go module downloads.
Initial Go archives are seeded from `AURORA_INPLACE_GO_SEED_ARCHIVES`, then
`AURORA_INPLACE_SEED_ARCHIVES`, then `.pi-tools/downloads`. Its private cache is
`AURORA_BOOTSTRAP_GO_CACHE` or `.cache/bootstrap-go`. The Java, UI, and Thrift
commands retain the current `AURORA_INPLACE_*_CACHE` and seed-archive environment
variables. The agents command retains `--cache`, `--seed-archives`, and `--output`.

General archive extraction refuses traversal, duplicate paths, ZIP links,
special files, escaping/dangling/cyclic TAR links, and oversized content. Safe
TAR links are materialized into regular files/directories and charged against
the expanded-size budget. All cached/seed archives are rehashed before use;
executables are freshly extracted. Paths reject symlink components before
normalization, caches must belong to the current user, and lock files serialize
shared builds. Thrift generated-source mtimes are retained to avoid unintended
autotools regeneration.

Run the module tests with a verified pinned SDK:

```
go -C tools test ./...
go -C tools vet ./...
```

The tests cover adversarial archives, cache/seed corruption, symlink paths,
fresh extraction, manifest duplication, shell pin consistency, managed Gradle
options, npm invocation, provenance changes, and compiler-recipe validation.

## Isolated lab ports

The Go dispatcher also provides `inplace-cluster` and `inplace-check`, retaining
the existing actions/options and ownership JSON format. The cluster command
supports `up`, `status`, restart/crash of scheduler or a selected agent,
`refresh-scheduler`, and `down`. The acceptance command retains `smoke`,
`recovery`, `policy`, and `soak`, including the original API field numbers,
rolling update/rollback checks, maintenance and cron checks, physical process
identity checks, and twenty batches/forty tasks over ten minutes.

```
build-support/bootstrap-go inplace-cluster up --root /absolute/new-lab --dry-run
build-support/bootstrap-go inplace-check --root /absolute/owned-lab --phase smoke --dry-run
```

`--dry-run` renders the requested scope without Docker calls, API calls, or file
creation. It does not claim to validate a running lab. Live operations still
require exact recorded IDs, names, ownership labels, image, commands, mounts,
and sandbox settings; unknown or mismatched resources are preserved. Refresh
keeps the bind-mounted directory inode and retains pending evidence until the
keeper confirms the scheduler is stopped and later restarted. These commands do
not discover or clean up unrelated labs.

The port adds collision-resistant fixture prefixes, rejects HTTP redirects and
missing false-valued sandbox fields, and bounds captured command output and
aggregate API report data (64 MiB). The complete acceptance assertions are tested
with fake HTTP/Docker and virtual time; such tests are not evidence of a live
cluster acceptance run. The production CLI always uses real time and Docker.

`build-support/bootstrap-go check-tools --offline` runs uncached tests and vet
for this entire Go module using a fresh verified SDK. `build-support/bootstrap-go
build-client --offline --output .pi-tools/client/aurora` builds the standalone
JSON client and a SHA256 provenance receipt beside it. Both accept `--cache` and
`--seed-archives` and inherit `AURORA_INPLACE_GO_OFFLINE`,
`AURORA_INPLACE_GO_CACHE`, and Go-specific or shared seed archive settings.
Client output must be outside `tools/` so it cannot contaminate its source digest.

`build-support/bootstrap-go stage-java --output .pi-tools/lab-jdk` materializes
only the pinned Java archive from `build-support/java/toolchains.json`; it does
not fetch Gradle. The fresh output is a stable JDK root suitable for the lab's
`--java` flag, with an adjacent `.provenance.json` containing the archive pin and
materialized tree SHA256. Existing output directories, receipts, and symlinks
are refused. `--offline`, `--cache`, and `--seed-archives` are supported, with
`AURORA_INPLACE_OFFLINE`, `AURORA_INPLACE_CACHE`, and
`AURORA_INPLACE_SEED_ARCHIVES` defaults. Each invocation safely extracts the
verified archive afresh; it never trusts an installed JDK merely by its version.
