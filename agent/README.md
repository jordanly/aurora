# Aurora agent

This directory is the original integration agent used by the isolated lab. It
is a standalone Go module and does not depend on the replacement Java scheduler
or a `build-support/native` tree.

Build the two lab binaries reproducibly from the repository root:

```sh
build-support/lab/build-agents --offline
```

The helper uses the checksum-pinned Go 1.27.1 Linux ARM64 archive, seeded from
`.pi-tools/downloads` in offline mode, and writes binaries plus provenance
records under `.pi-tools/agent-original-integration`. A clean checkout may
omit `--offline` to download the same pinned archive into `.cache/inplace-go`.
The provenance records bind each binary to the source tree digest and build
recipe; `inplace-cluster` verifies them before staging a lab.

For focused development with the already seeded toolchain:

```sh
GOCACHE="$PWD/.pi-tools/go-cache" GOMODCACHE="$PWD/.pi-tools/go-mod" \
  GOTOOLCHAIN=local GOMAXPROCS=2 .pi-tools/go1.27.1/go/bin/go -C agent test ./...
```

The agent's local and authenticated HTTPS interfaces are documented in the
source package and exercised by its Go tests. The lab remains the only supported
integration boundary for scheduler communication.
