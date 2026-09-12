# Bounded native-v1alpha1 contract

This directory defines the original scheduler integration wire contract and
its canonical JSON fixtures. It is independent of the legacy Thrift build and
does not introduce a replacement scheduler or a `build-support/native` build
tree. Consumers must apply the schema and semantic checks before accepting a
message; fixture acceptance is not evidence that a workload ran.

The contract uses strict objects, opaque case-sensitive identifiers, decimal
unsigned counters, explicit argv and environment, and canonical UTF-8 JSON
without a trailing newline for hashes. Duplicate or unknown fields, malformed
counters, unresolved port references, and mismatched body hashes are rejected.
Run and Stop bodies are immutable deduplication inputs, while authority and
session fields are checked separately by the authenticated transport.

The executable fixtures under `fixtures/` are the reviewable wire and hash
vectors. The Go agent and the original scheduler adapter consume this contract
through their own package boundaries; this directory contains no scheduler
implementation.

To run the fixture checker with the pinned Go toolchain:

```sh
GOCACHE="$PWD/.pi-tools/go-cache" GOTOOLCHAIN=local GOMAXPROCS=2 \
  .pi-tools/go1.27.1/go/bin/go -C agent test ./...
```

The isolated lab stages only the original scheduler distribution and verifies
the agent/helper provenance records before creating containers.
