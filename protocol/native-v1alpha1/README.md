# Bounded native-v1alpha1 contract

This is the first CONTRACT-01 slice: executable wire/schema and canonicalization
fixtures, independent of the legacy scheduler build. It does not implement a
scheduler, agent, authentication, persistent admission, state transitions,
reconciliation, or durable acknowledgements. Fixture observations are examples,
not evidence that a workload ran. The alpha contract may change before integration.

The next increment now adds [Java structural/semantic validation](../java/README.md)
and a [Go validator and durable admission store](../../agent/README.md). Both use
this authoritative schema and run the complete shared rejection corpus. The
standalone canonical adapters below remain deliberately smaller comparison tools;
the schema package itself still implements no running cluster.

`schema.json` uses JSON Schema draft 2020-12. Every message and nested fixed
object rejects unknown fields. There are no implicit defaults. `conformance/check.py`
adds bounds and cross-field rules that JSON Schema alone does not express here.
Consumers must apply both structural and semantic validation before use.

## Initial capability boundary

The batch fixture has one desired instance and retains its completed state; the
service fixture has two desired instances, at most one per agent. This bounded
schema permits service counts 0..2 (including zero desired instances),
and exactly one batch instance; these are initial profile limits, not general
Aurora replica limits. Both run one trusted,
preinstalled host process with explicit argv and environment. No shell evaluation,
Python config, arbitrary artifacts, credentials, OCI, retries, DAGs, or rolling
updates are accepted. The environment is the complete requested process environment;
launchers must not silently inherit the scheduler or agent environment. Absolute
fixture paths refer to future lab images; this package does not install binaries.

CPU is integer millicores and memory is integer bytes; these are reservations,
not claims of enforcement. `hard` memory requires `hard-memory` in the manifest
AND in the target agent's verified advertised capabilities. The initial fixture
profile advertises none. The conformance check proves rejection without that
capability and acceptance of the capability predicate when supplied; it does not
prove actual memory enforcement. Placement capacity is a separate admission check.

Job templates declare named TCP/IPv4 ports without numbers or network domains.
Template argv may contain typed `{ "portRef": "http" }` arguments; a reference
resolves to one complete decimal argv element and must name a declared port.
Run assignments require fully resolved string argv and exact named TCP/IPv4
sockets scoped by network domain. Name and socket
collisions reject within an assignment; the future agent must check other active
reservations atomically. Readiness is either none or a reference to an assigned
TCP port. Readiness is independent of outcome and cleanup. The two service Run fixtures resolve the same template to ports 18080/18081
on distinct agents. Their template digests match and assignment digests differ.
The additional `run-distinct-socket-number` and `run-distinct-socket-network`
fixtures vary the two supported socket identity components independently.
`duplicate-assigned-socket` uses different port names for the same socket and
must reject. TCP and IPv4 remain fixed by this protocol version.
The checker verifies fixture resolution; future scheduler intake must perform
that same resolution before committing a Run. There is no string interpolation
or shell substitution language. A schema cannot verify executable behavior. No service may be replaced until
cleanup or external fencing satisfies the eventual execution contract.

## Identity, ordering, and authority

`identity` includes cluster, recovery incarnation, `jobKey` (role, environment,
name), instance, immutable
attempt, process, and run. New execution retries would change run identity; new
placement attempts change attempt identity. Retry is disabled (`maxRuns: 1`).
`target`/`source` includes stable enrolled node, durable journal incarnation,
host boot, and container/runtime incarnation. Daemon restart preserves runtime;
container recreation changes runtime even if boot and journal persist. Session
belongs to delivery authority, not the immutable execution identity. IDs are
opaque, case-sensitive ASCII tokens; their human-readable fixture values are not
an identity generation algorithm.

Desired revision, per-attempt observation sequence, and per-node journal outbox
cursor are different counters. All are decimal strings in unsigned 64-bit range
0..18446744073709551615, with no sign or leading zero. Sequence scopes to immutable
attempt; cursor scopes to `(cluster, incarnation, node, journal)`. A journal reset
must change journal identity. Do not compare counters across these scopes or
convert them to floating-point numbers. Zero is the empty/baseline counter.

Run and Stop bodies are immutable. Body identity/content is the dedupe key:
identical command ID and body digest returns the prior outcome; conflicting ID
reuse rejects. A Stop targets the attempt in its identity (including the one
process/run of this slice), carries required `reason` (`cancel`, `restart`, or
`drain`), remains permanent, and its deadline cannot be extended
by replay. Cancel removes desired membership; restart retains membership but waits for
confirmed cleanup/fencing before a replacement, and drain follows that same
no-overlap rule. Stop reason preserves intent; it does not by itself prove the
scheduler committed that intent. These are required future reducer behaviors,
not implemented reducers.

`Delivery` carries the body and its SHA-256 separately from scheduler epoch and
connection session. Refreshing authority changes the envelope but preserves the
body digest. The two delivery fixtures and test assert this. A receiver must
validate authenticated peer, enrolled node, cluster/incarnation, current session
and accepted epoch before admission. Matching JSON fields and a digest are not
authentication. Epoch change alone cannot fence a partitioned worker. This package
checks envelope/body cluster consistency but has no trusted peer/session store.

## Canonical bytes and hashes

The alpha fixture profile accepts printable ASCII string values, keys defined by
the schema, nonnegative integral JSON numbers no greater than 2^53-1, booleans,
arrays, and objects. Duplicate keys, fractional/exponent numbers, non-finite
numbers, unknown versions, and conflicting nested versions reject. Counters use
strings as above. Non-ASCII manifests are outside this bounded profile.

Canonical encoding recursively sorts object keys by ASCII byte order, preserves
array order, emits no whitespace, encodes strings with only quote and backslash
escaped, and emits numbers in unsigned base 10 with no leading zeros. Bytes are
UTF-8 (identical to ASCII for this profile), with **no final newline**. This is a
bounded Aurora encoding, not a claim of full RFC 8785 implementation. Human-readable
fixture whitespace/key order has no effect. Checkers add one newline only for
stdout framing and remove it for hash comparison.

* Template digest: SHA-256 of canonical `Job.template`; job identity, desired
  instance count, revision, and completion policy are excluded.
* Assignment digest: SHA-256 of canonical object containing exactly `identity`,
  `target`, `desiredRevision`, `templateSha256`, and `assignment` from Run. Includes resolved instance,
  process/run values and ports, excluding command ID and authority.
* Command dedupe digest: SHA-256 of the entire canonical Run or Stop body, including
  command ID and target. This is `Delivery.bodySha256`.

`fixtures/golden.json` contains reviewable canonical text plus whole-document
SHA-256 vectors. `fixtures/hashes.json` pins template and assignment vectors.
Expected files are checked, never regenerated by the conformance runner.

## Inventory and ACK requirements

Inventory pages share source, snapshot ID, generation, and observation watermark.
Pages start at zero and advance contiguously; exactly the final page sets `last`.
The fixture's first page alone cannot establish absence. A receiver must collect
all pages from the same frozen snapshot, reject conflicting duplicates/mixed
metadata, then atomically apply the complete inventory and later observations
above its watermark. Retention gaps require resnapshot, not inferred absence.
Snapshot generation is scoped to source/journal and snapshot identity.

Observations have separate state, readiness, and cleanup; succeeded with pending
cleanup intentionally demonstrates that terminal outcome does not release capacity.
The source journals observations before send. Scheduler ACK names only the highest
contiguous **committed** node cursor and can never cross a missing/uncommitted event.
Receiving or parsing a message does not authorize an ACK. These examples establish
wire shape and large-counter preservation; persistence, replay dedupe, snapshot
assembly, ACK gap handling and crash tests remain AGENT-01/STORE-02/EXEC-01 work.

## Reproduce checks

From repository root, with a Java 25+ JDK on PATH, Python 3 and the declared dependency
`jsonschema==4.19.2` (`conformance/requirements.txt`):

```sh
python3 protocol/native-v1alpha1/conformance/check.py
mkdir -p /tmp/aurora-protocol-classes
javac --release 25 -d /tmp/aurora-protocol-classes protocol/native-v1alpha1/conformance/Canonical.java
GOCACHE="$PWD/.pi-tools/go-cache" GOTOOLCHAIN=local GOMAXPROCS=2 .pi-tools/go1.27.1/go/bin/go build -o /tmp/aurora-protocol-go protocol/native-v1alpha1/conformance/canonical.go
python3 protocol/native-v1alpha1/conformance/check.py 'java -cp /tmp/aurora-protocol-classes Canonical' /tmp/aurora-protocol-go
```

Current Java 25/26 and Go tool archives are pinned in
[`build-support/native/toolchains.json`](../../build-support/native/toolchains.json).
The Java 8 archive used for the original experiment is historical provenance in
Git and is no longer a build requirement.

The independent adapters use Java 25+ and Go standard libraries. They parse and
re-encode all valid wire vectors, including numbers above JavaScript's safe range
encoded as strings, escaped characters and refreshed authority. Python verifies
their exact output bytes and SHA-256 values. These standalone adapters do not
implement schema or admission checks. Structural/semantic rejection tests now
also run through the reusable Java/Go validators with
`conformance/check.py --validator 'command'`; see the linked validator packages.
Twelve shared parser-negative vectors execute in Python and both adapters:
duplicate keys, trailing JSON, malformed syntax, fractional/exponent/negative
numbers, leading zero, unsafe numeric integer, malformed Unicode escape,
non-ASCII/control object keys, and negative zero. Python checks raw integer
lexemes before decoding can normalize `-0` to zero; keys and string values share
the same printable-ASCII check. Each adapter subprocess has a 10-second timeout. These are bounded canonical-profile
rejections, separate from Python schema/semantic rejection tests.
Neither adapter is suitable as a production protocol parser without integration,
input limits and security review. No scheduler dependency or global tool install
is required. Use the temporary toolchain provenance in the build-support tooling;
the paths above can be replaced with equivalent local compilers.
