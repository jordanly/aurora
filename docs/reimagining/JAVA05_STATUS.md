# JAVA-05: bounded input and JDK helper qualification

JAVA-05 is complete for the retained standalone profile at source commit
`ad0b73b8dd9b213cd68be9b3dadfe0e0e9d55840`. Formatting was committed separately
at `a18d315b4`. The machine-readable
[evidence ledger](java05-evidence.json) binds the qualification to the frozen source.
Java 25 remains the default. Fork and local `master` were rechecked against
Apache upstream at `11ebaeeb071cb182c388a40755e84f60dda32260`; the implementation
remains on `codex/standalone-foundations`.

The change replaces handwritten SHA-256 hex loops with `HexFormat`, changes a
private fixed counter set to `Set.of`, and replaces manual bounded readers with
`readNBytes(limit + 1)`. Exact-limit documents remain accepted; oversized input
consumes at most limit plus one byte before rejection. `Json.read` leaves its
caller-owned stream open, while `ProtocolTool` retains its try-with-resources
ownership of the file. Existing error categories/messages, canonical bytes,
lowercase hash output, UTF-8 handling, and protocol limits are preserved.
The five added `JsonTest` cases cover partial bulk reads at the exact limit,
limit-plus-one rejection, source-exception identity, UTF-8 hashes and leading-zero
SHA digests. The three input tests also check that streams remain open after
success and both failure paths. The limit remains 1 MiB, inclusive.

The original packaged CLI accepts a valid document padded to exactly 1 MiB and
rejects 1 MiB + 1 byte with exit 2, empty stdout and the exact redacted error.
The previous JAVA-03 bundle and all three new profiles produce identical exit
codes and output bytes for these boundary probes. Canonical/hash and refreshed
authority fixtures also retain their exact results.

| Profile | Java tests | Launcher commands | Physical cases | Bytecode major |
| --- | ---: | ---: | ---: | ---: |
| java25 | 46 | 61 | 23 | 69 |
| java26-runtime | 46 | 61 | 23 | 69 |
| java26 | 46 | 61 | 23 | 70 |

Each profile passed 46 Java tests and 61 packaged launcher commands. The physical
gate passed 23 cases total: authentication, three recovery/restore rounds,
and a mixed workload lasting at least 600 seconds with 20 batches and two replicas.
Each qualification ended with 40 attempts terminal, cleaned and unreserved;
all owned containers and networks were removed.
Six archives, two installed trees and 29 compiled classes were verified per
profile; 48 packaging tests, 72 lab tests and test/vet/module verification for
all three Go modules passed. Each
profile recorded 13 compatibility executions. The two own application JARs are
identical between Java 25 and runtime-only Java 26. The ledger records seven unchanged
external JARs, three unchanged Go executables, strict native-access/runtime
flags, zero JDK internal-API edges and no own JDK deprecation references; 35
optional excluded edges remain documented. The pinned JDK and runtime releases
remain those qualified in JAVA-03. All three builds used a clean checkout and a
verified warm cache. Raw bundles, logs and private lab state remain on this Pi
under `.cache/java05-*` and `.pi-lab/java05-*`; the tracked ledger records their
hashes and references. See the [native build guide](../../build-support/native/README.md).

The four exploratory benchmark variants recorded these medians (RSS is MiB,
threads/FDs are sampled, and GC is p95 milliseconds):

| Variant | State GET ms | Batch submit to complete ms | RSS MiB | Threads/FDs | GC p95 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| JAVA-03 baseline | 26.82 | 733.11 | 118.02 | 27/23 | 67.22 |
| New Java 25 | 27.16 | 679.27 | 120.91 | 27/23 | 57.55 |
| Java 26 runtime-only | 27.60 | 679.50 | 127.12 | 28/23 | 39.52 |
| Compiled Java 26 | 27.53 | 654.95 | 124.44 | 28/23 | 61.93 |

The twelve trials ran sequentially after the other gates, using the same
harness, workload, diagnostics and resource limits. Each ran six batches and
two services, including scheduler recovery and cleanup. These exploratory
measurements do not establish statistical significance or a production SLO.
RSS, threads and file descriptors are sampled rather than lifetime peaks;
API timings include fresh mTLS connections and controller/polling delays where
applicable. File references, checksums and cleanup receipts are bound in the ledger.

The original MVP remains unchanged: the ledger verifies two nodes, five
containers, and the unrelated healthy Home Assistant container. Java 8
compatibility adapters, legacy SQL classes and the Java 8 root build remain
unchanged. This slice does not alter persisted schemas, protocol models,
generated code or legacy Guava/DI/HTTP/Quartz consumers. The next bounded task
is [JAVA-06](JAVA25_REFACTOR_TASKS.md).
