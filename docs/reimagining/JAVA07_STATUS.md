# JAVA-07 and LAB-03: grouped validation and complete HTTP responses

JAVA-07 and LAB-03 are complete at frozen source
`c2e9061ba4cc23f629e3ff0c26a98a7a9e17c0a8`. The lab fix is separately committed
at `1c77990c3`; the Java switch change is `c2e9061ba`. The
[evidence ledger](java07-evidence.json) binds the builds and qualification to
that source. Fork and local master still match upstream
`11ebaeeb071cb182c388a40755e84f60dda32260`. Java 25 remains the default, with
implementation on `codex/standalone-foundations`.

`ProtocolValidator.validate` groups token cases in an arrow switch. The null
loop sentinel, nesting increments/decrements, ASCII checks, original integer
spelling and range, float rejection and schema fallback retain their previous
behavior. All 57 protocol fixture files, including canonical/hash goldens, are
byte-for-byte unchanged. This refactor has zero net production Java lines.

The lab operator client retains its one bounded read of at most 1 MiB + 1 byte,
size check and HTTP-status check. It then rejects an unsatisfied declared body
length with `http.client.IncompleteRead`, before JSON parsing. Existing recovery
polling already retries that exception. Complete malformed JSON continues to
raise `JSONDecodeError`; chunked and close-delimited responses retain their
handling. Connection closure and timeouts are unchanged.

Three new lab test methods cover real HTTP response framing, exact-limit and
oversized reads, exception precedence, connection closure and narrow recovery
retry behavior. The oversized fixture proves only limit plus one bytes are read.
A targeted old/new regression proof shows that the old method accepts a valid
JSON prefix from an incomplete body and reports malformed truncated JSON as a
parse error. The fixed method passes both cases as framing failures. This fixes
the mechanism implicated by the [JAVA-06 benchmark failure](JAVA06_STATUS.md);
the original failed response bytes remain unavailable, so its exact cause is
not retrospectively proven. The lab fix adds two implementation lines and 86
test lines.

| Profile | Java tests | Launcher commands | Physical cases | Mixed batches | Final attempts |
| --- | ---: | ---: | ---: | ---: | ---: |
| java25 | 50 | 64 | 23 | 20 | 40 |
| java26-runtime | 50 | 64 | 23 | 20 | 40 |
| java26 | 50 | 64 | 23 | 20 | 40 |

Each profile passed three recovery/restore rounds and at least 600 seconds of
mixed service/batch work. All final attempts were terminal, cleaned and
unreserved. All owned qualification containers and networks were removed.
Each build also passed 75 lab tests, 48 packaging tests and test/vet/module
verification for all three Go modules. Six archives, two installed trees and
32 compiled classes were verified per profile; the additional compiler-generated
class implements the enum switch. Bytecode majors remain 69/69/70, with no
preview features. All 64 installed launcher exit codes and stdout/stderr bytes
match the corresponding JAVA-06 profile.

Scheduler application JARs match JAVA-06 byte-for-byte for their respective
compiler profiles. Seven external JARs, three Go executables and all stored-model
sources are unchanged. SQL/dependency compatibility was therefore not rerun;
its previous qualification is recorded in [JAVA-06](JAVA06_STATUS.md). The new
profile gates exercise the changed protocol and lab client with actual mTLS,
process execution, recovery and isolated restore.

The first Java 25 qualification attempt stopped after seven successful cases
on a backup-response read timeout. Snapshot files were created, but the client
did not reach the restore stage. Cleanup succeeded. This failed run is retained
separately in the ledger. The complete gate was repeated in a new root after
all builds finished, with unchanged binaries and timeouts, and passed. Concurrent
build I/O is a possible contributor; the evidence does not establish the
underlying wait. The LAB-03 completeness check runs after reading and cannot
cause this read timeout.

After all profile gates, three sequential compiled Java 26 benchmark trials
passed with six batches, two services, scheduler crash recovery and cleanup in
each trial. These exercise the previously failing workload path. Raw timings
and resource samples are retained and their 13 aggregates recomputed, but no
cross-version performance comparison or production SLO is claimed.

Final host verification found all seven owned runs removed, including the
aborted qualification. The original Java 8 MVP retains its same five containers,
two service attempts and zero restarts; Home Assistant is unchanged and healthy.
Raw bundles, logs and private state remain under `.cache/java07-*` and
`.pi-lab/java07-*`. Legacy source, persisted schemas and public protocol models
are unchanged. The next Java task is [JAVA-08](JAVA25_REFACTOR_TASKS.md), store
resource ownership and daemon lifecycle hardening. Public SQL records and
concurrency remain conditional follow-ups.
