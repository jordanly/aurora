# JAVA-06: private records and committed polling results

JAVA-06 is complete for the retained standalone profile at source commit
`cc84b98b90727b8f1f48e6f525f63a72b5f57934`. The protocol change is separate
at `a72899e9e`; the scheduler change is `cc84b98b9`. The
[evidence ledger](java06-evidence.json) binds all qualification to this frozen
source. Java 25 remains the default. Fork and local `master` were rechecked
against upstream at `11ebaeeb071cb182c388a40755e84f60dda32260`; implementation
remains on `codex/standalone-foundations`.

`ProtocolValidator` uses a private `AssignedSocket` record in place of a
positional list and names the port-validation phase. All four identity
components remain strings: network, protocol, family and number. Duplicate
names still reject before duplicate sockets. Two valid fixtures exercise
different networks and numbers; a third rejects identical assigned sockets
with distinct names. The prior 53 fixture files and 13 existing canonical/hash
goldens remain unchanged; the two new valid fixtures add two golden entries.
TCP and IPv4 remain fixed by the existing schema.

`NativeEngine.poll` separates agent-state validation, observation validation
and transactional reduction. One write transaction returns a private
`PollResult`, replacing the captured mutable boolean array. The order remains:
validate, reduce observations and inventory, commit, publish the flag for unknown
reservations, check the committed cursor, then ACK and advance the page.
Neither record is a public SQL row or a serialized API object.

Three new polling tests verify rollback after inventory failure, preservation
of an existing unknown-reservation flag after observation failure, and an ACK
that observes committed cursor and attempt state through an independent reader.
All three also pass against the previous engine. Mutation probes confirm that
each test catches its intended violation: early flag addition, early flag
clearing, or ACK before commit. The last probe observes the old pending,
reserved attempt instead of the committed succeeded, cleaned attempt.
Probe sources, test classes, runtime JARs and intended assertion failures are
bound to the fresh build in the ledger.

| Profile | Java tests | Launcher commands | Physical cases | Bytecode major |
| --- | ---: | ---: | ---: | ---: |
| java25 | 50 | 64 | 23 | 69 |
| java26-runtime | 50 | 64 | 23 | 69 |
| java26 | 50 | 64 | 23 | 70 |

Each profile passed the complete three-round recovery and ten-minute mixed
workload gate. `java25` completed 19 batches and ended with 39 total attempts;
`java26-runtime` completed 19 batches and ended with 39 total attempts;
`java26` completed 20 batches and ended with 40 total attempts.
All final attempts were terminal, cleaned and unreserved.
All owned qualification containers and networks were removed. Each also passed
13 private snapshot compatibility executions, 48 packaging tests, 72 lab tests,
and test/vet/module verification for all three Go modules. Six archives, two
installed trees and 31 compiled classes were verified per profile. The original
installed launchers retain exact boundary and new socket-vector results against
the JAVA-05 baseline. Seven external JARs, three Go executables and the default
JRE are unchanged; the two own application JARs match between Java 25 and
runtime-only Java 26. JDK diagnostics report zero internal-API edges and no own
JDK deprecation references, with 35 documented optional excluded edges.

The exploratory benchmark ran twelve sequential trials after all other gates,
using the same harness, workload, instrumentation and resource limits. These
are the accepted trials; an earlier two-trial Java 26 series was aborted and
is retained separately below. The table reports medians except for GC p95:

| Variant | State GET ms | Batch submit to complete ms | RSS MiB | Threads/FDs | GC p95 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| JAVA-05 baseline | 26.14 | 732.33 | 119.19 | 27/23 | 8.35 |
| New Java 25 | 26.49 | 716.93 | 119.32 | 27/23 | 34.59 |
| Java 26 runtime-only | 26.40 | 650.04 | 126.01 | 28/23 | 7.78 |
| Compiled Java 26 | 26.75 | 756.00 | 127.16 | 28/23 | 58.48 |

Each trial ran six batches and two services, including scheduler recovery and
cleanup. These measurements do not establish statistical significance or a
production SLO. RSS, threads and file descriptors are sampled rather than
lifetime peaks; timings include fresh mTLS connections and controller/polling
delays where applicable. The ledger records raw trial references and recomputed
aggregates. Bundles, logs and private lab state remain on this Pi under
`.cache/java06-*` and `.pi-lab/java06-*`.

The first compiled Java 26 series passed trial 0, then trial 1 failed with a
JSON parse error during polling after the deliberate scheduler crash. Both
trials cleaned up. The complete three-trial series was repeated in a new run
root with unchanged binaries and harness; all three passed. The ledger retains
both original trial receipts, the failure and all cleanup identities, rather
than combining selected trials. A deterministic local probe shows that Python's
bounded HTTP read can return a short declared-length body without raising
`IncompleteRead`. This explains a likely lab-client failure path; the original
wire body was not retained, so its exact truncation is unconfirmed.
The focused LAB-03 follow-up will detect incomplete responses before parsing,
retain bounded reads, and continue rejecting complete malformed JSON.

The original continuously running Java 8 MVP still has the same five containers
and service attempts, with zero container restarts. The unrelated Home Assistant
container remains healthy and unchanged. Legacy Mesos source, Java 8 adapters,
SQL schemas and public/wire models remain unchanged. This slice adds a net 64
physical production Java lines, including formatting, and 106 Java test lines;
it deletes no files. The next bounded task is [JAVA-07](JAVA25_REFACTOR_TASKS.md),
grouped validation control flow. Lifecycle hardening remains ahead of any
concurrency experiment.
