# JAVA-08: store ownership and daemon lifecycle

JAVA-08 is complete and qualified at frozen implementation source
`e446dda4284ef5b99bf4f5843bfcfc269634b81f`.

The SQL store now attempts every acquired ownership cleanup when initialization
fails, preserves the primary exception, and attaches secondary cleanup failures
as suppressed exceptions. Failed JDBC configuration closes its connection too.
Repeated close is explicitly a no-op after cleanup has been attempted, including
when that attempt reported an error. The shared SQL sources still compile with
the actual Java 8 compiler and with Java 25 `--release 8`.

The standalone main now uses a `NativeDaemon` lifecycle owner for the store,
HTTPS server, controller, request executor and shutdown hook. Partial startup
unwinds through that owner. Close requests server shutdown and cancellation of
queued and active executor work, then waits against one shared 15-second deadline. It
releases the store only after the server has stopped successfully and both
worker pools have terminated. A timeout retains the store and hook; a later
close can finish once work has stopped. Interruption is restored after cleanup.
Hook failures release the main wait and report only the exception class.

The JDK server needs special care during partial startup. Stopping an unstarted
server alone did not drain its registered selector during partial-startup probes.
A named daemon cleanup thread starts a cleanup dispatcher, using an ephemeral
loopback binding if the listener never bound, and stops it. That helper also
keeps the JDK's internal stop/join outside the caller's bounded wait. A VM-level
failure to create those threads is reported while ownership is retained; it is
not claimed recoverable. The bound covers waiting for server/workers, not an
arbitrary blocking implementation of store close or the entire JVM exit.

These are hardening findings from source review and failure injection. The
original audit did not reproduce a production resource leak.

## Qualification

The [evidence ledger](java08-evidence.json) binds every result to the source,
artifacts and images. Each of `java25`, `java26-runtime` and `java26` passed:

| Gate | Per-profile result |
| --- | --- |
| Java | 62 tests: protocol 8, JSON 5, engine 16, SQL 19, daemon 6, HTTP 8 |
| Python | 75 lab tests and 56 packaging tests |
| Go | Tests, vet and module verification for all three modules |
| Packaging | Reproducible six Java archives, two installed trees and all 40 compiled classes; exact boundary/image payload checks |
| Installed launchers | 64 commands; all exit codes and stdout unchanged against JAVA-07 |
| Physical cluster | 23 passing cases, three recovery rounds and a 600-second mixed workload with two services and 20 batches |

Each physical gate ended with 40 terminal, cleaned, unreserved attempts. Live
runtime audits verified all five containers per profile and the intended JRE and
strict access flags. All scheduler JARs changed as expected; the protocol JAR,
seven external JARs, three Go executables and all 57 fixtures remain byte-identical
to the same-profile JAVA-07 bundle. Four negative scheduler CLI cases per profile
changed only Aurora stack-frame source line numbers. The ledger retains all 12
raw differences; it does not claim byte-identical stderr for those cases.

New coverage comprises five SQL failure/cleanup tests, six daemon lifecycle
tests and one blocked HTTPS handler test. It covers primary/suppressed exception
identity, contested owner locks, connection setup failure, occupied ports,
shutdown-hook registration failure, interruption, cooperative cancellation,
uncooperative work retaining the real owner lock through a timeout, and close
retry after quiescence. The existing owner/schema, rollback, replay, request
bounds, TLS and crash/recovery checks remain part of qualification.

An isolated Java 25 process performed five warmups and 32 occupied-port failed
starts, observed 37 store-close callbacks, and retained exactly 17 other
descriptors and two sockets before and after the measured loop. This is a bounded descriptor
probe, not a general leak proof. Its compiled production classes are matched to
the final fresh build. A separate SQL preflight passed all 19 cases. Both Java 8
source-compilation variants produced 11 class files with major version 52.

Initial preflight failures and the first fresh-build attempt are preserved. That
attempt's Java 25 build passed, but Java 26 runtime exposed a TCP-reset race in
the new shutdown test's listener probe. The test now keeps waiting through a
reset and still requires connection refusal within the original deadline. Only
the test changed in `e446dda42`; all three final profiles were rebuilt from it.
The archived attempt is excluded from accepted qualification. Development
preflight preceded the frozen commit; its compiled production classes are checked against the final build. The separate Java 8 compilation
receipt uses `af82e5420`, with unchanged SQL source hashes checked separately.

## Compatibility and measurement

Four private compatibility pairs passed 13 state operations each: JAVA-07 Java
25 against each current profile, plus the historical Java 8 bundle against
current Java 25. They cover inspection, epoch advancement, backup/reopen,
bidirectional writes and command replay. Every original input hash remained
unchanged. The exact bodies defining `JobKey`, journal/row classes and all three
schema methods also match JAVA-07.

Six sequential normal workload trials compare JAVA-07 and JAVA-08 on Java 25.
Median state reads were 25.7/26.8 ms, batch completion 690.4/698.7 ms and recovery
3529.8/3536.6 ms. GC pause p95 was 7.6/65.7 ms; that difference is retained, and
the small ordered sample does not establish performance neutrality or benefit.
Six further paused-agent/load trials passed their progress, recovery and cleanup
conditions while preserving every unsuccessful burst request in the evidence.

The [JAVA-09 decision](JAVA09_SQL_RECORD_DECISION.md) retains the public SQL row
classes and their established Java 8 source/helper contracts. The
[JAVA-10 study](JAVA10_CONCURRENCY_DECISION.md) records the normal workload and
paused-agent/operator-burst measurements, their limits, and the separate
concurrency and HTTP transport decisions. Java 25 remains the default.

Final host verification found no containers or networks remaining for the 15
accepted test runs and no packaging probe containers. The original Java 8 MVP
kept its five container IDs/start times, zero restarts, the same two physically
ready service attempts and completed batch. Home Assistant kept its container
identity, start time and healthy state. No physical run was aborted; the earlier
failed build remains separately recorded. Local master and both remote master
refs were reverified at upstream `11ebaeeb071cb182c388a40755e84f60dda32260`.

Against the preceding JAVA-07 checkpoint, the frozen slice changes add 199 net
production Java lines, 407 Java test lines, 214 Python test lines and a 331-line
load-study harness. Most added executable code is validation; this is lifecycle
hardening rather than a source-deletion slice. Documentation and evidence are
counted separately.

The next execution-capability task in the broader Aurora roadmap remains
SUPERVISE-01. Completing these Java audit follow-ups does not add supervision,
Thermos, update policy, cron, UI or production HA capabilities.
