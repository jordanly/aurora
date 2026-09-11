# JAVA-03: qualified native HTTP, launchers and Java profiles

JAVA-03 is complete for the retained Linux ARM64 native scheduler/protocol
profile. Three clean builds, three full physical cluster qualifications and
twelve sequential measurement trials passed on this Pi. [The evidence
ledger](java03-evidence.json) binds new-profile results to source commit
`c929970d048ade532c52bddbdf9b9104e255c6d5`, bundle hashes and image IDs;
the JAVA-02 comparison retains its own recorded source and bundle identity.
Java 25 remains the default; both Java 26 profiles are explicit options.

The implementation follows the qualified [JAVA-02 dependency baseline](JAVA02_STATUS.md).
The native HTTP client now constructs URLs through `URI.toURL()`. Seven real
loopback TLS tests cover authenticated success, absent/untrusted clients, wrong
operator role, server hostname verification, redirect refusal, response/read
bounds, operator request/reply limits, strict routes, durable submission
replay/conflict, cancellation and snapshot reopen. Tests and the production
daemon share the TLS configurator. Unit fixtures explicitly trust private test
leaves; physical cluster tests separately exercise generated CA chains and Go agents.

Three build profiles distinguish changing the runtime from raising the minimum
class-file version. The runtime-only Java 26 profile compiles for release 25;
its two application JARs are byte-identical to the Java 25 profile. All own
classes have the selected major version and minor version zero, excluding
preview bytecode. Java 26 execution denies illegal final-field mutation in
addition to the existing strict native-access policy. The seven external JARs,
three Go executables, Job contract and SQL schema remain unchanged from JAVA-02.
The default JRE's 263 files also remain byte-identical.

The checksum-pinned [Temurin 26.0.2.1+1 release](https://github.com/adoptium/temurin26-binaries/releases/tag/jdk-26.0.2.1%2B1)
provides separate ARM64 JDK and JRE archives. The retained
[Gradle compatibility matrix](https://docs.gradle.org/current/userguide/compatibility.html)
supports Java 26; Oracle documents the
[final-field-mutation restrictions](https://docs.oracle.com/en/java/javase/26/migrate/preparing-final-field-mutation-restrictions.html).
Both Java 26 profiles actually executed on Linux ARM64 with 16 KiB pages.

| Profile | Compiler / runtime | Class major | Physical run | Mixed workload |
| --- | --- | --- | --- | --- |
| `java25` | 25 / 25 | 69 | `f8a2696aae90fbef` | 600.701 s |
| `java26-runtime` | 25 / 26 | 69 | `dd46658c01200f63` | 601.022 s |
| `java26` | 26 / 26 | 70 | `f7b09b1bfd263481` | 601.006 s |

All three builds ran sequentially from the clean `.cache/java03-fresh-source`
checkout, without preexisting `.pi-tools`. Their bundles remain under
`.cache/java03-{profile}-bundle`. Artifact/report hashes and all five actual
image identities per profile were independently audited. Each build removed
its eight probe containers.

| Gate | Result per profile |
| --- | --- |
| Java tests | 41 passed: 7 protocol, 13 scheduler engine, 14 SQL and 7 HTTP; no failures, errors or skips. |
| Python checks | 48 packaging and 72 lab tests passed. |
| Go checks | Tests, vet and module verification passed for the agent and both lab helpers. |
| Reproducibility | Six JAR/TAR/ZIP files and two installed trees match across separate output roots; all 27 compiled main/test classes match the expected bytecode version. |
| Original installed launchers | 59 commands passed: 13 valid/hash, 27 invalid and 12 parser vectors; exact JRE version; two equivalent empty SQLite inspections; four CLI rejection cases. Original inputs and raw stdout/stderr hashes verified. |
| Bidirectional state compatibility | JAVA-02 Java 25 against each new stack: 13 complete-state outputs matched, including canonical writes, replay/conflict, rollback and new-stack snapshot reopening. |
| Physical cluster | 23 cases passed: authenticated access, three rounds of batch/service execution, scheduler crash, network partition/stop/reconnect, agent crash/replacement, no resurrection, isolated backup restore, then ten minutes of mixed work. |
| Final state and cleanup | 20 mixed batches and two service replicas; all 40 final attempts terminal, cleaned and unreserved; zero qualification containers/networks remain. |

The three compatibility lanes preserve original history at epoch 7 with
9 jobs, 18 attempts and 30 commands. Advancing to epoch 8 changes no other
state; new writes produce 10/19/32 and reverse old-stack writes produce
11/20/34. Every execution has empty stderr. All original input hashes remain
unchanged (692 for Java 25; 686 for each Java 26 lane). These private snapshot
checks include PKCS12 loading/key-manager initialization, without claiming live
migration, keystore rewriting or TLS handshakes. Actual TLS and process behavior
are covered by the separate HTTP and physical tests.

Static `jdeps` scans across all nine shipped JARs found zero JDK-internal API
edges; `jdeprscan` found zero remaining JDK deprecation references in native
Aurora classes.
The same 35 optional missing edges remain in excluded validator features and
SQLite Graal native-image integration. Static analysis does not exhaustively
detect reflection. The exact previously audited ARM64 SQLite JAR is retained
and loads in all three actual runtimes.

The benchmark ran after all builds and qualifications finished, in the order
JAVA-02 baseline, new Java 25, runtime-only Java 26, and compiled Java 26.
Each variant ran three fresh two-agent trials through the same frozen harness,
with one-CPU container limits, 128 PIDs, 32–192 MiB JVM heap, two active JVM
processors and identical GC/NMT instrumentation. Every trial completed six
batches, kept the same two service attempts across scheduler recovery, stopped
them with physical port-release proof and cleaned all eight attempts and its
owned Docker resources. All twelve trials passed.

The table gives pooled sample medians except the explicitly labeled p95 rows;
startup, recovery and native-memory rows have three observations per variant.
Each variant has 90 timed GETs,
18 batch submissions and 24 process-resource samples. Full distributions,
per-trial values, raw GC/NMT logs, host temperature/load/frequency and cleanup
receipts are linked from the ledger.

| Measurement (median unless labeled p95) | JAVA-02 / Java 25 | New Java 25 | Java 26 runtime-only | Compiled Java 26 |
| --- | --- | --- | --- | --- |
| Cluster start, s | 7.12 | 7.18 | 7.10 | 7.12 |
| State GET, ms | 26.09 | 27.15 | 27.09 | 27.43 |
| Batch submit ACK, ms | 82.46 | 53.52 | 72.85 | 86.35 |
| Batch submit to completion, ms | 703.10 | 669.53 | 668.17 | 712.19 |
| Service submit to ready, ms | 1838.77 | 1781.81 | 1711.59 | 1742.87 |
| Scheduler crash to recovery, ms | 3729.44 | 3565.99 | 3739.85 | 3763.19 |
| Service stop to cleanup, ms | 762.35 | 791.09 | 831.64 | 732.43 |
| Backup ACK, ms | 49.87 | 45.95 | 49.60 | 46.80 |
| GC pause, ms | 4.44 | 4.19 | 3.99 | 3.42 |
| Sampled RSS, MiB | 120.06 | 119.63 | 127.37 | 125.18 |
| Sampled threads | 27.00 | 27.00 | 28.00 | 28.00 |
| Sampled file descriptors | 23.00 | 23.00 | 23.00 | 23.00 |
| State GET p95, ms | 74.79 | 93.70 | 89.94 | 87.74 |
| GC pause p95, ms | 19.90 | 72.09 | 70.23 | 9.01 |
| HotSpot-tracked non-heap committed, MiB | 90.07 | 90.73 | 92.39 | 91.35 |

These are exploratory sequential measurements on one Pi, with short warmup
and background host services. API timings include fresh mTLS connections;
submit ACK includes durable commit, and convergence includes polling and
controller delays. RSS, thread and descriptor samples are not lifetime peaks.
HotSpot-tracked non-heap commitment is measured from the recovered JVM after
cancellation; it excludes some third-party native allocations and adds NMT
overhead ([Oracle NMT documentation](https://docs.oracle.com/en/java/javase/26/troubleshoot/diagnostic-tools.html)).
Across the four variants, median state GET latency was about 26–27 ms and
scheduler recovery about 3.6–3.8 seconds. Both Java 26 profiles sampled higher
median RSS (125–127 MiB versus about 120 MiB) and 28 threads versus 27 on
Java 25. GC pause p95 ranged from about 9 to 72 ms across the variants.
Latency tails vary across these short trials. These results establish a measured
local baseline, without isolating JDK effects or establishing statistical
significance or a production performance SLO.
Java 25 remains the default deployment choice.

The original Java 8 MVP is preserved: the same five containers, original start
times and zero restarts; both agents reachable; the same two ready service
attempts passing physical HTTP probes; and one completed batch. Home Assistant
is unchanged and healthy. All Docker resources for the fifteen temporary
qualification/benchmark labs and all packaging probes are removed; private
evidence/state directories are retained. Earlier bundles and snapshot inputs
remain available. Local, fork and upstream master still agree at
`11ebaeeb071cb182c388a40755e84f60dda32260`.

The [native build guide](../../build-support/native/README.md) provides profile,
launcher, compatibility and benchmark commands. This completes P5 modernization
for the retained native profile. Legacy-root/buildSrc/Thrift/JMH/DI/HTTP targets,
other platform certification, production HA, rolling updates and workload
migration remain separate. Reproducibility covers Java archive bytes and
installed content, without claiming identical Docker rebuild IDs.

The next bounded slice is SUPERVISE-01: surviving attempt supervisors and daemon
reattachment, so an agent daemon restart can preserve execution and exact task
outcomes. Its acceptance must distinguish daemon, supervisor and whole-container
loss and rerun the physical recovery corpus on this modern native baseline.
