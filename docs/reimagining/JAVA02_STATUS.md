# JAVA-02: maintained native dependencies

JAVA-02 is complete for the retained native profile. The clean-checkout build,
both compatibility lanes and full Pi cluster qualification passed on
2026-09-10. [The evidence ledger](java02-evidence.json) binds the results to source
commit `7bfb6b8ce4fb152c5bc003c0224778bd608b4a25` and immutable image IDs.

This advances the retained native runtime after the qualified
[Java 25 baseline](JAVA01_STATUS.md). The native dependency graph contains seven
external JARs; legacy Guice, Jetty, Netty, Mesos and Python worker dependencies
are outside it. This slice keeps the native API, canonical JSON contract and SQL
schema stable.

## Selected dependency family

| Component | JAVA-01 | JAVA-02 |
| --- | --- | --- |
| Jackson core/databind | 2.18.4 | 2.22.2 |
| Jackson annotations | 2.18.4 | 2.22 |
| networknt JSON Schema Validator | 2.0.4 | 2.0.7 |
| SLF4J API/NOP | 2.0.17 | 2.0.19 |
| SQLite JDBC | 3.53.4.0 | 3.53.4.0, retained |

The released [Jackson BOM](https://repo.maven.apache.org/maven2/com/fasterxml/jackson/jackson-bom/2.22.2/jackson-bom-2.22.2.pom)
aligns annotations 2.22 with core/databind 2.22.2. Jackson's
[2.22.2 release](https://github.com/FasterXML/jackson/wiki/Jackson-Release-2.22.2)
includes parser-limit security fixes. Its maintained 2.x API fits this native
profile. These fixes justify the update; they do not establish exploitability
through Aurora's bounded JSON input profile.

The validator's [released Maven POM](https://repo.maven.apache.org/maven2/com/networknt/json-schema-validator/2.0.7/json-schema-validator-2.0.7.pom)
already selects Jackson 2.22.1. Updating to that branch's latest patch avoids
overriding it down to the separate 2.21 LTS line. Its maintained
[2.x changelog](https://raw.githubusercontent.com/networknt/json-schema-validator/2.x/CHANGELOG.md)
records the security dependency update and subsequent API compatibility fixes.
The GitHub 2.0.7 tag currently exposes a Jackson 3 POM while the released Maven
POM and maintained 2.x branch agree on Jackson 2. This build verifies the released
Maven binaries by SHA-256; it does not claim source equivalence to that tag.

[SLF4J 2.0.19](https://www.slf4j.org/news.html) is the current stable API/provider
pair. Maven metadata also lists prereleases; no alpha or release-candidate
artifacts were selected. [SQLite JDBC](https://github.com/xerial/sqlite-jdbc/releases/tag/3.53.4.0)
is already current, so its exact qualified native library is retained. Java,
Gradle, Go and the Debian image pins remain those of JAVA-01.

## Implementation and verification

Both Gradle projects use the Jackson BOM, and SLF4J API/provider versions agree.
The six protocol JAR hashes and scheduler's seven-JAR boundary were updated
together. The builder now records both resolved runtime dependency graphs.
The validator uses Jackson's current property-iteration API in place of two
deprecated `fields()` calls; parsing and canonicalization rules are unchanged.

The rollback harness supports explicit old/new JVM majors and opt-in dependency
writes. On private snapshot copies, it verifies existing history, new canonical
Job/Run/Stop records, replay/conflict behavior and SQL rollback in both directions.
Snapshots made by the new stack reopen on both stacks;
reverse writes reopen on the new stack without creating a further snapshot.
The harness preserves the original snapshot, config, TLS files and bundles.
These synthetic store checks make no workload execution claim; the physical
two-agent qualification is a separate gate.

## Verified results

The build ran from `.cache/java02-fresh-source`, a clean checkout with no
preexisting `.pi-tools`. Its bundle is `.cache/java02-fresh-bundle`; the manifest
SHA-256 is `e22ff2dc06f95f9f1d0cb89b4f3f9e561868f62cb56aae8749616ab60ed42ef9`.
All 280 artifact hashes and 62 report hashes were independently verified, along
with all five actual Docker image identities and their content boundaries.
Eight build probes were removed. The toolchain pins, all 263 JRE files and all
three Go executables are identical to the JAVA-01 bundle.

| Gate | Actual result |
| --- | --- |
| Java tests | 34 passed: 7 protocol, 13 scheduler engine, 14 SQL; no failures, errors or skips. |
| Python checks | 41 packaging and 70 lab tests passed. |
| Go checks | Tests, vet and module verification passed for the agent and both lab helpers. |
| Java artifacts | Six JAR/TAR/ZIP hashes and both installed distribution trees match across separate output roots; all 26 compiled main/test classes target major 69. |
| Installed launchers | Java protocol and Go CLI each passed 13 valid/hash, 27 invalid and 12 parser-rejection vectors; the generated scheduler launcher opened SQLite with empty stderr. |
| Dependency compatibility | JAVA-01 Java 25 ↔ JAVA-02 Java 25 and CUT-01 Java 8 ↔ JAVA-02 Java 25 both passed complete-state, canonical write/replay, conflict, rollback and new-stack snapshot checks. |
| Existing TLS material | Both stacks loaded earlier PKCS12 key/trust stores and initialized key managers in each lane. All 691 and 536 original input hashes remained unchanged. |
| Physical cluster | All 23 cases passed over three recovery rounds and 600.778 seconds of mixed work: 20 batches and two HTTP replicas. |
| Terminal state and cleanup | All 40 attempts terminal, cleanup complete and unreserved; zero qualification containers/networks or packaging probes remain. |

Both compatibility lanes started with 9 jobs, 18 attempts and 30 commands.
Advancing epoch 7 to 8 changed no other state. New-stack writes added one job,
one attempt and Run/Stop commands (10/19/32); reverse old-stack writes added
another set (11/20/34). All 13 state outputs per lane matched their expected
complete state, with empty execution stderr. Only private snapshot copies were
written. These checks do not claim a live workload migration, TLS handshake or
PKCS12 rewrite; the physical cluster gate separately exercises mTLS and real
processes. Full compatibility results remain in `.cache/java02-dependency-compat`
and `.cache/java02-java8-compat`.

The qualification run was `80206aa359fd5388`. Its full results, private state and
cleanup receipt remain in `.pi-lab/java02-qualification`. The runtime audit
confirmed the five manifest image IDs, production entrypoints and absence of
executable bind mounts.

The original Java 8 MVP remains healthy: both agents reachable, the same two
ready service attempts passing physical HTTP probes, and one completed batch.
All five original containers retain their IDs, start times and zero restart
counts. The unrelated Home Assistant container is unchanged and healthy.
Local master, fork master and upstream master were rechecked at
`11ebaeeb071cb182c388a40755e84f60dda32260`.

## JDK and native library audit

Static `jdeps` analysis of all nine shipped JARs found no JDK-internal API edges.
The 35 missing edges belong to excluded optional validator date/time, YAML and
regex engines, or SQLite's Graal native-image integration. They do not originate
in the native Aurora classes. The optional features are outside the bounded
JSON schema profile. Static analysis does not exhaustively detect reflection.
`jdeprscan` found only the existing native HTTP client's supported deprecated
`URL(String)` constructor. No new runtime module-path or access flags were added.

SQLite's retained Linux/AArch64 JNI entry has SHA-256
`4e253e3f886f8da539e6d8fbd92c3a281f50e395bcba568ced953865e8bea5be`.
Both ELF load segments use 64 KiB alignment and are congruent for this Pi's
16 KiB pages. The production scheduler entrypoint, generated launcher, SQL tests
and physical qualification all loaded the actual library successfully. The
ledger links the exact JAR, ELF, diagnostics and resolved-graph evidence.

Java reproducibility covers archive bytes and installed content; it does not
claim identical Docker rebuild IDs. Dependency selection and hash verification
are not a comprehensive vulnerability audit.

## Scope after JAVA-02

JAVA-03 covers broader launcher/HTTP/auth qualification, the Java 26 lane and
measured runtime/resource comparisons. Jackson 3 is a separate migration with
package and default-behavior changes, not a requirement for using Java 25.
The native HTTP client's existing supported deprecated URL constructor remains
for that HTTP work. Legacy-root build targets and production HA stay outside
this native profile.
