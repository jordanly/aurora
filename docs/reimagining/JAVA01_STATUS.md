# JAVA-01: native Java 25 and Gradle 9

JAVA-01 is complete for the retained native profile. The clean-checkout build,
Java 8/25 state compatibility check and full Pi cluster qualification passed on
2026-09-10. [The evidence ledger](java01-evidence.json) binds the results to source
commit `22387f356dd63c23e5d34737a81351fd9a23460d` and immutable image IDs.

The native scheduler/protocol build now uses pinned Temurin 25.0.4.1+1 JDK and
JRE archives with Gradle 9.7.1. This advances the isolated runtime qualified by
[CUT-01](CUT01_STATUS.md). The historical six-task research reports remain
unchanged; this slice modernizes the native code and packaging retained after
Mesos isolation.

Java compile tasks explicitly select Temurin and `--release 25`; tests and
JavaExec tasks use the same toolchain. Toolchain discovery/download is disabled
by the builder. Java 25 bytecode (major 69) is mandatory for every own runtime
class, and the builder checks compiled main/test outputs too. Seven external
runtime JAR pins, protocol semantics and durable state formats remain unchanged.

Removed Gradle APIs have been replaced with provider-based task configuration and
Exec tasks. Gradle deprecations fail the build. Two separate output roots must produce
identical scheduler/protocol JARs, TARs, ZIPs and installed distributions. The
runtime uses a separately verified JRE; actual module enumeration checks for
required runtime APIs and excludes development tools. SQLite JNI is explicitly
authorized for the classpath with other unauthorized native access denied.

Use [the native build guide](../../build-support/native/README.md). New clusters
require a verified image bundle. Existing Java 8 lab instances retain their
operational commands and are preserved throughout this migration.

## Verified results

The build ran from `.cache/java01-fresh-source`, a clean checkout with no
preexisting `.pi-tools`. Its bundle is `.cache/java01-fresh-bundle`; the manifest
SHA-256 is `d2c9caebf6a6e1ec25287b30460a3c6669458c9b9efea240696f978bbe7c36de`.
All 280 artifact hashes and 61 report hashes were independently checked, along
with the five actual Docker image identities and contents. Eight build probes
were removed. The production scheduler entrypoint opened SQLite successfully
on this ARM64 Pi's 16 KiB-page kernel.

| Gate | Actual result |
| --- | --- |
| Java tests | 34 passed: 7 protocol, 13 scheduler engine, 14 SQL; no failures, errors or skips. |
| Python checks | 38 packaging and 70 lab tests passed. |
| Go checks | Tests, vet and module verification passed for the agent and both lab helpers. |
| Java artifacts | Six JAR/TAR/ZIP hashes and both installed distribution trees match across separate output roots; all 26 compiled main/test classes target major 69. |
| Installed launchers | Java protocol and Go CLI passed 13 valid/hash, 27 invalid and 12 parser-rejection vectors each; generated scheduler launcher opened SQLite with empty stderr. |
| Java 8/25 compatibility | Both JVMs read identical complete state: 9 jobs, 18 attempts and 30 commands. Java 25 advanced epoch 7 to 8 and made a backup; Java 8 and 25 reopened the resulting state/backup identically. |
| Existing TLS material | Both JVMs loaded the earlier PKCS12 key/trust stores. All 535 original input hashes remained unchanged. |
| Physical cluster | All 23 cases passed over three recovery rounds and 600.831 seconds of mixed work: 20 batches and two HTTP replicas. |
| Terminal state and cleanup | All 40 attempts terminal, cleanup complete and unreserved; zero qualification containers/networks or packaging probes remain. |

The qualification run was `e7ed4eaa3090fd04`. Full results and its cleanup receipt
remain under `.pi-lab/java01-qualification`. Compatibility evidence remains under
`.cache/java01-jvm-compat`; only private copies of the earlier standalone backup
were opened for SQL writes. This verifies the recorded state/epoch/backup boundary,
not a live workload migration.

The original Java 8 MVP was preserved. Final authenticated reads and physical HTTP
probes confirmed both agents reachable, the same two ready service attempts and
one completed batch. Its five containers have no restarts; the unrelated Home
Assistant container remains healthy. Local master, fork master and upstream
master were rechecked at `11ebaeeb071cb182c388a40755e84f60dda32260`.

## Scope after JAVA-01

JAVA-02 covers any justified dependency-family upgrades on the native graph.
JAVA-03 includes the broader release/launcher matrix and Java 26 latest-GA lane
from the original research recommendation. The retired root build, legacy
buildSrc/Thrift/JMH/DI/HTTP stacks are outside this native profile. This slice
makes no claim that those legacy targets now build with Gradle 9.

The native HTTP client still uses a supported deprecated URL constructor; javac
reports that note. Java API deprecation cleanup remains separate from the Gradle
deprecation gate. Reproducibility here covers Java archive bytes and installed
content, not identical Docker rebuild IDs. Existing MVP capability limits remain
as documented in [the cluster guide](CLUSTER_MVP.md).

Release selection was checked against the official
[Temurin 25.0.4.1+1 release](https://github.com/adoptium/temurin25-binaries/releases/tag/jdk-25.0.4.1%2B1),
[Gradle release metadata](https://services.gradle.org/versions/current) and
[Java compatibility matrix](https://docs.gradle.org/current/userguide/compatibility.html).
Exact archives and SHA-256 values are committed in
[toolchains.json](../../build-support/native/toolchains.json).
