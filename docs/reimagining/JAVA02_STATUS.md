# JAVA-02: maintained native dependencies

JAVA-02 updates the retained native runtime after the qualified
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
Job/Run/Stop records, replay/conflict behavior, SQL rollback and snapshot reopen
in both directions. It preserves the original snapshot, config, TLS files and
bundles. These synthetic store checks make no workload execution claim; the
physical two-agent qualification remains a separate required gate.

The initial resolved-graph build passed all 34 Java tests and both installed
runtime boundaries. Final clean-checkout packaging, artifact reproducibility,
bidirectional dependency compatibility and the full three-round/ten-minute Pi
qualification remain in progress. JAVA-02 will be marked complete only after
those results and cleanup are recorded.

## Scope after JAVA-02

JAVA-03 covers broader launcher/HTTP/auth qualification, the Java 26 lane and
measured runtime/resource comparisons. Jackson 3 is a separate migration with
package and default-behavior changes, not a requirement for using Java 25.
The native HTTP client's existing supported deprecated URL constructor remains
for that HTTP work. Legacy-root build targets and production HA stay outside
this native profile.
