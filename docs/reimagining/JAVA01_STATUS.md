# JAVA-01: native Java 25 and Gradle 9

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
Exec tasks. Deprecations fail the build. Two separate output roots must produce
identical scheduler/protocol JARs, TARs, ZIPs and installed distributions. The
runtime uses a separately verified JRE; actual module enumeration checks for
required runtime APIs and excludes development tools. SQLite JNI is explicitly
authorized for the classpath with other unauthorized native access denied.

Use [the native build guide](../../build-support/native/README.md). New clusters
require a verified image bundle. Existing Java 8 lab instances retain their
operational commands and are preserved throughout this migration.

## Verification status

The modern Gradle build has passed all 34 Java tests and both installed runtime
boundaries. All six Java archives match across independent output roots.
The clean-checkout image build, saved Java 8 state/TLS compatibility check and
full three-round/ten-minute cluster qualification are in progress. This report
will record their final evidence before JAVA-01 is marked complete.

## Scope after JAVA-01

JAVA-02 covers any justified dependency-family upgrades on the native graph.
JAVA-03 includes the broader release/launcher matrix and Java 26 latest-GA lane
from the original research recommendation. The retired root build, legacy
buildSrc/Thrift/JMH/DI/HTTP stacks are outside this native profile. This slice
makes no claim that those legacy targets now build with Gradle 9.

Release selection was checked against the official
[Temurin 25.0.4.1+1 release](https://github.com/adoptium/temurin25-binaries/releases/tag/jdk-25.0.4.1%2B1),
[Gradle release metadata](https://services.gradle.org/versions/current) and
[Java compatibility matrix](https://docs.gradle.org/current/userguide/compatibility.html).
Exact archives and SHA-256 values are committed in
[toolchains.json](../../build-support/native/toolchains.json).
