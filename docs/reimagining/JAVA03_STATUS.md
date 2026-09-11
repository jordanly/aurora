# JAVA-03: native HTTP, launchers and JVM qualification

JAVA-03 is in progress after the qualified [JAVA-02 dependency baseline](JAVA02_STATUS.md).
Java 25 remains the default deployment profile. Completion requires fresh builds,
the full physical Pi gate, installed-launcher and state compatibility checks,
JDK diagnostics and a measured comparison with that baseline.

The native HTTP client now constructs URLs through `URI.toURL()`. Seven new
real loopback TLS tests cover authenticated success, absent/untrusted client
identities, a trusted certificate with the wrong operator role, server hostname
verification, redirect refusal, response/read bounds, operator request/reply
limits, strict routes, durable submission replay/conflict, cancellation and
snapshot reopen. The TLS configurator is shared with the production daemon.
Unit fixtures use private, explicitly trusted test leaves; the physical cluster
separately exercises its generated CA chains and actual Go agents.

Three explicit build profiles distinguish runtime compatibility from raising
the minimum class-file version: `java25` compiles/runs 25, `java26-runtime`
compiles release 25 and tests/runs 26, and `java26` compiles/tests/runs release 26.
All own classes must have exactly the selected non-preview bytecode version.
Java 26 execution enables strict final-field-mutation denial in addition to
the existing native-access policy. The default dependency graph, Job contract,
SQL schema and agent runtime remain unchanged.

The selected [Temurin 26.0.2.1+1 release](https://github.com/adoptium/temurin26-binaries/releases/tag/jdk-26.0.2.1%2B1)
provides separately checksum-pinned ARM64 JDK and JRE archives. The retained
[Gradle 9.7.1 matrix](https://docs.gradle.org/current/userguide/compatibility.html)
supports Java 26. Oracle documents the
[final-field-mutation diagnostic](https://docs.oracle.com/en/java/javase/26/migrate/preparing-final-field-mutation-restrictions.html).
Published platform support does not substitute for actual execution on this
Pi's Linux ARM64/16 KiB-page environment.

The new launcher checker runs original generated scripts from verified installed
trees. The new benchmark uses disposable image-backed two-agent trials and the
same workload/resource settings on each compared bundle. It measures API and
convergence latency, startup/recovery, GC pauses and sampled process resources;
its native memory metric is HotSpot-tracked committed memory outside the heap,
not all native allocation. The [build guide](../../build-support/native/README.md)
records commands, instrumentation and measurement limits.

Final evidence is pending. The benchmark smoke run and focused HTTP/SQL tests
are implementation checks, not substitutes for fresh, frozen-source qualification.
The original MVP, earlier bundles and snapshot inputs are preserved.

This completion scope is the retained Linux ARM64 native scheduler/protocol
profile. Legacy-root/Thrift/JMH/DI/HTTP stacks, production HA, other platform
certification, rolling updates and workload migration remain separate work.
