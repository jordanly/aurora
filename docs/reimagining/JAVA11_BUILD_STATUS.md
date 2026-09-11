# JAVA-11: maintained Java 25 build entry point

JAVA-11 step 2 is complete. The repository root is the maintained development
entry point: `./gradlew build`, `./gradlew check` and `./gradlew installDist` use
the pinned Linux ARM64 Java 25+/Gradle 9 graph containing
`:aurora-native-scheduler` and `:protocol`. The packaged builder uses this same
graph, including both projects' distribution and dependency checks.

The Java 8/Gradle 4 bootstrap, local wrapper, old root plugin graph, `buildSrc`
and binary wrapper are removed. The early Java 8 smoke-build command now reports
its replacement and exits before creating a run. Its shared lab utilities live
in `lab_common.py`; the maintained container harness uses the current toolchain
manifest. Fresh labs require bundle-provided Java tools and use current OpenSSL
PKCS12 defaults. Existing recorded lab operations remain available.

The root launcher defaults to Java 25 and supports the reviewed Java 26 runtime
and compiled profiles. It verifies pinned tool archives, extracts tools afresh,
uses a private cache and supports offline operation. Direct Gradle invocation
requires the pinned Gradle version and at least Java 25. The current pinned host
platform is Linux ARM64.

## Qualification

The accepted source is `2a8e19310ce67ecde738edad3d518217f21d624c`, built from a
separate clean checkout. [The evidence receipt](java11-build-evidence.json)
records bundle, log, test and physical-run hashes.

| Check | Result |
| --- | --- |
| Fresh root `./gradlew --offline build check installDist` | 73 Java tests; both distributions |
| Each supported profile's packaged build | 73 Java tests, 65 lab Python tests, 62 packaging Python tests; Go tests and vet |
| Reproducibility | Six Java archives and both installed trees identical across isolated output roots |
| Previous qualified runtime comparison | All packaged context files, Java archives and installed trees byte-identical for each matching profile |
| Installed launcher checks | 64 per profile; 192 total |
| Supported JVM comparisons | Previous Java 25 bundle to each new profile; bidirectional fixture writes, snapshot reopen, replay and PKCS12 loading; Java 25 helper bytecode |
| Physical cluster recovery | 23 cases per profile; 69 total, including three recovery rounds and ten minutes of mixed workload per profile |
| Protocol adapters | Java 25 and Go canonical/parser conformance passed |
| Cleanup and existing services | All three owned lab runs and packaging probes removed; original MVP and Home Assistant unchanged |

Physical checks used separate containers for the scheduler, two agents and two
fault proxies. Runtime audits verified actual image IDs and arguments, Java
versions, nonroot users, read-only roots, dropped capabilities and absence of
executable bind mounts. The runs used new certificates, exercising Java 25/26
TLS with the current PKCS12 generation path.

An initial fresh packaging attempt correctly failed before publishing a bundle:
it selected only scheduler distribution tasks and omitted protocol archives.
The accepted source fixes this by invoking root aggregate tasks. The failed
log is preserved and excluded from accepted qualification.

The implementation changed 40 files, adding 584 and deleting 2,168 text lines
(net removal of 1,584), plus removing the old wrapper JAR. This excludes the
subsequent qualification documentation. Application Java/Go behavior and durable
formats are unchanged; the only Java-source edit is a conformance-adapter comment.
The previous P6 execution/policy evidence remains linked through identical
runtime artifacts; those optional physical lanes were not rerun in this slice.
Go race instrumentation remains unavailable on this Pi's 47-bit VMA.

Fork master, upstream master and local master references were verified equal at
`11ebaeeb071cb182c388a40755e84f60dda32260`; the implementation contains that head.

## Remaining JAVA-11 work

Step 3 inventories and retires unused historical scheduler, Mesos, Python, UI
and auxiliary build/configuration source, keeping any required migration readers
or fixtures in explicit maintained boundaries. Step 4 revisits shared SQL records
and other Java 25 idioms. The entire historical source tree has not yet converged.

See the [Java 25 baseline and convergence plan](JAVA25_BASELINE.md), the
[ordered backlog](IMPLEMENTATION_BACKLOG.md), and the
[native build and qualification guide](../../build-support/native/README.md).
