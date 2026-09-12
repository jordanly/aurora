# Original Aurora build restoration

This records the initial baseline. Subsequent passing coverage qualification and
the current remaining gates are recorded in [INPLACE-02 progress](INPLACE02_BEHAVIOR_STATUS.md).

Original Java build/test baseline established on `codex/in-place-java25`, based on
upstream master
`11ebaeeb071cb182c388a40755e84f60dda32260`. Upstream, fork and local master were
refreshed and verified equal on 2026-09-11. The reviewed plan is preserved at
`420afd103` on `codex/in-place-plan`; the prototype branch remains at `dc8d7908d`.

This branch restores the original application, whose entry point is
`org.apache.aurora.scheduler.app.SchedulerMain`. It contains no replacement
scheduler engine or Go backend. INPLACE-00 is complete. The initial INPLACE-01
build/test increment is implemented;
full build qualification remains open, as defined in the [plan](IN_PLACE_MODERNIZATION_PLAN.md).

## Original component inventory

Counts below are tracked source/input files at upstream, not executed tests.
The inventory command records every original input and compares it with the
current checkout, including symbolic links. Missing original inputs fail the
audit; modified inputs are listed for review.

```sh
python3 build-support/java/source-inventory.py \
  --output .cache/inplace-validation/source-inventory.json
```

| Component | Original inputs | Build/verification owner |
| --- | --- | --- |
| Scheduler/application Java | 327 Java files | Root `compileJava`; original `SchedulerMain` distribution and recovery tool. |
| Scheduler Java test sources | 203 Java files | Root `compileTestJava`, selected `focusedTest`, then full `test` and integration coverage. Includes helpers; this is not a count of test cases. |
| Commons | 62 production + 24 test Java files | `:commons:compileJava`, `:commons:compileTestJava`, `:commons:test`. |
| API schemas | 6 inputs, including BUILD metadata | `:api` Thrift Java/client-resource and immutable-entity generation. Thermos schema/client generation remains part of the Python execution toolchain. |
| Benchmarks | 18 Java files | Original JMH source set, compilation and runner; separate from correctness tests. |
| Runtime resources | 18 files | Root resource processing, generated build properties and UI bundling. |
| Test resources | 84 files | Original scheduler integration, storage goldens and Thermos process fixtures. |
| Python runtime/tools | 180 inputs | Generator now runs on Python 3; broader client/executor runtime qualification remains a separate gate. |
| Python tests | 126 inputs | Generator regressions now; original client/executor/Thermos suites in subsequent compatibility work. |
| UI | 132 inputs | Original npm install, lint, test and webpack tasks in the full Gradle graph. |
| Build plugins | 4 inputs | Original Thrift/entity plugins adapted and compiled through `buildSrc`. |
| `commons-args` | No tracked source tree | Preserve the historical project identity while identifying any required publication/task role; no source or test coverage is claimed. |

The remaining root build scripts, wrapper, distribution/start scripts, quality
configuration and public API are reviewed separately from source counts. Generated
sources are audited after generation and must not replace or exclude the original
source sets. Historical experimental test totals are not used here.

## Changes and validation

The build migration targets Java 25 and Gradle 9.7.1. The original source sets,
API generation, commons and UI graph are retained. Focused Java tests have a
resource path independent of frontend installation; full packaging still needs
the UI assets. Generated Java also targets Java 25.

The first application-source changes are scoped compiler-warning annotations at
six existing sites. They do not change runtime logic or preserve a Java 8 target.
The build retains `-Werror`; the new dangling-Javadoc warning is
disabled for the existing Apache license headers rather than rewriting every
source file. The following runtime improvements remain explicit follow-ups after
their original behavior is tested:

| Site | Required follow-up |
| --- | --- |
| `FrameworkInfoFactoryImpl.getFrameworkInfo` | Preserve URL formatting while replacing the Mesos adapter, or qualify URI-based construction if the adapter remains. |
| `Kerberos5ShiroRealmModule.configure` | Qualify `Subject.callAs` credential context and failure propagation before replacing the deprecated call. |
| `MoreModules.instantiate` | Replace deprecated reflective construction while preserving constructor selection and reporting missing/inaccessible/failing constructors. |
| `ThriftStatsExporterInterceptor.invoke` | Modernize counter construction and verify failure propagation from the interception path. |
| `GsonMessageBodyHandler.createUnion` | Modernize union construction and preserve API deserialization/error handling. |
| `BatchWorker` constructor | Review service naming and metric callbacks during construction before changing initialization order. |

JaCoCo is updated to 0.8.14 for Java 25; the original full-suite coverage thresholds
remain. The annotation API is supplied explicitly to compile Thrift's
generated `javax.annotation.Generated` use. Thrift generation omits annotation
dates, so the generated source change is limited to deterministic annotation
metadata and does not change schemas or wire IDs.

The broader test run also required a targeted dependency compatibility update:
Guice and its assisted-injection/servlet extensions move from 4.1 to 6.0, with
Guava 31.0.1 matching Guice's declared dependency and Error Prone annotations
aligned at 2.18.0. Multibindings now come from Guice core. Guice 6 retains the
existing `javax.inject` and `javax.servlet` contracts, allowing the broader HTTP
and namespace migration to remain separate.
[Guice 6 release notes](https://github.com/google/guice/wiki/Guice600).

Three small source adaptations accompany that update: `GuiceUtils` implements
the current matcher interface; service listeners explicitly use the direct
executor used by the old overload; and the in-process ZooKeeper directory uses
the JDK temporary-directory API while retaining an unchecked creation failure.
No scheduling or policy algorithm is replaced. The required test results and
remaining runtime limitations are recorded below.

The full Java run then exposed EasyMock 3.4's reflective class generation and
private-JDK-field access in two existing paths. EasyMock is now 5.6.0 with
Objenesis 3.4; its resolved Byte Buddy 1.17.5 and ASM 9.8 support Java 25 class
files. The two capture-constructor call sites use the corresponding factory
methods with their original capture modes.
[Byte Buddy release notes](https://github.com/raphw/byte-buddy/releases/tag/byte-buddy-1.17.5).

`TaskStateChange.toJson` now writes the existing webhook envelope explicitly,
retaining the `task` payload and the `oldState: {}` / `oldState: {"value": ...}`
forms. All 16 initialization/state outputs match the original serializer exactly;
only the historical reference capture used a temporary module opening. The new
implementation and its tests run without JDK module-opening flags. The storage
test utility handles exact primitive-wrapper types through public APIs and still
rejects default values, empty containers and unset nested fields. New regressions
cover both JSON forms and those storage assertions. Logging-interceptor tests
clear the global metric registry before and after each case, retaining their
original counter assertions. These changes do not alter scheduler state or policy.
The existing HTTP error provider also receives JAXB annotations explicitly through
`javax.xml.bind:jaxb-api:2.3.1`. That restores original 400/404 responses previously
failing with missing `XmlElement`; the project still compiles and runs on Java 25.
HTTP fixtures now expect the monitor's single close call, and the updater failure
test injects its intended error through an expected call instead of relying on an
unexpected mock invocation. All original HTTP status and shutdown assertions stay
in place.

The Python 3 generator port preserves field traversal, immutable wrappers,
metadata and deterministic output. The eight source-derived generator regression
tests pass, including comparison of 89 wrapper/metadata outputs across hash seeds.
The parent reviewer reran these tests and checked the plugin changes separately.
Thrift compiler selection is explicit and version checked; the existing compiler
on this Pi reports version 0.10.0.

Gradle 9 removed the project-level external-process methods used by the old
plugins; the port uses injected process execution services.
[Gradle 9 migration guide](https://docs.gradle.org/current/userguide/upgrading_major_version_9.html).

The pinned launcher compiled all original scheduler/commons main and test sources
and passed all 21 original `TaskStateMachineTest` cases (zero failures, errors or
skips). Parent verification confirmed all 634 original Java source files in the
configured Gradle inputs, including 18 benchmarks whose compilation and execution remain open.
All compiled application, test, API/entity, commons and build-plugin class files
were checked for Java 25 class-file major 69. The 240 generated Java/resource
files matched independent generation in a separate directory. The API JAR contains
original RPC classes, immutable entities and browser bindings, with no duplicate
entries or compiler bookkeeping files.

Final results cover 193 suites: **1,294 Java tests passed, zero failed, and one
existing test remains skipped**. Scheduler results contain 1,176 passes and the
skip; commons contributes 118 passes. Of those passes, 1,283 are original tests
and 11 are new compatibility regressions. The commons result was up-to-date in
the final command, with its last execution recorded in the preceding full run.
The [test ledger](inplace01-test-ledger.json) lists every suite and its totals.
The skip is the original `@Ignore` on
`AbstractTaskStoreTest.testReadSecondaryIndexMultipleThreads`; this remains an
explicit storage-concurrency qualification gap.

Executed original suites include the state machines, scheduling/filter/quota,
cron, reconciliation, maintenance/SLA, storage compatibility, `JobUpdaterIT`,
`SchedulerIT`, `HttpSecurityIT`, `ApiIT`, `ThriftIT`, `CronIT` and
`SnapshotterImplIT`. These Java integration tests do not qualify a deployed
Mesos/JNI cluster. The parent also reran all 13 Python tooling tests successfully.
Astra reviewed complex build/dependency and behavior changes; Luna handled bounded
ports and test fixtures; the parent reviewed and corrected delegated work.

Failed receipts remain preserved: the early Guice run had 26 failures; the first
full scheduler run had 311, and the next had 36. The
[evidence ledger](inplace01-evidence.json) records the accepted results, source
counts, tool pins and receipt hashes. All tests ran without module-opening flags,
and no original test source or feature was removed. `focusedTest` deliberately
omits frontend packaging and coverage collection, so these results do not close
full packaging, production integration or quality gates.

## Remaining gates

- Original coverage thresholds, the existing ignored storage concurrency test,
  benchmark compilation/execution, and JNI-dependent production scenarios.
- UI dependency/tooling execution, browser-facing behavior and complete distribution
  packaging, including the recovery tool and installed launchers.
- Java 25-compatible quality integrations. Original commons license-check
  entrypoints are preserved as explicit failures until ported, including their
  requirement from `:commons:check`. Running `:commons:licenseMain` confirmed its
  explicit pending-port failure; this remains a full-build blocker.
- A fresh-checkout build with documented/pinned tool prerequisites. The current
  Pi run used its explicit Thrift 0.10.0 compiler and cached dependencies.
- Broader dependency modernization, including old Guava/Netty `Unsafe` warnings,
  and qualification of real Kerberos credentials and the Python client/executor.

The original Mesos driver and replicated-log dependencies remain intentionally
present while the build is restored. This increment does not claim deployable
Mesos/JNI startup on the Pi, a Mesos-free scheduler or Docker cluster qualification.
