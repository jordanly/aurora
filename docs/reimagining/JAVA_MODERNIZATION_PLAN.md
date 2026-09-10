# Java modernization plan

Research date: **2026-09-09 America/New_York** (remote clock: 2026-09-10 UTC). Source baseline: `e3350f63d446cca17e8f1763ce30cc94346e64cf`, supplied `codex/aurora-context` revision. Research branch: `codex/java-modernization-plan`.

## Recommendation and scope

**Qualify Java 26, the latest generally available feature release, while recommending Java 25 LTS for deployment.** Java 26 was released March 17, 2026. Oracle's release calendar lists 26.0.2.1 and 25.0.4.1 as August 18 updates, and Java 27 as upcoming September 15, 2026; 27 is not GA on this research date. Oracle identifies 25 as the newest LTS. Update identifiers and support periods depend on the chosen distribution; record its exact build and checksum rather than assuming vendors publish identical updates. [Oracle release announcement](https://docs.oracle.com/en-us/iaas/releasenotes/java-management/jdk-26-release-note.htm), [Oracle release calendar](https://ops.java/releases/), [Oracle support roadmap](https://www.oracle.com/java/technologies/java-se-support-roadmap.html).

The recommendation is an engineering judgment: a retired scheduler with native dependencies benefits from a stable support target while maintaining a separate latest-GA qualification lane. Java 21 is a useful intermediate diagnostic checkpoint, not the proposed final destination. Supporting 26 entails keeping pace with subsequent non-LTS releases. A successful Java upgrade alone does not establish that Aurora's dependency stack is suitable for production.

This is a source audit and bounded experiment, **not a migration or successful Java build**. Preserve the scheduler's existing API, persistence, state transitions, and Mesos interactions throughout the compatibility work. Defer language-style and architecture changes until behavior is established.

## Execution evidence and limitations

- Verified execution host before research: `raspberrypi`, Linux `6.18.39+rpt-rpi-2712`, `aarch64`, Debian 13.6. Isolated checkout: `/home/jordanly/.codex/worktrees/3faa/aurora`; initial HEAD matched the requested commit and the tree was clean.
- Read [AURORA_CONTEXT.md](../../AURORA_CONTEXT.md) first. No `AGENTS.md` was found in the checkout or ancestor directories. Its earlier macOS inventory does not describe this host.
- `java -version` failed because Java is absent; `javac`, Gradle, Thrift, Python 2 and Python 2.7 were not found on PATH; `/usr/lib/jvm` was absent. Python is **3.13.5**. `rg` was unavailable, so searches used `git grep` and `find`.
- No wrapper bootstrap, dependency download, installation, Java test, simulator, native build, or service launch was performed. Consequently dependency resolution, compiler compatibility, and native behavior remain unverified.
- Budget checks before research and between major phases returned **4% main weekly usage** with ordinary usage allowed. Initial 4% established an additional task stop threshold of 11%; the shared early threshold was 50%. No subagents, resets, credits, or purchases were used. Account samples cannot measure this task's individual consumption. The final usage request did not return and was terminated; no new research was started after that unavailable check. The report was already complete; only local report verification and correction followed.

Report verification: local Markdown links all resolve, the added file passes `git diff --no-index --check /dev/null docs/reimagining/JAVA_MODERNIZATION_PLAN.md`, and only `docs/reimagining/` is untracked. The report is saved on the isolated branch without a commit, push or merge. Java build/test validation remains pending the prerequisites above.

### Bounded code-generation experiment

Invoked the unchanged `src/main/python/apache/aurora/tools/java/thrift_wrapper_codegen.py` using Python 3.13.5, separately for `api.thrift` and `storage.thrift`, with a 20-second subprocess timeout per input and outputs under `/tmp/aurora-java-codegen-0m502lcv`.

| Input | Exit | Observed result |
| --- | --- | --- |
| `api/src/main/thrift/org/apache/aurora/gen/api.thrift` | 0 | 89 Java files, including service metadata; no resource files |
| `api/src/main/thrift/org/apache/aurora/gen/storage.thrift` | 0 | No generated files; generator explicitly skips inputs without services |

Both emitted Python `SyntaxWarning` diagnostics for non-raw regular-expression strings. These results narrow the Python obstacle: the wrapper generator executes under Python 3, but Gradle's `checkPython` explicitly rejects it. They do **not** establish equivalence with Python 2 output, generated Java correctness, Thrift compiler availability, or Pants compatibility. Temporary outputs are diagnostic scratch, not required report dependencies. The reproducible invocation is:

```sh
python3 src/main/python/apache/aurora/tools/java/thrift_wrapper_codegen.py \
  api/src/main/thrift/org/apache/aurora/gen/api.thrift \
  /tmp/aurora-java-probe/java /tmp/aurora-java-probe/resources
```

## Build and code-generation audit

Source anchors: [root build](../../build.gradle), [buildSrc build](../../buildSrc/build.gradle), [version pin](../../buildSrc/gradle.properties), [wrapper](../../gradle/wrapper/gradle-wrapper.properties), [settings](../../settings.gradle), [ThriftPlugin](../../buildSrc/src/main/groovy/org/apache/aurora/build/ThriftPlugin.groovy), [ThriftEntitiesPlugin](../../buildSrc/src/main/groovy/org/apache/aurora/build/ThriftEntitiesPlugin.groovy).

| Observed constraint | Necessary migration work |
| --- | --- |
| Wrapper and `GRADLE_VERSION` pin 4.10.2; buildSrc throws on any other Gradle version | Update both together. Replace the root `task wrapper(type: Wrapper)` declaration with configuration of the existing wrapper task on modern Gradle. Review `include 'buildSrc'` and root references to `project(':buildSrc')`; modern build logic must not depend on treating the special buildSrc build as an ordinary subproject. |
| `compileJava` alone sets Java 8 source/target; custom `classesThrift` and `classesThriftEntities` compile generated sources independently | Configure toolchain/release for **every JavaCompile**, including tests, JMH and custom generation tasks. Set matching test and JavaExec launchers separately. The build JVM, compiler, emitted bytecode, and application JVM are four distinct choices. |
| Root compile enables `-Werror -Xlint:all`, with processing/serial exceptions | Triage new warnings individually. Generated compilation currently suppresses warnings. Preserve `-parameters`: `ApiBeta` uses reflected parameter names and generated method metadata. |
| `compile`, `testCompile`, `configurations.runtime`, `mainClassName`, `destinationDir`, archive/report setters and eager task creation | Migrate removed configurations/properties using each intervening Gradle upgrade guide. Use `java-library` and `api` where published types expose dependencies; blindly converting every `compile` to `implementation` can break API consumers. |
| Custom tasks call project/script `exec` inside `doLast` | Refactor to typed Exec tasks or injected `ExecOperations`; project-level execution methods are removed in Gradle 9. Correct task inputs/outputs, including generator version, resource directories and transitive Thrift includes. |
| Old JS, versions, license, Node, JMH and SpotBugs plugins | Resolve plugin compatibility before application compilation. Remove unused integrations only after checking their task use; replace necessary plugins with tested equivalents. A root plugin still resolves even if its feature is not exercised. |
| API `checkPython` demands 2.7; `thriftw` falls back to Pants | First accept an explicit Python 3 executable for the standalone wrapper generator after golden-output tests; convert regexes to raw strings. Separately supply a checksum-pinned Thrift compiler for each host architecture, avoiding the Pants bootstrap for Java-only builds. Keep compiler/runtime at 0.10.0 initially to reduce simultaneous changes. |
| UI Node 12.14.1 download, npm plugin/install tasks, webpack coupled to `processResources` | Give core Java tests a defined resource path that does not require rebuilding the UI, and test full distributions separately. Preserve packaged UI and generated Thrift JS/HTML in release builds. A skipped webpack task without a validated bundle is not a valid distribution. |
| JaCoCo not explicitly pinned; report uses `dist/classes/main`; quality tools Checkstyle 7.3, SpotBugs 3.1.0, PMD 5.5.3, JMH 1.15 | Upgrade agents/parsers for selected bytecode, modernize task DSL, and derive coverage directories from source sets rather than the obsolete hard-coded layout. Pin JaCoCo **0.8.15** for official Java 26 support; 0.8.14 supports 25. Keep quality checks runnable under `-Pq`. |

Gradle documents Java 25 support starting at 9.1.0 and Java 26 at 9.4.0. The reviewed current matrix is 9.7.1; use a pinned, verified supported release at implementation time. Gradle 9 requires JVM 17 or later for its daemon, independently of target bytecode. `--release` constrains platform API use more effectively than source/target alone. [Gradle compatibility](https://docs.gradle.org/current/userguide/compatibility.html), [Gradle 9 migration](https://docs.gradle.org/current/userguide/upgrading_major_version_9.html), [Java build options](https://docs.gradle.org/current/userguide/building_java_projects.html), [JaCoCo releases](https://www.jacoco.org/jacoco/trunk/doc/changes.html).

## Dependency audit and upgrade units

These are **declared pins**, not a resolved dependency graph or vulnerability scan. `allprojects.resolutionStrategy` forces versions and fails on conflicts, including transitive Objenesis 2.2. Capture `dependencies`/`dependencyInsight`, artifact checksums and locks after toolchain reconstruction; review forces as part of each change rather than suppressing conflict failures.

| Family and current declarations | Plan and compatibility gate |
| --- | --- |
| Guice 4.1.0, Guava 23.2-jre, Gson 2.3.1 | High-priority reflection/code-generation upgrade unit. Qualify injector/AOP creation without broad JDK opens. A Guice 6 bridge retains `javax` compatibility; Guice 7 changes namespace requirements, so it is not a drop-in bump. Review multibindings packaging and all extensions together. Preserve Guava immutable collection and null semantics in generated entities. |
| Jetty 9.3.11, RESTEasy 3.1.4, Servlet 3.1, JAX-RS 2.0, Shiro 1.4.0; Jersey 1.19 tests | Treat server, DI, auth and test HTTP client as one coherent stack. Start with a `javax` compatible bridge if needed; choose supported final versions as a coordinated upgrade. Do not perform a global `javax`→`jakarta` substitution merely because the JDK changed. Test BASIC and NEGOTIATE, unauthorized operations, filters, JSON and binary APIs. |
| Jackson 2.5.1, protobuf JSON adapter 0.9.3, protobuf-java 3.5.1, Mesos 1.6.1 | Align Jackson components and protobuf adapter; verify Mesos protobuf serialization with the exact Mesos JAR. A forced protobuf runtime bump is not proof of compatibility with its generated classes. |
| ZooKeeper 3.4.8, Curator 2.12.0 | Upgrade as a client pair, separately from ensemble server upgrades. Gate with session expiry, election, reconnect, ACL/digest and embedded-server tests. Preserve discovery payloads. |
| Netty 4.0.52, AsyncHttpClient 2.0.37, HttpClient 4.5.2/HttpCore 4.4.4 | Resolve a mutually compatible network stack; test TLS, coordinator timeouts, cancellation and thread shutdown. Investigate transitive Unsafe/reflection usage rather than assuming absence from Aurora source proves safety. |
| SLF4J 1.7.25, Logback 1.2.3, Quartz 2.2.2, Commons Lang 2.6, StringTemplate 3.2.1 | Audit and pin replacements; logging API/binding must agree. Test startup/configuration, cron misfires/recovery, template rendering. Old age alone does not prove a JDK compile failure. |
| Thrift compiler/runtime 0.10.0 | Upgrade only with regenerated Java/JS/Python bindings and API/persistence fixture gates. Check generated annotations and custom wrapper assumptions before selecting the replacement version. |
| JUnit 4.12, EasyMock 3.4, PowerMock 1.6.4, forced Objenesis 2.2 | Keep JUnit 4 initially; upgrade mocking/bytecode support as necessary. No tracked Java PowerMock imports were found (only a TODO mentioning it); verify removal of apparently unused PowerMock before carrying it forward. JUnit Jupiter migration is optional. |

Guice's maintainers explicitly distinguish the Guice 6/7 namespace choices. This is why modern Java and Jakarta migration should have separate acceptance criteria. No exact final version set for the entire dependency graph is claimed validated here. [Guice release guidance](https://github.com/google/guice/wiki/Guice700).

## Removed APIs, reflection and concrete source improvements

The table separates confirmed usages from investigation targets. A textual scan cannot inspect unresolved dependency bytecode or reflection-driven calls.

| Evidence | Classification and action |
| --- | --- |
| `GsonMessageBodyHandler.createUnion`, around line 173, calls `Class.newInstance()` | **Compatibility cleanup:** use `getDeclaredConstructor().newInstance()`, explicitly preserving JSON error handling for constructor/access/invocation failures. This deprecated call can trip root `-Werror` when compiling against modern APIs. |
| `Kerberos5ShiroRealmModule`, around line 166, calls `Subject.doAs` | **Compatibility cleanup:** migrate to `Subject.callAs` after raising the release floor sufficiently; preserve subject scope, exception conversion and GSS credential lifecycle. JDK 26 still has `doAs`, deprecated for removal. It is incorrect to describe it as removed. Test real SPNEGO with a disposable KDC as well as mocked realm tests. |
| `Credentials.digestCredentials`, `commons/.../zookeeper/Credentials.java:44`, uses charset-default `getBytes()` | **Behavioral compatibility:** make encoding explicit after confirming the historical credential convention (normally UTF-8). The existing source TODO explicitly warns that the server also uses its default charset. Test non-ASCII credentials and existing ACL authentication before rollout; silently changing pre-existing non-UTF-8 credential bytes can lock clients out. |
| `JvmStats` uses `com.sun.management` MXBeans and `getFreePhysicalMemorySize()` | **Targeted modernization:** use the current memory accessor once the release floor permits, preserving metric names and units. `com.sun.management` and `com.sun.security.auth` here are documented JDK extension APIs, not automatically forbidden internals. Retain `jdk.management`/`jdk.security.auth` if a reduced runtime is later built. Check metrics on ARM and under memory/CPU limits. |
| `MesosLogTest` reflects private Mesos constructors; `StorageEntityUtil` reads fields with `setAccessible` and uses `com.google.gson.internal.Primitives` | **Dependency/test work:** keep the external constructor seam under test; replace the Gson internal helper with public equivalents such as Guava's primitives utilities. Reflective field reads are not final-field writes. Avoid falsely attributing JDK 26 final mutation warnings to this helper. |
| Generator emits final immutable fields; Gson deserialization exists in API handlers | **Investigation:** inspect actual deserialization types under JDK 26 with `--illegal-final-field-mutation=debug`, then `deny` in a dedicated test lane. If a path mutates final fields, provide constructor-based adapters. Current source alone does not establish that generated immutable entities are mutated this way. |
| Source imports `javax.annotation.Nullable` and `ParametersAreNonnullByDefault` | **Dependency hygiene:** declare their provider explicitly, rather than depending on Guava transitives. These JSR-305 annotations differ from `javax.annotation.Generated`. Inspect Thrift 0.10 generated output for the latter and supply a matching annotation API or change generation before compiling on 11+. Do not add JAXB indiscriminately. |
| `docs/operations/configuration.md` recommends CMS; example service enables old JDWP flags | **Launch compatibility:** remove CMS guidance/configuration for new JDKs; baseline with the chosen JDK's supported GC and remeasure. Modernize debug launch syntax and test binding behavior. Inventory real deployment JVM flags, agents and external `CLASSPATH_PREFIX`, which the root build supports. |

JDK 11 removed bundled Java EE/CORBA modules, JDK 14 removed CMS, and JDK 15 removed Nashorn. The targeted scan found no direct Aurora JAXB, activation, Nashorn, `sun.misc.Unsafe`, or SecurityManager implementation; transitive checks remain necessary. JDK 18 changed the default charset to UTF-8. Strong encapsulation requires dependency upgrades rather than treating `--illegal-access` as a durable remedy. [JDK migration guide](https://docs.oracle.com/en/java/javase/26/migrate/jdk-migration-guide.pdf), [removed components](https://docs.oracle.com/en/java/javase/26/migrate/removed-tools-components.html).

Java 26 warns about final-field mutation through deep reflection; it does not universally prohibit all reflection. Use temporary access flags only for identified paths with removal criteria. Similarly, JAAS authentication survives the Security Manager changes: update the affected APIs without replacing Aurora authorization wholesale. [Final-field migration](https://docs.oracle.com/en/java/javase/26/migrate/preparing-final-field-mutation-restrictions.html), [Subject API](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/javax/security/auth/Subject.html).

## Native JVM integration and ARM64

**Native Mesos is a separate release gate.** `DriverFactoryImpl` constructs `MesosSchedulerDriver`; `LibMesosLoadingModule` also binds native-dependent `V0Mesos` and `V1Mesos`. `MesosLogStreamModule.provideLog` constructs `org.apache.mesos.Log` for durable storage. The recovery tool has a native-log mode. Choosing V1 does not make this checkout Java-only, and replacing only the scheduler driver would still leave native persistence.

The example service sets `MESOS_NATIVE_JAVA_LIBRARY=/usr/lib/libmesos.so` and `java.library.path`; the packer script hard-codes Java 8 amd64, amd64 package sources, and a Python 2.7 `linux-x86_64` Mesos egg. These are observed architecture assumptions, not evidence of ARM binaries. Source anchors: [native wiring](../../src/main/java/org/apache/aurora/scheduler/mesos/LibMesosLoadingModule.java), [native log](../../src/main/java/org/apache/aurora/scheduler/log/mesos/MesosLogStreamModule.java), [packer](../../build-support/packer/build.sh), [service](../../examples/vagrant/systemd/aurora-scheduler.service).

Modern Java and Gradle have AArch64 support, but published configuration tables do not certify this Raspberry Pi Debian combination or the Aurora/Mesos stack. [Oracle JDK 26 configurations](https://www.oracle.com/java/technologies/javase/products-doc-jdk26certconfig.html), [Gradle platforms](https://docs.gradle.org/current/userguide/compatibility.html).

Stage ARM validation independently:

1. Provision an ARM64 JDK and compiler/codegen tools in an isolated environment; validate architecture and checksums. Limit Gradle workers/test forks to 1–2 initially: the current build forks once per reported processor, which is an unbounded memory choice for this host.
2. Run pure Java/API/commons tests and the fake-cluster simulator on ARM64. Compare them with an amd64 CI lane using identical dependency locks. Separate application support from build-host support if historical generation initially requires amd64.
3. Produce a matching ARM64 Mesos Java/native set, including JNI, libstdc++, protobuf/native dependencies and OS ABI. Inspect `file`, ELF headers and dynamic dependencies before loading. Whether Mesos 1.6.1 builds on this OS is **unknown**; do not promise a trivial rebuild or install an x86 library on ARM.
4. In disposable processes, test native driver registration/callbacks/shutdown and replicated-log append/read/truncation/recovery/quorum loss with the exact JDK. Add `-Xcheck:jni` in diagnostic runs. Check explicit cleanup as well as native memory growth and crash logs.
5. Exercise real Mesos tasks with the Python executor separately; Java simulator success says nothing about that native Python egg, process supervision or containerizers.

JDK 24 introduced JNI access warnings. For a retained classpath-based Mesos integration, explicitly document `--enable-native-access=ALL-UNNAMED` for relevant application/test/recovery launchers and validate a strict diagnostic lane. This is native permission configuration, not a repair for an incompatible ELF/ABI. [JDK 24 native-access changes](https://docs.oracle.com/en/java/javase/24/migrate/significant-changes-jdk-24.html), [restricted methods](https://docs.oracle.com/en/java/javase/24/docs/api/java.base/java/lang/doc-files/RestrictedMethods.html).

If ARM native reconstruction fails, a useful scoped result is Java 25/26 scheduler logic on ARM plus an amd64 native integration lane. Mark ARM production support blocked until the native gate passes. Replacing Mesos or its log is a separate architecture project, not a prerequisite refactor to conceal inside this upgrade.

## Incremental implementation matrix

Each row is a reviewable change set with a retained previous artifact. Historical Gradle versions below are diagnostic bridges, not final supported deployment recommendations. Follow intervening upgrade guides and use warning output to remove obsolete APIs before crossing a major boundary.

| Stage | Build JVM / Gradle | Compile/release and test runtime | Exit criterion |
| --- | --- | --- | --- |
| 0. Recover baseline | JDK 8 / 4.10.2 | Java 8, tests on 8 | Reproducible dependency/generator manifest; baseline generated output and representative fixtures/tests. If reconstruction fails, record exact failure and carry the lack of baseline as a risk. |
| 1. Make build portable | JDK 8, then 11 / 5.6.x → 6.9.x | Keep 8 bytecode initially | Python 3 wrapper output equivalence, explicit Thrift binary, plugin replacements, configuration migration, UI/core split. Gradle 5+ is the documented Java 11 bridge. |
| 2. Encapsulation checkpoint | JDK 17 / 7.6.x | First 8/11 release as needed, then 17; tests on 17 | Generated/custom/test compiler settings agree; explicit missing APIs; DI/mocks and focused tests pass with no unexplained JDK opens. Dependency releases requiring Java 11 must not enter an 8-runtime lane. |
| 3. Modern baseline | JDK 21 / 8.14.x | Release 17 then 21; tests on 21 | Full Java suite, quality tools, packaged simulator, HTTP/auth and persistence fixtures pass. Native amd64 checkpoint is strongly advised before moving farther. |
| 4. Recommended deployment | JDK 25 / pinned Gradle 9.4+ | Release 25; tests/runtime 25 | Gradle 9 buildSrc/Exec migration, warning cleanup, exact final dependencies, distribution and native gates on supported architectures. |
| 5. Latest GA qualification | Same supported Gradle / JDK 26 | Run release-25 artifact on 26; separately compile/test release 26 | Distinguish runtime compatibility from a Java-26-minimum artifact. JaCoCo 0.8.15; final-mutation/native diagnostics; full integration/regression evidence. No preview features. |

For row 4 a Gradle 9.7.1 candidate is consistent with the reviewed current documentation; pin its verified distribution checksum and qualify the actual plugin set. The minimum runtime support pairs are 17/7.3, 21/8.5, 25/9.1, 26/9.4. These describe Gradle support, not proof that Aurora builds. [Compatibility matrix](https://docs.gradle.org/current/userguide/compatibility.html).

Do not demand a production rollout at every checkpoint. Once failure causes are isolated, bridge commits can be consolidated into a final build change with retained validation evidence. When making release-25 source changes, keep earlier checkpoint commits available rather than expecting the same branch to continue compiling with `--release 8`.

## Validation and acceptance criteria

1. **Build integrity:** clean generation twice; compare generated file manifests and semantic/golden outputs against the Java 8 baseline. Compile API, commons, scheduler, test and JMH sources; inspect representative class major versions and reflected parameter names. Confirm JS/HTML and scheduler bundle packaging, API publication metadata, start scripts, recovery script and required licenses. Do not mistake empty or misplaced coverage output for success.
2. **Dependency/API diagnostics:** once artifacts exist, run the target JDK's `jdeps --jdk-internals` with the complete runtime classpath and `jdeprscan --release 26`; record missing dependencies instead of hiding them. These tools do not discover every reflective call. Include resolved artifacts in the audit, and run with diagnostic flags to identify actual runtime access.
3. **Focused behavior:** begin with `TaskStateMachineTest`, `StateManagerImplTest`, `StorageTransactionTest`, `DurableStorageTest`, `DataCompatibilityTest`, `TaskSchedulerImplTest`, `MesosCallbackHandlerTest`, and `JobUpdaterIT`. Preserve deliberate lack of local write rollback, post-persistence Mesos acknowledgements, stale-offer handling and restart recovery. Add targeted tests for the changed charset/JAAS/union-construction paths.
4. **Complete Java suite:** preserve JUnit 4 test discovery, including classes named `*IT`; verify executed counts, not just task exit codes. Run with instrumented and uninstrumented JVMs when diagnosing agents. `test` finalizes into JaCoCo reporting and 0.87 instruction/0.79 branch verification: provide a focused-test task without suite-wide coverage finalizers, while retaining the original full-suite thresholds. Run `-Pq` after adapting parsers and rules; explain rule/coverage deltas rather than relaxing thresholds to pass.
5. **HTTP and persistence:** run `ApiIT`, `ApiBetaTest`, `HttpSecurityIT`, Kerberos filter tests, and a real KDC test; check `/api` binary/JSON and `/apibeta` parameter names and error responses with old client bindings. Run old snapshots/log transactions through new readers and new writes through the retained old reader where rollback requires it. Preserve fixtures instead of regenerating them to mask incompatibility. Include `SnapshotterImplIT` and updater recovery.
6. **Simulator:** after rebuilding required resources, start `LocalSchedulerMain` and check create/launch/update/kill API flows and UI loading. Its fake master bypasses actual executor and native persistence; classify this explicitly as a simulator result.
7. **Native/distribution acceptance:** on disposable amd64 and ARM64 environments, run installed scripts, driver choices actually supported, log quorum loss, leadership failover, crash/restart, backup/recovery and end-to-end task execution. Compare old/new JVM scheduling latency, durable-write latency, GC pauses, RSS/native memory, thread/FD counts and recovery duration using the same workload and resource limits. Define regression budgets from measured baseline and service SLOs; no defensible numeric performance threshold exists yet.
8. **Rollout:** retain old binaries/JDK/configuration and pre-upgrade backups. Canary a standby/isolated cluster first; test mixed-version coordination and log replay before rolling a quorum. Roll back the artifact/JDK only while written data remains old-reader compatible; otherwise use the tested recovery procedure. Never initialize an existing log as an upgrade step.

Suggested commands after prerequisites and task wiring are repaired—not commands validated in this report:

```sh
./gradlew --no-daemon :api:classes :commons:test compileJava compileTestJava
./gradlew --no-daemon test --tests '*DataCompatibilityTest'
./gradlew --no-daemon -Pq clean build
./gradlew --no-daemon distTar distZip
```

The filtered `test` command needs the coverage treatment described above. Use capped workers/forks on the Pi; avoid full cluster scripts until their service/workload mutations are deliberately isolated.

## Optional refactoring after compatibility

- **Records and collection factories:** useful for new internal value types, but replacing generated `I*` entities changes builders, equality, serialization and null behavior. Keep those contracts in the compatibility pass.
- **Virtual threads:** investigate blocking HTTP/coordinator work only after profiling. Preserve serialized storage writes, callback ordering, Guava service lifecycles and bounded backpressure. They are not an automatic scheduler-throughput upgrade.
- **Java time APIs, streams and switch expressions:** adopt in small touched areas with tests; avoid broad conversions of persisted timestamps, quantity types and fake-clock scheduling.
- **JUnit Jupiter, Jakarta-wide migration, JPMS/jlink, dependency catalogs:** worthwhile independent work when their benefits justify the changes. A supported final HTTP stack may eventually require Jakarta changes, but Java language compatibility itself does not. Delay a custom runtime until native, management and Kerberos module requirements are proven.
- **Mesos replacement, persistence redesign and Python executor modernization:** separate projects with their own protocols and failure models. None is established as necessary merely to compile Java 25, though native/runtime viability can determine whether this upgrade is deployable.

The immediate next implementation task is a small build-reconstruction change: pin an available JDK/toolchain, update the Python wrapper gate with output tests, supply a deterministic Thrift compiler and capture the first actual Gradle/dependency failure. Stop widening the work once that checkpoint has concrete evidence.
