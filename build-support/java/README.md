# In-place Java build launcher

`./gradlew` delegates to `build-support/bootstrap-go gradle`, which runs the
original Aurora Gradle graph with the pinned Eclipse
Temurin 25.0.4.1 JDK and Gradle 9.7.1 archives. The first supported platform is
Linux ARM64 (the Raspberry Pi build host); other operating systems and
architectures are rejected until their archive pins are reviewed.

Tool archives are verified with SHA-256 before extraction. They are cached in
`.cache/inplace-build/archives`, Gradle state is kept in
`.cache/inplace-build/gradle`, and generated build output is rooted at
`.cache/inplace-build/build`. Set `AURORA_INPLACE_SEED_ARCHIVES` to a directory
containing the pinned archives when bootstrapping offline. `--offline` also
passes through to Gradle.

The launcher limits Gradle to two workers and a 256 MiB heap and rejects Java 8
selection or attempts to override the managed Java installation. A Thrift 0.10.0
compiler is required for generation and supplied to the Gradle graph with
`-PthriftCompiler=/absolute/path/to/thrift` property. The shell entrypoint first
bootstraps checksum-pinned Go 1.27.1; Go then manages Java, Gradle, Thrift and UI
tools. No Python, Pants, PEX or Mesos installation is required. The scheduler
runtime still requires Java 25, and the UI build still uses Node.

Bootstrap the compiler from the checksum-pinned official source archive (requires
`make`, `g++`, and Boost headers):

```sh
compiler="$(build-support/bootstrap-go thrift)"
```

The pinned Thrift configure recipe disables libraries, tests, tutorials, and the
optional compiler plugin. Ubuntu systems with Boost headers can otherwise enable
the plugin automatically; building that plugin expects `libthrift.la`, while this
build only needs the standalone Thrift 0.10.0 compiler for the original Java
generation path.

The helper preserves release-file timestamps, so the generated parser and
Autotools files work without regenerating them. It compiles with one worker to
fit the Pi. Its receipt binds the source checksum, build recipe, exact version,
and compiler checksum; cached binaries are verified before execution. Set
`AURORA_INPLACE_THRIFT_SEED_ARCHIVES` and pass `--offline` to bootstrap from a
local verified archive. It prints only the compiler path to stdout.

Run the focused original application baseline with that compiler:

```sh
build-support/bootstrap-go check-tools
./gradlew -PthriftCompiler="$compiler" \
  compileJava compileTestJava focusedTest \
  --tests org.apache.aurora.scheduler.state.TaskStateMachineTest
```

Run all discovered original scheduler and commons Java tests, plus the new
compatibility regressions, without frontend packaging or coverage collection:

```sh
./gradlew -PthriftCompiler="$compiler" \
  focusedTest :commons:test --continue
```

Results are under `.cache/inplace-build/build/scheduler/test-results/focusedTest`
and `.cache/inplace-build/build/commons/test-results/test`. This command includes
original integration suites and requires local HTTP and ZooKeeper sockets.

A separately supplied Thrift 0.10.0 executable is also supported. The compiler
must support Java, JavaScript and HTML generation. Tool downloads need
network access unless the pinned archives are seeded; `--offline` also requires
the Maven dependencies to have been cached already.

Qualify all original Java behavior with application coverage, independently of UI
packaging:

```sh
./gradlew -PthriftCompiler="$compiler" \
  verifyOriginalBehavior --continue
```

This runs `behaviorTest` and `:commons:test`, reports all root production class
directories with JaCoCo, and enforces the existing 87% instruction / 79% branch
thresholds. `behaviorTest` rejects test filters; the aggregate gate rejects skipped,
disabled or excluded prerequisites while accepting Gradle's valid up-to-date and
cache results. Use `focusedTest` for selected tests. The previously ignored secondary-index
concurrency test is enabled for both the memory and SQLite backends; skipped tests are recorded explicitly in the ledger.

Application test XML is in `scheduler/test-results/behaviorTest` under the build
root. Coverage XML and HTML are in `scheduler/reports/jacoco/behaviorCoverageReport`.
This gate does not include UI/distribution, full static analysis, live Go-agent
acceptance or real-KDC integration. Mesos/JNI execution is retired, not an
additional deployment prerequisite.

Build and test the original UI, then verify both installed entrypoints and all
packaged API/UI artifacts on the pinned Java runtime:

```sh
./gradlew -PthriftCompiler="$compiler" \
  verifyOriginalBehavior :ui:build verifyInstalledDistribution compileJmhJava --continue
```

The original scheduler and recovery help paths are exercised without starting
services. The receipt checks every launcher classpath entry, generated API/browser
bindings, UI assets, and Java 25 bytecode for all three Aurora artifacts. Third-party
JARs may target older Java. The verifier rejects packaged Mesos execution classes. These checks do not claim
live task execution, production HA or real-KDC startup.
The UI uses checksum-pinned Node and `npm ci`; see [UI setup](../../ui/README.md).

Run the historically optional Java quality checks explicitly:

```sh
./gradlew -PthriftCompiler="$compiler" -Pq verifyOriginalQuality --continue
```

Checkstyle, PMD, SpotBugs and Apache headers are checked independently. Analyzer
migration fixtures guard compatibility; they do not substitute for analysis of
the application. CI has separate behavior/distribution and quality jobs so an
analysis finding cannot hide behavior-test results. All reports are retained.

The full `build` graph also includes the original UI and test/quality
requirements. These instructions do not assert that the current checkout has
passed qualification. Preserve the generated reports and receipts with source
and artifact hashes; older [build evidence](../../docs/reimagining/INPLACE01_BUILD_STATUS.md)
is historical evidence for its recorded revision only.

## Go tools and client

The Go tool suite includes archive bootstrap, immutable Java entity generation,
license/distribution/source-inventory validation, analyzer fixtures and the lab
commands. Run its tests and vet with the pinned SDK:

```sh
build-support/bootstrap-go check-tools
build-support/bootstrap-go build-client --output "$PWD/.pi-tools/client/aurora"
./aurora job validate examples/jobs/process-service.json
```

`build-client` writes a standalone binary and an adjacent provenance record.
`./aurora` uses the checkout bootstrap; the built binary needs no compiler at
runtime. See the [Go client guide](../../docs/reference/go-client.md) for JSON
examples and the supported process profile. The [lab guide](../lab/README.md)
covers agent/helper builds and live acceptance.

The initial shell bootstrap caches the pinned Go archive under
`.cache/bootstrap-go`; subsequent Go-managed tool caches are private and use
fresh verified SDK extractions per invocation. Set
`AURORA_INPLACE_GO_SEED_ARCHIVES` for the Go archive, or use the general
`AURORA_INPLACE_SEED_ARCHIVES`. Pass `--offline` to prevent bootstrap downloads;
Java/Gradle, Thrift, UI and module dependencies also need their respective cached
or seeded inputs. A preinstalled unverified Go executable is not substituted.
