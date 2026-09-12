# In-place Java build launcher

`./gradlew` runs the original Aurora Gradle graph with the pinned Eclipse
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
`-PthriftCompiler=/absolute/path/to/thrift` property; the launcher does not
download Go, a JRE, or a second Java profile.

Run the focused original application baseline on this Pi with its existing
Thrift compiler:

```sh
python3 -m unittest discover -s build-support/java -p 'test_*.py'
./gradlew -PthriftCompiler="$PWD/.pi-tools/thrift-0.10.0/compiler/cpp/thrift" \
  compileJava compileTestJava focusedTest \
  --tests org.apache.aurora.scheduler.state.TaskStateMachineTest
```

Run all discovered original scheduler and commons Java tests, plus the new
compatibility regressions, without frontend packaging or coverage collection:

```sh
./gradlew -PthriftCompiler="$PWD/.pi-tools/thrift-0.10.0/compiler/cpp/thrift" \
  focusedTest :commons:test --continue
```

Results are under `.cache/inplace-build/build/scheduler/test-results/focusedTest`
and `.cache/inplace-build/build/commons/test-results/test`. This command includes
original integration suites and requires local HTTP and ZooKeeper sockets.

On a fresh checkout, supply your Thrift 0.10.0 executable instead. This increment
does not bootstrap the compiler through the old Python 2/Pants wrapper. The
compiler must support Java, JavaScript and HTML generation. Tool downloads need
network access unless the pinned archives are seeded; `--offline` also requires
the Maven dependencies to have been cached already.

Qualify all original Java behavior with application coverage, independently of UI
packaging:

```sh
./gradlew -PthriftCompiler="$PWD/.pi-tools/thrift-0.10.0/compiler/cpp/thrift" \
  verifyOriginalBehavior --continue
```

This runs `behaviorTest` and `:commons:test`, reports all root production class
directories with JaCoCo, and enforces the existing 87% instruction / 79% branch
thresholds. `behaviorTest` rejects test filters; the aggregate gate rejects skipped,
disabled or excluded prerequisites while accepting Gradle's valid up-to-date and
cache results. Use `focusedTest` for selected tests. The existing ignored storage
test remains visible in the XML ledger and is not counted as passing.

Application test XML is in `scheduler/test-results/behaviorTest` under the build
root. Coverage XML and HTML are in `scheduler/reports/jacoco/behaviorCoverageReport`.
This gate does not include UI/distribution, full static analysis, native Mesos or
real-KDC integration; those retain their separate qualification requirements.

The full `build` graph also includes the original UI and full test/quality
requirements. Consult the [build status](../../docs/reimagining/INPLACE01_BUILD_STATUS.md)
before interpreting a focused test result as full application qualification.
