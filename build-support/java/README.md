# Temporary Java build foundation

This is BUILD-01 and a bounded BUILD-02 lane, not the full Java modernization.
It retains Gradle 4.10.2, Java 8 bytecode, Thrift 0.10.0 and existing dependency
versions. Full modernization follows the Go agent and Mesos removal.

On Linux ARM64, run `build-support/java/bootstrap.sh` from this checkout. It
requires curl, tar, unzip, a C++ compiler and make. Archives, extracted tools and
Gradle caches stay in `.pi-tools/`; it installs no host packages. It verifies
all downloads against `toolchains.sha256` before extraction and builds only the
Thrift compiler with two jobs (not the Python library or Mesos). Existing extracted
tools are reused; remove their individual directories to rebuild from the verified
archives. This script does not qualify JNI, native Mesos, or a scheduler deployment.

The pins are Temurin **8u462-b08 Linux aarch64**, Gradle **4.10.2**, and Apache
Thrift **0.10.0**. The JDK SHA256 comes from the Adoptium release metadata; the
Gradle SHA256 comes from its official distribution checksum endpoint. Apache's
historical `.sha` file supplies SHA1
`5c67fb6d2e01fa4ee02823a9c577f4d985f0ecfe` (with the release-candidate filename);
that matches the downloaded release archive. Its SHA256 was computed locally and
is pinned here. This is archive-integrity evidence, not a signature verification.

```sh
# Dependency-free generator regression tests (Python 3.8+).
python3 -m unittest discover -s build-support/java -p 'test_*.py' -v

# Through the recovered build; cap Gradle workers and test forks at two.
build-support/java/gradle-local :api:testThriftWrapperGenerator :api:classes
build-support/java/gradle-local focusedTest --tests '*TaskStateMachineTest'
build-support/java/gradle-local focusedTest --tests '*JobUpdaterIT'

# Existing full frontend / test / package gates remain separate.
build-support/java/gradle-local :ui:build
build-support/java/gradle-local test
build-support/java/gradle-local build distTar distZip
```

`gradle-local` selects the checkout's Java and Gradle and passes an absolute
`-PthriftCompiler` path. Outside this helper the normal wrapper still works;
`-PthriftCompiler=/absolute/path/to/thrift` bypasses Pants and checks the exact
compiler version before generation. Omitting it preserves the legacy wrapper.
`-PwrapperPython=/path/to/python3` overrides the Java wrapper generator's
interpreter. This does not change Pants or executor Python requirements.

`focusedTest` supports Gradle's `--tests` for both unit and integration test names.
It uses normal compiled scheduler test classes and core resources, has its own
reports, and does not run frontend tasks or write full-suite coverage data.
`test`, `run`, and scheduler JAR packaging still build webpack before processing
resources. The full test task retains its original JaCoCo finalizers and 0.87
instruction / 0.79 branch thresholds. `:api:check` includes generator regressions.
Other integration tests may require native libraries, services or frontend assets;
the focused task does not make those dependencies available.

## Generator comparison boundary

Eight tests cover reusable fields, equality/hash/string generation, empty structs,
immutable collections and nested structs, enums, union copy/discriminant behavior,
inherited service metadata, storage files without services, and deterministic API
output under two hash seeds. CLI subprocesses have a 20-second timeout each.
The 89-file `wrapper-api-sha256.json` is a **source-derived golden**, not a captured
Python 2 runtime or a deployed API compatibility fixture. It matches unchanged
baseline generator source running under Python 3 with Python 2's eager `map`
semantics emulated. No Python 2 interpreter was available for a genuine cross-runtime
comparison. Review changes to this golden rather than automatically accepting
regenerated output.

The historical generator's lazy Python 3 `map` exhausted fields after accessor
creation, producing invalid equality and missing hash/string fields even though
it exited successfully. Materializing those fields restores the historical eager
semantics. Raw regex strings also allow generation with Python warnings as errors.

## Executed evidence (2026-09-10, Raspberry Pi Linux ARM64)

- All three archive checksums verified; Temurin Java 8 and Gradle 4.10.2 started;
  compiler-only Thrift 0.10.0 built successfully with host g++/make and its bundled
  generated lexer/parser. No bison/flex or host package installation was needed.
- Eight Python generator tests passed directly and through Gradle. API generation,
  generated Java compilation, commons, scheduler main and scheduler test compilation
  passed. The generated Java set is 139 Thrift files plus 89 wrapper files; there
  are 12 generated resources. Repeating all generation tasks after deleting their
  owned output directories reproduced all **240** file hashes exactly.
- `focusedTest --tests '*TaskStateMachineTest'` executed **21 tests**, with zero
  skipped, failed or errored tests. The command including API generation and
  compilation completed in 58 seconds. Generated Thrift, wrapper and scheduler
  class files all have major version 52 (Java 8).
- Focused dry-run excluded frontend and coverage tasks. The combined `run jar test`
  dry-run retained webpack before core resource processing, and the original
  JaCoCo report and coverage verification tasks. An explicitly incompatible compiler
  (`/usr/bin/true`) failed the version check without invoking Pants.

Local evidence is in `.pi-tools/gradle-focused-test.log`,
`.pi-tools/gradle-regeneration.log`, `.pi-tools/generated-before.json`,
`.pi-tools/gradle-focused-dry-run.log`, `.pi-tools/gradle-full-dry-run.log`, and
`dist/test-results/focusedTest/`. These are disposable ignored build outputs.
The restricted sandbox initially prevented Gradle service initialization; the
successful checks used normal local networking with checkout-local caches. No
historical artifact/dependency failure remained in the executed focused lane.
Full Java coverage, frontend execution, distribution packaging, native Mesos/JNI,
and full integration tests were **not executed** by this build reconstruction.
