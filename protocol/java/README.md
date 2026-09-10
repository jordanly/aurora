# Java native protocol validation

This Temurin 25 library applies the authoritative `native-v1alpha1/schema.json`
and the semantic checks used by the Python reference and Go agent. It is an
independent Gradle project so the legacy scheduler's forced Jackson versions
do not change the validator's classpath. Integration into native scheduler
intake is still required; no legacy dependency upgrade is implied.

The input boundary rejects duplicate keys, trailing documents, unsafe or
negative JSON numbers (including negative zero), fractional/exponent numbers,
non-ASCII string content, BOM/UTF-16/UTF-32 input, inputs over 1 MiB and nesting
beyond 64 containers. Original numeric tokens are checked before tree parsing
can normalize them. Escaped printable ASCII is accepted. JSON Schema then
validates every message and nested object against draft 2020-12, followed by
counter bounds, port/readiness references, process identity, capability
requirements and immutable command hash/envelope consistency.

`ProtocolValidator.Message` stores validated data privately and returns copies.
`requireCapabilities` checks advertised capabilities separately from manifest
structure. `requireResolution` checks that an assignment resolves the Job's
identity, revision, template digest, declared ports and typed argv references.
Neither operation authenticates peers, admits capacity, commits state, assembles
inventory snapshots or authorizes an ACK. Those are separate runtime operations.

The bundled schema is copied directly by Gradle from the authoritative source;
only its local fragment references are accepted. User messages cannot supply
schemas or trigger external schema retrieval. The CLI emits canonical bytes
with a framing newline on success, and a generic error without the input payload
on failure. Successful output contains the full input: use it only for fixtures
or other intentionally inspectable documents.

Use the [verified native build](../../build-support/native/README.md) from repository root.
The protocol is built and tested with the scheduler, using the same pinned JRE:

```sh
aurora_bundle=/absolute/new/bundle
build-support/native/native-build --output "$aurora_bundle" --cache /absolute/cache
export JAVA_HOME="$aurora_bundle/context/scheduler/jre"
python3 protocol/native-v1alpha1/conformance/check.py \
  --validator "$aurora_bundle/build/java/protocol/install/protocol/bin/protocol" \
  --validator "$aurora_bundle/context/agent/aurora-agent validate --document"
```

Build the Go executable using [the agent instructions](../../agent/README.md)
with output `.pi-tools/agent-dist/aurora-agent`. `--validator` runs the complete
structural/semantic rejection corpus as well as valid canonical/hash and parser
vectors. Positional arguments retain the earlier canonical-only adapter mode.

Seven Java tests cover the shared corpus, immutable result copies, actual
capability requirements, Job/Run resolution, authority refresh and input bounds.
The shared files currently contain 13 valid, 27 invalid and 12 parser-negative
documents. They remain expected-value fixtures, not automatically regenerated
test outputs.

Pinned runtime: networknt JSON Schema Validator 2.0.4, Jackson 2.18.4 and SLF4J
2.0.17. The resolved Maven artifact set is checked against
`runtime-dependencies.sha256` before tests or installation. networknt's 2.x line
supports Java 8 and Jackson 2; YAML/date-time support is omitted because this
contract uses JSON and no date-time formats. These artifacts were compiled and
executed with the verified ARM64 Temurin 25 toolchain.
[Upstream compatibility and API documentation](https://github.com/networknt/json-schema-validator),
[published dependency metadata](https://repo.maven.apache.org/maven2/com/networknt/json-schema-validator/2.0.4/json-schema-validator-2.0.4.pom)

## Portable output and packaged boundary

`-PnativeBuildRoot=/absolute/build` sets this project's build directory to
`/absolute/build/protocol`, including when it is the native scheduler's subproject.
Without the property, `.pi-tools/protocol-java-dist` remains the default. Relative
roots reject. JAVA-01 uses Gradle 9.7.1, `--release 25`, and exact class-file
major version 69. The build JDK and separate runtime JRE are pinned Temurin
25.0.4.1+1; the runtime retains the Java compiler API/JVM JIT while excluding
`javac` and source compiler tools. The protocol’s six external JAR pins and native
runtime boundary remain unchanged; the scheduler adds pinned SQLite as the seventh.

`test`, `check`, and `installDist` require `verifyNativeBoundary` in addition to the
six-external-JAR SHA manifest check. The boundary checker examines actual JAR entries
and class constant pools, restricts own classes to ProtocolValidator/ProtocolTool
and nested classes, and admits only the schema and manifest as own resources.
Installation rechecks the actual copied `lib` directory. Reports are written under
`buildDir/reports/{native-runtime,installed-native-runtime}.json`.

For a copied standalone protocol distribution (six external JARs plus its own JAR):

```sh
python3 build-support/native/verify-boundary --profile protocol \
  --lib-dir /absolute/protocol/lib --report /absolute/protocol/runtime-dependencies.json
```

The default checker profile instead requires the complete native scheduler runtime:
seven external JARs, the native protocol JAR, and the native scheduler JAR.
