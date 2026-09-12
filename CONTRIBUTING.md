## Working on this fork

Work from this fork's checkout and target its active branch. Apache Aurora's
upstream history is retained, but the maintained application is the original
scheduler on Java 25 with SQLite and Go process agents. The standalone scheduler
prototype, Python/Pants/PEX workflow and Mesos/Thermos runtime are not development
targets. Production multi-scheduler HA remains deferred.

Java follows the existing [Twitter Commons style guide](https://github.com/twitter/commons/blob/master/src/java/com/twitter/common/styleguide.md)
and repository Checkstyle rules, including 100-character lines. Format Go with
`gofmt`. Keep original scheduler policy and public Thrift compatibility tests;
changes to execution must not quietly replace those behavioral contracts.

## Build and test

The checksum-pinned bootstrap supports Linux ARM64 and requires no Python.
Thrift compilation requires `make`, `g++` and Boost headers. Read the
[build guide](build-support/java/README.md) for cache, offline and platform details.
From the repository root:

```sh
build-support/bootstrap-go check-tools
compiler="$(build-support/bootstrap-go thrift)"
./gradlew -PthriftCompiler="$compiler" focusedTest \
  --tests org.apache.aurora.scheduler.state.TaskStateMachineTest
build-support/lab/build-agents --check
```

`check-tools` runs the Go build-helper/client tests and `go vet`.
`build-agents --check` does the same for the agent and lab helper before building them. Selected
Java tests belong in `focusedTest`. Before claiming full behavior or distribution
qualification, run the corresponding gates:

```sh
./gradlew -PthriftCompiler="$compiler" \
  verifyOriginalBehavior :ui:build verifyInstalledDistribution compileJmhJava --continue
./gradlew -PthriftCompiler="$compiler" -Pq verifyOriginalQuality --continue
```

These commands can require downloads and local test sockets. The original UI
uses pinned Node tooling. Preserve reports, failures and skipped-test details;
a focused pass is not a full qualification result. Run live acceptance only in
an owned [private integration lab](build-support/lab/README.md).

Use the [Go client and JSON examples](docs/reference/go-client.md) to exercise
supported process jobs. Historical `.aurora` Python configurations require an
explicit migration; the client does not execute or translate Python.

## Submit a change

Open a pull request against this fork with the concrete behavior change, relevant
issue, test commands and results. Identify compatibility limits and deferred work,
and link any qualification receipts to the exact source and artifact versions.
Do not present historical upstream reports as evidence for a new build.

Keep patches reviewable and preserve Apache attribution and license headers.
Maintainers of the target fork control review and merging; historical Apache
reviewer lists and release procedures do not appoint reviewers for this fork.
