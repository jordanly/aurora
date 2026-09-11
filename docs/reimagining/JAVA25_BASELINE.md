# Java 25+ support policy

The whole maintained Java project will target **Java 25+**. Java 8 runtime,
bytecode, source and helper compatibility are no longer requirements. This
supersedes older constraints in the research and experimental Java tasks.
Libraries may still be used when they support Java 25 even if their own minimum
JVM is older. Java 26 or later can be an additional qualified target; Java 25
is the baseline and does not require preview features.

## Apply the policy to the original application

The [in-place modernization plan](IN_PLACE_MODERNIZATION_PLAN.md) supersedes
the parallel scheduler approach and the JAVA-11 source-retirement proposal.
Preserve the original Aurora scheduler, state/policy controllers, API and UI.
Original code is not obsolete merely because the experimental build excluded it.

1. Restore the original build graph and make its source sets, generators and
   tests work on Java 25, using minimal necessary build/dependency changes.
2. Preserve original behavior while replacing Mesos execution and persistence
   dependencies and integrating Go agents.
3. Modernize the retained Java code and remaining libraries broadly after Mesos
   removal, with original behavior and compatibility tests as the acceptance gate.

An isolated old-JDK environment may be useful for executing an immutable
historical comparator. It never imposes Java 8 compatibility on current source,
normal builds, comparison helpers or future releases.

Public API and persisted-data compatibility are separate from Java language
compatibility. Preserve wire IDs, task identities and required data/migration
contracts; internal Java classes can change with reviewed semantics and tests.
Records, newer APIs or concurrency features should be adopted where they help
rather than as mechanical project-wide transformations.

## Current evidence and limits

The preserved `codex/standalone-foundations` build targets the experimental
scheduler and protocol. The `codex/in-place-java25` branch restores the original
application; its [build status](INPLACE01_BUILD_STATUS.md) records actual qualification.
[JAVA-11 build status](https://github.com/jordanly/aurora/blob/dc8d7908d9d957389b0780aa1cf70aa822f8457c/docs/reimagining/JAVA11_BUILD_STATUS.md) and its
[evidence receipt](https://github.com/jordanly/aurora/blob/dc8d7908d9d957389b0780aa1cf70aa822f8457c/docs/reimagining/java11-build-evidence.json) record that experiment's results.
The [earlier baseline receipt](https://github.com/jordanly/aurora/blob/dc8d7908d9d957389b0780aa1cf70aa822f8457c/docs/reimagining/java11-baseline-evidence.json) likewise remains
historical evidence; none of these results close the new in-place backlog.

The original source is still present. Its proposed retirement is cancelled.
Completion now means a normal repository build and supported distribution that
include the original application as modernized, Java 25+ across all maintained
Java source sets/tools, passing original behavior and relevant recovery checks,
and no active Mesos runtime dependency after its replacement is qualified.
