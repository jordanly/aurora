# Java 25 minimum and repository convergence

The user clarified on 2026-09-11 that the entire project should target Java 25+.
Java 8 runtime, bytecode, source and helper compatibility are no longer supported
requirements. This supersedes the compatibility constraints in the earlier Java
audit, JAVA-08 acceptance text and JAVA-09 SQL record decision. Historical test
receipts remain records of what ran; they do not impose future release gates.

The supported scheduler and protocol already use Java 25 compiler, test and
runtime toolchains by default. Java 26 runtime and compiled profiles remain
available. All maintained Java code, including shared SQL sources and test
helpers, may use Java 25 language and library features where they improve the
design. Libraries are not disqualified merely because their own bytecode can
also run on older Java versions.

The `native-jvm-compat` checker now targets Java 25 bytecode, defaults both sides
to Java 25 and rejects Java 8. It continues to test declared durable-state and
TLS contracts across supported bundles. Java 8 compilation and Java 8 rollback
are removed from future qualification requirements.

Persisted job identities, transaction behavior, command replay and explicit
format rejection remain correctness requirements. They do not require preserving
an internal Java field-based API or running an obsolete JVM. When a Java API
changes, adapt the comparison helper or use each bundle's own inspection tools;
do not hold current source at Java 8 for a historical adapter.

## What still needs to converge

The repository still contains historical Aurora source and build machinery. The
whole tree has **not** yet been rebuilt or made idiomatic Java 25. The native
distribution excludes those paths, but that alone does not finish the user's
repository-wide objective.

| Remaining path | Required disposition |
| --- | --- |
| Root `build.gradle`, `settings.gradle`, Gradle 4 wrapper, `buildSrc`, old subprojects | Make the maintained Java 25 build the normal repository entry point; remove the obsolete build graph and its unsupported plugins. Preserve historical reconstruction through Git history. |
| `build-support/java` Java 8/Gradle 4 bootstrap and launcher | Retire current-source Java 8 build instructions and scripts after redirecting maintained callers. |
| `build-support/lab/native-smoke` current-SQL Java 8 compilation | Replace or retire this early durable-core lane in favor of the maintained Java 25 tests and packaged lab. Shared lab utilities must remain available to `clusterctl`. |
| Legacy scheduler, Mesos adapters, Python workers and old UI | Inventory any needed migration readers/fixtures, then remove unused runtime code and dependencies. Any retained Java implementation joins the Java 25 build. |
| Shared SQL classes and historical comparison helper | Revisit records and other useful Java 25 simplifications without a Java 8 constraint. Decide construction, nullability, equality and safe diagnostics explicitly. |
| Historical lab operations | Preserve access needed to inspect and clean the existing lab without making its old JVM a supported development target. Replace the running MVP only through a separately verified upgrade. |

## Ordered follow-up: JAVA-11

Repository convergence takes priority over adding cron capabilities:

1. Remove the Java 8 support requirement and retarget the active compatibility
   helper. **Complete.** Keep prior qualification receipts unchanged.
2. Replace the root development/build entry points with the maintained Java 25
   graph. Remove the obsolete bootstrap and current-source smoke-build route;
   carry over only lab utilities still used by the maintained harness.
3. Inventory and retire unused legacy code, build plugins and dependencies. Move
   any required migration readers or fixtures into explicit maintained boundaries.
4. Revisit shared SQL records and remaining Java idioms against actual current
   consumers. Refactor in focused changes, with explicit durable-state projections
   and useful failure tests rather than mechanically changing every class.

Completion requires a fresh repository build through its normal entry point,
Java 25+ across every supported Java source set and tool, no Java 8 qualification
lane, no active legacy build fallback, and passing relevant unit, packaging and
cluster recovery checks. Keep the existing single-owner scheduler and durability
guarantees unless a separate measured change justifies altering them.

This support-policy change does not upgrade the existing live MVP or alter any
stored format. Schema migrations and actual deployment remain distinct actions.

## Validation of step 1

All 58 native packaging tests passed, including rejection of unsupported
requested JVMs before bundle/output access and rejection of an actual Java 8
runtime before helper compilation or state copying. Real Java 25-to-25 and
Java 25-to-26 comparisons passed with bidirectional fixture writes, snapshot
reopen, replay and PKCS12 loading. Both helpers have class-file major 69, and
original bundle, snapshot, config and TLS inputs were unchanged.
[The receipt](java11-baseline-evidence.json) records the checker and result hashes.
Application Java/Go source and stored formats did not change in this step;
whole-repository convergence remains the outstanding JAVA-11 work above.
