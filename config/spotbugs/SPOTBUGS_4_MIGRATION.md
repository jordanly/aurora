<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at:

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# SpotBugs 3.1.0 to 4.10.4 compatibility

This migration runs the analyzer on Java 25 while retaining the historical
quality policy. It does not declare all newly introduced findings harmless.
The original integration (`ad86177a0`, `build.gradle`) selected SpotBugs 3.1.0,
maximum effort and the existing exclusion file. Quality tasks were opt-in with
`-Pq` for developers and expected in CI. Continue using the explicit
`verifyOriginalQuality -Pq` gate in CI; ordinary builds do not establish quality
qualification. Analyzer errors remain failures.

## Verified detector differences

Compared the official Maven artifacts' `findbugs.xml` and the 3.1.0 source
implementations `FindReturnRef` and `MutableStaticFields`:

- [SpotBugs 3.1.0 jar](https://repo.maven.apache.org/maven2/com/github/spotbugs/spotbugs/3.1.0/spotbugs-3.1.0.jar),
  SHA-256 `ba5d6d6a635506e83fad3919ab8621b2eb063b3037ecd11abe41a4d36816ea66`.
- [SpotBugs 3.1.0 sources](https://repo.maven.apache.org/maven2/com/github/spotbugs/spotbugs/3.1.0/spotbugs-3.1.0-sources.jar),
  SHA-256 `791029b8162b20e8936163ab024397adef1abd0c2cf2df9e42e703dd0fc7df0d`.

Four reported patterns did not exist in the original detector registry:
`CT_CONSTRUCTOR_THROW`, `AT_NONATOMIC_64BIT_PRIMITIVE`,
`JUA_DONT_ASSERT_INSTANCEOF_IN_TESTS`, and `DCN_NULLPOINTER_EXCEPTION`.
The filter defers those **exact patterns**, not their categories, prefixes or
other patterns from the same detector. Their adoption requires separate review.
The [upstream changelog](https://github.com/spotbugs/spotbugs/blob/master/CHANGELOG.md)
also records the later constructor and shared-variable detectors.

The old `FindReturnRef` already selected `EI_EXPOSE_REP`, `EI_EXPOSE_REP2`,
`EI_EXPOSE_STATIC_REP2` and `MS_EXPOSE_REP`. Its mutable-signature predicate
accepted arrays, `java.util.Hashtable`, `java.util.Date`, `java.sql.Date`, and
`java.sql.Timestamp`. It did not inspect arbitrary service, collection, or
protobuf reference types. Disabling these patterns wholesale would remove
historical coverage and is not permitted by this compatibility filter.

Instead, the filter lists 32 exact, newly analyzed reference types observed in
the migration reports. Only the four named representation-exposure patterns
are deferred for these field types. Arrays of those types are still checked;
so are the original five type families, unlisted types, missing-field reports,
other exposure patterns, null checks and lock checks. New reference types need
an explicit review before adding them. This is a bounded compatibility list,
not an exact emulation of every old detector implementation detail.

## Interpretation and follow-up

Some new reports describe intentional identity sharing: injected services,
transaction-scoped store providers and native immutable protobuf messages.
Copying a storage service or driver would violate its lifecycle contract.
Other reports concern mutable collection aliases and may justify defensive
copies after checking callers, ordering and null behavior. The compatibility
list does not classify all of these as false positives; review them separately
when adopting the expanded detector scope.

Constructor-throw reports concern finalizer attacks. Java 25 still enables
finalization by default, according to the
[Java 25 launcher documentation](https://docs.oracle.com/en/java/javase/25/docs/specs/man/java.html#standard-options).
Deprecation does not eliminate that concern. This slice neither removes
constructor validation nor makes Guice-intercepted classes final, and does not
change runtime finalization flags. The constructor pattern is deferred because
it is new policy, not because Java 25 makes it universally irrelevant.

Historical null/lock/array checks remain actionable. For example, root storage
paths have no parent and should be rejected explicitly; byte-array aliasing in
log implementations should be reviewed rather than excluded. A conditional lock
warning needs control-flow inspection; the SQLite epoch is assigned during
private initialization before publication, not concurrently incremented at runtime.

## Filter verification

`build-support/java/SpotBugsMigrationFixtures.java` loads the production filter
through SpotBugs 4.10.4's actual `Filter` API. Its 76 exact assertions verify both
excluded and retained cases, including primitive/reference/multidimensional
arrays, all old mutable class signatures, new reference types, missing fields,
unlisted classes, unrelated patterns and existing scoped exclusions. Parsing or
matching failures fail the fixture process. Run from the repository root:

```text
java -cp <resolved SpotBugs runtime classpath> build-support/java/SpotBugsMigrationFixtures.java
```

Passing these fixtures does not mean application bytecode passes SpotBugs.
The normal main/test/JMH analysis tasks must also pass with the production filter.
