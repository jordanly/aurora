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

# PMD 7 migration notes

The Aurora rulesets use explicit PMD 7.27.0 category references instead
of enabling new categories wholesale. The comparison baseline is the PMD 5.5.3
ruleset XML, with PMD 6.55.0 deprecated aliases and PMD 7.27.0 category XML
checked from the official `net.sourceforge.pmd:pmd-java` artifacts. Built-in
successors can have broader checks and different reporting locations; this is
not a claim that every default or diagnostic is identical across PMD versions.

| Historical rule(s) | PMD 7 disposition |
| --- | --- |
| Four `*StmtsMustUseBraces` rules | `ControlStatementBraces` |
| Empty control-statement/block/initializer rules | `EmptyControlStatement` |
| `EmptyStatementNotInLoop` | `UnnecessarySemicolon` |
| `BooleanInstantiation` | `PrimitiveWrapperInstantiation` |
| `VariableNamingConventions` | Field, local-variable and formal-parameter naming rules |
| `MisleadingVariableName` | Local-variable/formal-parameter naming rules reject `m_` names |
| `SuspiciousConstantFieldName` | `FieldNamingConventions` rejects uppercase mutable fields |
| `UnusedModifier`, `UnnecessaryFinalModifier` | `UnnecessaryModifier`, including implied interface modifiers |
| Duplicate, java.lang, same-package and unused imports | `UnnecessaryImport` |
| `InvalidSlf4jMessageFormat` | `InvalidLogMessageFormat` |
| `BadComparison` | `ComparisonWithNaN` |
| `MissingBreakInSwitch` | `ImplicitSwitchFallThrough` |
| Both literal-order rules | `LiteralsFirstInComparisons` |
| `UnsynchronizedStaticDateFormatter` | `UnsynchronizedStaticFormatter` |
| Five JUnit assertion simplification rules | `SimplifiableTestAssertion` |
| `UseSingleton` / `UseUtilityClass` | `InstantiableUtilityClass` in main; explicit test exclusion preserved |
| `AvoidConstantsInterface` / `ConstantsInInterface` | Consolidated into `ConstantsInInterface` |
| `AbstractNaming` | Compatibility XPath in test only; common explicitly excluded it |
| `ReturnEmptyArrayRatherThanNull` | Compatibility XPath in main and test |

`ClassNamingConventions` defaults do **not** require the `Abstract` prefix.
Using that rule alone would silently drop the test ruleset's original
`AbstractNaming` check. The compatibility rule preserves both the abstract-class
prefix requirement and the prohibition on that prefix for non-abstract classes.
The common/main rules do not gain that requirement.

`ReturnEmptyCollectionRatherThanNull` is the documented successor to
`ReturnEmptyArrayRatherThanNull`, but also checks collections/maps and nested
returns. The compatibility XPath preserves the historical array-return and
direct-body `return null` scope, including its priority 1. This avoids adding
unrelated collection-return requirements during the tooling migration.

`ConstantsInInterface` retains the historically enabled check for fields in
interfaces without methods. Its PMD 7 selector uses direct interface members,
where the old XPath used descendants; nested-type corner cases can therefore
differ. Neither constants-interface coverage nor unused-modifier coverage is
retired. `SignatureDeclareThrowsException` remains explicitly excluded in test;
`LoggerIsNotStaticFinal` was already excluded in common.

## Custom annotation rules and fixture gate

`timed.xml` uses PMD 7 `ModifierList/Annotation` nodes, as described in the
[official AST migration guide](https://pmd.github.io/pmd/pmd_userdocs_migrating_to_pmd7.html#java).
The method rule inspects annotations directly attached to that method. The
class rule inspects annotations on immediate class-body members, retaining the
historical constructor/member scope without attributing a nested class's methods
to its outer class. Parameter annotations and nested annotation values are not
method annotations. The historical simple, unqualified `@Timed` spelling is
preserved; adding fully qualified annotation matching is a separate change.

`build-support/java/PmdMigrationFixtures.java` executes 106 exact positive and
negative cases using production rules and PMD's analysis API. Cases cover method
modifiers, class visibility/finality, nested/local classes, parameters, annotation
values, constructors, array-return scope, abstract names, and built-in successor
behavior. It also checks main/common/test ruleset inclusion and exclusion wiring.
Any missing/unexpected violation, configuration error or processing error fails
the gate; incremental analysis is disabled. Run from the repository root with
Java 25 and the resolved PMD runtime plus test dependency classpath using source-file launch mode:

```text
java -cp <PMD runtime classpath> build-support/java/PmdMigrationFixtures.java
```

The fixture gate checks migration semantics. Passing it does not imply that
application/test sources pass PMD, Checkstyle, or the behavior/coverage gates.

## Type analysis and narrowly scoped compatibility

The historical Gradle integration set each PMD task's `classpath = null` to
avoid then-current type-resolution false positives. This migration keeps resolved
type analysis; removing the auxiliary classpath would weaken unrelated rules.

PMD 5.5.3 `CloseResource` defaulted to `java.sql.Connection`,
`java.sql.Statement`, and `java.sql.ResultSet` (verified from its rule class).
Both production rulesets specify those types explicitly. PMD 7's broader
AutoCloseable checks, including Java 25 executor interfaces, are separate policy
adoption: injected resources often transfer to service lifecycle ownership, and
closing them at construction would be incorrect. Fixtures ensure all three SQL
leaks remain reported, a try-with-resources connection passes, and generic
AutoCloseable/executor checks are not silently introduced.

Two rule-specific suppression XPath expressions address demonstrated AST false
positives. `OverrideBothEqualsAndHashcode` ignores only a `hashCode` method
immediately within a record body, which retains compiler-generated equality.
Ordinary classes, classes nested in records, and explicit record `equals` methods
remain checked. `CheckResultSet` recognizes navigation passed directly to resolved
`org.junit.Assert.assertTrue(boolean)` or `assertTrue(String, boolean)`. Calls to
an unrelated method named `assertTrue`, arbitrary consumers, and a navigation
call nested inside another consumer are not suppressed. These fixtures require
JUnit 4 on the auxiliary classpath and fail immediately if it is absent.

`ClassWithOnlyPrivateConstructorsShouldBeFinal` uses the original PMD 5.5.3
XPath scope translated to PMD 7 nodes: a single top-level type, at least one
explicit private constructor, no non-private constructors and no nested class
or interface. The old rule did not require final on implicitly constructed
private nested classes. Fixtures preserve an actual explicit-private-constructor
violation and cover the historical exclusions. Adoption of PMD 7's broader rule
is separate from restoring this gate.


## Original benchmark analyzer scope

The original build applied the JMH 0.4.4 plugin, which added the `jmh` source
set. Gradle 4.10's `AbstractCodeQualityPlugin` defaulted its source-set collection
to **all Java source sets**, created a task for each and attached those tasks
to `check`. Both the PMD plugin and SpotBugs plugin 1.6.4 inherited that behavior.
The JMH plugin did not override analyzer scope. Therefore `-Pq check` originally
included `pmdJmh` and `spotbugsJmh`; Checkstyle also explicitly included `jmh`.
Restoring only main/test analyzers would silently reduce the original scope.

The PMD distinction is significant: only `pmdMain` and `pmdTest` received Aurora's
custom `ruleSetFiles`. `pmdJmh` retained Gradle's default `java-basic` ruleset.
PMD 5.5.3's `rulesets/java/basic.xml` contains 24 rules. `jmh.xml` preserves that
exact inventory using the same migrated mappings and narrow compatibility
properties as `common.xml`, including `BooleanInstantiation` mapped to
`PrimitiveWrapperInstantiation`. It does not inherit Aurora's additional
naming, unused-code, imports or main/test policy rules. Use `ruleSets = []`
and `ruleSetFiles = files('config/pmd/jmh.xml')` for the modern `pmdJmh` task.
The historical global `Pmd.classpath = null` disabled auxiliary type resolution;
modern type-aware compatibility fixtures are documented separately above.

SpotBugs JMH uses the same maximum effort and shared exclusion filter as the
other source sets, analyzing `sourceSets.jmh.output.classesDirs` with
`sourceSets.jmh.compileClasspath` as auxiliary classes. Checkstyle JMH uses
the shared Checkstyle configuration over `sourceSets.jmh.allJava`. All three
JMH analyzer tasks belong in the explicit original-quality gate as well as
`check` when `-Pq` enables quality checks.

Primary implementation evidence:

- [Gradle 4.10.3 AbstractCodeQualityPlugin](https://github.com/gradle/gradle/blob/v4.10.3/subprojects/code-quality/src/main/groovy/org/gradle/api/plugins/quality/internal/AbstractCodeQualityPlugin.java)
- [Gradle 4.10.3 PmdPlugin default java-basic rules](https://github.com/gradle/gradle/blob/v4.10.3/subprojects/code-quality/src/main/groovy/org/gradle/api/plugins/quality/PmdPlugin.java)
- [SpotBugs plugin 1.6.4 source-set configuration](https://github.com/spotbugs/spotbugs-gradle-plugin/blob/1.6.4/src/main/java/com/github/spotbugs/SpotBugsPlugin.java)
- [JMH plugin 0.4.4 source-set creation](https://github.com/melix/jmh-gradle-plugin/blob/RELEASE_0_4_4/src/main/groovy/me/champeau/gradle/JMHPlugin.groovy)
- [Official PMD 5.5.3 Java artifact containing basic.xml](https://repo.maven.apache.org/maven2/net/sourceforge/pmd/pmd-java/5.5.3/pmd-java-5.5.3.jar)

`LooseCoupling` also expanded its scope. The PMD 5.5.3 type-resolved rule
selected in the original test ruleset and the older coupling-category rule both
use `CollectionUtil.isCollectionType(..., false)`. The exact-class table contains
only `ArrayList`, `LinkedList`, `Vector`, `HashMap`, `LinkedHashMap`, `TreeMap`,
`TreeSet`, `HashSet`, `LinkedHashSet` and `Hashtable`, all from `java.util`;
`TypeMap.contains(Class)` uses equality, not subtype matching. This is verified
in the official [PMD 5.5.3 core sources](https://repo.maven.apache.org/maven2/net/sourceforge/pmd/pmd-core/5.5.3/pmd-core-5.5.3-sources.jar).
The old rule checked field, formal-parameter and method-result type positions.

The test ruleset retains the modern rule, with a suppression XPath limiting
reported nodes to those exact resolved types and declaration positions. This
preserves all ten collection checks without treating Guava immutable collections,
Shiro `Ini`/`Section`, local variables or custom subclasses as newly prohibited
API types. Fixtures check every historical type in all three declaration
positions, retain array-field detection, and pair each of five Guava/Shiro types
against both unmodified PMD 7 (reported) and the compatibility rule (accepted).
The fixture's auxiliary classpath includes the actual test dependencies, so these
are resolved-type assertions rather than unresolved-symbol false negatives.
