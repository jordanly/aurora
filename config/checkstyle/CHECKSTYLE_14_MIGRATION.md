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

# Checkstyle 14 migration notes

The Aurora rules in `checkstyle.xml` remain the source of truth. This file
records compatibility changes needed by Checkstyle 14.1.0 so that a future
upgrade does not silently drop a historical rule.

## Javadoc summary checks

Checkstyle 13.9.0 removed `JavadocStyle` and directs users to
`SummaryJavadoc`. The configuration therefore uses `SummaryJavadoc` in its
place. Checkstyle's release notes describe the replacement as fully covering
the old check. Its default summary and Javadoc parsing validation preserve the
old first-sentence/HTML behavior, and its default missing-summary validation
covers the former `checkEmptyJavadoc=true` setting.

## Star-import exclusions

The historical configuration declared `AvoidStarImport.excludes` twice.
Checkstyle configuration properties are keyed by name, so the later value
(`java.util.*`) replaces the earlier `org.junit.Assert` value. The migrated
configuration keeps that effective behavior rather than broadening the
exclusion set.

## Module placement and removed module

`LineLength` checks file text and is a direct `Checker` child in Checkstyle
14.1.0; it was moved out of `TreeWalker` while retaining its original
`max=100` and import-ignore pattern. `FileContentsHolder` was removed in
Checkstyle 8.2 because file contents are available to filters through the
TreeWalker audit event. The old configuration only included it as support for
a filter that is no longer configured, so it was removed without changing an
active rule.

Header and suppression paths use `${config_loc}` so isolated Gradle analyzer
workers resolve them from the configuration directory.

## Effective field Javadoc scope

The historical `JavadocVariable` configuration had default `scope=private` and
explicit `excludeScope=private`. In Checkstyle 7.3, `Scope.isIn` compares enum
ordinals: public, protected and package scopes are all included in private.
`JavadocVariableCheck.shouldCheck` first requires both field and surrounding
scope to be inside private, then excludes exactly those same combinations.
Consequently this rule checked **no fields**, including public fields and fields
inside nested types. Mapping it to `public,protected,package` accidentally adds
requirements that the original configuration never enforced.

The migrated module remains present with an explicit empty `accessModifiers`
list. This preserves the effective historical policy; requiring field Javadocs
would be a separate policy change. `JavadocType` and summary checks remain.

Primary sources: [7.3 JavadocVariableCheck](https://github.com/checkstyle/checkstyle/blob/checkstyle-7.3/src/main/java/com/puppycrawl/tools/checkstyle/checks/javadoc/JavadocVariableCheck.java)
and [7.3 Scope](https://github.com/checkstyle/checkstyle/blob/checkstyle-7.3/src/main/java/com/puppycrawl/tools/checkstyle/api/Scope.java).

## Import grouping

The regex `/^javax?\./` intentionally puts `java` and `javax` in the **same**
group. Checkstyle 7.3 required a separator between distinct groups but allowed
separators inside a group. Checkstyle 14 adds an extra-separation diagnostic;
`SuppressionSingleFilter` suppresses only that diagnostic from `ImportOrder`.
Wrong ordering and missing required separators remain errors. The historical
custom message listed java and javax separately, but did not change the regex.

The old check applied the same groups, separation requirements and lexical
ordering to static imports at the bottom. The new check has separate static
settings, so `staticGroups`, `separatedStaticGroups` and
`sortStaticImportsAlphabetically` now explicitly preserve that behavior.

Primary sources: [7.3 ImportOrderCheck](https://github.com/checkstyle/checkstyle/blob/checkstyle-7.3/src/main/java/com/puppycrawl/tools/checkstyle/checks/imports/ImportOrderCheck.java)
and [14.1.0 ImportOrderCheck](https://github.com/checkstyle/checkstyle/blob/checkstyle-14.1.0/src/main/java/com/puppycrawl/tools/checkstyle/checks/imports/ImportOrderCheck.java).

## Paired regression fixtures

`verify-migration.py` extracts the migrated scope/import rules directly from
`checkstyle.xml` and compares them with their historical settings. Run it with
`--java PATH`, `--old-classpath CLASSPATH`, `--new-classpath CLASSPATH` and
`--receipt FILE`; each classpath must include that Checkstyle version and its
runtime dependencies. The helper runs no downloads or Gradle tasks.

Eighteen paired fixtures cover all field visibilities and nested types; allowed java/javax
and within-group gaps; rejected wrong group order, lexical order and missing
separators; and corresponding static import cases, including bottom placement.
They also retain FinalClass failures for explicit private constructors and
EqualsHashCode failures for ordinary classes with only one counterpart method.
Both 7.3 and 14.1.0 must report each expected violation count and the matching
CLI exit count. Three additional modern-parser fixtures exercise records
(Checkstyle 7.3 predates record syntax): custom hashCode alone, custom equals
alone, and a nested ordinary class that must still fail. These fixtures passed on pinned Java 25. They qualify these
specific mappings, not equivalence of every rule in the full configuration.

These mappings follow the [release notes](https://checkstyle.org/release-notes.html)
and [JavadocVariable documentation](https://checkstyle.org/checks/javadoc/javadocvariable.html).


## FinalClass selector compatibility

Checkstyle 7.3 required an explicitly declared private constructor before
reporting a non-final class. It also excluded classes directly contained in
interfaces and annotation definitions. Checkstyle 14 additionally diagnoses
private nested classes with implicit constructors, and checks the formerly
excluded interface/annotation members. Two exact AST conditions in a
`SuppressionXpathSingleFilter` preserve the old selector: no direct `CTOR_DEF`,
or a class whose parent object block belongs to an interface/annotation.
This does not suppress nested classes generally: a nested class with an
explicit private constructor still fails, as the paired negative fixture proves.
In particular, implicit-constructor AOP fixtures need not become final merely
to satisfy a newly broadened check.

Primary sources: [7.3 FinalClassCheck](https://github.com/checkstyle/checkstyle/blob/checkstyle-7.3/src/main/java/com/puppycrawl/tools/checkstyle/checks/design/FinalClassCheck.java)
and [14.1.0 FinalClassCheck](https://github.com/checkstyle/checkstyle/blob/checkstyle-14.1.0/src/main/java/com/puppycrawl/tools/checkstyle/checks/design/FinalClassCheck.java).

## Compiler-provided record counterparts

Java records supply equals and hashCode when those methods are not explicitly
declared. Checkstyle 14.1.0 EqualsHashCode tracks explicit method AST nodes and
can report a missing counterpart when a record customizes just one of them.
A filter restricted to `RECORD_DEF/OBJBLOCK/METHOD_DEF` accommodates these
compiler-provided methods. Methods in ordinary classes, including ordinary
classes nested inside records, remain checked. This is a syntax compatibility
extension beyond what the old parser could process, not a claim that 7.3
understood records or that this style check proves semantic equality correctness.

Primary source: [14.1.0 EqualsHashCodeCheck](https://github.com/checkstyle/checkstyle/blob/checkstyle-14.1.0/src/main/java/com/puppycrawl/tools/checkstyle/checks/coding/EqualsHashCodeCheck.java).
