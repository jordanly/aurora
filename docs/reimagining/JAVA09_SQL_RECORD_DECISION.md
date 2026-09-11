# JAVA-09 SQL record decision

Decision: keep `NativeSqlStore.AttemptRecord` and `CommandRecord` as final
classes with private constructors and public final fields. No implementation or
API change is requested by JAVA-09. The decision is based on the current
consumers, the Java 8 source path, and the historical-bundle compatibility
contract; it makes no allocation or performance claim.

## Consumer inventory

The SQL queries and constructors are in
[`NativeSqlStore.Tx.attempts` and `commands`](../../src/main/java/org/apache/aurora/scheduler/storage/sql/NativeSqlStore.java#L452).
`AttemptRecord` is constructed from an attempts/observations/allocations left
join at lines 462–464. `CommandRecord` is constructed from the commands table at
lines 475–476. The class definitions and private constructors are at
[lines 556–581](../../src/main/java/org/apache/aurora/scheduler/storage/sql/NativeSqlStore.java#L556).

The standalone scheduler is the principal Java consumer:

- [`NativeEngine.cancel` and replay handling](../../scheduler/native/src/main/java/org/apache/aurora/nativescheduler/NativeEngine.java#L87)
  read attempt and command fields, bodies, pending state, and command IDs.
- [`NativeEngine.poll` and matching](../../scheduler/native/src/main/java/org/apache/aurora/nativescheduler/NativeEngine.java#L216)
  index attempts, parse `runBody`, and reduce observations.
- [`NativeEngine.place`](../../scheduler/native/src/main/java/org/apache/aurora/nativescheduler/NativeEngine.java#L269)
  uses node, job, instance, update time, and `reserved()` for placement and
  resource accounting.
- [`NativeEngine.deliver`](../../scheduler/native/src/main/java/org/apache/aurora/nativescheduler/NativeEngine.java#L345)
  reads pending command fields and validates delivery receipts.
- [`NativeEngine.state`](../../scheduler/native/src/main/java/org/apache/aurora/nativescheduler/NativeEngine.java#L372)
  projects fields into explicit JSON objects. The wire/state projection is
  independent of the Java representation.

Focused assertions in
[`NativeEngineTest`](../../scheduler/native/src/test/java/org/apache/aurora/nativescheduler/NativeEngineTest.java#L119)
read public attempt fields and call `reserved()`; the independent ACK snapshot
assertion is at [line 279](../../scheduler/native/src/test/java/org/apache/aurora/nativescheduler/NativeEngineTest.java#L279).

The embedded compatibility helper at
[`native-jvm-compat:48`](../../build-support/native/native-jvm-compat#L48)
reads `CommandRecord.body`, `.command`, and `.attempt`. The helper source is
written and compiled with `javac --release 8` at
[lines 290–292](../../build-support/native/native-jvm-compat#L290). Its
version-neutral dispatch runs the same helper against the old and new selected
libraries; the helper file and dispatch contract are unchanged.

No tracked consumer constructs either class outside `NativeSqlStore`. No tracked
consumer compares these objects with `equals`, uses their `hashCode`, or relies
on their current identity `toString`.

## Current contract

| Contract | Current class behavior | Record conversion risk |
| --- | --- | --- |
| Fields | Public final fields consumed directly by scheduler, tests, and helper | Records require accessor calls and would break source-level field consumers |
| Construction | Private constructors; only SQL row mapping constructs instances | A public record exposes canonical construction unless a different public API is designed |
| Nullability | `AttemptRecord.node` and `runBody` may be null from the allocation left join; command columns and base attempt columns are SQL `NOT NULL` | A record would still need an explicit nullable-component policy |
| Equality/hash | Inherited identity semantics; neither class overrides them | Records introduce structural equality and component hashing |
| `toString` | Inherited identity representation; payloads are not emitted | Generated record `toString` would include command/run body strings and requires a redaction decision |
| Behavior | `AttemptRecord.reserved()` is a named method used by placement and recovery | A record conversion must preserve behavior outside components |
| JSON | `NativeEngine` explicitly projects fields and parses bodies into JSON | Java representation must remain separate from canonical wire/state JSON |
| `JobKey` and DDL | `JobKey.toString()` is the persisted slash-separated key and its hash follows that exact string; SQL primary keys and stored spellings remain unchanged | Record work must not alter key spelling, DDL, or persisted identity |

## Java 8 and historical compatibility

`native-jvm-compat` compiles the helper's source with `--release 8`, then can run
that Java 8 class file under the selected old or new JRE while loading the
selected libraries. The concern is therefore not that Java 8 bytecode cannot
load on a newer JRE. The concern is the historical public-field ABI and shared
Java 8 source contract: an accessor-only replacement would not compile the
current helper source, and old bundles do not acquire accessors retroactively.

The independent smoke path also compiles current SQL sources with the pinned
Java 8 compiler using `-source 8 -target 8` in
[`native-smoke`](../../build-support/lab/native-smoke#L157). Java records cannot
be introduced into that shared SQL source path without a separately designed
adapter or versioned source boundary.

The original Java 8 conformance adapter
[`Canonical.java`](../../protocol/native-v1alpha1/conformance/Canonical.java)
is independent and has no record consumer. Any old/new qualification after a
future JAVA-08 SQL change remains pending; this decision does not claim a new
SQL compatibility qualification or change persisted schemas.

Keeping the classes preserves the established public-field, private-constructor,
nullable-row, identity-equality, diagnostic, `reserved()`, JSON-projection,
`JobKey`, and DDL contracts. Revisit only after a versioned helper/adapter design
defines construction, nullability, redacted diagnostics, and old/new snapshot,
write/replay, rollback, and launcher evidence.
