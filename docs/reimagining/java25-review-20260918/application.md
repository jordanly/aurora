# Application, HTTP, security, discovery and Thrift review

Baseline: `91b9bd74746d27102a96fa47d4e06e0dd25097ca`. Review date: 2026-09-18. This is a bounded source review of all 166 assigned Java files, including tests, against the supplied SHA-256 manifest. No production/test code was edited and no builds or tests were run. Findings describe the current original scheduler application; the already completed Java 25/Jakarta/Jetty/RESTEasy/Guice/Shiro migration is not proposed again. Prior audit conclusions were not treated as current evidence. Priorities indicate implementation value: P1 correctness/security first, P2 concrete reliability or ownership improvement, P3 optional simplification.

## Findings

### APP-001 — P1: Normalize an explicitly empty prune status set

**Bug.** `src/main/java/org/apache/aurora/scheduler/thrift/SchedulerThriftInterface.java:1013` rejects active statuses but only supplies terminal defaults when the field is unset. An explicitly set empty set passes both branches. `src/main/java/org/apache/aurora/scheduler/storage/TaskStore.java:134` treats an empty status set as unrestricted; `src/main/java/org/apache/aurora/scheduler/storage/sqlite/SqliteTaskStore.java:50` uses that filter. The selected IDs are then passed directly to deletion at `SchedulerThriftInterface.java:1033`; `src/main/java/org/apache/aurora/scheduler/state/StateManagerImpl.java:411` does not enforce terminal status. This can delete active task records through an admin API documented as terminal-only.

Normalize absent **or empty** statuses to terminal states on a copied query, then reject any explicitly non-terminal status. Keep role/job/task restrictions, response conventions and limit handling. Do not silently change pagination semantics in this fix. Future validation: real-store tests containing active and terminal tasks for unset, empty, terminal-only and mixed statuses, proving active records and counters remain intact. Existing coverage starts at `src/test/java/org/apache/aurora/scheduler/thrift/SchedulerThriftInterfaceTest.java:692` but omits empty sets.

### APP-002 — P1: Validate audit payloads before entering fail-stop writes

**Bug and clearer ownership.** `src/main/java/org/apache/aurora/scheduler/thrift/SchedulerThriftInterface.java:955` constructs `AuditData` inside a storage write; start-update does likewise at line 926. Its constructor rejects messages longer than 1024 characters (`src/main/java/org/apache/aurora/scheduler/updater/JobUpdateController.java:46`). That unchecked rejection escapes the callback. The current SQLite adapter invokes its failure policy for escaping write exceptions (`src/main/java/org/apache/aurora/scheduler/storage/sqlite/SqliteStorage.java:175`), and production connects that policy to execution abort (`src/main/java/org/apache/aurora/scheduler/execution/go/GoAgentModule.java:91`). An ordinary invalid update request can therefore stop the scheduler.

Prepare and validate audit data outside the write boundary, returning the intended invalid-request response before touching storage. Keep the existing shared `changeJobUpdateState` operation abstraction and the fail-stop policy for actual write failures; do not broadly catch exceptions after mutations. Future validation: overlong start/pause/resume/abort/rollback messages with real SQLite and a failure observer, followed by a successful write. `src/test/java/org/apache/aurora/scheduler/thrift/SchedulerThriftInterfaceTest.java:1903` currently expects the raw exception for an overlong pause message; revise that contract deliberately. The neighboring real-SQLite quota tests provide a useful pattern.

### APP-003 — P1: Apply existing secret-redaction intent at every logging entry point

**Bug.** `src/main/java/org/apache/aurora/scheduler/config/CommandLine.java:132` logs every converted option value, including the literal ZooKeeper `user:password` declared at `src/main/java/org/apache/aurora/scheduler/discovery/FlaggedZooKeeperConfig.java:69`. `src/main/java/org/apache/aurora/scheduler/http/api/ApiBeta.java:122` logs the raw JSON body before the Thrift logger can blank executor data. `src/main/java/org/apache/aurora/scheduler/http/api/security/AuthorizeHeaderToken.java:38` includes a rejected authorization header in an exception which `ShiroKerberosAuthenticationFilter.java:65` logs at INFO. A Basic credential mistakenly sent to a Negotiate endpoint follows this path.

Coordinate with the foundation review of `commons/src/main/java/org/apache/aurora/common/stats/JvmStats.java:207`: diagnostic export needs the same explicit allowlist/redaction policy. The application binds `/vars` and `/vars.json` at `JettyServerModule.java:242`, and `HttpSecurityModule.java:182` installs the Shiro servlet filter only for `API_PATH`, not these stats endpoints. `VarsHandler.java:42` and `VarsJsonHandler.java:68` render supplied stat values without redaction. Network/proxy ACLs were not inspected; application-level authentication cannot be assumed for these endpoints.

Give logging one explicit redaction policy: suppress credential-bearing option values and parsed authentication/configuration objects; log API method/metadata rather than raw bodies; report malformed header shape without its value. Keep useful option names and safe diagnostics. Preserve `LoggingInterceptor`'s deep-copy redaction and never mutate request objects. Future validation: capture log events and assert sentinel passwords, header payloads and executor data are absent on success and malformed-input paths. `CommandLineTest.java:167`, `AuthorizeHeaderTokenTest.java:29`, `ApiBetaTest.java:89` and `LoggingInterceptorTest.java:129` already supply relevant fixtures but do not assert log redaction.

### APP-004 — P2: Use wide arithmetic for update validation

**Bug.** `src/main/java/org/apache/aurora/scheduler/thrift/SchedulerThriftInterface.java:841` multiplies two `int` values before comparing to the failure cap. For example, two instances and `Integer.MAX_VALUE` per-instance failures produce a negative product and bypass the cap. Variable group sizes also sum into an `int` at line 824, so individually valid positive groups can overflow and be rejected or misclassified.

Promote operands before multiplication and use a `long` sum (for example `mapToLong(...).sum()`), keeping existing rejection messages and normal-input behavior. Keep validation local; a generic numeric validation framework adds little. Future validation: exact limit, one above limit, `Integer.MAX_VALUE` and overflowing group totals, asserting rejection precedes storage or controller calls. Existing ordinary failure-cap tests are at `src/test/java/org/apache/aurora/scheduler/thrift/SchedulerThriftInterfaceTest.java:1600`.

### APP-005 — P2: Make malformed discovery data obey one decode-error contract

**Bug.** `src/main/java/org/apache/aurora/scheduler/discovery/Encoding.java:68` throws `JsonParseException` for missing required fields, but `CuratorServiceGroupMonitor.java:99` catches only its narrower `JsonSyntaxException` subtype. Thus `{}` is rejected by the codec but escapes the monitor instead of being skipped. A null additional endpoint also dereferences null at `Encoding.java:63`. Downstream leader-health and redirect requests consume this monitor.

Validate each endpoint and normalize malformed payload failures to one documented JSON parse exception; catch that contract at the monitor boundary. Keep malformed nodes isolated while retaining valid members. Preserve legacy JSON names, `ALIVE` default and omitted additional-endpoints behavior—these make a mechanical record conversion inappropriate. Future validation: monitor tests mixing valid nodes with `{}`, missing host and null additional endpoint; retain exact wire tests at `src/test/java/org/apache/aurora/scheduler/discovery/EncodingTest.java:50`. Codec tests already catch `JsonParseException` at line 94, explaining why existing tests miss the boundary mismatch.

### APP-006 — P2: Make nearest-fit publication and replacement atomic

**Bug.** `src/main/java/org/apache/aurora/scheduler/metadata/NearestFit.java:67` publishes a `Fit` whose `vetoes` starts null; line 121 mutates it after retrieving it from a concurrent cache. Readers return that field at line 86 without synchronization. Concurrent updates compare and replace at lines 162–170 without a common lock. `getPendingReasons` being synchronized does not protect the unsynchronized writers. HTTP/Thrift readers can observe null/stale values, and overlapping writers can retain a worse score.

Keep the cache and its expiration semantics; put synchronization on the actual per-fit state, expose a snapshot getter returning `NO_VETO` before initialization, and serialize compare/update. An immutable snapshot plus atomic reference is also reasonable, but do not reset the existing expire-after-write clock on every veto by casually moving the whole value into `compute`. `NotifyingSchedulingFilter.java:68` is the scheduler-side writer; the HTTP and read-only Thrift paths are readers. Future validation: controlled concurrent read/first-update and competing scores, plus existing scoring, invalidation and fake-ticker expiration cases in `NearestFitTest.java:77`.

### APP-007 — P2: Use one pending-group snapshot per response

**Bug and simplification.** `src/main/java/org/apache/aurora/scheduler/http/PendingTasks.java:61` obtains groups to compute reasons, then obtains them again at line 67. `TaskGroups.java:247` returns a fresh snapshot of a concurrent map. A group added between calls has no reason entry and `.toString()` at line 70 fails.

Store the first group snapshot in a local and use it for both operations. Reuse a configured mapper only if its lifecycle is clear; that is secondary to snapshot consistency. Preserve the existing string-valued `reason` JSON contract. Future validation: a mock returning different memberships on successive calls, plus existing JSON assertions at `src/test/java/org/apache/aurora/scheduler/http/PendingTasksTest.java:150`; assert a single group fetch. This addresses membership consistency, not complete immutability of mutable `TaskGroup` members.

### APP-008 — P2: Resolve leader status and redirect from one membership observation

**Reliability improvement.** `src/main/java/org/apache/aurora/scheduler/http/LeaderRedirect.java:133` observes membership, then line 138 calls a helper that observes it again. `LeaderRedirectFilter.java:69` gets status and later gets a redirect using another observation. Existing tests explicitly encode two reads per status (`src/test/java/org/apache/aurora/scheduler/http/LeaderRedirectTest.java:82`). Changes between reads create contradictory status/target combinations and avoidable 503 responses.

Create one internal leader observation per request and derive status/optional endpoint from it; a small private record is justified here because it carries correlated state, unlike a blanket conversion of HTTP beans. Keep fail-closed handling of zero/multiple leaders, the bypass header, 307 status, original rewritten path and query-string handling. Avoid an extra global cache. Future validation: membership changes between potential reads, all three statuses, local-host/different-port, original paths and query strings. A separate small URI improvement may use `HostAndPort.toString()` for bracketed IPv6 authority, but must preserve already encoded paths.

### APP-009 — P2: Remove the redundant HTTP response-status wrapper

**Bug and simplification.** `src/main/java/org/apache/aurora/scheduler/http/HttpStatsFilter.java:43` keeps a second status field updated only by `setStatus`. Calls such as `sendError` and `sendRedirect` alter the servlet response without updating that field. The Kerberos filter uses `sendError` at `ShiroKerberosAuthenticationFilter.java:66` and line 91, so completed 400/401 requests can be counted as 200.

Pass the original response through the chain and use its `getStatus()` after a normal return. This removes a class and follows the current servlet API rather than emulating an older API limitation. Preserve metric names and the current treatment of throwing requests; separately deciding how to count exception/error redispatch or asynchronous completion should be an explicit change. Future validation: status set directly, `sendError`, `sendRedirect`, default success and a throwing chain, with no double counting.

### APP-010 — P2: Give Jetty startup failure an explicit cleanup path

**Ownership gap.** `src/main/java/org/apache/aurora/scheduler/http/JettyServerModule.java:393` opens the connector before starting the server. A subsequent startup failure is wrapped without stopping or destroying the partially started server; host resolution at line 403 can also fail after the server is running. Cleanup lives only in `shutDown` at line 415, which does not establish rollback ownership for a failed `startUp`.

Use a local server during acquisition, guard the complete initialization/address-resolution sequence, and stop/destroy acquired resources on failure while preserving the primary exception and suppressed cleanup failures. Publish the address only after success. Keep the original application and Guice/RESTEasy bridge, endpoint mappings, compression and shutdown order. Future validation: connector success followed by listener/start failure and address-resolution failure, demonstrating released port/threads; retain `ServletFilterTest` and `ApiIT` wire behavior. This is a source-level ownership finding, not an observed leak from a test run.

### APP-011 — P2: Replace the generated JAAS file with programmatic configuration and owned credentials

**Coordinated simplification and lifecycle improvement.** `src/main/java/org/apache/aurora/scheduler/http/api/security/Kerberos5ShiroRealmModule.java:140` constructs configuration text, writes a temp file, reparses it with `ConfigFile`, logs in during Guice configuration and binds a shared credential. Neither `LoginContext.logout()` nor shared credential disposal has an owner. Interpolated paths/principals also require text escaping that a map-based configuration avoids.

Use an explicit `javax.security.auth.login.Configuration` returning the existing Krb5LoginModule entry/options. Move login and credential acquisition into an owned provider/service with shutdown cleanup and partial-failure rollback. Dispose the shared credential only after request handling stops; request-local GSS contexts remain owned by `Kerberos5Realm`. Preserve `Subject.callAs`, both mechanism OIDs, ACCEPT_ONLY, existing option values and exception-cause semantics. Future validation: quoted/backslash keytab paths, login/acquisition failures, one-time logout/disposal and HTTP-before-credential shutdown. Keep all current scoped-subject and cause-preservation tests in `Kerberos5ShiroRealmModuleTest`; do not simplify the carefully tested request-context cleanup in `Kerberos5Realm` merely to reduce lines.

### APP-012 — P3: Remove one-use validation machinery in ConfigurationManager

**Preference with a small error-path benefit.** `src/main/java/org/apache/aurora/scheduler/configuration/ConfigurationManager.java:81` declares a generic `Validator` implemented only by `GreaterThan`, then creates three one-use objects at lines 446–454. Container selection at lines 413–429 wraps both branches in `Optional.of`, making the subsequent empty check unreachable; an unset union field fails in `of` first.

Replace the one-use validator class/interface with a plainly named local helper and select the container enum directly, with an explicit null/empty-union rejection where intended. Use `anyMatch` for the GPU existence check. Keep the order and wording of resource validation, backfill, tier rules and backend validation; do not change non-finite-number policy incidentally. Do not introduce a general validation framework or split every check into its own class. Future validation: existing `ConfigurationManagerTest` matrix plus an unset container union; expected invalid descriptions should remain `TaskDescriptionException`.

### APP-013 — P2: Strip response configs eagerly and tolerate absent executors

**Bug on the retained executor-free task path and simplification.** `src/main/java/org/apache/aurora/scheduler/thrift/ReadOnlySchedulerImpl.java:172` uses a lazy `Lists.transform` whose function mutates each task and unconditionally dereferences executor config. The application still has an explicitly tested executor-free Docker configuration (`src/test/java/org/apache/aurora/scheduler/thrift/ThriftIT.java:164`, `ConfigurationManagerTest.java:199`), even though the production Go backend requires its own executor cohort. Requesting the stripped task view for those stored tasks can fail while the lazy response list is consumed.

Obtain the already copied task builders once, loop over them, and unset data only when executor config is present. Return that concrete list. This makes mutation ownership and serialization timing explicit without a helper abstraction. Preserve task order, pagination, all non-executor fields and immutability of stored entities. Future validation: mixed tasks with/without executors, repeated response iteration/serialization, and unchanged full stored configuration; extend `ReadOnlySchedulerImplTest.java:356` and the retained integration path.

### APP-014 — P2: Publish Curator connection state safely and simplify its counters

**Concurrency bug plus optional local cleanup.** `src/main/java/org/apache/aurora/scheduler/discovery/CuratorServiceDiscoveryModule.java:63` holds plain `currentState`; the connection callback writes it at line 145 while exported gauges read it at line 100 on another thread. The atomic counters do not make this field visible to independent gauge reads.

Make the state volatile (or an explicitly shared atomic reference). Replace the anonymous gauge supplier with a lambda. An enum-keyed counter map can replace the five parallel fields and repeated switch increments if that makes ownership clearer; retain exact metric names, initial all-zero gauges and transition counting. Do not change leader election handling of LOST versus SUSPENDED. Future validation: invoke captured state listener and read every gauge/counter, including publication from another thread; extend `CuratorDiscoveryModuleTest` beyond binding/ACL coverage.

### APP-015 — P2: Repair fixtures that silently erase distinct cases

**Test defects.** `src/test/java/org/apache/aurora/scheduler/thrift/ReadOnlySchedulerImplTest.java:642` builds every requested update key using `"id" + 1`; conversion to a set at line 654 collapses five supposed updates to one. `src/test/java/org/apache/aurora/scheduler/http/StateTest.java:76` and line 77 construct equal configs despite the test comment claiming different configs for one job. `src/test/java/org/apache/aurora/scheduler/thrift/SchedulerThriftInterfaceTest.java:473` unsets all resources after supplying a zero disk, so that case does not isolate disk validation.

Use distinct update IDs/config values, assert fixture cardinality before the endpoint call, and retain the zero-disk resource set. These are targeted corrections, not grounds to replace the broad test suite or introduce a fixture framework. Future validation: updated tests must prove multiple update entries survive, distinct same-job configs remain distinct, and zero disk is rejected independently of missing-resource validation.

## Abstractions and Java idioms worth retaining

- Keep `AnnotatedAuroraAdmin`, `AuthorizingParam`, typed `FieldGetter` composition and the permission SPI. They define authorization scope across generated APIs; a runtime pattern switch cannot replace the metadata/type validation contract without careful redesign.
- Keep `ReadOnlySchedulerImpl` separate from the decorated write facade and preserve interceptor ordering. Keep `Responses`, `AuditMessages`, `SanitizedConfiguration`, `ExecutorSettings` and the existing `ExecutorConfig` record: each has a concrete boundary or invariant.
- Keep the Guice/RESTEasy resource factory bridge and custom Thrift union codec. The newer APIs are already in use. Reflection exception propagation, fresh workload counter instances and stream-close suppression are explicitly tested; removing compatibility helpers or caching counter instances would change behavior.
- Keep bounded log readers and explicit interruption restoration in `TaskLogs`. Virtual threads are already used appropriately in its concurrency test; removing the semaphore because threads are cheap would discard backpressure.
- Keep Guava where it encodes behavior: contiguous instance ranges, immutable iteration order, multimaps, timed caches and service lifecycle. `Set.of`, records and streams are not interchangeable replacements for those contracts. In particular, `Mname`'s ordered port preference and discovery's Gson defaulting must survive any modernization.
- Small converters, qualifiers, validators, endpoint adapters and tests marked `retain` in the ledger were read and have no implementation recommendation from this pass. Their simplicity is a reason to leave them alone.

## Coverage and limits

Coverage: **166/166 files, 23,323 source lines; 121 retain, 16 local, 29 coordinated, 0 not-reviewed.** All manifest hashes matched at artifact generation. The adjacent `application.jsonl` records every assigned path, original SHA-256, file-specific rationale and applicable finding IDs. Source and test bodies were read; related storage, execution, task-group and lifecycle callers were checked for substantive findings. There was no runtime reproduction, external API compatibility experiment, Kerberos server or live ZooKeeper session. The proposed tests are future validation, not reported results. Java 25 idioms are used only where they clarify state or remove redundant machinery; no preview-feature migration is needed here.

The highest implementation value is APP-001 (terminal-only deletion), APP-002 (invalid audit input crossing the fail-stop boundary), and APP-003 (secret logging). Then address concrete publication/snapshot bugs before optional syntax cleanup. The user’s stop-at-20%-usage condition cannot be measured with available tools; this report makes no claim that such a threshold was observed.
