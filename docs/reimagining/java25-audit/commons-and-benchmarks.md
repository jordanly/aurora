# Commons and JMH benchmarks

[Audit overview and ordering](../JAVA25_FILE_AUDIT.md).

104 files: 3 local-change candidates, 31 deferred, 70 retained.

Source baseline: `6cf7f0ea07c6355af62ceafb029fe086f55b8c65`. Findings describe proposed work; validation is not yet executed.

Retain means no separate improvement prioritized in this pass. A local candidate still needs
the stated contract checks. Framework findings are grouped migrations, not separate PRs per file.

| Source file | Assessment | Recommendation or retain rationale |
| --- | --- | --- |
| [commons/src/main/java/org/apache/aurora/common/application/Lifecycle.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/application/Lifecycle.java#L68) | [defer](#file-1) | Lifecycle owns the wait monitor and converts interruption into shutdown. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/application/ShutdownRegistry.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/application/ShutdownRegistry.java#L75) | [defer](#file-2) | ShutdownRegistry executes reverse-registered actions once under synchronization and logs each action failure; defer lock-boundary changes pending callback tests. |
| [commons/src/main/java/org/apache/aurora/common/application/ShutdownStage.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/application/ShutdownStage.java#L1) | retain | ShutdownStage defines a Guice runtime binding annotation. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/base/Command.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/base/Command.java#L1) | retain | Command provides the checked-exception-free command typedef. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/base/Commands.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/base/Commands.java#L1) | retain | Commands exports the shared no-op command singleton. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/base/Consumers.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/base/Consumers.java#L1) | retain | Consumers combines Java Consumer callbacks while retaining Guava predicate adapters. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/base/ExceptionalCommand.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/base/ExceptionalCommand.java#L1) | retain | ExceptionalCommand exposes a generic checked-exception execute contract. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/base/ExceptionalSupplier.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/base/ExceptionalSupplier.java#L1) | retain | ExceptionalSupplier exposes a generic checked-exception supplier contract. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/base/MorePreconditions.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/base/MorePreconditions.java#L58) | [change](#file-9) | MorePreconditions centralizes validation and currently depends on Apache blank-string semantics; retain until whitespace behavior is characterized. |
| [commons/src/main/java/org/apache/aurora/common/collections/Iterables2.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/collections/Iterables2.java#L1) | retain | Iterables2 implements lazy iterable zipping and a cache-backed iterator helper. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/collections/Pair.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/collections/Pair.java#L32) | [defer](#file-11) | Pair is a nullable immutable tuple with legacy EqualsBuilder equality and Guava Function factories; defer any record conversion pending API and equality compatibility checks. |
| [commons/src/main/java/org/apache/aurora/common/inject/Bindings.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/inject/Bindings.java#L1) | retain | Bindings builds Guice private-module exposure and validates annotation qualifiers. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/inject/TimedInterceptor.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/inject/TimedInterceptor.java#L1) | retain | TimedInterceptor measures intercepted calls with monotonic nanoseconds and records the configured unit. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/net/InetSocketAddressHelper.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/net/InetSocketAddressHelper.java#L50) | [change](#file-14) | InetSocketAddressHelper parses endpoint host/port pairs and uses StringUtils.isEmpty after split validation; the Java replacement is localized and semantics-preserving. |
| [commons/src/main/java/org/apache/aurora/common/net/http/handlers/AbortHandler.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/net/http/handlers/AbortHandler.java#L1) | retain | AbortHandler exposes the abort HTTP endpoint and delegates process control to injected collaborators. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/net/http/handlers/ContentionPrinter.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/net/http/handlers/ContentionPrinter.java#L1) | retain | ContentionPrinter renders JVM monitor contention using management-bean snapshots. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/net/http/handlers/HealthHandler.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/net/http/handlers/HealthHandler.java#L1) | retain | HealthHandler adapts an injected health Supplier to the HTTP endpoint. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/net/http/handlers/QuitHandler.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/net/http/handlers/QuitHandler.java#L63) | [defer](#file-18) | QuitHandler starts an unmanaged asynchronous listener thread for each request; defer lifecycle modernization pending ownership and listener idempotence. |
| [commons/src/main/java/org/apache/aurora/common/net/http/handlers/ThreadStackPrinter.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/net/http/handlers/ThreadStackPrinter.java#L1) | retain | ThreadStackPrinter renders live thread stack traces from the JVM snapshot API. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/net/http/handlers/TimeSeriesDataSource.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/net/http/handlers/TimeSeriesDataSource.java#L1) | retain | TimeSeriesDataSource parses nullable query parameters and emits time-series response DTOs. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/net/http/handlers/VarsHandler.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/net/http/handlers/VarsHandler.java#L1) | retain | VarsHandler sorts rendered stat lines before returning the text response. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/net/http/handlers/VarsJsonHandler.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/net/http/handlers/VarsJsonHandler.java#L1) | retain | VarsJsonHandler builds insertion-ordered JSON variable output from stat values. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/quantity/Amount.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/quantity/Amount.java#L130) | [defer](#file-23) | Amount preserves numeric type, unit conversion, equality, comparison, and overflow behavior; defer immutable-representation or arithmetic modernization pending characterization tests. |
| [commons/src/main/java/org/apache/aurora/common/quantity/Data.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/quantity/Data.java#L1) | retain | Data defines the binary unit enum and its multiplier relationships. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/quantity/Time.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/quantity/Time.java#L1) | retain | Time defines time units and their explicit java.util.concurrent.TimeUnit mapping. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/quantity/Unit.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/quantity/Unit.java#L1) | retain | Unit defines the self-typed unit multiplier contract. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/stats/JvmStats.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/stats/JvmStats.java#L1) | retain | JvmStats exports management-bean and system property gauges with explicit wall-clock sampling. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/stats/Percentile.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/stats/Percentile.java#L1) | retain | Percentile maintains synchronized bounded samples and percentile statistics. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/stats/Rate.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/stats/Rate.java#L1) | retain | Rate uses atomic counters and an injected ticker for rate calculations. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/stats/Ratio.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/stats/Ratio.java#L75) | [change](#file-30) | Review ineffective equality comparisons with Double.NaN; use Double.isNaN if the intended zero fallback is confirmed, with an explicit numeric behavior change. |
| [commons/src/main/java/org/apache/aurora/common/stats/RecordingStat.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/stats/RecordingStat.java#L1) | retain | RecordingStat marks stats that retain samples for repository export. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/stats/RecordingStatImpl.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/stats/RecordingStatImpl.java#L1) | retain | RecordingStatImpl wraps a Stat with its recording state and stable name. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/stats/RequestStats.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/stats/RequestStats.java#L1) | retain | RequestStats tracks request counts and timing with atomic state. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/stats/SampledStat.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/stats/SampledStat.java#L1) | retain | SampledStat coordinates volatile previous samples with stat recording. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/stats/SlidingStats.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/stats/SlidingStats.java#L1) | retain | SlidingStats executes timed actions through its checked-exception functional contracts. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/stats/Stat.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/stats/Stat.java#L1) | retain | Stat defines the public stat name and read contract. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/stats/StatImpl.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/stats/StatImpl.java#L1) | retain | StatImpl provides immutable stat-name validation for concrete stats. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/stats/StatRegistry.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/stats/StatRegistry.java#L1) | retain | StatRegistry defines the iterable registry boundary for numeric stats. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/stats/Stats.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/stats/Stats.java#L1) | retain | Stats preserves global registration order and duplicate-name cache semantics. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/stats/StatsProvider.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/stats/StatsProvider.java#L1) | retain | StatsProvider defines counter, gauge, and request-timer provider contracts. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/stats/TimeSeries.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/stats/TimeSeries.java#L1) | retain | TimeSeries defines the name and sample iteration boundary. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/stats/TimeSeriesRepository.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/stats/TimeSeriesRepository.java#L1) | retain | TimeSeriesRepository defines the available-series and timestamp repository boundary. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/stats/TimeSeriesRepositoryImpl.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/stats/TimeSeriesRepositoryImpl.java#L1) | retain | TimeSeriesRepositoryImpl owns the daemon sampler executor and shuts it down in the service lifecycle; retain this already-correct resource handling. |
| [commons/src/main/java/org/apache/aurora/common/testing/TearDownTestCase.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/testing/TearDownTestCase.java#L1) | retain | TearDownTestCase runs teardown actions in reverse registration order and aggregates failures. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/testing/easymock/EasyMockTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/testing/easymock/EasyMockTest.java#L1) | retain | EasyMockTest adds reflective EasyMock setup and teardown behavior on TearDownTestCase. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/util/BackoffHelper.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/util/BackoffHelper.java#L1) | retain | BackoffHelper retries checked suppliers with quantity-based delays and stop conditions. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/util/BackoffStrategy.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/util/BackoffStrategy.java#L1) | retain | BackoffStrategy defines the next-delay strategy contract. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/util/BuildInfo.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/util/BuildInfo.java#L1) | retain | BuildInfo loads build properties and exposes an immutable property snapshot. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/util/Clock.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/util/Clock.java#L1) | retain | Clock separates wall-clock and monotonic time through an injectable interface. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/util/Random.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/util/Random.java#L1) | retain | Random defines random generation and its system-backed implementation. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/util/Sampler.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/util/Sampler.java#L1) | retain | Sampler samples using the injected Random implementation and validates bounds. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/util/StateMachine.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/util/StateMachine.java#L80) | [defer](#file-52) | StateMachine guards transitions with a fair ReentrantReadWriteLock and invokes callback chains; defer concurrency changes pending re-entry tests. |
| [commons/src/main/java/org/apache/aurora/common/util/TruncatedBinaryBackoff.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/util/TruncatedBinaryBackoff.java#L1) | retain | TruncatedBinaryBackoff calculates bounded quantity-based exponential backoff. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/util/templating/StringTemplateHelper.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/util/templating/StringTemplateHelper.java#L1) | retain | StringTemplateHelper streams template output through a Consumer while translating failures. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/util/testing/FakeBuildInfo.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/util/testing/FakeBuildInfo.java#L1) | retain | FakeBuildInfo provides a deterministic BuildInfo fixture. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/util/testing/FakeClock.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/util/testing/FakeClock.java#L1) | retain | FakeClock provides independently controlled wall-clock and monotonic test time. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/util/testing/FakeTicker.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/util/testing/FakeTicker.java#L1) | retain | FakeTicker provides a manually advanced Guava ticker for tests. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/zookeeper/Credentials.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/zookeeper/Credentials.java#L44) | [defer](#file-58) | Credentials exposes mutable authentication bytes and a platform-charset digest token; defer charset and defensive-copy changes pending ZooKeeper interoperability. |
| [commons/src/main/java/org/apache/aurora/common/zookeeper/SingletonService.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/zookeeper/SingletonService.java#L1) | retain | SingletonService coordinates ZooKeeper singleton registration and lifecycle callbacks. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/zookeeper/ZooKeeperUtils.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/zookeeper/ZooKeeperUtils.java#L1) | retain | ZooKeeperUtils wraps ZooKeeper connection and path operations with checked failures. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/zookeeper/testing/BaseZooKeeperTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/zookeeper/testing/BaseZooKeeperTest.java#L1) | retain | BaseZooKeeperTest owns embedded ZooKeeper test setup and cleanup. Retain this contract during Java 25 cleanup. |
| [commons/src/main/java/org/apache/aurora/common/zookeeper/testing/ZooKeeperTestServer.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/zookeeper/testing/ZooKeeperTestServer.java#L1) | retain | ZooKeeperTestServer starts and stops the embedded ZooKeeper server fixture. Retain this contract during Java 25 cleanup. |
| [commons/src/test/java/org/apache/aurora/common/base/ConsumersTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/base/ConsumersTest.java#L36) | [defer](#file-63) | ConsumersTest inherits EasyMockTest and its TearDownTestCase lifecycle; retain its semantics during the coordinated Jupiter migration. |
| [commons/src/test/java/org/apache/aurora/common/base/MorePreconditionsTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/base/MorePreconditionsTest.java#L28) | [defer](#file-64) | MorePreconditionsTest uses JUnit 4 expected-exception annotations; retain its semantics during the coordinated Jupiter migration. |
| [commons/src/test/java/org/apache/aurora/common/collections/Iterables2Test.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/collections/Iterables2Test.java#L31) | [defer](#file-65) | Iterables2Test uses JUnit 4 test annotations and assertions; retain its semantics during the coordinated Jupiter migration. |
| [commons/src/test/java/org/apache/aurora/common/collections/PairTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/collections/PairTest.java#L28) | [defer](#file-66) | PairTest uses JUnit 4 test annotations and assertions; retain its semantics during the coordinated Jupiter migration. |
| [commons/src/test/java/org/apache/aurora/common/inject/BindingsTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/inject/BindingsTest.java#L51) | [defer](#file-67) | BindingsTest uses JUnit 4 test annotations and assertions; retain its semantics during the coordinated Jupiter migration. |
| [commons/src/test/java/org/apache/aurora/common/net/InetSocketAddressHelperTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/net/InetSocketAddressHelperTest.java#L28) | [defer](#file-68) | InetSocketAddressHelperTest uses JUnit 4 test annotations and assertions; retain its semantics during the coordinated Jupiter migration. |
| [commons/src/test/java/org/apache/aurora/common/net/http/handlers/StatSupplierTestBase.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/net/http/handlers/StatSupplierTestBase.java#L31) | [defer](#file-69) | StatSupplierTestBase inherits EasyMockTest and its TearDownTestCase lifecycle; retain its semantics during the coordinated Jupiter migration. |
| [commons/src/test/java/org/apache/aurora/common/net/http/handlers/TimeSeriesDataSourceTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/net/http/handlers/TimeSeriesDataSourceTest.java#L62) | [defer](#file-70) | TimeSeriesDataSourceTest inherits EasyMockTest and its TearDownTestCase lifecycle; retain its semantics during the coordinated Jupiter migration. |
| [commons/src/test/java/org/apache/aurora/common/net/http/handlers/VarsHandlerTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/net/http/handlers/VarsHandlerTest.java#L46) | [defer](#file-71) | VarsHandlerTest uses JUnit 4 lifecycle annotations; retain its semantics during the coordinated Jupiter migration. |
| [commons/src/test/java/org/apache/aurora/common/net/http/handlers/VarsJsonHandlerTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/net/http/handlers/VarsJsonHandlerTest.java#L35) | [defer](#file-72) | VarsJsonHandlerTest uses JUnit 4 lifecycle annotations; retain its semantics during the coordinated Jupiter migration. |
| [commons/src/test/java/org/apache/aurora/common/quantity/AmountTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/quantity/AmountTest.java#L31) | [defer](#file-73) | AmountTest uses JUnit 4 expected-exception annotations; retain its semantics during the coordinated Jupiter migration. |
| [commons/src/test/java/org/apache/aurora/common/stats/PercentileTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/stats/PercentileTest.java#L44) | [defer](#file-74) | PercentileTest uses JUnit 4 lifecycle annotations; retain its semantics during the coordinated Jupiter migration. |
| [commons/src/test/java/org/apache/aurora/common/stats/RateTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/stats/RateTest.java#L62) | [defer](#file-75) | RateTest uses JUnit 4 lifecycle annotations; retain its semantics during the coordinated Jupiter migration. |
| [commons/src/test/java/org/apache/aurora/common/stats/SlidingStatsTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/stats/SlidingStatsTest.java#L44) | [defer](#file-76) | SlidingStatsTest uses JUnit 4 lifecycle annotations; retain its semantics during the coordinated Jupiter migration. |
| [commons/src/test/java/org/apache/aurora/common/stats/StatsTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/stats/StatsTest.java#L36) | [defer](#file-77) | StatsTest uses JUnit 4 lifecycle annotations; retain its semantics during the coordinated Jupiter migration. |
| [commons/src/test/java/org/apache/aurora/common/stats/TimeSeriesRepositoryImplTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/stats/TimeSeriesRepositoryImplTest.java#L60) | [defer](#file-78) | TimeSeriesRepositoryImplTest inherits EasyMockTest and its TearDownTestCase lifecycle; retain its semantics during the coordinated Jupiter migration. |
| [commons/src/test/java/org/apache/aurora/common/testing/easymock/EasyMockTestTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/testing/easymock/EasyMockTestTest.java#L33) | [defer](#file-79) | EasyMockTestTest inherits EasyMockTest and its TearDownTestCase lifecycle; retain its semantics during the coordinated Jupiter migration. |
| [commons/src/test/java/org/apache/aurora/common/util/BackoffHelperTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/util/BackoffHelperTest.java#L43) | [defer](#file-80) | BackoffHelperTest inherits EasyMockTest and its TearDownTestCase lifecycle; retain its semantics during the coordinated Jupiter migration. |
| [commons/src/test/java/org/apache/aurora/common/util/SamplerTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/util/SamplerTest.java#L47) | [defer](#file-81) | SamplerTest uses JUnit 4 expected-exception annotations; retain its semantics during the coordinated Jupiter migration. |
| [commons/src/test/java/org/apache/aurora/common/util/StateMachineTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/util/StateMachineTest.java#L41) | [defer](#file-82) | StateMachineTest inherits EasyMockTest and its TearDownTestCase lifecycle; retain its semantics during the coordinated Jupiter migration. |
| [commons/src/test/java/org/apache/aurora/common/util/TruncatedBinaryBackoffTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/util/TruncatedBinaryBackoffTest.java#L29) | [defer](#file-83) | TruncatedBinaryBackoffTest uses JUnit 4 expected-exception annotations; retain its semantics during the coordinated Jupiter migration. |
| [commons/src/test/java/org/apache/aurora/common/util/templating/StringTemplateHelperTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/util/templating/StringTemplateHelperTest.java#L51) | [defer](#file-84) | StringTemplateHelperTest uses JUnit 4 expected-exception annotations; retain its semantics during the coordinated Jupiter migration. |
| [commons/src/test/java/org/apache/aurora/common/util/testing/FakeClockTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/util/testing/FakeClockTest.java#L35) | [defer](#file-85) | FakeClockTest uses JUnit 4 lifecycle annotations; retain its semantics during the coordinated Jupiter migration. |
| [commons/src/test/java/org/apache/aurora/common/zookeeper/ZooKeeperUtilsTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/zookeeper/ZooKeeperUtilsTest.java#L27) | [defer](#file-86) | ZooKeeperUtilsTest uses JUnit 4 test annotations and assertions; retain its semantics during the coordinated Jupiter migration. |
| [src/jmh/java/org/apache/aurora/benchmark/BenchmarkSettings.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/jmh/java/org/apache/aurora/benchmark/BenchmarkSettings.java#L1) | retain | BenchmarkSettings.java: retain because it has no strong semantics-preserving Java 25 change identified in this audit. |
| [src/jmh/java/org/apache/aurora/benchmark/Hosts.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/jmh/java/org/apache/aurora/benchmark/Hosts.java#L1) | retain | Hosts.java: retain because it uses immutable collection boundaries that protect API and ownership semantics. |
| [src/jmh/java/org/apache/aurora/benchmark/JobUpdates.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/jmh/java/org/apache/aurora/benchmark/JobUpdates.java#L1) | retain | JobUpdates.java: retain because it uses immutable collection boundaries that protect API and ownership semantics. |
| [src/jmh/java/org/apache/aurora/benchmark/Offers.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/jmh/java/org/apache/aurora/benchmark/Offers.java#L1) | retain | Offers.java: retain because it uses immutable collection boundaries that protect API and ownership semantics. |
| [src/jmh/java/org/apache/aurora/benchmark/SchedulingBenchmarks.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/jmh/java/org/apache/aurora/benchmark/SchedulingBenchmarks.java#L1) | retain | SchedulingBenchmarks.java: retain because it preserves explicit time units through the existing quantity API. |
| [src/jmh/java/org/apache/aurora/benchmark/SnapshotBenchmarks.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/jmh/java/org/apache/aurora/benchmark/SnapshotBenchmarks.java#L1) | retain | SnapshotBenchmarks.java: retain because it preserves explicit time units through the existing quantity API. |
| [src/jmh/java/org/apache/aurora/benchmark/StateManagerBenchmarks.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/jmh/java/org/apache/aurora/benchmark/StateManagerBenchmarks.java#L1) | retain | StateManagerBenchmarks.java: retain because it preserves explicit time units through the existing quantity API. |
| [src/jmh/java/org/apache/aurora/benchmark/StatusUpdateBenchmark.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/jmh/java/org/apache/aurora/benchmark/StatusUpdateBenchmark.java#L1) | retain | StatusUpdateBenchmark.java: retain because it already uses java.util.Optional at its nullable boundary. |
| [src/jmh/java/org/apache/aurora/benchmark/TaskStoreBenchmarks.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/jmh/java/org/apache/aurora/benchmark/TaskStoreBenchmarks.java#L1) | retain | TaskStoreBenchmarks.java: retain because it preserves explicit time units through the existing quantity API. |
| [src/jmh/java/org/apache/aurora/benchmark/Tasks.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/jmh/java/org/apache/aurora/benchmark/Tasks.java#L1) | retain | Tasks.java: retain because it uses immutable collection boundaries that protect API and ownership semantics. |
| [src/jmh/java/org/apache/aurora/benchmark/ThriftApiBenchmarks.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/jmh/java/org/apache/aurora/benchmark/ThriftApiBenchmarks.java#L1) | retain | ThriftApiBenchmarks.java: retain because it preserves explicit time units through the existing quantity API. |
| [src/jmh/java/org/apache/aurora/benchmark/UpdateStoreBenchmarks.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/jmh/java/org/apache/aurora/benchmark/UpdateStoreBenchmarks.java#L1) | retain | UpdateStoreBenchmarks.java: retain because it preserves explicit time units through the existing quantity API. |
| [src/jmh/java/org/apache/aurora/benchmark/fakes/FakeDriver.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/jmh/java/org/apache/aurora/benchmark/fakes/FakeDriver.java#L1) | retain | FakeDriver.java: retain because it has no strong semantics-preserving Java 25 change identified in this audit. |
| [src/jmh/java/org/apache/aurora/benchmark/fakes/FakeEventSink.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/jmh/java/org/apache/aurora/benchmark/fakes/FakeEventSink.java#L1) | retain | FakeEventSink.java: retain because it has no strong semantics-preserving Java 25 change identified in this audit. |
| [src/jmh/java/org/apache/aurora/benchmark/fakes/FakeOfferManager.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/jmh/java/org/apache/aurora/benchmark/fakes/FakeOfferManager.java#L1) | retain | FakeOfferManager.java: retain because it already uses java.util.Optional at its nullable boundary. |
| [src/jmh/java/org/apache/aurora/benchmark/fakes/FakeRescheduleCalculator.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/jmh/java/org/apache/aurora/benchmark/fakes/FakeRescheduleCalculator.java#L1) | retain | FakeRescheduleCalculator.java: retain because it has no strong semantics-preserving Java 25 change identified in this audit. |
| [src/jmh/java/org/apache/aurora/benchmark/fakes/FakeSchedulerDriver.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/jmh/java/org/apache/aurora/benchmark/fakes/FakeSchedulerDriver.java#L1) | retain | FakeSchedulerDriver.java: retain because it has no strong semantics-preserving Java 25 change identified in this audit. |
| [src/jmh/java/org/apache/aurora/benchmark/fakes/FakeStatsProvider.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/jmh/java/org/apache/aurora/benchmark/fakes/FakeStatsProvider.java#L1) | retain | FakeStatsProvider.java: retain because it already uses standard functional interfaces/lambdas where applicable. |

## Findings and required validation

<a id="file-1"></a>

### `commons/src/main/java/org/apache/aurora/common/application/Lifecycle.java`

**P2 · concurrency · after-boundary**

Investigate restoring interrupt status and coordinating shutdown before changing interruption handling.

Evidence: [line 68](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/application/Lifecycle.java#L68)

```java
} catch (InterruptedException e) {
```

Contract: Preserve the current shutdown-on-interrupt behavior and avoid duplicate shutdown execution.

Required validation: Run Lifecycle-focused tests plus an interrupted awaitShutdown scenario.

<a id="file-2"></a>

### `commons/src/main/java/org/apache/aurora/common/application/ShutdownRegistry.java`

**P2 · concurrency · after-boundary**

Investigate snapshotting actions under the monitor, then invoking callbacks after releasing it, or document why callbacks must run under the lock.

Evidence: [line 75](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/application/ShutdownRegistry.java#L75)

```java
public synchronized void execute() {
```

Contract: Preserve reverse registration order, addAction/execute races, per-action failure logging, and at-most-once execution semantics.

Required validation: Exercise concurrent registration/execution and callbacks that re-enter the registry before changing locking.

<a id="file-9"></a>

### `commons/src/main/java/org/apache/aurora/common/base/MorePreconditions.java`

**P2 · jdk-api · local**

Compare String.isBlank with the current Apache StringUtils.isBlank behavior, then migrate only with explicit compatibility tests.

Evidence: [line 58](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/base/MorePreconditions.java#L58)

```java
Preconditions.checkArgument(!StringUtils.isBlank(argument), message, args);
```

Contract: Whitespace classification and null handling are observable validation behavior; removing the dependency is not mechanical.

Required validation: Run MorePreconditionsTest, including null, empty, and whitespace-only cases.

<a id="file-11"></a>

### `commons/src/main/java/org/apache/aurora/common/collections/Pair.java`

**P2 · language · after-boundary**

Evaluate a record-based representation only after preserving getFirst/getSecond, nullable fields, static Function factories, and binary/source compatibility.

Evidence: [line 32](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/collections/Pair.java#L32)

```java
public class Pair<A, B>
```

Contract: A record changes accessor names, inheritance/finality, serialization shape, and may alter equality/hash behavior; do not mechanically rewrite.

Required validation: Run PairTest and compatibility checks for nulls, equality, hash codes, and callers.

<a id="file-14"></a>

### `commons/src/main/java/org/apache/aurora/common/net/InetSocketAddressHelper.java`

**P2 · jdk-api · local**

Replace StringUtils.isEmpty(host) with host.isEmpty() after the existing non-null split result is established.

Evidence: [line 50](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/net/InetSocketAddressHelper.java#L50)

```java
return StringUtils.isEmpty(host)
```

Contract: Preserve the empty-host wildcard behavior and the existing null, malformed-spec, and port exception behavior.

Required validation: Run InetSocketAddressHelperTest with empty host, hostname, malformed spec, wildcard port, and invalid port cases.

<a id="file-18"></a>

### `commons/src/main/java/org/apache/aurora/common/net/http/handlers/QuitHandler.java`

**P2 · concurrency · after-boundary**

Investigate a managed, named executor or thread lifecycle for the quit listener.

Evidence: [line 63](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/net/http/handlers/QuitHandler.java#L63)

```java
new Thread(quitListener).start();
```

Contract: Preserve asynchronous HTTP response behavior, per-request listener invocation, thread ownership policy, and listener idempotence requirements.

Required validation: Run handler tests and an integration scenario covering repeated quit requests and process shutdown.

<a id="file-23"></a>

### `commons/src/main/java/org/apache/aurora/common/quantity/Amount.java`

**P2 · boundary · after-boundary**

Investigate whether a Java 25 value type or arithmetic rewrite can preserve cross-unit conversion, truncation, and compareTo/equals consistency.

Evidence: [line 130](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/quantity/Amount.java#L130)

```java
public int compareTo(Amount<T, U> other)
```

Contract: Rounding, overflow sentinel behavior, numeric wrapper types, and unit ownership are public semantics; do not replace with a generic record or primitive arithmetic without characterization tests.

Required validation: Run AmountTest with every numeric factory, cross-unit conversion, equality, comparison, and overflow case.

<a id="file-30"></a>

### `commons/src/main/java/org/apache/aurora/common/stats/Ratio.java`

**P2 · jdk-api · local**

Replace equality comparisons against Double.NaN with Double.isNaN only after deciding whether invalid numerator/denominator samples should produce the intended zero fallback or propagate NaN.

Evidence: [line 75](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/stats/Ratio.java#L75)

```java
|| (denominatorValue == Double.NaN)
```

Contract: NaN is unequal to itself, so the current comparisons never match. Returning zero instead of NaN is a correctness/metric behavior change, not a semantics-preserving syntax cleanup. Preserve the separate zero-denominator rule and characterize infinities.

Required validation: Add focused ratio tests for finite values, zero denominator, NaN on either side and infinities; run SlidingStatsTest and verify request-rate metric consumers.

<a id="file-52"></a>

### `commons/src/main/java/org/apache/aurora/common/util/StateMachine.java`

**P2 · concurrency · after-boundary**

Investigate the read/write lock and transition callback boundary before changing concurrency primitives.

Evidence: [line 80](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/util/StateMachine.java#L80)

```java
ReadWriteLock stateLock = new ReentrantReadWriteLock(true /* fair */)
```

Contract: Callbacks, fair-lock behavior, re-entrancy, transition atomicity, and exception behavior must remain unchanged.

Required validation: Run StateMachineTest plus targeted concurrent transition and callback re-entry scenarios.

<a id="file-58"></a>

### `commons/src/main/java/org/apache/aurora/common/zookeeper/Credentials.java`

**P1 · boundary · framework**

Resolve the digest charset explicitly only with a ZooKeeper server/client compatibility plan, and consider defensive authToken copies at the same boundary.

Evidence: [line 44](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/main/java/org/apache/aurora/common/zookeeper/Credentials.java#L44)

```java
return new Credentials("digest", (username + ":" + password).getBytes());
```

Contract: Changing the platform charset changes authentication bytes; returning the mutable byte array exposes credentials and may be relied upon by callers.

Required validation: Run ZooKeeperUtilsTest plus an authenticated interoperability test across supported default charsets.

<a id="file-63"></a>

### `commons/src/test/java/org/apache/aurora/common/base/ConsumersTest.java`

**P2 · test-infrastructure · framework**

Migrate this EasyMockTest subclass in the coordinated Jupiter task, retaining TearDownTestCase ordering and mock verification.

Evidence: [line 36](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/base/ConsumersTest.java#L36)

```java
@Test
```

Contract: Preserve inherited EasyMock setup, reverse teardown ordering, expected exceptions, and mock verification behavior.

Required validation: Run this test and the full Commons test suite under the coordinated Jupiter configuration.

<a id="file-64"></a>

### `commons/src/test/java/org/apache/aurora/common/base/MorePreconditionsTest.java`

**P2 · test-infrastructure · framework**

Migrate the JUnit 4 expected-exception assertions to the coordinated Jupiter task.

Evidence: [line 28](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/base/MorePreconditionsTest.java#L28)

```java
@Test(expected = NullPointerException.class)
```

Contract: Preserve the exact exception type and the test setup that must run before the failure.

Required validation: Run this test and the full Commons test suite under the coordinated Jupiter configuration.

<a id="file-65"></a>

### `commons/src/test/java/org/apache/aurora/common/collections/Iterables2Test.java`

**P2 · test-infrastructure · framework**

Migrate this JUnit 4 test to the coordinated Jupiter task while retaining its assertions and fixture behavior.

Evidence: [line 31](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/collections/Iterables2Test.java#L31)

```java
@Test
```

Contract: Preserve assertion semantics, mutable fixture ownership, and test isolation.

Required validation: Run this test and the full Commons test suite under the coordinated Jupiter configuration.

<a id="file-66"></a>

### `commons/src/test/java/org/apache/aurora/common/collections/PairTest.java`

**P2 · test-infrastructure · framework**

Migrate this JUnit 4 test to the coordinated Jupiter task while retaining its assertions and fixture behavior.

Evidence: [line 28](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/collections/PairTest.java#L28)

```java
@Test
```

Contract: Preserve assertion semantics, mutable fixture ownership, and test isolation.

Required validation: Run this test and the full Commons test suite under the coordinated Jupiter configuration.

<a id="file-67"></a>

### `commons/src/test/java/org/apache/aurora/common/inject/BindingsTest.java`

**P2 · test-infrastructure · framework**

Migrate this JUnit 4 test to the coordinated Jupiter task while retaining its assertions and fixture behavior.

Evidence: [line 51](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/inject/BindingsTest.java#L51)

```java
@Test
```

Contract: Preserve assertion semantics, mutable fixture ownership, and test isolation.

Required validation: Run this test and the full Commons test suite under the coordinated Jupiter configuration.

<a id="file-68"></a>

### `commons/src/test/java/org/apache/aurora/common/net/InetSocketAddressHelperTest.java`

**P2 · test-infrastructure · framework**

Migrate this JUnit 4 test to the coordinated Jupiter task while retaining its assertions and fixture behavior.

Evidence: [line 28](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/net/InetSocketAddressHelperTest.java#L28)

```java
@Test
```

Contract: Preserve assertion semantics, mutable fixture ownership, and test isolation.

Required validation: Run this test and the full Commons test suite under the coordinated Jupiter configuration.

<a id="file-69"></a>

### `commons/src/test/java/org/apache/aurora/common/net/http/handlers/StatSupplierTestBase.java`

**P2 · test-infrastructure · framework**

Migrate this EasyMockTest subclass in the coordinated Jupiter task, retaining TearDownTestCase ordering and mock verification.

Evidence: [line 31](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/net/http/handlers/StatSupplierTestBase.java#L31)

```java
public abstract class StatSupplierTestBase extends EasyMockTest {
```

Contract: Preserve inherited EasyMock setup, reverse teardown ordering, expected exceptions, and mock verification behavior.

Required validation: Run this test and the full Commons test suite under the coordinated Jupiter configuration.

<a id="file-70"></a>

### `commons/src/test/java/org/apache/aurora/common/net/http/handlers/TimeSeriesDataSourceTest.java`

**P2 · test-infrastructure · framework**

Migrate this EasyMockTest subclass in the coordinated Jupiter task, retaining TearDownTestCase ordering and mock verification.

Evidence: [line 62](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/net/http/handlers/TimeSeriesDataSourceTest.java#L62)

```java
@Test
```

Contract: Preserve inherited EasyMock setup, reverse teardown ordering, expected exceptions, and mock verification behavior.

Required validation: Run this test and the full Commons test suite under the coordinated Jupiter configuration.

<a id="file-71"></a>

### `commons/src/test/java/org/apache/aurora/common/net/http/handlers/VarsHandlerTest.java`

**P2 · test-infrastructure · framework**

Migrate the JUnit 4 lifecycle annotations to the coordinated Jupiter task.

Evidence: [line 46](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/net/http/handlers/VarsHandlerTest.java#L46)

```java
@Test
```

Contract: Preserve setup/cleanup ordering, mutable fixture reset, and assertion behavior.

Required validation: Run this test and the full Commons test suite under the coordinated Jupiter configuration.

<a id="file-72"></a>

### `commons/src/test/java/org/apache/aurora/common/net/http/handlers/VarsJsonHandlerTest.java`

**P2 · test-infrastructure · framework**

Migrate the JUnit 4 lifecycle annotations to the coordinated Jupiter task.

Evidence: [line 35](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/net/http/handlers/VarsJsonHandlerTest.java#L35)

```java
@Test
```

Contract: Preserve setup/cleanup ordering, mutable fixture reset, and assertion behavior.

Required validation: Run this test and the full Commons test suite under the coordinated Jupiter configuration.

<a id="file-73"></a>

### `commons/src/test/java/org/apache/aurora/common/quantity/AmountTest.java`

**P2 · test-infrastructure · framework**

Migrate the JUnit 4 expected-exception assertions to the coordinated Jupiter task.

Evidence: [line 31](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/quantity/AmountTest.java#L31)

```java
@Test
```

Contract: Preserve the exact exception type and the test setup that must run before the failure.

Required validation: Run this test and the full Commons test suite under the coordinated Jupiter configuration.

<a id="file-74"></a>

### `commons/src/test/java/org/apache/aurora/common/stats/PercentileTest.java`

**P2 · test-infrastructure · framework**

Migrate the JUnit 4 lifecycle annotations to the coordinated Jupiter task.

Evidence: [line 44](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/stats/PercentileTest.java#L44)

```java
@Test
```

Contract: Preserve setup/cleanup ordering, mutable fixture reset, and assertion behavior.

Required validation: Run this test and the full Commons test suite under the coordinated Jupiter configuration.

<a id="file-75"></a>

### `commons/src/test/java/org/apache/aurora/common/stats/RateTest.java`

**P2 · test-infrastructure · framework**

Migrate the JUnit 4 lifecycle annotations to the coordinated Jupiter task.

Evidence: [line 62](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/stats/RateTest.java#L62)

```java
@Test
```

Contract: Preserve setup/cleanup ordering, mutable fixture reset, and assertion behavior.

Required validation: Run this test and the full Commons test suite under the coordinated Jupiter configuration.

<a id="file-76"></a>

### `commons/src/test/java/org/apache/aurora/common/stats/SlidingStatsTest.java`

**P2 · test-infrastructure · framework**

Migrate the JUnit 4 lifecycle annotations to the coordinated Jupiter task.

Evidence: [line 44](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/stats/SlidingStatsTest.java#L44)

```java
@Test
```

Contract: Preserve setup/cleanup ordering, mutable fixture reset, and assertion behavior.

Required validation: Run this test and the full Commons test suite under the coordinated Jupiter configuration.

<a id="file-77"></a>

### `commons/src/test/java/org/apache/aurora/common/stats/StatsTest.java`

**P2 · test-infrastructure · framework**

Migrate the JUnit 4 lifecycle annotations to the coordinated Jupiter task.

Evidence: [line 36](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/stats/StatsTest.java#L36)

```java
@Test
```

Contract: Preserve setup/cleanup ordering, mutable fixture reset, and assertion behavior.

Required validation: Run this test and the full Commons test suite under the coordinated Jupiter configuration.

<a id="file-78"></a>

### `commons/src/test/java/org/apache/aurora/common/stats/TimeSeriesRepositoryImplTest.java`

**P2 · test-infrastructure · framework**

Migrate this EasyMockTest subclass in the coordinated Jupiter task, retaining TearDownTestCase ordering and mock verification.

Evidence: [line 60](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/stats/TimeSeriesRepositoryImplTest.java#L60)

```java
@Test
```

Contract: Preserve inherited EasyMock setup, reverse teardown ordering, expected exceptions, and mock verification behavior.

Required validation: Run this test and the full Commons test suite under the coordinated Jupiter configuration.

<a id="file-79"></a>

### `commons/src/test/java/org/apache/aurora/common/testing/easymock/EasyMockTestTest.java`

**P2 · test-infrastructure · framework**

Migrate this EasyMockTest subclass in the coordinated Jupiter task, retaining TearDownTestCase ordering and mock verification.

Evidence: [line 33](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/testing/easymock/EasyMockTestTest.java#L33)

```java
@Test
```

Contract: Preserve inherited EasyMock setup, reverse teardown ordering, expected exceptions, and mock verification behavior.

Required validation: Run this test and the full Commons test suite under the coordinated Jupiter configuration.

<a id="file-80"></a>

### `commons/src/test/java/org/apache/aurora/common/util/BackoffHelperTest.java`

**P2 · test-infrastructure · framework**

Migrate this EasyMockTest subclass in the coordinated Jupiter task, retaining TearDownTestCase ordering and mock verification.

Evidence: [line 43](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/util/BackoffHelperTest.java#L43)

```java
@Test
```

Contract: Preserve inherited EasyMock setup, reverse teardown ordering, expected exceptions, and mock verification behavior.

Required validation: Run this test and the full Commons test suite under the coordinated Jupiter configuration.

<a id="file-81"></a>

### `commons/src/test/java/org/apache/aurora/common/util/SamplerTest.java`

**P2 · test-infrastructure · framework**

Migrate the JUnit 4 expected-exception assertions to the coordinated Jupiter task.

Evidence: [line 47](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/util/SamplerTest.java#L47)

```java
@Test
```

Contract: Preserve the exact exception type and the test setup that must run before the failure.

Required validation: Run this test and the full Commons test suite under the coordinated Jupiter configuration.

<a id="file-82"></a>

### `commons/src/test/java/org/apache/aurora/common/util/StateMachineTest.java`

**P2 · test-infrastructure · framework**

Migrate this EasyMockTest subclass in the coordinated Jupiter task, retaining TearDownTestCase ordering and mock verification.

Evidence: [line 41](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/util/StateMachineTest.java#L41)

```java
@Test
```

Contract: Preserve inherited EasyMock setup, reverse teardown ordering, expected exceptions, and mock verification behavior.

Required validation: Run this test and the full Commons test suite under the coordinated Jupiter configuration.

<a id="file-83"></a>

### `commons/src/test/java/org/apache/aurora/common/util/TruncatedBinaryBackoffTest.java`

**P2 · test-infrastructure · framework**

Migrate the JUnit 4 expected-exception assertions to the coordinated Jupiter task.

Evidence: [line 29](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/util/TruncatedBinaryBackoffTest.java#L29)

```java
@Test(expected = NullPointerException.class)
```

Contract: Preserve the exact exception type and the test setup that must run before the failure.

Required validation: Run this test and the full Commons test suite under the coordinated Jupiter configuration.

<a id="file-84"></a>

### `commons/src/test/java/org/apache/aurora/common/util/templating/StringTemplateHelperTest.java`

**P2 · test-infrastructure · framework**

Migrate the JUnit 4 expected-exception assertions to the coordinated Jupiter task.

Evidence: [line 51](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/util/templating/StringTemplateHelperTest.java#L51)

```java
@Test
```

Contract: Preserve the exact exception type and the test setup that must run before the failure.

Required validation: Run this test and the full Commons test suite under the coordinated Jupiter configuration.

<a id="file-85"></a>

### `commons/src/test/java/org/apache/aurora/common/util/testing/FakeClockTest.java`

**P2 · test-infrastructure · framework**

Migrate the JUnit 4 lifecycle annotations to the coordinated Jupiter task.

Evidence: [line 35](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/util/testing/FakeClockTest.java#L35)

```java
@Test
```

Contract: Preserve setup/cleanup ordering, mutable fixture reset, and assertion behavior.

Required validation: Run this test and the full Commons test suite under the coordinated Jupiter configuration.

<a id="file-86"></a>

### `commons/src/test/java/org/apache/aurora/common/zookeeper/ZooKeeperUtilsTest.java`

**P2 · test-infrastructure · framework**

Migrate this BaseZooKeeperTest subclass in the coordinated Jupiter task, retaining embedded ZooKeeper setup and cleanup.

Evidence: [line 27](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/commons/src/test/java/org/apache/aurora/common/zookeeper/ZooKeeperUtilsTest.java#L27)

```java
@Test
```

Contract: Preserve server lifecycle, connection timing, expected exceptions, and test isolation.

Required validation: Run this test and the full Commons test suite under the coordinated Jupiter configuration.
