# Application tests and helpers

[Audit overview and ordering](../JAVA25_FILE_AUDIT.md).

205 files: 14 local-change candidates, 46 deferred, 145 retained.

Source baseline: `6cf7f0ea07c6355af62ceafb029fe086f55b8c65`. Findings describe proposed work; validation is not yet executed.

Retain means no separate improvement prioritized in this pass. A local candidate still needs
the stated contract checks. Framework findings are grouped migrations, not separate PRs per file.

| Source file | Assessment | Recommendation or retain rationale |
| --- | --- | --- |
| [src/test/java/org/apache/aurora/GuavaUtilsTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/GuavaUtilsTest.java#L1) | retain | GuavaUtilsTest (testToImmutableSet) exercises immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/GuiceUtilsTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/GuiceUtilsTest.java#L114) | [defer](#file-2) | GuiceUtilsTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/LifecycleShutdownListenerTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/LifecycleShutdownListenerTest.java#L1) | retain | LifecycleShutdownListenerTest (testShutdownOnFailure) exercises EasyMockTest lifecycle; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/ProtobufsTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/ProtobufsTest.java#L1) | retain | ProtobufsTest: converts protobuf messages to stable text for serialization assertions; retaining its exact formatting fixtures avoids changing compatibility expectations. |
| [src/test/java/org/apache/aurora/codec/ThriftBinaryCodecTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/codec/ThriftBinaryCodecTest.java#L47) | [defer](#file-5) | ThriftBinaryCodecTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/BatchWorkerTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/BatchWorkerTest.java#L1) | retain | BatchWorkerTest (testExecute, testExecuteThrows) exercises EasyMockTest lifecycle, explicit synchronization; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/HostOfferTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/HostOfferTest.java#L1) | retain | HostOfferTest (testHasCpuOrMem) exercises immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/SchedulerLifecycleTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/SchedulerLifecycleTest.java#L1) | retain | SchedulerLifecycleTest (testAutoFailover, testRegistrationTimeout) exercises EasyMockTest lifecycle; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/TaskStatusHandlerImplTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/TaskStatusHandlerImplTest.java#L1) | retain | TaskStatusHandlerImplTest (testForwardsStatusUpdates, testFailedStatusUpdate) exercises EasyMockTest lifecycle, explicit synchronization; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/TaskVarsTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/TaskVarsTest.java#L1) | retain | TaskVarsTest (testStartsAtZero, testNoEarlyExport) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/TierManagerTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/TierManagerTest.java#L75) | [defer](#file-11) | TierManagerTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/TierModuleTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/TierModuleTest.java#L45) | [defer](#file-12) | TierModuleTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/app/MoreModulesTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/app/MoreModulesTest.java#L1) | retain | MoreModulesTest (testInstantiate) exercises immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/app/SchedulerIT.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/app/SchedulerIT.java#L226) | [change](#file-14) | SchedulerIT exercises the original scheduler through fake external dependencies; use direct construction for the statically known SchedulerMain instead of deprecated reflection. |
| [src/test/java/org/apache/aurora/scheduler/app/VolumeConverterTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/app/VolumeConverterTest.java#L41) | [defer](#file-15) | VolumeConverterTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/app/local/FakeMaster.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/app/local/FakeMaster.java#L1) | retain | FakeMaster: implements the local SchedulerDriver/DriverFactory simulator, including task state, offers, callbacks, and its long-lived join/stop contract; retain those mutable and blocking semantics. |
| [src/test/java/org/apache/aurora/scheduler/app/local/FakeNonVolatileStorage.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/app/local/FakeNonVolatileStorage.java#L1) | retain | FakeNonVolatileStorage: provides the local non-volatile storage lifecycle fake; retain its start/prepare/stop behavior because callers depend on the lifecycle contract. |
| [src/test/java/org/apache/aurora/scheduler/app/local/LocalSchedulerMain.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/app/local/LocalSchedulerMain.java#L1) | retain | LocalSchedulerMain: assembles the local scheduler command-line application with fake external components; retain its Guice bindings and immutable argument construction. |
| [src/test/java/org/apache/aurora/scheduler/app/local/simulator/ClusterSimulatorModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/app/local/simulator/ClusterSimulatorModule.java#L1) | retain | ClusterSimulatorModule: binds the cluster simulator and registers its idle service; retain startup/shutdown ordering because it models simulator lifecycle. |
| [src/test/java/org/apache/aurora/scheduler/app/local/simulator/FakeSlaves.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/app/local/simulator/FakeSlaves.java#L1) | retain | FakeSlaves: models fake slave startup and offer-accepted callbacks for local simulation; retain callback sequencing and mutable simulator state. |
| [src/test/java/org/apache/aurora/scheduler/app/local/simulator/events/OfferAccepted.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/app/local/simulator/events/OfferAccepted.java#L1) | retain | OfferAccepted: is the simulator event value object carrying an accepted offer; retain its event payload shape and constructor semantics. |
| [src/test/java/org/apache/aurora/scheduler/app/local/simulator/events/Started.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/app/local/simulator/events/Started.java#L1) | retain | Started: is the simulator startup event marker/value object; retain its empty event semantics because consumers use its type as the signal. |
| [src/test/java/org/apache/aurora/scheduler/async/AsyncModuleTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/async/AsyncModuleTest.java#L1) | retain | AsyncModuleTest (testBindings) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/base/AsyncUtilTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/base/AsyncUtilTest.java#L53) | [change](#file-24) | AsyncUtilTest: Bound this wait with a test timeout and assert the wait result so a lost signal fails diagnostically instead of hanging the suite. |
| [src/test/java/org/apache/aurora/scheduler/base/ConversionsTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/base/ConversionsTest.java#L1) | retain | ConversionsTest (testAllStatesHandled) exercises direct assertions and domain fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/base/JobsTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/base/JobsTest.java#L1) | retain | JobsTest (testGetJobStats) exercises immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/base/NumbersTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/base/NumbersTest.java#L1) | retain | NumbersTest (testToRanges) exercises immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/base/TaskTestUtil.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/base/TaskTestUtil.java#L1) | retain | TaskTestUtil: constructs scheduled-task, tier, and executor fixtures shared by scheduler tests; retain immutable collections and default field values as test data contracts. |
| [src/test/java/org/apache/aurora/scheduler/base/TasksTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/base/TasksTest.java#L1) | retain | TasksTest (testOrderedStatusesForCompleteness, testLatestTransitionedTasks) exercises immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/config/CommandLineTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/config/CommandLineTest.java#L1) | retain | CommandLineTest (testCustomOptions, testParseAllOptions) exercises immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/config/CustomModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/config/CustomModule.java#L1) | retain | CustomModule: defines the Guice custom test binding and options used by command-line tests; retain binding keys and module configuration. |
| [src/test/java/org/apache/aurora/scheduler/configuration/ConfigurationManagerTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/configuration/ConfigurationManagerTest.java#L77) | [defer](#file-32) | ConfigurationManagerTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/configuration/executor/ExecutorModuleTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/configuration/executor/ExecutorModuleTest.java#L1) | retain | ExecutorModuleTest (testMakeExecutorCommand, testSingleCommand) exercises immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/configuration/executor/ExecutorSettingsLoaderTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/configuration/executor/ExecutorSettingsLoaderTest.java#L96) | [defer](#file-34) | ExecutorSettingsLoaderTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/cron/CrontabEntryTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/cron/CrontabEntryTest.java#L1) | retain | CrontabEntryTest (testHashCodeAndEquals, testEqualsCoverage) exercises immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/cron/ExpectedPrediction.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/cron/ExpectedPrediction.java#L1) | retain | ExpectedPrediction: stores expected cron trigger times and exposes immutable prediction lists; retain ordering because cron assertions compare sequence. |
| [src/test/java/org/apache/aurora/scheduler/cron/quartz/AuroraCronJobTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/cron/quartz/AuroraCronJobTest.java#L1) | retain | AuroraCronJobTest (testExecuteNonexistentIsNoop, testEmptyStorage) exercises EasyMockTest lifecycle, immutable fixtures, explicit synchronization; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/cron/quartz/CronIT.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/cron/quartz/CronIT.java#L153) | [change](#file-38) | CronIT: Bound this wait with a test timeout and assert the wait result so a lost signal fails diagnostically instead of hanging the suite. |
| [src/test/java/org/apache/aurora/scheduler/cron/quartz/CronJobManagerImplTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/cron/quartz/CronJobManagerImplTest.java#L78) | [defer](#file-39) | CronJobManagerImplTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/cron/quartz/CronPredictorImplTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/cron/quartz/CronPredictorImplTest.java#L1) | retain | CronPredictorImplTest (testValidSchedule, testCronExpressions) exercises direct assertions and domain fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/cron/quartz/QuartzTestUtil.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/cron/quartz/QuartzTestUtil.java#L1) | retain | QuartzTestUtil: holds shared Quartz job/key constants and setup values; retain those identifiers because tests use them as cross-component keys. |
| [src/test/java/org/apache/aurora/scheduler/discovery/BaseCuratorDiscoveryTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/discovery/BaseCuratorDiscoveryTest.java#L1) | retain | BaseCuratorDiscoveryTest: provides Curator discovery setup, session/disconnection actions, and group-event expectations for subclasses; retain teardown and event ordering. |
| [src/test/java/org/apache/aurora/scheduler/discovery/CuratorDiscoveryModuleTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/discovery/CuratorDiscoveryModuleTest.java#L89) | [defer](#file-43) | CuratorDiscoveryModuleTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/discovery/CuratorServiceGroupMonitorTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/discovery/CuratorServiceGroupMonitorTest.java#L1) | retain | CuratorServiceGroupMonitorTest (testNominalLifecycle, testNeverStarted) exercises immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/discovery/CuratorSingletonServiceTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/discovery/CuratorSingletonServiceTest.java#L196) | [change](#file-45) | CuratorSingletonServiceTest: Bound this wait with a test timeout and assert the wait result so a lost signal fails diagnostically instead of hanging the suite. |
| [src/test/java/org/apache/aurora/scheduler/discovery/EncodingTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/discovery/EncodingTest.java#L1) | retain | EncodingTest (testEncodingRoundTrip, testJsonCompatibility) exercises immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/discovery/ZooKeeperConfigTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/discovery/ZooKeeperConfigTest.java#L37) | [defer](#file-47) | ZooKeeperConfigTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/events/NotifyingSchedulingFilterTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/events/NotifyingSchedulingFilterTest.java#L1) | retain | NotifyingSchedulingFilterTest (testNotifies, testNoVetoes) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/events/PubsubEventModuleTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/events/PubsubEventModuleTest.java#L1) | retain | PubsubEventModuleTest (testHandlesDeadEvent, testPubsubExceptionTracking) exercises EasyMockTest lifecycle; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/events/TaskStateChangeTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/events/TaskStateChangeTest.java#L1) | retain | TaskStateChangeTest (testInitializedJsonRetainsEmptyOldState, testTransitionJsonRetainsOldStateValue) exercises direct assertions and domain fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/events/WebhookTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/events/WebhookTest.java#L115) | [change](#file-51) | WebhookTest: Bound callback completion waiting and fail on interruption instead of silently continuing to assertions. |
| [src/test/java/org/apache/aurora/scheduler/filter/AttributeAggregateTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/filter/AttributeAggregateTest.java#L57) | [defer](#file-52) | AttributeAggregateTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/filter/SchedulingFilterImplTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/filter/SchedulingFilterImplTest.java#L1) | retain | SchedulingFilterImplTest (testMeetsOffer, testSufficientPorts) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/http/AbstractJettyTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/AbstractJettyTest.java#L1) | retain | AbstractJettyTest: provides shared Jetty test module setup, leadership toggles, and service startup around EasyMock; retain lifecycle ownership in the base class. |
| [src/test/java/org/apache/aurora/scheduler/http/CorsFilterTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/CorsFilterTest.java#L1) | retain | CorsFilterTest (testCorsSupport) exercises EasyMockTest lifecycle; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/http/CronTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/CronTest.java#L1) | retain | CronTest (testDumpContents) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/http/LeaderHealthTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/LeaderHealthTest.java#L1) | retain | LeaderHealthTest (testLeader, testNotLeader) exercises EasyMockTest lifecycle; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/http/LeaderRedirectTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/LeaderRedirectTest.java#L1) | retain | LeaderRedirectTest (testLeader, testNotLeader) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/http/MaintenanceTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/MaintenanceTest.java#L1) | retain | MaintenanceTest (testNoDrainingHosts) exercises immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/http/MnameTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/MnameTest.java#L1) | retain | MnameTest (testGetUsage, testHttpMethods) exercises immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/http/OffersTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/OffersTest.java#L1) | retain | OffersTest (testNoOffers, testOneOffer) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/http/PendingTasksTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/PendingTasksTest.java#L1) | retain | PendingTasksTest (testNoOffers, testOffers) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/http/QuitCallbackTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/QuitCallbackTest.java#L1) | retain | QuitCallbackTest (testInvoke) exercises EasyMockTest lifecycle; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/http/ServicesTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/ServicesTest.java#L1) | retain | ServicesTest (testGetServices) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/http/ServletFilterTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/ServletFilterTest.java#L1) | retain | ServletFilterTest (testGzipEncoding, testLeaderRedirect) exercises direct assertions and domain fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/http/StateTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/StateTest.java#L1) | retain | StateTest (testJson) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/http/StructDumpTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/StructDumpTest.java#L1) | retain | StructDumpTest (testGetUsage, testTaskConfigDoesNotIncludeMetadata) exercises immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/http/TestUtils.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/TestUtils.java#L1) | retain | TestUtils: provides HTTP test fixture constants/builders; retain the immutable fixture values consumed by endpoint tests. |
| [src/test/java/org/apache/aurora/scheduler/http/TiersTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/TiersTest.java#L1) | retain | TiersTest (testGetTiers) exercises EasyMockTest lifecycle; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/http/api/ApiBetaTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/api/ApiBetaTest.java#L1) | retain | ApiBetaTest (testCreateJob, testGetRoleSummary) exercises immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/http/api/ApiIT.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/api/ApiIT.java#L1) | retain | ApiIT (testGzipFilterApplied, testThriftJsonAccepted) exercises immutable fixtures, integration setup/teardown; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/http/api/security/AuthorizeHeaderTokenTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/api/security/AuthorizeHeaderTokenTest.java#L28) | [defer](#file-72) | AuthorizeHeaderTokenTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/http/api/security/HttpSecurityIT.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/api/security/HttpSecurityIT.java#L1) | retain | HttpSecurityIT (testReadOnlyScheduler, testAuroraSchedulerManager) exercises immutable fixtures, integration setup/teardown; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/http/api/security/Kerberos5ShiroRealmModuleTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/api/security/Kerberos5ShiroRealmModuleTest.java#L1) | retain | Kerberos5ShiroRealmModuleTest (testConfigure) exercises EasyMockTest lifecycle; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/http/api/security/KerberosPrincipalConverterTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/api/security/KerberosPrincipalConverterTest.java#L33) | [defer](#file-75) | KerberosPrincipalConverterTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/http/api/security/ShiroAuthenticatingThriftInterceptorTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/api/security/ShiroAuthenticatingThriftInterceptorTest.java#L47) | [defer](#file-76) | ShiroAuthenticatingThriftInterceptorTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/http/api/security/ShiroAuthorizingInterceptorTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/api/security/ShiroAuthorizingInterceptorTest.java#L1) | retain | ShiroAuthorizingInterceptorTest (testAuthorized, testNotAuthorized) exercises EasyMockTest lifecycle; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/http/api/security/ShiroAuthorizingParamInterceptorTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/api/security/ShiroAuthorizingParamInterceptorTest.java#L152) | [defer](#file-78) | ShiroAuthorizingParamInterceptorTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/http/api/security/ShiroIniConverterTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/api/security/ShiroIniConverterTest.java#L60) | [defer](#file-79) | ShiroIniConverterTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/http/api/security/ShiroKerberosAuthenticationFilterTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/api/security/ShiroKerberosAuthenticationFilterTest.java#L1) | retain | ShiroKerberosAuthenticationFilterTest (testDoesNotPermitUnauthenticated, testRejectsMalformedMechanism) exercises direct assertions and domain fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/http/api/security/ShiroKerberosPermissiveAuthenticationFilterTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/api/security/ShiroKerberosPermissiveAuthenticationFilterTest.java#L1) | retain | ShiroKerberosPermissiveAuthenticationFilterTest (testPermitsUnauthenticated, testInterceptsUnauthenticatedException) exercises direct assertions and domain fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/http/api/security/ThriftFieldGetterTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/api/security/ThriftFieldGetterTest.java#L1) | retain | ThriftFieldGetterTest (testStructFieldGetter, testStructFieldGetterUnsetField) exercises direct assertions and domain fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/log/mesos/MesosLogTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/log/mesos/MesosLogTest.java#L212) | [defer](#file-83) | MesosLogTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/maintenance/MaintenanceControllerImplTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/maintenance/MaintenanceControllerImplTest.java#L1) | retain | MaintenanceControllerImplTest (testMaintenanceCycle, testUnknownHost) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/mesos/CommandLineDriverSettingsModuleTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/mesos/CommandLineDriverSettingsModuleTest.java#L42) | [defer](#file-85) | CommandLineDriverSettingsModuleTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/mesos/FrameworkInfoFactoryImplTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/mesos/FrameworkInfoFactoryImplTest.java#L1) | retain | FrameworkInfoFactoryImplTest (testHostnameAndURLAdded) exercises EasyMockTest lifecycle; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/mesos/MesosCallbackHandlerTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/mesos/MesosCallbackHandlerTest.java#L447) | [defer](#file-87) | MesosCallbackHandlerTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/mesos/MesosSchedulerImplTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/mesos/MesosSchedulerImplTest.java#L142) | [defer](#file-88) | MesosSchedulerImplTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/mesos/MesosTaskFactoryImplTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/mesos/MesosTaskFactoryImplTest.java#L1) | retain | MesosTaskFactoryImplTest (testExecutorInfoUnchanged, testTaskInfoRevocable) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/mesos/ProtosConversionTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/mesos/ProtosConversionTest.java#L1) | retain | ProtosConversionTest (testOfferIDRoundTrip, testOfferRoundTrip) exercises direct assertions and domain fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/mesos/SchedulerDriverServiceTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/mesos/SchedulerDriverServiceTest.java#L124) | [defer](#file-91) | SchedulerDriverServiceTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/mesos/TaskExecutors.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/mesos/TaskExecutors.java#L1) | retain | TaskExecutors: contains executor-settings constants used by Mesos scheduler tests; retain these shared values because they define fixture identity. |
| [src/test/java/org/apache/aurora/scheduler/mesos/TaskStatusStatsTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/mesos/TaskStatusStatsTest.java#L1) | retain | TaskStatusStatsTest (testAccumulateEvents) exercises EasyMockTest lifecycle; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/mesos/VersionedMesosSchedulerImplTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/mesos/VersionedMesosSchedulerImplTest.java#L272) | [change](#file-94) | VersionedMesosSchedulerImplTest: Replace the fixed sleep with an observable condition and bounded await/assertion for the event under test. |
| [src/test/java/org/apache/aurora/scheduler/mesos/VersionedSchedulerDriverServiceTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/mesos/VersionedSchedulerDriverServiceTest.java#L109) | [change](#file-95) | VersionedSchedulerDriverServiceTest: Replace the fixed sleep with an observable condition and bounded await/assertion for the event under test. |
| [src/test/java/org/apache/aurora/scheduler/metadata/NearestFitTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/metadata/NearestFitTest.java#L1) | retain | NearestFitTest (testNoReason, testScoring) exercises immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/offers/OfferManagerImplTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/offers/OfferManagerImplTest.java#L309) | [defer](#file-97) | OfferManagerImplTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/offers/Offers.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/offers/Offers.java#L1) | retain | Offers: builds resource-offer fixtures for scheduling tests; retain resource quantities and collection shape because assertions depend on them. |
| [src/test/java/org/apache/aurora/scheduler/offers/RandomJitterReturnDelayTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/offers/RandomJitterReturnDelayTest.java#L52) | [defer](#file-99) | RandomJitterReturnDelayTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/preemptor/BiCacheTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/preemptor/BiCacheTest.java#L90) | [defer](#file-100) | BiCacheTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/preemptor/ClusterStateImplTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/preemptor/ClusterStateImplTest.java#L50) | [defer](#file-101) | ClusterStateImplTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/preemptor/PendingTaskProcessorTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/preemptor/PendingTaskProcessorTest.java#L290) | [defer](#file-102) | PendingTaskProcessorTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/preemptor/PreemptionVictimFilterTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/preemptor/PreemptionVictimFilterTest.java#L1) | retain | PreemptionVictimFilterTest (testPreempted, testLowestPriorityPreempted) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/preemptor/PreemptionVictimTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/preemptor/PreemptionVictimTest.java#L1) | retain | PreemptionVictimTest (testBeanMethods) exercises immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/preemptor/PreemptorImplTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/preemptor/PreemptorImplTest.java#L1) | retain | PreemptorImplTest (testPreemptTasksSuccessful, testPreemptTasksValidationFailed) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/preemptor/PreemptorModuleTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/preemptor/PreemptorModuleTest.java#L1) | retain | PreemptorModuleTest (testPreemptorDisabled) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/pruning/JobUpdateHistoryPrunerTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/pruning/JobUpdateHistoryPrunerTest.java#L1) | retain | JobUpdateHistoryPrunerTest (testPruneHistory) exercises immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/pruning/TaskHistoryPrunerTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/pruning/TaskHistoryPrunerTest.java#L1) | retain | TaskHistoryPrunerTest (testNoPruning, testStorageStartedWithPruning) exercises EasyMockTest lifecycle, immutable fixtures, explicit synchronization; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/quota/QuotaCheckResultTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/quota/QuotaCheckResultTest.java#L1) | retain | QuotaCheckResultTest (testGreaterOrEqualPass, testGreaterOrEqualFailsCpu) exercises direct assertions and domain fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/quota/QuotaManagerImplTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/quota/QuotaManagerImplTest.java#L711) | [defer](#file-110) | QuotaManagerImplTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/reconciliation/KillRetryTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/reconciliation/KillRetryTest.java#L1) | retain | KillRetryTest (testRetries, testDoesNotRetry) exercises EasyMockTest lifecycle; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/reconciliation/TaskReconcilerTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/reconciliation/TaskReconcilerTest.java#L153) | [defer](#file-112) | TaskReconcilerTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/reconciliation/TaskTimeoutTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/reconciliation/TaskTimeoutTest.java#L1) | retain | TaskTimeoutTest (testNormalTransitions, testTransientToTransient) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/resources/AcceptedOfferTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/resources/AcceptedOfferTest.java#L1) | retain | AcceptedOfferTest (testReservedPredicates, testAllocateEmpty) exercises immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/resources/AuroraResourceConverterTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/resources/AuroraResourceConverterTest.java#L1) | retain | AuroraResourceConverterTest (testRoundtrip) exercises direct assertions and domain fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/resources/MesosResourceConverterTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/resources/MesosResourceConverterTest.java#L86) | [defer](#file-116) | MesosResourceConverterTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/resources/PortMapperTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/resources/PortMapperTest.java#L40) | [defer](#file-117) | PortMapperTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/resources/ResourceBagTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/resources/ResourceBagTest.java#L1) | retain | ResourceBagTest (testAdd, testSubtract) exercises immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/resources/ResourceManagerTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/resources/ResourceManagerTest.java#L1) | retain | ResourceManagerTest (testGetOfferResources, testGetTaskResources) exercises immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/resources/ResourceTestUtil.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/resources/ResourceTestUtil.java#L1) | retain | ResourceTestUtil: builds Mesos scalar/resource fixtures and converts resource bags for tests; retain exact resource units and immutable defaults. |
| [src/test/java/org/apache/aurora/scheduler/resources/ResourceTypeTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/resources/ResourceTypeTest.java#L1) | retain | ResourceTypeTest (testFindValueById, testFindByResource) exercises direct assertions and domain fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/scheduling/RescheduleCalculatorImplTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/scheduling/RescheduleCalculatorImplTest.java#L1) | retain | RescheduleCalculatorImplTest (testNoPenaltyForNoAncestor, testNoPenaltyDeletedAncestor) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/scheduling/TaskAssignerImplTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/scheduling/TaskAssignerImplTest.java#L1) | retain | TaskAssignerImplTest (testAssignNoTasks, testAssignmentClearedOnError) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/scheduling/TaskGroupsTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/scheduling/TaskGroupsTest.java#L1) | retain | TaskGroupsTest (testEvaluatedAfterFirstSchedulePenalty, testTaskDeletedBeforeEvaluating) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/scheduling/TaskSchedulerImplTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/scheduling/TaskSchedulerImplTest.java#L1) | retain | TaskSchedulerImplTest (testSchedule, testScheduleNoTask) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/scheduling/TaskThrottlerTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/scheduling/TaskThrottlerTest.java#L1) | retain | TaskThrottlerTest (testIgnoresNonThrottledTasks, testThrottledTask) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/sla/MetricCalculatorTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/sla/MetricCalculatorTest.java#L1) | retain | MetricCalculatorTest (testRun) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/sla/SlaAlgorithmTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/sla/SlaAlgorithmTest.java#L94) | [defer](#file-128) | SlaAlgorithmTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/sla/SlaManagerTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/sla/SlaManagerTest.java#L757) | [change](#file-129) | SlaManagerTest: Bound this wait with a test timeout and assert the wait result so a lost signal fails diagnostically instead of hanging the suite. |
| [src/test/java/org/apache/aurora/scheduler/sla/SlaModuleTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/sla/SlaModuleTest.java#L132) | [change](#file-130) | SlaModuleTest: Bound this wait with a test timeout and assert the wait result so a lost signal fails diagnostically instead of hanging the suite. |
| [src/test/java/org/apache/aurora/scheduler/sla/SlaTestUtil.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/sla/SlaTestUtil.java#L1) | retain | SlaTestUtil: builds SLA task/event fixtures, including event timelines; retain event ordering and timestamps because SLA calculations are sequence-sensitive. |
| [src/test/java/org/apache/aurora/scheduler/sla/SlaUtilTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/sla/SlaUtilTest.java#L1) | retain | SlaUtilTest (testPercentileEmpty, testPercentileSingleValue) exercises direct assertions and domain fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/state/PartitionManagerTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/state/PartitionManagerTest.java#L1) | retain | PartitionManagerTest (testNonPartitionedTransition, testPartitionPolicyNoReschedule) exercises EasyMockTest lifecycle, explicit synchronization; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/state/PubsubTestUtil.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/state/PubsubTestUtil.java#L1) | retain | PubsubTestUtil: provides pubsub event/task fixtures for state tests; retain payload construction because subscribers assert event identity. |
| [src/test/java/org/apache/aurora/scheduler/state/StateManagerImplTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/state/StateManagerImplTest.java#L544) | [defer](#file-135) | StateManagerImplTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/state/TaskStateMachineTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/state/TaskStateMachineTest.java#L1) | retain | TaskStateMachineTest (testSimpleTransition, testServiceRescheduled) exercises immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/stats/AsyncStatsModuleTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/stats/AsyncStatsModuleTest.java#L1) | retain | AsyncStatsModuleTest (testOfferAdapter) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/stats/ResourceCounterTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/stats/ResourceCounterTest.java#L1) | retain | ResourceCounterTest (testNoTasks, testComputeConsumptionTotals) exercises immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/stats/SlotSizeCounterTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/stats/SlotSizeCounterTest.java#L1) | retain | SlotSizeCounterTest (testNoOffers, testTinyOffers) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/storage/AbstractAttributeStoreTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/AbstractAttributeStoreTest.java#L115) | [defer](#file-140) | AbstractAttributeStoreTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/storage/AbstractCronJobStoreTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/AbstractCronJobStoreTest.java#L1) | retain | AbstractCronJobStoreTest (testJobStore, testJobStoreSameEnvironment) exercises immutable fixtures, storage behavior; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/storage/AbstractHostMaintenanceStoreTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/AbstractHostMaintenanceStoreTest.java#L1) | retain | AbstractHostMaintenanceStoreTest (testReadHostMaintenanceRequestNonExistant, testReadHostMaintenanceRequest) exercises immutable fixtures, storage behavior; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/storage/AbstractJobUpdateStoreTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/AbstractJobUpdateStoreTest.java#L205) | [defer](#file-143) | AbstractJobUpdateStoreTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/storage/AbstractQuotaStoreTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/AbstractQuotaStoreTest.java#L1) | retain | AbstractQuotaStoreTest (testCrud, testDeleteNonExistent) exercises immutable fixtures, storage behavior; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/storage/AbstractSchedulerStoreTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/AbstractSchedulerStoreTest.java#L1) | retain | AbstractSchedulerStoreTest (testSchedulerStore) exercises storage behavior; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/storage/AbstractTaskStoreTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/AbstractTaskStoreTest.java#L603) | [change](#file-146) | AbstractTaskStoreTest: Bound this wait with a test timeout and assert the wait result so a lost signal fails diagnostically instead of hanging the suite. |
| [src/test/java/org/apache/aurora/scheduler/storage/backup/RecoveryTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/backup/RecoveryTest.java#L154) | [defer](#file-147) | RecoveryTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/storage/backup/StorageBackupTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/backup/StorageBackupTest.java#L1) | retain | StorageBackupTest (testBackup, testDirectoryMissing) exercises EasyMockTest lifecycle, immutable fixtures, storage behavior; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/storage/durability/DataCompatibilityTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/durability/DataCompatibilityTest.java#L1) | retain | DataCompatibilityTest (testReadCompatibility, testWriteFormatUnchanged) exercises immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/storage/durability/DurableStorageTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/durability/DurableStorageTest.java#L1) | retain | DurableStorageTest (testStart, testSaveFrameworkId) exercises EasyMockTest lifecycle, immutable fixtures, storage behavior; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/storage/durability/Generator.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/durability/Generator.java#L117) | [change](#file-151) | Generator builds deterministic populated Thrift fixtures; modernize its deprecated Class.newInstance call while retaining field ordering and union selection. |
| [src/test/java/org/apache/aurora/scheduler/storage/durability/RecoveryTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/durability/RecoveryTest.java#L1) | retain | RecoveryTest (testRecover, testRecoverWithDeleteAll) exercises direct assertions and domain fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/storage/durability/ThriftBackfillTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/durability/ThriftBackfillTest.java#L59) | [defer](#file-153) | ThriftBackfillTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/storage/durability/TransactionRecorderTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/durability/TransactionRecorderTest.java#L1) | retain | TransactionRecorderTest (testCoalesce) exercises immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/storage/durability/WriteRecorderTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/durability/WriteRecorderTest.java#L141) | [defer](#file-155) | WriteRecorderTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/storage/entities/IHostAttributesTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/entities/IHostAttributesTest.java#L1) | retain | IHostAttributesTest (testObjectDetachment) exercises immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/storage/log/FakeLog.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/log/FakeLog.java#L1) | retain | FakeLog: implements the in-memory log fake, including read and truncate operations; retain position and record behavior used by storage log tests. |
| [src/test/java/org/apache/aurora/scheduler/storage/log/LogManagerTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/log/LogManagerTest.java#L1) | retain | LogManagerTest (testStreamManagerReadFromUnknownNone, testStreamManagerReadFromUnknownSome) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/storage/log/LogPersistenceTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/log/LogPersistenceTest.java#L1) | retain | LogPersistenceTest (testRecoverEmpty, testRecoverSnapshot) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/storage/log/NonVolatileStorageTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/log/NonVolatileStorageTest.java#L1) | retain | NonVolatileStorageTest (testDurability) exercises storage behavior; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/storage/log/SnapshotDeduplicatorImplTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/log/SnapshotDeduplicatorImplTest.java#L110) | [defer](#file-161) | SnapshotDeduplicatorImplTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/storage/log/SnapshotServiceTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/log/SnapshotServiceTest.java#L143) | [change](#file-162) | SnapshotServiceTest: Bound this wait with a test timeout and assert the wait result so a lost signal fails diagnostically instead of hanging the suite. |
| [src/test/java/org/apache/aurora/scheduler/storage/log/SnapshotterImplIT.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/log/SnapshotterImplIT.java#L1) | retain | SnapshotterImplIT (testBackfill) exercises immutable fixtures, integration setup/teardown; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/storage/log/testing/LogOpMatcher.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/log/testing/LogOpMatcher.java#L1) | retain | LogOpMatcher: matches and records expected log append/snapshot transactions for EasyMock; retain matcher equality and stream ordering. |
| [src/test/java/org/apache/aurora/scheduler/storage/mem/InternerTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/mem/InternerTest.java#L1) | retain | InternerTest (testReferenceCounting, testNonEqual) exercises immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/storage/mem/MemAttributeStoreTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/mem/MemAttributeStoreTest.java#L1) | retain | MemAttributeStoreTest (testStoreSize) exercises storage behavior; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/storage/mem/MemCronJobStoreTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/mem/MemCronJobStoreTest.java#L1) | retain | MemCronJobStoreTest (testStoreSize) exercises storage behavior; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/storage/mem/MemHostMaintenanceStoreTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/mem/MemHostMaintenanceStoreTest.java#L1) | retain | MemHostMaintenanceStoreTest (testStoreSize) exercises storage behavior; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/storage/mem/MemJobUpdateStoreTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/mem/MemJobUpdateStoreTest.java#L1) | retain | MemJobUpdateStoreTest (testStoreSize) exercises storage behavior; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/storage/mem/MemQuotaStoreTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/mem/MemQuotaStoreTest.java#L1) | retain | MemQuotaStoreTest (testStoreSize) exercises storage behavior; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/storage/mem/MemSchedulerStoreTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/mem/MemSchedulerStoreTest.java#L1) | retain | MemSchedulerStoreTest: specializes the abstract scheduler-store suite with an in-memory store module; retain inherited coverage and module binding. |
| [src/test/java/org/apache/aurora/scheduler/storage/mem/MemStorageTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/mem/MemStorageTest.java#L78) | [change](#file-172) | MemStorageTest: Bound test-thread waits and overall completion, and release the inner reader barrier in finally; keep the slow reader blocked until the concurrency assertion has run. |
| [src/test/java/org/apache/aurora/scheduler/storage/mem/MemTaskStoreTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/mem/MemTaskStoreTest.java#L1) | retain | MemTaskStoreTest (testSecondaryIndexConsistency) exercises immutable fixtures, storage behavior; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/storage/mem/StorageTransactionTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/mem/StorageTransactionTest.java#L77) | [change](#file-174) | StorageTransactionTest: Bound test-thread waits and overall completion, and release the inner reader barrier in finally; keep the slow reader blocked until the concurrency assertion has run. |
| [src/test/java/org/apache/aurora/scheduler/storage/testing/StorageEntityUtil.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/testing/StorageEntityUtil.java#L1) | retain | StorageEntityUtil: reflectively validates storage entity fields and ignored-field rules; retain null/scalar validation semantics because compatibility tests rely on failures. |
| [src/test/java/org/apache/aurora/scheduler/storage/testing/StorageEntityUtilTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/testing/StorageEntityUtilTest.java#L39) | [defer](#file-176) | StorageEntityUtilTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/storage/testing/StorageTestUtil.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/testing/StorageTestUtil.java#L1) | retain | StorageTestUtil: encapsulates EasyMock expectations for storage reads, writes, and task queries; retain expectation cardinality and captured work behavior. |
| [src/test/java/org/apache/aurora/scheduler/testing/BatchWorkerUtil.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/testing/BatchWorkerUtil.java#L1) | retain | BatchWorkerUtil: provides EasyMock expectations for BatchWorker futures; retain future completion behavior because callers assert batch result propagation. |
| [src/test/java/org/apache/aurora/scheduler/testing/FakeScheduledExecutor.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/testing/FakeScheduledExecutor.java#L1) | retain | FakeScheduledExecutor: combines FakeClock with deterministic scheduled-work queues and manual time advancement; retain queue ordering and clock semantics. |
| [src/test/java/org/apache/aurora/scheduler/thrift/AuditMessagesTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/thrift/AuditMessagesTest.java#L1) | retain | AuditMessagesTest (testEmptySubject, testPresentSubject) exercises EasyMockTest lifecycle; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/thrift/Fixtures.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/thrift/Fixtures.java#L1) | retain | Fixtures: builds default Thrift scheduled-task and response fixtures; retain field population and collection mutability/ordering expected by API tests. |
| [src/test/java/org/apache/aurora/scheduler/thrift/ReadOnlySchedulerImplTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/thrift/ReadOnlySchedulerImplTest.java#L1) | retain | ReadOnlySchedulerImplTest (testGetJobSummary, testGetJobSummaryWithoutNextRun) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/thrift/SchedulerThriftInterfaceTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/thrift/SchedulerThriftInterfaceTest.java#L1311) | [defer](#file-183) | SchedulerThriftInterfaceTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/thrift/ThriftIT.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/thrift/ThriftIT.java#L1) | retain | ThriftIT (testSetQuota, testSubmitNoExecutorDockerTask) exercises EasyMockTest lifecycle, immutable fixtures, integration setup/teardown; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/thrift/aop/AnnotatedAuroraAdminTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/thrift/aop/AnnotatedAuroraAdminTest.java#L1) | retain | AnnotatedAuroraAdminTest (testAllAuroraSchedulerManagerIfaceMethodsHaveAuthorizingParam) exercises direct assertions and domain fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/thrift/aop/LoggingInterceptorTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/thrift/aop/LoggingInterceptorTest.java#L1) | retain | LoggingInterceptorTest (testInvokeTransientStorageException, testInvokeRuntimeException) exercises EasyMockTest lifecycle; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/thrift/aop/MockDecoratedThrift.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/thrift/aop/MockDecoratedThrift.java#L1) | retain | MockDecoratedThrift: implements the annotated Thrift mock used by AOP interceptor tests; retain annotation and forwarding method shape because binding discovery depends on it. |
| [src/test/java/org/apache/aurora/scheduler/thrift/aop/ServerInfoInterceptorTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/thrift/aop/ServerInfoInterceptorTest.java#L1) | retain | ServerInfoInterceptorTest (testServerInfoIsSet) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/thrift/aop/ThriftStatsExporterInterceptorTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/thrift/aop/ThriftStatsExporterInterceptorTest.java#L1) | retain | ThriftStatsExporterInterceptorTest (testIncrementStat, testMeasuredMethod) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/updater/AddTaskTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/updater/AddTaskTest.java#L111) | [defer](#file-190) | AddTaskTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/updater/EnumsTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/updater/EnumsTest.java#L1) | retain | EnumsTest (testInstanceAction, testInstanceUpdateStatus) exercises direct assertions and domain fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/updater/InstanceUpdaterTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/updater/InstanceUpdaterTest.java#L275) | [defer](#file-192) | InstanceUpdaterTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/updater/JobDiffTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/updater/JobDiffTest.java#L1) | retain | JobDiffTest (testNoDiff, testInstancesAdded) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/updater/JobUpdateEventSubscriberTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/updater/JobUpdateEventSubscriberTest.java#L1) | retain | JobUpdateEventSubscriberTest (testStateChange, testDeleted) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/updater/JobUpdateStateMachineTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/updater/JobUpdateStateMachineTest.java#L1) | retain | JobUpdateStateMachineTest (testTransition) exercises immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/updater/JobUpdaterIT.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/updater/JobUpdaterIT.java#L770) | [defer](#file-196) | JobUpdaterIT: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/updater/KillTaskTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/updater/KillTaskTest.java#L1) | retain | KillTaskTest (testInstanceKill, testKillForUpdateReservesAgentForInstance) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/updater/NullAgentReserverTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/updater/NullAgentReserverTest.java#L1) | retain | NullAgentReserverTest (testNullReserver) exercises EasyMockTest lifecycle; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/updater/OneWayJobUpdaterTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/updater/OneWayJobUpdaterTest.java#L1) | retain | OneWayJobUpdaterTest (testSuccessfulUpdate, testFailedUpdate) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/updater/SlaKillControllerTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/updater/SlaKillControllerTest.java#L1) | retain | SlaKillControllerTest (testSlaKill, testSlaKillRetry) exercises EasyMockTest lifecycle, immutable fixtures, explicit synchronization; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/updater/UpdateAgentReserverImplTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/updater/UpdateAgentReserverImplTest.java#L1) | retain | UpdateAgentReserverImplTest (testReserve, testRelease) exercises EasyMockTest lifecycle, immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/updater/UpdateFactoryImplTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/updater/UpdateFactoryImplTest.java#L1) | retain | UpdateFactoryImplTest (testRollingForward, testRollingBack) exercises immutable fixtures; retain its current setup and assertions without a standalone syntax rewrite; preserve the current behavior and fixture contracts. |
| [src/test/java/org/apache/aurora/scheduler/updater/strategy/BatchStrategyTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/updater/strategy/BatchStrategyTest.java#L31) | [defer](#file-203) | BatchStrategyTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/updater/strategy/QueueStrategyTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/updater/strategy/QueueStrategyTest.java#L31) | [defer](#file-204) | QueueStrategyTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |
| [src/test/java/org/apache/aurora/scheduler/updater/strategy/VariableBatchStrategyTest.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/updater/strategy/VariableBatchStrategyTest.java#L32) | [defer](#file-205) | VariableBatchStrategyTest: Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form). |

## Findings and required validation

<a id="file-2"></a>

### `src/test/java/org/apache/aurora/GuiceUtilsTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 114](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/GuiceUtilsTest.java#L114)

```java
@Test(expected = CreationException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-5"></a>

### `src/test/java/org/apache/aurora/codec/ThriftBinaryCodecTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 47](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/codec/ThriftBinaryCodecTest.java#L47)

```java
@Test(expected = NullPointerException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-11"></a>

### `src/test/java/org/apache/aurora/scheduler/TierManagerTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 75](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/TierManagerTest.java#L75)

```java
@Test(expected = IllegalArgumentException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-12"></a>

### `src/test/java/org/apache/aurora/scheduler/TierModuleTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 45](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/TierModuleTest.java#L45)

```java
@Test(expected = IllegalArgumentException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-14"></a>

### `src/test/java/org/apache/aurora/scheduler/app/SchedulerIT.java`

**P2 · jdk-api · local**

Construct new SchedulerMain() directly; the class and public no-argument constructor are statically known and already used by the production entry point.

Evidence: [line 226](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/app/SchedulerIT.java#L226)

```java
SchedulerMain main = SchedulerMain.class.newInstance();
```

Contract: Keep injector.injectMembers(main), lifecycle startup and existing bounded wait/cleanup logic intact. Do not alter fake-cluster scheduling assertions.

Required validation: Run SchedulerIT and confirm the same original policy, lifecycle and storage assertions execute.

<a id="file-15"></a>

### `src/test/java/org/apache/aurora/scheduler/app/VolumeConverterTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 41](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/app/VolumeConverterTest.java#L41)

```java
@Test(expected = ParameterException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-24"></a>

### `src/test/java/org/apache/aurora/scheduler/base/AsyncUtilTest.java`

**P2 · concurrency · local**

Bound this wait with a test timeout and assert the wait result so a lost signal fails diagnostically instead of hanging the suite.

Evidence: [line 53](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/base/AsyncUtilTest.java#L53)

```java
latch.await();
```

Contract: Keep the latch ordering and event handoff unchanged; choose a timeout longer than the deterministic test operation.

Required validation: Run this test repeatedly with delayed and missing callback paths and verify timeout failures are reported.

<a id="file-32"></a>

### `src/test/java/org/apache/aurora/scheduler/configuration/ConfigurationManagerTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 77](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/configuration/ConfigurationManagerTest.java#L77)

```java
public ExpectedException expectedException = ExpectedException.none();
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-34"></a>

### `src/test/java/org/apache/aurora/scheduler/configuration/executor/ExecutorSettingsLoaderTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 96](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/configuration/executor/ExecutorSettingsLoaderTest.java#L96)

```java
@Test(expected = ExecutorConfigException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-38"></a>

### `src/test/java/org/apache/aurora/scheduler/cron/quartz/CronIT.java`

**P2 · concurrency · local**

Bound this wait with a test timeout and assert the wait result so a lost signal fails diagnostically instead of hanging the suite.

Evidence: [line 153](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/cron/quartz/CronIT.java#L153)

```java
cronRan.await();
```

Contract: Keep the latch ordering and event handoff unchanged; choose a timeout longer than the deterministic test operation.

Required validation: Run this test repeatedly with delayed and missing callback paths and verify timeout failures are reported.

<a id="file-39"></a>

### `src/test/java/org/apache/aurora/scheduler/cron/quartz/CronJobManagerImplTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 78](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/cron/quartz/CronJobManagerImplTest.java#L78)

```java
@Test(expected = CronException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-43"></a>

### `src/test/java/org/apache/aurora/scheduler/discovery/CuratorDiscoveryModuleTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 89](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/discovery/CuratorDiscoveryModuleTest.java#L89)

```java
@Test(expected = NullPointerException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-45"></a>

### `src/test/java/org/apache/aurora/scheduler/discovery/CuratorSingletonServiceTest.java`

**P2 · concurrency · local**

Bound this wait with a test timeout and assert the wait result so a lost signal fails diagnostically instead of hanging the suite.

Evidence: [line 196](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/discovery/CuratorSingletonServiceTest.java#L196)

```java
host1Defeated.await();
```

Contract: Keep the latch ordering and event handoff unchanged; choose a timeout longer than the deterministic test operation.

Required validation: Run this test repeatedly with delayed and missing callback paths and verify timeout failures are reported.

**P2 · concurrency · local**

Replace the fixed sleep with an observable condition and bounded await/assertion for the event under test.

Evidence: [line 241](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/discovery/CuratorSingletonServiceTest.java#L241)

```java
Thread.sleep(1L);
```

Contract: Preserve the intended scheduling window; do not make the assertion pass before the asynchronous work actually completes.

Required validation: Exercise both prompt and delayed execution and verify the test remains deterministic.

<a id="file-47"></a>

### `src/test/java/org/apache/aurora/scheduler/discovery/ZooKeeperConfigTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 37](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/discovery/ZooKeeperConfigTest.java#L37)

```java
@Test(expected = IllegalArgumentException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-51"></a>

### `src/test/java/org/apache/aurora/scheduler/events/WebhookTest.java`

**P2 · concurrency · local**

Bound callback completion waiting and fail on interruption instead of silently continuing to assertions.

Evidence: [line 115](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/events/WebhookTest.java#L115)

```java
latch.await();
```

Contract: Keep the AURORA-1961 ordering guarantee: assertions must wait for onThrowable, not just future completion. Restore interruption when translating it to test failure.

Required validation: Run this test repeatedly with delayed and missing callback paths and verify timeout failures are reported.

<a id="file-52"></a>

### `src/test/java/org/apache/aurora/scheduler/filter/AttributeAggregateTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 57](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/filter/AttributeAggregateTest.java#L57)

```java
@Test(expected = NoSuchElementException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-72"></a>

### `src/test/java/org/apache/aurora/scheduler/http/api/security/AuthorizeHeaderTokenTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 28](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/api/security/AuthorizeHeaderTokenTest.java#L28)

```java
@Test(expected = IllegalArgumentException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-75"></a>

### `src/test/java/org/apache/aurora/scheduler/http/api/security/KerberosPrincipalConverterTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 33](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/api/security/KerberosPrincipalConverterTest.java#L33)

```java
@Test(expected = ParameterException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-76"></a>

### `src/test/java/org/apache/aurora/scheduler/http/api/security/ShiroAuthenticatingThriftInterceptorTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 47](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/api/security/ShiroAuthenticatingThriftInterceptorTest.java#L47)

```java
@Test(expected = UnauthenticatedException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-78"></a>

### `src/test/java/org/apache/aurora/scheduler/http/api/security/ShiroAuthorizingParamInterceptorTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 152](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/api/security/ShiroAuthorizingParamInterceptorTest.java#L152)

```java
@Test(expected = IllegalStateException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-79"></a>

### `src/test/java/org/apache/aurora/scheduler/http/api/security/ShiroIniConverterTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 60](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/http/api/security/ShiroIniConverterTest.java#L60)

```java
@Test(expected = ParameterException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-83"></a>

### `src/test/java/org/apache/aurora/scheduler/log/mesos/MesosLogTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 212](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/log/mesos/MesosLogTest.java#L212)

```java
@Test(expected = StreamAccessException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-85"></a>

### `src/test/java/org/apache/aurora/scheduler/mesos/CommandLineDriverSettingsModuleTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 42](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/mesos/CommandLineDriverSettingsModuleTest.java#L42)

```java
@Test(expected = IllegalStateException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-87"></a>

### `src/test/java/org/apache/aurora/scheduler/mesos/MesosCallbackHandlerTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 447](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/mesos/MesosCallbackHandlerTest.java#L447)

```java
@Test(expected = SchedulerException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-88"></a>

### `src/test/java/org/apache/aurora/scheduler/mesos/MesosSchedulerImplTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 142](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/mesos/MesosSchedulerImplTest.java#L142)

```java
@Test(expected = IllegalStateException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-91"></a>

### `src/test/java/org/apache/aurora/scheduler/mesos/SchedulerDriverServiceTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 124](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/mesos/SchedulerDriverServiceTest.java#L124)

```java
@Test(expected = IllegalStateException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-94"></a>

### `src/test/java/org/apache/aurora/scheduler/mesos/VersionedMesosSchedulerImplTest.java`

**P2 · concurrency · local**

Replace the fixed sleep with an observable condition and bounded await/assertion for the event under test.

Evidence: [line 272](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/mesos/VersionedMesosSchedulerImplTest.java#L272)

```java
Thread.sleep(1000);
```

Contract: Preserve the intended scheduling window; do not make the assertion pass before the asynchronous work actually completes.

Required validation: Exercise both prompt and delayed execution and verify the test remains deterministic.

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 329](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/mesos/VersionedMesosSchedulerImplTest.java#L329)

```java
@Test(expected = IllegalStateException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-95"></a>

### `src/test/java/org/apache/aurora/scheduler/mesos/VersionedSchedulerDriverServiceTest.java`

**P2 · concurrency · local**

Replace the fixed sleep with an observable condition and bounded await/assertion for the event under test.

Evidence: [line 109](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/mesos/VersionedSchedulerDriverServiceTest.java#L109)

```java
Thread.sleep(1000L);
```

Contract: Preserve the intended scheduling window; do not make the assertion pass before the asynchronous work actually completes.

Required validation: Exercise both prompt and delayed execution and verify the test remains deterministic.

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 90](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/mesos/VersionedSchedulerDriverServiceTest.java#L90)

```java
@Test(expected = IllegalStateException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-97"></a>

### `src/test/java/org/apache/aurora/scheduler/offers/OfferManagerImplTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 309](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/offers/OfferManagerImplTest.java#L309)

```java
@Test(expected = OfferManager.LaunchException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-99"></a>

### `src/test/java/org/apache/aurora/scheduler/offers/RandomJitterReturnDelayTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 52](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/offers/RandomJitterReturnDelayTest.java#L52)

```java
@Test(expected = IllegalArgumentException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-100"></a>

### `src/test/java/org/apache/aurora/scheduler/preemptor/BiCacheTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 90](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/preemptor/BiCacheTest.java#L90)

```java
@Test(expected = NullPointerException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-101"></a>

### `src/test/java/org/apache/aurora/scheduler/preemptor/ClusterStateImplTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 50](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/preemptor/ClusterStateImplTest.java#L50)

```java
@Test(expected = UnsupportedOperationException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-102"></a>

### `src/test/java/org/apache/aurora/scheduler/preemptor/PendingTaskProcessorTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 290](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/preemptor/PendingTaskProcessorTest.java#L290)

```java
@Test(expected = IllegalArgumentException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-110"></a>

### `src/test/java/org/apache/aurora/scheduler/quota/QuotaManagerImplTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 711](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/quota/QuotaManagerImplTest.java#L711)

```java
@Test(expected = QuotaException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-112"></a>

### `src/test/java/org/apache/aurora/scheduler/reconciliation/TaskReconcilerTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 153](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/reconciliation/TaskReconcilerTest.java#L153)

```java
@Test(expected = IllegalArgumentException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-116"></a>

### `src/test/java/org/apache/aurora/scheduler/resources/MesosResourceConverterTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 86](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/resources/MesosResourceConverterTest.java#L86)

```java
@Test(expected = ResourceManager.InsufficientResourcesException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-117"></a>

### `src/test/java/org/apache/aurora/scheduler/resources/PortMapperTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 40](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/resources/PortMapperTest.java#L40)

```java
@Test(expected = IllegalStateException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-128"></a>

### `src/test/java/org/apache/aurora/scheduler/sla/SlaAlgorithmTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 94](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/sla/SlaAlgorithmTest.java#L94)

```java
@Test(expected = IllegalArgumentException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-129"></a>

### `src/test/java/org/apache/aurora/scheduler/sla/SlaManagerTest.java`

**P2 · concurrency · local**

Bound this wait with a test timeout and assert the wait result so a lost signal fails diagnostically instead of hanging the suite.

Evidence: [line 757](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/sla/SlaManagerTest.java#L757)

```java
coordinatorResponded.await();
```

Contract: Keep the latch ordering and event handoff unchanged; choose a timeout longer than the deterministic test operation.

Required validation: Run this test repeatedly with delayed and missing callback paths and verify timeout failures are reported.

<a id="file-130"></a>

### `src/test/java/org/apache/aurora/scheduler/sla/SlaModuleTest.java`

**P2 · concurrency · local**

Bound this wait with a test timeout and assert the wait result so a lost signal fails diagnostically instead of hanging the suite.

Evidence: [line 132](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/sla/SlaModuleTest.java#L132)

```java
latch.await();
```

Contract: Keep the latch ordering and event handoff unchanged; choose a timeout longer than the deterministic test operation.

Required validation: Run this test repeatedly with delayed and missing callback paths and verify timeout failures are reported.

<a id="file-135"></a>

### `src/test/java/org/apache/aurora/scheduler/state/StateManagerImplTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 544](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/state/StateManagerImplTest.java#L544)

```java
@Test(expected = IllegalArgumentException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-140"></a>

### `src/test/java/org/apache/aurora/scheduler/storage/AbstractAttributeStoreTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 115](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/AbstractAttributeStoreTest.java#L115)

```java
@Test(expected = IllegalArgumentException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-143"></a>

### `src/test/java/org/apache/aurora/scheduler/storage/AbstractJobUpdateStoreTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 205](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/AbstractJobUpdateStoreTest.java#L205)

```java
@Test(expected = IllegalArgumentException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-146"></a>

### `src/test/java/org/apache/aurora/scheduler/storage/AbstractTaskStoreTest.java`

**P2 · concurrency · local**

Bound this wait with a test timeout and assert the wait result so a lost signal fails diagnostically instead of hanging the suite.

Evidence: [line 603](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/AbstractTaskStoreTest.java#L603)

```java
read.await();
```

Contract: Keep the latch ordering and event handoff unchanged; choose a timeout longer than the deterministic test operation.

Required validation: Investigate the existing ignored multiple-reader test, capture worker failures, then verify both safe parallel reads and detection of an intentionally broken index implementation.

**P1 · test-infrastructure · after-boundary**

Review the ignored test condition and either remove the ignore after fixing its cause or document a bounded, tracked external prerequisite before migration.

Evidence: [line 576](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/AbstractTaskStoreTest.java#L576)

```java
@Ignore
```

Contract: Do not exclude or weaken this coverage merely to make Java 25 execution green.

Required validation: Investigate the existing ignored multiple-reader test, capture worker failures, then verify both safe parallel reads and detection of an intentionally broken index implementation.

<a id="file-147"></a>

### `src/test/java/org/apache/aurora/scheduler/storage/backup/RecoveryTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 154](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/backup/RecoveryTest.java#L154)

```java
@Test(expected = RecoveryException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-151"></a>

### `src/test/java/org/apache/aurora/scheduler/storage/durability/Generator.java`

**P2 · jdk-api · local**

Replace Class.newInstance with explicit no-argument constructor lookup and invocation in newStruct; keep the reflection-driven fixture generator.

Evidence: [line 117](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/durability/Generator.java#L117)

```java
struct = structClass.newInstance();
```

Contract: Preserve sorted setter order, skipping setFieldValue/IsSet and choosing only one union field. Review InvocationTargetException unwrapping so constructor failures do not gain accidental exception layers.

Required validation: Run DataCompatibilityTest across generated structs/unions and add a focused throwing-constructor case if needed to characterize failure wrapping.

<a id="file-153"></a>

### `src/test/java/org/apache/aurora/scheduler/storage/durability/ThriftBackfillTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 59](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/durability/ThriftBackfillTest.java#L59)

```java
@Test(expected = IllegalArgumentException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-155"></a>

### `src/test/java/org/apache/aurora/scheduler/storage/durability/WriteRecorderTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 141](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/durability/WriteRecorderTest.java#L141)

```java
@Test(expected = UnsupportedOperationException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-161"></a>

### `src/test/java/org/apache/aurora/scheduler/storage/log/SnapshotDeduplicatorImplTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 110](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/log/SnapshotDeduplicatorImplTest.java#L110)

```java
@Test(expected = CodingException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-162"></a>

### `src/test/java/org/apache/aurora/scheduler/storage/log/SnapshotServiceTest.java`

**P2 · concurrency · local**

Bound this wait with a test timeout and assert the wait result so a lost signal fails diagnostically instead of hanging the suite.

Evidence: [line 143](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/log/SnapshotServiceTest.java#L143)

```java
snapshotCalled.await();
```

Contract: Keep the latch ordering and event handoff unchanged; choose a timeout longer than the deterministic test operation.

Required validation: Run this test repeatedly with delayed and missing callback paths and verify timeout failures are reported.

<a id="file-172"></a>

### `src/test/java/org/apache/aurora/scheduler/storage/mem/MemStorageTest.java`

**P2 · concurrency · local**

Bound test-thread waits and overall completion, and release the inner reader barrier in finally; keep the slow reader blocked until the concurrency assertion has run.

Evidence: [line 78](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/mem/MemStorageTest.java#L78)

```java
slowReadStarted.await();
```

Contract: A timeout that merely releases the inner reader can hide serialization of reads; timeout must fail the test and failures in worker futures must be observed.

Required validation: Run the concurrent-reader test against working and intentionally serialized implementations; the latter must fail within the deadline.

<a id="file-174"></a>

### `src/test/java/org/apache/aurora/scheduler/storage/mem/StorageTransactionTest.java`

**P2 · concurrency · local**

Bound test-thread waits and overall completion, and release the inner reader barrier in finally; keep the slow reader blocked until the concurrency assertion has run.

Evidence: [line 77](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/mem/StorageTransactionTest.java#L77)

```java
slowReadStarted.await();
```

Contract: A timeout that merely releases the inner reader can hide serialization of reads; timeout must fail the test and failures in worker futures must be observed.

Required validation: Run the concurrent-reader test against working and intentionally serialized implementations; the latter must fail within the deadline.

<a id="file-176"></a>

### `src/test/java/org/apache/aurora/scheduler/storage/testing/StorageEntityUtilTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 39](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/storage/testing/StorageEntityUtilTest.java#L39)

```java
@Test(expected = AssertionError.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-183"></a>

### `src/test/java/org/apache/aurora/scheduler/thrift/SchedulerThriftInterfaceTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 1311](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/thrift/SchedulerThriftInterfaceTest.java#L1311)

```java
@Test(expected = StorageException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-190"></a>

### `src/test/java/org/apache/aurora/scheduler/updater/AddTaskTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 111](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/updater/AddTaskTest.java#L111)

```java
@Test(expected = IllegalStateException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-192"></a>

### `src/test/java/org/apache/aurora/scheduler/updater/InstanceUpdaterTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 275](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/updater/InstanceUpdaterTest.java#L275)

```java
@Test(expected = IllegalArgumentException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-196"></a>

### `src/test/java/org/apache/aurora/scheduler/updater/JobUpdaterIT.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 770](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/updater/JobUpdaterIT.java#L770)

```java
@Test(expected = IllegalStateException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-203"></a>

### `src/test/java/org/apache/aurora/scheduler/updater/strategy/BatchStrategyTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 31](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/updater/strategy/BatchStrategyTest.java#L31)

```java
@Test(expected = IllegalArgumentException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-204"></a>

### `src/test/java/org/apache/aurora/scheduler/updater/strategy/QueueStrategyTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 31](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/updater/strategy/QueueStrategyTest.java#L31)

```java
@Test(expected = IllegalArgumentException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.

<a id="file-205"></a>

### `src/test/java/org/apache/aurora/scheduler/updater/strategy/VariableBatchStrategyTest.java`

**P2 · test-infrastructure · framework**

Migrate this exception assertion to an explicit assertion API when the project’s coordinated JUnit framework plan selects it (Jupiter assertThrows or the compatible JUnit 4.13 form).

Evidence: [line 32](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/test/java/org/apache/aurora/scheduler/updater/strategy/VariableBatchStrategyTest.java#L32)

```java
@Test(expected = IllegalArgumentException.class)
```

Contract: Retain the current exception type and meaningful message/cause assertions; do not perform a blind annotation replacement or require Jupiter in this file alone.

Required validation: Run the affected negative cases and confirm both the thrown type and existing diagnostic assertions remain covered.
