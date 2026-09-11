# Core, scheduling, state, storage and updater

[Audit overview and ordering](../JAVA25_FILE_AUDIT.md).

176 files: 15 local-change candidates, 26 deferred, 135 retained.

Source baseline: `6cf7f0ea07c6355af62ceafb029fe086f55b8c65`. Findings describe proposed work; validation is not yet executed.

Retain means no separate improvement prioritized in this pass. A local candidate still needs
the stated contract checks. Framework findings are grouped migrations, not separate PRs per file.

| Source file | Assessment | Recommendation or retain rationale |
| --- | --- | --- |
| [src/main/java/org/apache/aurora/scheduler/cron/CronException.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/cron/CronException.java#L1) | retain | Keep the checked cron exception constructors and cause propagation unchanged. |
| [src/main/java/org/apache/aurora/scheduler/cron/CronJobManager.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/cron/CronJobManager.java#L1) | retain | Keep the cron management interface and checked failure contract; its storage and Quartz implementations already define behavior. |
| [src/main/java/org/apache/aurora/scheduler/cron/CronPredictor.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/cron/CronPredictor.java#L1) | retain | Keep Optional<Date> at this Quartz-facing prediction boundary until its callers migrate together. |
| [src/main/java/org/apache/aurora/scheduler/cron/CronScheduler.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/cron/CronScheduler.java#L1) | retain | Keep the small Optional-based schedule lookup interface. |
| [src/main/java/org/apache/aurora/scheduler/cron/CrontabEntry.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/cron/CrontabEntry.java#L278) | [defer](#file-5) | Simplify equality with a pattern binding while preserving range normalization and custom hash order. |
| [src/main/java/org/apache/aurora/scheduler/cron/SanitizedCronJob.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/cron/SanitizedCronJob.java#L60) | [change](#file-6) | Express invalid cron parsing through Optional.orElseThrow without altering checked CronException validation. |
| [src/main/java/org/apache/aurora/scheduler/cron/quartz/AuroraCronJob.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/cron/quartz/AuroraCronJob.java#L242) | [change](#file-7) | Separate failed asynchronous work from actual interruption in cron launch waiting. |
| [src/main/java/org/apache/aurora/scheduler/cron/quartz/AuroraCronJobFactory.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/cron/quartz/AuroraCronJobFactory.java#L1) | retain | Keep the Quartz JobFactory adapter and its injected provider ownership. |
| [src/main/java/org/apache/aurora/scheduler/cron/quartz/CronJobManagerImpl.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/cron/quartz/CronJobManagerImpl.java#L198) | [defer](#file-9) | Replace the diagnostic trigger FluentIterable chain with a sequential stream retaining first-trigger selection. |
| [src/main/java/org/apache/aurora/scheduler/cron/quartz/CronLifecycle.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/cron/quartz/CronLifecycle.java#L1) | retain | Keep Quartz lifecycle startup, recovered-job scheduling, and shutdown ordering. |
| [src/main/java/org/apache/aurora/scheduler/cron/quartz/CronModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/cron/quartz/CronModule.java#L1) | retain | Keep reflective CLI fields and Quartz TimeZone/thread-pool configuration until framework migration. |
| [src/main/java/org/apache/aurora/scheduler/cron/quartz/CronPredictorImpl.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/cron/quartz/CronPredictorImpl.java#L1) | retain | Keep the Date conversion immediately at Quartz's legacy next-valid-time boundary. |
| [src/main/java/org/apache/aurora/scheduler/cron/quartz/CronSchedulerImpl.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/cron/quartz/CronSchedulerImpl.java#L1) | retain | Keep exactly-one-trigger validation; findFirst would weaken the current malformed-schedule contract. |
| [src/main/java/org/apache/aurora/scheduler/cron/quartz/Quartz.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/cron/quartz/Quartz.java#L1) | retain | Keep Quartz weekday translation, wildcard handling, and persisted trigger descriptions. |
| [src/main/java/org/apache/aurora/scheduler/log/Log.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/log/Log.java#L1) | retain | Keep the log abstraction pending replacement of native log position and stream ownership semantics. |
| [src/main/java/org/apache/aurora/scheduler/log/mesos/LogInterface.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/log/mesos/LogInterface.java#L1) | retain | Retain the current implementation; defer the Mesos reader/writer timeout interface until native log removal establishes the replacement boundary. |
| [src/main/java/org/apache/aurora/scheduler/log/mesos/MesosLog.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/log/mesos/MesosLog.java#L1) | retain | Retain the current implementation; defer native log state-machine modernization; nullable Optional encodes disabled versus uninitialized writers. |
| [src/main/java/org/apache/aurora/scheduler/log/mesos/MesosLogStreamModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/log/mesos/MesosLogStreamModule.java#L1) | retain | Retain the current implementation; defer Mesos native-log provisioning and timeout changes with native persistence removal. |
| [src/main/java/org/apache/aurora/scheduler/maintenance/MaintenanceController.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/maintenance/MaintenanceController.java#L313) | [defer](#file-19) | Inject the existing testable clock for maintenance request timestamps and expiration decisions. |
| [src/main/java/org/apache/aurora/scheduler/maintenance/MaintenanceModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/maintenance/MaintenanceModule.java#L1) | retain | Keep maintenance bindings and reflective timeout options; executor ownership requires service-wide treatment. |
| [src/main/java/org/apache/aurora/scheduler/mesos/CommandLineDriverSettingsModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/mesos/CommandLineDriverSettingsModule.java#L156) | [change](#file-21) | Close the credentials file stream at its owning getCredentials call site. |
| [src/main/java/org/apache/aurora/scheduler/mesos/Driver.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/mesos/Driver.java#L1) | retain | Retain the current implementation; defer the Mesos Driver service contract with the scheduler boundary replacement. |
| [src/main/java/org/apache/aurora/scheduler/mesos/DriverFactory.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/mesos/DriverFactory.java#L1) | retain | Retain the current implementation; defer the native SchedulerDriver factory with its Mesos dependency. |
| [src/main/java/org/apache/aurora/scheduler/mesos/DriverFactoryImpl.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/mesos/DriverFactoryImpl.java#L1) | retain | Keep credentials conversion and explicit-acknowledgement selection until native driver replacement. |
| [src/main/java/org/apache/aurora/scheduler/mesos/DriverSettings.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/mesos/DriverSettings.java#L1) | retain | Keep DriverSettings as a reference-identity settings class while its native credentials boundary changes. |
| [src/main/java/org/apache/aurora/scheduler/mesos/FrameworkInfoFactory.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/mesos/FrameworkInfoFactory.java#L1) | retain | Retain the current implementation; defer deprecated URL construction until the Mesos framework-info adapter is replaced, preserving URL text. |
| [src/main/java/org/apache/aurora/scheduler/mesos/LibMesosLoadingModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/mesos/LibMesosLoadingModule.java#L1) | retain | Retain the current implementation; defer native driver-kind bindings with libmesos removal. |
| [src/main/java/org/apache/aurora/scheduler/mesos/MesosCallbackHandler.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/mesos/MesosCallbackHandler.java#L1) | retain | Keep callback queuing, rescind banning, and explicit status acknowledgement ordering through boundary migration. |
| [src/main/java/org/apache/aurora/scheduler/mesos/MesosSchedulerImpl.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/mesos/MesosSchedulerImpl.java#L1) | retain | Retain the current implementation; defer unversioned callback conversion and registration gating with native Scheduler removal. |
| [src/main/java/org/apache/aurora/scheduler/mesos/MesosTaskFactory.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/mesos/MesosTaskFactory.java#L1) | retain | Keep task/executor serialization, resource allocation, labels, and legacy source-field compatibility through boundary replacement. |
| [src/main/java/org/apache/aurora/scheduler/mesos/ProtosConversion.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/mesos/ProtosConversion.java#L1) | retain | Keep binary protobuf conversion until both old and versioned Mesos boundaries are removed. |
| [src/main/java/org/apache/aurora/scheduler/mesos/SchedulerDriverModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/mesos/SchedulerDriverModule.java#L1) | retain | Retain the current implementation; defer driver selection and serialized callback executor ownership to the boundary/service migration. |
| [src/main/java/org/apache/aurora/scheduler/mesos/SchedulerDriverService.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/mesos/SchedulerDriverService.java#L1) | retain | Keep stop(true), explicit acknowledgement and driver readiness checks until native service replacement. |
| [src/main/java/org/apache/aurora/scheduler/mesos/TaskStatusStats.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/mesos/TaskStatusStats.java#L1) | retain | Keep lazily exported status counters and existing microsecond clock-skew filtering. |
| [src/main/java/org/apache/aurora/scheduler/mesos/TestExecutorSettings.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/mesos/TestExecutorSettings.java#L1) | retain | Keep executor protocol test fixtures intact while the Mesos integration boundary is replaced. |
| [src/main/java/org/apache/aurora/scheduler/mesos/VersionedDriverFactory.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/mesos/VersionedDriverFactory.java#L1) | retain | Retain the current implementation; defer the versioned Mesos factory interface with its transport replacement. |
| [src/main/java/org/apache/aurora/scheduler/mesos/VersionedMesosSchedulerImpl.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/mesos/VersionedMesosSchedulerImpl.java#L136) | [change](#file-37) | Restore interruption when subscription backoff is interrupted, without reordering subscription callbacks. |
| [src/main/java/org/apache/aurora/scheduler/mesos/VersionedSchedulerDriverService.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/mesos/VersionedSchedulerDriverService.java#L244) | [change](#file-38) | Restore interruption when registration or termination latch waits are interrupted. |
| [src/main/java/org/apache/aurora/scheduler/offers/Deferment.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/offers/Deferment.java#L1) | retain | Keep delayed offer actions on the shared scheduled executor; Duration changes must retain configured unit conversion. |
| [src/main/java/org/apache/aurora/scheduler/offers/HostOffer.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/offers/HostOffer.java#L93) | [defer](#file-40) | Use a pattern binding in equality while preserving memoized resource suppliers and derived offer flags. |
| [src/main/java/org/apache/aurora/scheduler/offers/HostOffers.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/offers/HostOffers.java#L1) | retain | Keep synchronized offer indexes and lazy veto filtering; eager collection would alter scheduling work and cache side effects. |
| [src/main/java/org/apache/aurora/scheduler/offers/OfferManager.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/offers/OfferManager.java#L1) | retain | Retain the current implementation; defer protocol-specific launch parameters with the offer/driver boundary; preserve checked launch failures. |
| [src/main/java/org/apache/aurora/scheduler/offers/OfferManagerImpl.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/offers/OfferManagerImpl.java#L1) | retain | Keep remove-before-launch, decline compaction, and acceptable offer-race behavior. |
| [src/main/java/org/apache/aurora/scheduler/offers/OfferManagerModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/offers/OfferManagerModule.java#L1) | retain | Keep reflective offer-set configuration and cache-expiry policies until their injection boundary is migrated. |
| [src/main/java/org/apache/aurora/scheduler/offers/OfferOrder.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/offers/OfferOrder.java#L1) | retain | Keep stable offer-order enum names used by CLI configuration. |
| [src/main/java/org/apache/aurora/scheduler/offers/OfferOrderBuilder.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/offers/OfferOrderBuilder.java#L89) | [defer](#file-46) | Use a switch expression for resource-order selection while retaining Guava arbitrary identity ordering. |
| [src/main/java/org/apache/aurora/scheduler/offers/OfferSet.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/offers/OfferSet.java#L1) | retain | Keep the iterable offer-set extension point and weakly consistent iteration contract. |
| [src/main/java/org/apache/aurora/scheduler/offers/OfferSetImpl.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/offers/OfferSetImpl.java#L1) | retain | Keep ConcurrentSkipListSet and its comparator; launch code removes offers during iteration. |
| [src/main/java/org/apache/aurora/scheduler/offers/OfferSettings.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/offers/OfferSettings.java#L1) | retain | Keep the cache settings object because it owns a configured mutable CacheBuilder, not a pure value tuple. |
| [src/main/java/org/apache/aurora/scheduler/offers/RandomJitterReturnDelay.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/offers/RandomJitterReturnDelay.java#L1) | retain | Keep injected random and millisecond jitter semantics; changing random APIs also requires zero/large-bound policy decisions. |
| [src/main/java/org/apache/aurora/scheduler/preemptor/BiCache.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/preemptor/BiCache.java#L1) | retain | Keep bidirectional cache synchronization, removal callbacks, and explicit cleanup before inverse lookup. |
| [src/main/java/org/apache/aurora/scheduler/preemptor/PendingTaskProcessor.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/preemptor/PendingTaskProcessor.java#L1) | retain | Keep consuming-iterator round robin and reservation removal order; collection rewrites must not change fairness. |
| [src/main/java/org/apache/aurora/scheduler/preemptor/PendingTaskProcessorModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/preemptor/PendingTaskProcessorModule.java#L1) | retain | Keep the small preemption slot-finder binding module. |
| [src/main/java/org/apache/aurora/scheduler/preemptor/PreemptionProposal.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/preemptor/PreemptionProposal.java#L46) | [defer](#file-54) | Use a pattern binding in proposal equality while preserving immutable victim-set and cache-key hashing. |
| [src/main/java/org/apache/aurora/scheduler/preemptor/PreemptionVictim.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/preemptor/PreemptionVictim.java#L72) | [defer](#file-55) | Use a pattern binding in victim equality while retaining task-wrapper and resource projection semantics. |
| [src/main/java/org/apache/aurora/scheduler/preemptor/PreemptionVictimFilter.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/preemptor/PreemptionVictimFilter.java#L1) | retain | Keep lexicographic resource ordering and incremental victim accumulation; JDK changes must preserve revocable CPU exclusion. |
| [src/main/java/org/apache/aurora/scheduler/preemptor/PreemptionVictimFilterModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/preemptor/PreemptionVictimFilterModule.java#L1) | retain | Keep the concrete preemption victim-filter binding module. |
| [src/main/java/org/apache/aurora/scheduler/preemptor/Preemptor.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/preemptor/Preemptor.java#L1) | retain | Keep reservation revalidation and removal before preempting victims. |
| [src/main/java/org/apache/aurora/scheduler/preemptor/PreemptorMetrics.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/preemptor/PreemptorMetrics.java#L1) | retain | Keep eager metric-name export and stable production/non-production counter naming. |
| [src/main/java/org/apache/aurora/scheduler/preemptor/PreemptorModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/preemptor/PreemptorModule.java#L1) | retain | Keep configured fixed-rate preemption scheduling, private bindings, and disabled-preemptor behavior. |
| [src/main/java/org/apache/aurora/scheduler/quota/QuotaCheckResult.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/quota/QuotaCheckResult.java#L1) | retain | Keep quota result details and absent-versus-empty Optional distinctions; a record would introduce equality semantics. |
| [src/main/java/org/apache/aurora/scheduler/quota/QuotaInfo.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/quota/QuotaInfo.java#L95) | [defer](#file-62) | Use a pattern binding in quota-info equality while keeping all five consumption dimensions. |
| [src/main/java/org/apache/aurora/scheduler/quota/QuotaManager.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/quota/QuotaManager.java#L1) | retain | Keep cron maximum-consumption and update-overlap quota algorithms with existing range/multimap semantics. |
| [src/main/java/org/apache/aurora/scheduler/quota/QuotaModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/quota/QuotaModule.java#L1) | retain | Keep the quota service binding and required storage dependency. |
| [src/main/java/org/apache/aurora/scheduler/reconciliation/KillRetry.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/reconciliation/KillRetry.java#L1) | retain | Keep serial per-task kill backoff and status revalidation before retry. |
| [src/main/java/org/apache/aurora/scheduler/reconciliation/ReconciliationModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/reconciliation/ReconciliationModule.java#L1) | retain | Coordinate reconciliation executor lifecycle changes with its service rather than altering thread ownership in bindings alone. |
| [src/main/java/org/apache/aurora/scheduler/reconciliation/TaskReconciler.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/reconciliation/TaskReconciler.java#L174) | [defer](#file-67) | Give the reconciliation service explicit cancellation of owned periodic and pending batch work at shutdown. |
| [src/main/java/org/apache/aurora/scheduler/reconciliation/TaskTimeout.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/reconciliation/TaskTimeout.java#L1) | retain | Keep transient-state CAS and startup retry semantics; virtual-thread substitution does not solve timer ownership. |
| [src/main/java/org/apache/aurora/scheduler/resources/AcceptedOffer.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/resources/AcceptedOffer.java#L1) | retain | Keep shared mutable resource builders across task/executor allocation and reserved-first ordering. |
| [src/main/java/org/apache/aurora/scheduler/resources/AuroraResourceConverter.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/resources/AuroraResourceConverter.java#L1) | retain | Keep Longs.tryParse null-on-invalid behavior and existing resource quantization conversions. |
| [src/main/java/org/apache/aurora/scheduler/resources/MesosResourceConverter.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/resources/MesosResourceConverter.java#L1) | retain | Keep scalar tolerance and range allocation mutation; primitive reducers must preserve numeric behavior and boundaries. |
| [src/main/java/org/apache/aurora/scheduler/resources/ResourceBag.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/resources/ResourceBag.java#L214) | [defer](#file-72) | Use a pattern binding in resource-bag equality while preserving absent-versus-zero vectors and hash semantics. |
| [src/main/java/org/apache/aurora/scheduler/resources/ResourceManager.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/resources/ResourceManager.java#L1) | retain | Keep sequential floating-point reduction and resource-type filtering; summingDouble would change rounding behavior. |
| [src/main/java/org/apache/aurora/scheduler/resources/ResourceMapper.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/resources/ResourceMapper.java#L1) | retain | Keep mutable available-port collection because shuffling and sequential port assignment depend on it. |
| [src/main/java/org/apache/aurora/scheduler/resources/ResourceSettings.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/resources/ResourceSettings.java#L1) | retain | Keep reflective revocability option fields and their lazy supplier access. |
| [src/main/java/org/apache/aurora/scheduler/resources/ResourceType.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/resources/ResourceType.java#L1) | retain | Keep Thrift field IDs and Mesos names stable; enum ordinal is not a replacement identity. |
| [src/main/java/org/apache/aurora/scheduler/scheduling/RescheduleCalculator.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/scheduling/RescheduleCalculator.java#L92) | [defer](#file-77) | Use the JDK reversed list view for reverse event traversal while preserving flapping history selection. |
| [src/main/java/org/apache/aurora/scheduler/scheduling/SchedulingModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/scheduling/SchedulingModule.java#L1) | retain | Keep scheduler rate limiting, backoff configuration, and batch-worker lifecycle bindings. |
| [src/main/java/org/apache/aurora/scheduler/scheduling/TaskAssigner.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/scheduling/TaskAssigner.java#L1) | retain | Keep the assignment interface and externally supplied mutable transaction scope. |
| [src/main/java/org/apache/aurora/scheduler/scheduling/TaskAssignerImpl.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/scheduling/TaskAssignerImpl.java#L205) | [defer](#file-80) | Convert the private SchedulingMatch tuple to a record without changing assignment order or offer reservation semantics. |
| [src/main/java/org/apache/aurora/scheduler/scheduling/TaskAssignerImplModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/scheduling/TaskAssignerImplModule.java#L1) | retain | Keep the small task-assigner extension binding module. |
| [src/main/java/org/apache/aurora/scheduler/scheduling/TaskGroup.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/scheduling/TaskGroup.java#L1) | retain | Keep the synchronized mutable task queue and JSON debug getters; this is not a record candidate. |
| [src/main/java/org/apache/aurora/scheduler/scheduling/TaskGroups.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/scheduling/TaskGroups.java#L179) | [change](#file-83) | Split execution failure from interruption when awaiting a task-group batch result. |
| [src/main/java/org/apache/aurora/scheduler/scheduling/TaskScheduler.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/scheduling/TaskScheduler.java#L1) | retain | Keep the batch scheduling interface and caller-owned storage transaction. |
| [src/main/java/org/apache/aurora/scheduler/scheduling/TaskSchedulerImpl.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/scheduling/TaskSchedulerImpl.java#L1) | retain | Keep broad retry protection, task filtering, and preemption fallback ordering. |
| [src/main/java/org/apache/aurora/scheduler/scheduling/TaskThrottler.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/scheduling/TaskThrottler.java#L1) | retain | Keep delayed THROTTLED-to-PENDING CAS transitions and shared executor ownership. |
| [src/main/java/org/apache/aurora/scheduler/sla/MetricCalculator.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/sla/MetricCalculator.java#L1) | retain | Keep SLA metric grouping, time windows and lazily registered gauges; preserve metric names. |
| [src/main/java/org/apache/aurora/scheduler/sla/SlaAlgorithm.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/sla/SlaAlgorithm.java#L257) | [defer](#file-88) | Use a pattern binding for the private instance identifier equality, leaving uptime interval algorithms intact. |
| [src/main/java/org/apache/aurora/scheduler/sla/SlaGroup.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/sla/SlaGroup.java#L1) | retain | Keep SLA bucket ranges and multimap grouping because endpoint inclusion defines metric membership. |
| [src/main/java/org/apache/aurora/scheduler/sla/SlaManager.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/sla/SlaManager.java#L229) | [defer](#file-90) | Inject the scheduler clock for SLA running-duration checks instead of directly reading wall time. |
| [src/main/java/org/apache/aurora/scheduler/sla/SlaModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/sla/SlaModule.java#L1) | retain | Coordinate SLA executor ownership and HTTP client shutdown with the service lifecycle migration. |
| [src/main/java/org/apache/aurora/scheduler/sla/SlaUtil.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/sla/SlaUtil.java#L1) | retain | Keep Guava quantiles because percentile direction and interpolation define existing SLA metrics. |
| [src/main/java/org/apache/aurora/scheduler/state/ClusterState.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/state/ClusterState.java#L1) | retain | Keep the immutable-multimap snapshot contract used by preemption search. |
| [src/main/java/org/apache/aurora/scheduler/state/ClusterStateImpl.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/state/ClusterStateImpl.java#L1) | retain | Keep synchronized victim updates and immutable snapshots; ConcurrentHashMap alone cannot replace these semantics. |
| [src/main/java/org/apache/aurora/scheduler/state/PartitionManager.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/state/PartitionManager.java#L1) | retain | Keep timestamp-checked partition rescheduling and existing Duration conversion; executor ownership is a framework concern. |
| [src/main/java/org/apache/aurora/scheduler/state/SideEffect.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/state/SideEffect.java#L40) | [defer](#file-96) | Use a pattern binding in side-effect equality while retaining action formatting and nullable constructor behavior. |
| [src/main/java/org/apache/aurora/scheduler/state/StateChangeResult.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/state/StateChangeResult.java#L1) | retain | Keep state-change result enum identities and CAS outcome distinctions. |
| [src/main/java/org/apache/aurora/scheduler/state/StateManager.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/state/StateManager.java#L1) | retain | Retain the current implementation; defer Mesos AgentID removal from assignment with the state/driver boundary migration. |
| [src/main/java/org/apache/aurora/scheduler/state/StateManagerImpl.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/state/StateManagerImpl.java#L1) | retain | Keep explicit side-effect ordering, recursive transitions and delayed events inside the existing transaction model. |
| [src/main/java/org/apache/aurora/scheduler/state/StateModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/state/StateModule.java#L1) | retain | Retain the current implementation; defer Mesos task-factory binding changes with the scheduler boundary; preserve extension module initialization. |
| [src/main/java/org/apache/aurora/scheduler/state/TaskStateMachine.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/state/TaskStateMachine.java#L193) | [defer](#file-101) | Consolidate identical restart-state switch arms using arrow labels without changing the transition graph. |
| [src/main/java/org/apache/aurora/scheduler/state/TransitionResult.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/state/TransitionResult.java#L60) | [defer](#file-102) | Use a pattern binding in transition-result equality while preserving side-effect validation and hashing. |
| [src/main/java/org/apache/aurora/scheduler/state/UUIDGenerator.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/state/UUIDGenerator.java#L1) | retain | Keep UUID.randomUUID behind the injectable generator for deterministic callers/tests. |
| [src/main/java/org/apache/aurora/scheduler/storage/AttributeStore.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/AttributeStore.java#L108) | [change](#file-104) | Use Optional.map for maintenance-mode merging, preserving absent host behavior and immutable builder conversion. |
| [src/main/java/org/apache/aurora/scheduler/storage/CallOrderEnforcingStorage.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/CallOrderEnforcingStorage.java#L1) | retain | Keep enforced storage lifecycle and latest-activity initialization-event order. |
| [src/main/java/org/apache/aurora/scheduler/storage/CronJobStore.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/CronJobStore.java#L1) | retain | Keep the cron store's read/mutable interface split and Optional lookup contract. |
| [src/main/java/org/apache/aurora/scheduler/storage/HostMaintenanceStore.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/HostMaintenanceStore.java#L1) | retain | Keep the maintenance store's read/mutable split and immutable entity interface. |
| [src/main/java/org/apache/aurora/scheduler/storage/JobUpdateStore.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/JobUpdateStore.java#L1) | retain | Keep job-update query and mutation contracts; terminal status sets participate in persisted update semantics. |
| [src/main/java/org/apache/aurora/scheduler/storage/QuotaStore.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/QuotaStore.java#L1) | retain | Keep quota read/mutable interface split and nullable absence modeled by Optional. |
| [src/main/java/org/apache/aurora/scheduler/storage/SchedulerStore.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/SchedulerStore.java#L1) | retain | Retain the current implementation; defer persisted framework-ID meaning with Mesos removal rather than renaming persisted state mechanically. |
| [src/main/java/org/apache/aurora/scheduler/storage/SnapshotStore.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/SnapshotStore.java#L1) | retain | Keep explicit snapshot coding/storage exception boundaries. |
| [src/main/java/org/apache/aurora/scheduler/storage/Snapshotter.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/Snapshotter.java#L1) | retain | Keep Snapshotter's sequential operation stream contract and snapshot construction boundary. |
| [src/main/java/org/apache/aurora/scheduler/storage/Storage.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/Storage.java#L1) | retain | Keep typed checked-exception storage operations; JDK Function cannot express their throws contract. |
| [src/main/java/org/apache/aurora/scheduler/storage/TaskStore.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/TaskStore.java#L1) | retain | Keep task-query blank-role semantics; String.isBlank has different whitespace coverage from Guava CharMatcher. |
| [src/main/java/org/apache/aurora/scheduler/storage/Util.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/Util.java#L1) | retain | Keep metric names derived from existing update enum text. |
| [src/main/java/org/apache/aurora/scheduler/storage/backup/BackupModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/backup/BackupModule.java#L1) | retain | Coordinate asynchronous backup executor ownership with scheduler lifecycle, retaining backup-directory policy. |
| [src/main/java/org/apache/aurora/scheduler/storage/backup/BackupReader.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/backup/BackupReader.java#L1) | retain | Keep backup decoding delegated to Recovery.load and the read-only Persistence adapter. |
| [src/main/java/org/apache/aurora/scheduler/storage/backup/Recovery.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/backup/Recovery.java#L211) | [change](#file-118) | Close the backup input stream even when Thrift snapshot decoding fails. |
| [src/main/java/org/apache/aurora/scheduler/storage/backup/StorageBackup.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/backup/StorageBackup.java#L139) | [change](#file-119) | Replace shared SimpleDateFormat with an immutable DateTimeFormatter while retaining backup filename and zone semantics. |
| [src/main/java/org/apache/aurora/scheduler/storage/backup/TemporaryStorage.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/backup/TemporaryStorage.java#L1) | retain | Keep isolated recovery storage, captured snapshot clock, and explicit mutation transactions. |
| [src/main/java/org/apache/aurora/scheduler/storage/durability/DurableStorage.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/durability/DurableStorage.java#L178) | [change](#file-121) | Close the recovered edit stream in the consuming durable-storage recovery scope. |
| [src/main/java/org/apache/aurora/scheduler/storage/durability/DurableStorageModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/durability/DurableStorageModule.java#L1) | retain | Keep durable storage lifecycle wrapping and private dependency bindings. |
| [src/main/java/org/apache/aurora/scheduler/storage/durability/Loader.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/durability/Loader.java#L1) | retain | Keep ordered log replay and legacy-op handling; parallel stream consumption would corrupt state reconstruction. |
| [src/main/java/org/apache/aurora/scheduler/storage/durability/Persistence.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/durability/Persistence.java#L81) | [defer](#file-124) | Use a pattern binding in Edit equality while preserving the null sentinel for delete-all edits. |
| [src/main/java/org/apache/aurora/scheduler/storage/durability/Recovery.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/durability/Recovery.java#L70) | [change](#file-125) | Close the source recovery stream in copy just as requireEmpty already closes its stream. |
| [src/main/java/org/apache/aurora/scheduler/storage/durability/RecoveryTool.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/durability/RecoveryTool.java#L1) | retain | Retain the current implementation; defer native-log endpoint construction with persistence replacement and keep reflective CLI option structure. |
| [src/main/java/org/apache/aurora/scheduler/storage/durability/ThriftBackfill.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/durability/ThriftBackfill.java#L1) | retain | Keep compatibility backfill and quota field-set validation at replay boundaries. |
| [src/main/java/org/apache/aurora/scheduler/storage/durability/TransactionRecorder.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/durability/TransactionRecorder.java#L37) | [change](#file-128) | Use List.getLast with an explicit empty check when examining the previous operation. |
| [src/main/java/org/apache/aurora/scheduler/storage/durability/WriteRecorder.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/durability/WriteRecorder.java#L1) | retain | Keep operation recording, delegate mutation and event emission order, including legacy removal compatibility. |
| [src/main/java/org/apache/aurora/scheduler/storage/log/Entries.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/log/Entries.java#L1) | retain | Keep Thrift compression/encoding helpers and persisted log-entry wire layout. |
| [src/main/java/org/apache/aurora/scheduler/storage/log/EntrySerializer.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/log/EntrySerializer.java#L1) | retain | Keep lazy frame emission, hash function, and chunk boundaries; these are persistence format contracts. |
| [src/main/java/org/apache/aurora/scheduler/storage/log/LogManager.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/log/LogManager.java#L1) | retain | Keep the log/stream factory composition until replacement log resource ownership is known. |
| [src/main/java/org/apache/aurora/scheduler/storage/log/LogPersistence.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/log/LogPersistence.java#L93) | [defer](#file-133) | Use a switch expression in recovery flatMap while preserving reset-before-snapshot and transaction order. |
| [src/main/java/org/apache/aurora/scheduler/storage/log/LogPersistenceModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/log/LogPersistenceModule.java#L1) | retain | Keep MD5 framing checksums for existing log readability; replacing the algorithm needs a versioned format migration. |
| [src/main/java/org/apache/aurora/scheduler/storage/log/SnapshotDeduplicator.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/log/SnapshotDeduplicator.java#L1) | retain | Keep partial deep copies, shared task-config restoration and numeric snapshot references. |
| [src/main/java/org/apache/aurora/scheduler/storage/log/SnapshotModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/log/SnapshotModule.java#L1) | retain | Keep snapshot interval injection and active-service binding. |
| [src/main/java/org/apache/aurora/scheduler/storage/log/SnapshotService.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/log/SnapshotService.java#L1) | retain | Keep snapshots under the storage write lock and append-before-truncate behavior. |
| [src/main/java/org/apache/aurora/scheduler/storage/log/SnapshotterImpl.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/log/SnapshotterImpl.java#L1) | retain | Keep snapshot field order and parent-update-before-event replay streams. |
| [src/main/java/org/apache/aurora/scheduler/storage/log/StreamManager.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/log/StreamManager.java#L1) | retain | Keep frame-level stream abstraction until native log ownership is replaced. |
| [src/main/java/org/apache/aurora/scheduler/storage/log/StreamManagerFactory.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/log/StreamManagerFactory.java#L1) | retain | Keep the assisted stream factory interface required by Guice construction. |
| [src/main/java/org/apache/aurora/scheduler/storage/log/StreamManagerImpl.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/log/StreamManagerImpl.java#L1) | retain | Keep serialized frame append, interrupted-frame recovery, and snapshot append-before-truncate semantics. |
| [src/main/java/org/apache/aurora/scheduler/storage/mem/Interner.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/mem/Interner.java#L1) | retain | Keep synchronized association-counted interning and mutable association sets. |
| [src/main/java/org/apache/aurora/scheduler/storage/mem/MemAttributeStore.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/mem/MemAttributeStore.java#L56) | [change](#file-143) | Replace the allMatch FluentIterable validation with Collection.stream().allMatch while preserving validation order. |
| [src/main/java/org/apache/aurora/scheduler/storage/mem/MemCronJobStore.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/mem/MemCronJobStore.java#L1) | retain | Keep concurrent cron storage and immutable snapshots; mutable CLI/job entities already cross the immutable wrapper boundary. |
| [src/main/java/org/apache/aurora/scheduler/storage/mem/MemHostMaintenanceStore.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/mem/MemHostMaintenanceStore.java#L1) | retain | Keep concurrent maintenance request lookup and immutable request snapshots. |
| [src/main/java/org/apache/aurora/scheduler/storage/mem/MemJobUpdateStore.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/mem/MemJobUpdateStore.java#L171) | [defer](#file-146) | Use guarded List.getFirst/getLast for synthesized update state while preserving empty-event defaults. |
| [src/main/java/org/apache/aurora/scheduler/storage/mem/MemQuotaStore.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/mem/MemQuotaStore.java#L1) | retain | Keep concurrent quota map and immutable fetchQuotas snapshots. |
| [src/main/java/org/apache/aurora/scheduler/storage/mem/MemSchedulerStore.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/mem/MemSchedulerStore.java#L27) | [change](#file-148) | Replace the obsolete Atomics factory with direct AtomicReference construction preserving nullable framework ID. |
| [src/main/java/org/apache/aurora/scheduler/storage/mem/MemStorage.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/mem/MemStorage.java#L1) | retain | Keep checked-exception pass-through and anonymous store provider; transaction coordination belongs to DurableStorage. |
| [src/main/java/org/apache/aurora/scheduler/storage/mem/MemStorageModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/mem/MemStorageModule.java#L1) | retain | Keep volatile/private store bindings and isolated test-storage construction. |
| [src/main/java/org/apache/aurora/scheduler/storage/mem/MemTaskStore.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/mem/MemTaskStore.java#L282) | [defer](#file-151) | Use a pattern binding for private Task equality, preserving interning, secondary indexes and relaxed read consistency. |
| [src/main/java/org/apache/aurora/scheduler/updater/InstanceAction.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/updater/InstanceAction.java#L1) | retain | Keep enum-associated action handlers and absent reevaluation handler semantics. |
| [src/main/java/org/apache/aurora/scheduler/updater/InstanceActionHandler.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/updater/InstanceActionHandler.java#L1) | retain | Keep event-driven add/kill handling, SLA checks and reserve-after-kill ordering. |
| [src/main/java/org/apache/aurora/scheduler/updater/InstanceStateProvider.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/updater/InstanceStateProvider.java#L1) | retain | Keep the tiny instance state-provider interface; it expresses updater intent without implementation baggage. |
| [src/main/java/org/apache/aurora/scheduler/updater/InstanceUpdater.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/updater/InstanceUpdater.java#L1) | retain | Keep synchronized instance failure accounting, stability thresholds and replacement decisions. |
| [src/main/java/org/apache/aurora/scheduler/updater/JobDiff.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/updater/JobDiff.java#L114) | [defer](#file-156) | Use a pattern binding in JobDiff equality without changing collection aliasing or scoped-diff semantics. |
| [src/main/java/org/apache/aurora/scheduler/updater/JobUpdateController.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/updater/JobUpdateController.java#L69) | [defer](#file-157) | Use a pattern binding for AuditData equality while retaining message-length validation and hashing. |
| [src/main/java/org/apache/aurora/scheduler/updater/JobUpdateControllerImpl.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/updater/JobUpdateControllerImpl.java#L955) | [defer](#file-158) | Convert the private immutable PulseState tuple to a record while retaining synchronized pulse replacement. |
| [src/main/java/org/apache/aurora/scheduler/updater/JobUpdateEventSubscriber.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/updater/JobUpdateEventSubscriber.java#L1) | retain | Keep isolated per-event error counters and current recovery-failure handling until lifecycle policy changes. |
| [src/main/java/org/apache/aurora/scheduler/updater/JobUpdateStateMachine.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/updater/JobUpdateStateMachine.java#L1) | retain | Keep the explicit transition and inverse state maps; their contents define updater behavior. |
| [src/main/java/org/apache/aurora/scheduler/updater/OneWayJobUpdater.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/updater/OneWayJobUpdater.java#L294) | [change](#file-161) | Use an EvaluationResult<?> pattern to remove the unchecked generic cast in equality. |
| [src/main/java/org/apache/aurora/scheduler/updater/SideEffect.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/updater/SideEffect.java#L99) | [defer](#file-162) | Use a pattern binding in SideEffect equality while preserving the deliberate exclusion of failure. |
| [src/main/java/org/apache/aurora/scheduler/updater/SlaKillController.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/updater/SlaKillController.java#L331) | [defer](#file-163) | Use a switch expression for update-status action mapping while preserving unexpected-status failure behavior. |
| [src/main/java/org/apache/aurora/scheduler/updater/StateEvaluator.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/updater/StateEvaluator.java#L1) | retain | Keep enum result/action/failure mappings and stable failure reason text. |
| [src/main/java/org/apache/aurora/scheduler/updater/UpdateAgentReserver.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/updater/UpdateAgentReserver.java#L1) | retain | Keep cache-backed agent reservation and disabled implementation; expiry and inverse lookup are deliberate. |
| [src/main/java/org/apache/aurora/scheduler/updater/UpdateConfigurationException.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/updater/UpdateConfigurationException.java#L1) | retain | Keep the checked update-configuration exception and message constructor. |
| [src/main/java/org/apache/aurora/scheduler/updater/UpdateFactory.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/updater/UpdateFactory.java#L1) | retain | Keep update ordering, reversal and Thrift strategy validation; avoid replacing its Serializable Ordering mechanically. |
| [src/main/java/org/apache/aurora/scheduler/updater/UpdateInProgressException.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/updater/UpdateInProgressException.java#L1) | retain | Keep the update-in-progress exception subtype and attached persisted update summary. |
| [src/main/java/org/apache/aurora/scheduler/updater/UpdateStateException.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/updater/UpdateStateException.java#L1) | retain | Keep checked update-state exception constructors and cause propagation. |
| [src/main/java/org/apache/aurora/scheduler/updater/UpdaterModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/updater/UpdaterModule.java#L1) | retain | Coordinate updater timer ownership with lifecycle; preserve single-thread ordering and batch-worker bindings. |
| [src/main/java/org/apache/aurora/scheduler/updater/Updates.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/updater/Updates.java#L1) | retain | Keep immutable ranges, first matching configuration lookup and ordered last-update event semantics. |
| [src/main/java/org/apache/aurora/scheduler/updater/strategy/ActiveLimitedStrategy.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/updater/strategy/ActiveLimitedStrategy.java#L1) | retain | Keep max-active capacity calculation and sort-before-limit selection; changing the result set ordering is separate behavior. |
| [src/main/java/org/apache/aurora/scheduler/updater/strategy/BatchStrategy.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/updater/strategy/BatchStrategy.java#L1) | retain | Keep the batch barrier that admits idle instances only when active instances are empty. |
| [src/main/java/org/apache/aurora/scheduler/updater/strategy/QueueStrategy.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/updater/strategy/QueueStrategy.java#L1) | retain | Keep queue admission of idle instances under the inherited max-active limit. |
| [src/main/java/org/apache/aurora/scheduler/updater/strategy/UpdateStrategy.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/updater/strategy/UpdateStrategy.java#L1) | retain | Keep the update-strategy extension interface and set-based group contract. |
| [src/main/java/org/apache/aurora/scheduler/updater/strategy/VariableBatchStrategy.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/updater/strategy/VariableBatchStrategy.java#L1) | retain | Keep variable-batch rollback arithmetic and initial instance-count capture; collection changes must preserve batch selection. |

## Findings and required validation

<a id="file-5"></a>

### `src/main/java/org/apache/aurora/scheduler/cron/CrontabEntry.java`

**P3 · language · after-boundary**

Bind that in the existing negated instanceof guard and remove its immediately following cast.

Evidence: [line 278](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/cron/CrontabEntry.java#L278)

```java
if (!(o instanceof CrontabEntry))
```

Contract: Keep constructor range normalization, field comparisons and existing hash argument order.

Required validation: CrontabEntryTest

<a id="file-6"></a>

### `src/main/java/org/apache/aurora/scheduler/cron/SanitizedCronJob.java`

**P3 · jdk-api · local**

Assign crontabEntry using entry.orElseThrow(() -> new CronException("Invalid cron schedule: " + job.getCronSchedule())) and remove the duplicate presence branch.

Evidence: [line 60](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/cron/SanitizedCronJob.java#L60)

```java
this.crontabEntry = entry.get();
```

Contract: Preserve NO_CRON_SCHEDULE validation first, checked CronException type, and exact invalid-schedule message.

Required validation: SanitizedCronJob validation exercised by CronJobManagerImplTest and AuroraCronJobTest

<a id="file-7"></a>

### `src/main/java/org/apache/aurora/scheduler/cron/quartz/AuroraCronJob.java`

**P2 · concurrency · local**

Split these catches: restore the interrupt only for InterruptedException; retain the existing exception wrapping for ExecutionException.

Evidence: [line 242](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/cron/quartz/AuroraCronJob.java#L242)

```java
catch (ExecutionException | InterruptedException e)
```

Contract: Do not change batch completion waiting, schedule/launch order, or cancellation behavior.

Required validation: AuroraCronJobTest plus separate failed-future and interrupted-wait cases

<a id="file-9"></a>

### `src/main/java/org/apache/aurora/scheduler/cron/quartz/CronJobManagerImpl.java`

**P3 · collections · after-boundary**

Use scheduler.getTriggersOfJob(jobKey).stream().filter(CronTrigger.class::isInstance).map(CronTrigger.class::cast).findFirst().

Evidence: [line 198](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/cron/quartz/CronJobManagerImpl.java#L198)

```java
Optional<CronTrigger> trigger = FluentIterable.from(scheduler.getTriggersOfJob(jobKey))
```

Contract: Keep sequential first-match selection, no strict one-trigger assertion here, and existing SchedulerException handling.

Required validation: CronJobManagerImplTest

<a id="file-19"></a>

### `src/main/java/org/apache/aurora/scheduler/maintenance/MaintenanceController.java`

**P2 · jdk-api · after-boundary**

Inject the existing scheduler Clock and use clock.nowMillis for both creation and remaining-time calculations; migrate that clock centrally to java.time.Clock later.

Evidence: [line 313](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/maintenance/MaintenanceController.java#L313)

```java
.setCreatedTimestampMs(System.currentTimeMillis()))));
```

Contract: Keep persisted epoch milliseconds, timeoutSecs conversion, and strict remainingMs < 0 threshold.

Required validation: MaintenanceControllerImplTest with a controlled expiration clock

<a id="file-21"></a>

### `src/main/java/org/apache/aurora/scheduler/mesos/CommandLineDriverSettingsModule.java`

**P2 · resource-lifecycle · local**

Open the credentials InputStream in try-with-resources inside getCredentials and pass it to parseCredentials.

Evidence: [line 156](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/mesos/CommandLineDriverSettingsModule.java#L156)

```java
properties = parseCredentials(new FileInputStream(opts.frameworkAuthenticationFile));
```

Contract: The caller owns this stream; leave parseCredentials usable with caller-owned streams and preserve property encoding and required-key validation.

Required validation: CommandLineDriverSettingsModuleTest plus close-on-success/parse-failure checks

<a id="file-37"></a>

### `src/main/java/org/apache/aurora/scheduler/mesos/VersionedMesosSchedulerImpl.java`

**P2 · concurrency · local**

Restore Thread.currentThread().interrupt() before wrapping InterruptedException; apply to both latch waits in the driver service.

Evidence: [line 136](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/mesos/VersionedMesosSchedulerImpl.java#L136)

```java
} catch (InterruptedException e) {
```

Contract: Keep RuntimeException wrapping, no unconditional retry, and preserve serialized registration/callback order.

Required validation: VersionedMesosSchedulerImplTest interrupted wait/backoff scenario

<a id="file-38"></a>

### `src/main/java/org/apache/aurora/scheduler/mesos/VersionedSchedulerDriverService.java`

**P2 · concurrency · local**

Restore Thread.currentThread().interrupt() before wrapping InterruptedException; apply to both latch waits in the driver service.

Evidence: [line 244](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/mesos/VersionedSchedulerDriverService.java#L244)

```java
terminationLatch.await();
```

Contract: Keep RuntimeException wrapping, no unconditional retry, and preserve serialized registration/callback order.

Required validation: VersionedSchedulerDriverServiceTest interrupted wait/backoff scenario

<a id="file-40"></a>

### `src/main/java/org/apache/aurora/scheduler/offers/HostOffer.java`

**P3 · language · after-boundary**

Bind other in the existing negated instanceof guard and remove its immediately following cast.

Evidence: [line 93](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/offers/HostOffer.java#L93)

```java
if (!(o instanceof HostOffer))
```

Contract: Keep derived flag comparison and exclude memoized suppliers from equality.

Required validation: OfferManagerImplTest

<a id="file-46"></a>

### `src/main/java/org/apache/aurora/scheduler/offers/OfferOrderBuilder.java`

**P3 · language · after-boundary**

Return a switch expression from getOrdering with the current CPU/DISK/MEMORY/REVOCABLE_CPU mappings and unchanged default arbitrary comparator.

Evidence: [line 89](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/offers/OfferOrderBuilder.java#L89)

```java
switch(order) {
```

Contract: Retain Ordering.arbitrary identity tie breaking and the compound comparator order.

Required validation: OfferManagerImplTest offer ordering scenarios

<a id="file-54"></a>

### `src/main/java/org/apache/aurora/scheduler/preemptor/PreemptionProposal.java`

**P3 · language · after-boundary**

Bind other in the existing negated instanceof guard and remove its immediately following cast.

Evidence: [line 46](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/preemptor/PreemptionProposal.java#L46)

```java
if (!(o instanceof PreemptionProposal))
```

Contract: Keep set equality and existing Objects.hash cache key behavior.

Required validation: PreemptorImplTest and PendingTaskProcessorTest

<a id="file-55"></a>

### `src/main/java/org/apache/aurora/scheduler/preemptor/PreemptionVictim.java`

**P3 · language · after-boundary**

Bind other in the existing negated instanceof guard and remove its immediately following cast.

Evidence: [line 72](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/preemptor/PreemptionVictim.java#L72)

```java
if (!(o instanceof PreemptionVictim))
```

Contract: Keep task-wrapper equality and Objects.hashCode(task).

Required validation: PreemptionVictimFilterTest

<a id="file-62"></a>

### `src/main/java/org/apache/aurora/scheduler/quota/QuotaInfo.java`

**P3 · language · after-boundary**

Bind other in the existing negated instanceof guard and remove its immediately following cast.

Evidence: [line 95](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/quota/QuotaInfo.java#L95)

```java
if (!(o instanceof QuotaInfo))
```

Contract: Compare all five fields and preserve existing hash ordering.

Required validation: QuotaManagerImplTest

<a id="file-67"></a>

### `src/main/java/org/apache/aurora/scheduler/reconciliation/TaskReconciler.java`

**P2 · resource-lifecycle · framework**

Track periodic reconciliation and delayed batch ScheduledFutures, cancel owned work on service stop, and coordinate shutdown of the dedicated BackgroundWorker executor.

Evidence: [line 174](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/reconciliation/TaskReconciler.java#L174)

```java
protected void shutDown() {
```

Contract: Specify active-to-stopped behavior and pending-batch cancellation before implementation; never close shared AsyncExecutor or wait while holding storage locks.

Required validation: TaskReconcilerTest plus stop-with-pending-batches/restart lifecycle scenarios

<a id="file-72"></a>

### `src/main/java/org/apache/aurora/scheduler/resources/ResourceBag.java`

**P3 · language · after-boundary**

Bind other in the existing negated instanceof guard and remove its immediately following cast.

Evidence: [line 214](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/resources/ResourceBag.java#L214)

```java
if (!(o instanceof ResourceBag))
```

Contract: Do not normalize missing vectors to zero or change Objects.hash(resourceVectors).

Required validation: ResourceBagTest

<a id="file-77"></a>

### `src/main/java/org/apache/aurora/scheduler/scheduling/RescheduleCalculator.java`

**P3 · jdk-api · after-boundary**

Use task.getTaskEvents().reversed() for the read-only reverse view; preserve the existing search for the most recent active event.

Evidence: [line 92](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/scheduling/RescheduleCalculator.java#L92)

```java
List<ITaskEvent> events = Lists.reverse(task.getTaskEvents());
```

Contract: Do not copy/reorder stored events or change no-active-event exceptions; the empty guard must remain.

Required validation: RescheduleCalculatorImplTest

<a id="file-80"></a>

### `src/main/java/org/apache/aurora/scheduler/scheduling/TaskAssignerImpl.java`

**P3 · language · after-boundary**

Use a private record SchedulingMatch(IAssignedTask task, HostOffer offer) with the same non-null compact constructor checks and update private accesses.

Evidence: [line 205](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/scheduling/TaskAssignerImpl.java#L205)

```java
private static class SchedulingMatch {
```

Contract: This type only holds map values traversed by maybeAssign; retain references and map iteration order, and do not alter ReservationStatus behavior.

Required validation: TaskAssignerImplTest

<a id="file-83"></a>

### `src/main/java/org/apache/aurora/scheduler/scheduling/TaskGroups.java`

**P2 · concurrency · local**

Split these catches: restore the interrupt only for InterruptedException; retain the existing exception wrapping for ExecutionException.

Evidence: [line 179](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/scheduling/TaskGroups.java#L179)

```java
catch (ExecutionException | InterruptedException e)
```

Contract: Do not change batch completion waiting, schedule/launch order, or cancellation behavior.

Required validation: TaskGroupsTest plus separate failed-future and interrupted-wait cases

<a id="file-88"></a>

### `src/main/java/org/apache/aurora/scheduler/sla/SlaAlgorithm.java`

**P3 · language · after-boundary**

Bind other in the existing negated instanceof guard and remove its immediately following cast.

Evidence: [line 257](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/sla/SlaAlgorithm.java#L257)

```java
if (!(o instanceof InstanceId))
```

Contract: Keep job-key/instance equality and existing hash; no changes to interval calculations.

Required validation: SlaAlgorithmTest

<a id="file-90"></a>

### `src/main/java/org/apache/aurora/scheduler/sla/SlaManager.java`

**P2 · jdk-api · after-boundary**

Inject the scheduler Clock for elapsed-running calculations and route wall-time reads through it; migrate the common clock to java.time.Clock as a coordinated follow-up.

Evidence: [line 229](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/sla/SlaManager.java#L229)

```java
.map(t -> System.currentTimeMillis() - t > slaDuration.as(Time.MILLISECONDS))
```

Contract: Keep strict greater-than threshold, epoch-millisecond wire events, and reading time per evaluated task.

Required validation: SlaManagerTest controlled-clock duration-boundary cases

<a id="file-96"></a>

### `src/main/java/org/apache/aurora/scheduler/state/SideEffect.java`

**P3 · language · after-boundary**

Bind other in the existing negated instanceof guard and remove its immediately following cast.

Evidence: [line 40](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/state/SideEffect.java#L40)

```java
if (!(o instanceof SideEffect))
```

Contract: Preserve nextState comparison, null acceptance and string formatting.

Required validation: TaskStateMachineTest

<a id="file-101"></a>

### `src/main/java/org/apache/aurora/scheduler/state/TaskStateMachine.java`

**P3 · language · after-boundary**

Use an arrow switch with grouped ASSIGNED/STARTING/RUNNING and FINISHED/FAILED/KILLED labels in manageRestartingTask.

Evidence: [line 193](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/state/TaskStateMachine.java#L193)

```java
final Consumer<Transition<TaskState>> manageRestartingTask =
```

Contract: Preserve LOST kill-plus-reschedule and PARTITIONED transition-to-lost behavior; leave graph and callback ordering unchanged.

Required validation: TaskStateMachineTest

<a id="file-102"></a>

### `src/main/java/org/apache/aurora/scheduler/state/TransitionResult.java`

**P3 · language · after-boundary**

Bind other in the existing negated instanceof guard and remove its immediately following cast.

Evidence: [line 60](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/state/TransitionResult.java#L60)

```java
if (!(o instanceof TransitionResult))
```

Contract: Keep nonempty-side-effects validation and the existing hash formula.

Required validation: TaskStateMachineTest

<a id="file-104"></a>

### `src/main/java/org/apache/aurora/scheduler/storage/AttributeStore.java`

**P3 · jdk-api · local**

Return store.getHostAttributes(host).map(attributes -> IHostAttributes.build(attributes.newBuilder().setMode(mode))) from mergeMode.

Evidence: [line 108](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/AttributeStore.java#L108)

```java
Optional<IHostAttributes> stored = store.getHostAttributes(host);
```

Contract: An absent host must remain absent; retain newBuilder copying and do not create host attributes implicitly.

Required validation: AbstractAttributeStoreTest and MaintenanceControllerImplTest

<a id="file-118"></a>

### `src/main/java/org/apache/aurora/scheduler/storage/backup/Recovery.java`

**P2 · resource-lifecycle · local**

Declare the buffered file InputStream in try-with-resources around snapshot.read and build TIOStreamTransport from that stream.

Evidence: [line 211](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/backup/Recovery.java#L211)

```java
new TIOStreamTransport(new BufferedInputStream(new FileInputStream(backupFile))));
```

Contract: Keep TBinaryProtocol and distinct decode/read RecoveryException wrapping; preserve the original exception with close failure suppressed.

Required validation: storage.backup.RecoveryTest valid/corrupt backups and close-on-decode-failure scenario

<a id="file-119"></a>

### `src/main/java/org/apache/aurora/scheduler/storage/backup/StorageBackup.java`

**P2 · jdk-api · local**

Use immutable DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm", Locale.ENGLISH) with the default zone captured at construction and format Instant.ofEpochMilli(clock.nowMillis()).

Evidence: [line 139](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/backup/StorageBackup.java#L139)

```java
backupDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm", Locale.ENGLISH);
```

Contract: Preserve FILE_PREFIX, minute granularity, captured default timezone and overwrite/retention behavior; characterize legacy calendar behavior if dates before the Gregorian cutover are supported.

Required validation: StorageBackupTest naming/retention tests plus concurrent createBackupName calls and timezone case

<a id="file-121"></a>

### `src/main/java/org/apache/aurora/scheduler/storage/durability/DurableStorage.java`

**P2 · resource-lifecycle · local**

Bind persistence.recover() to a try-with-resources Stream<Edit> and pass that owned stream to Loader.load.

Evidence: [line 178](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/durability/DurableStorage.java#L178)

```java
Loader.load(stores, thriftBackfill, persistence.recover());
```

Contract: Keep sequential replay under the existing write lock, recovery failure wrapping and persistence-before-success transaction semantics.

Required validation: DurableStorageTest plus onClose hook for successful and failed replay

<a id="file-124"></a>

### `src/main/java/org/apache/aurora/scheduler/storage/durability/Persistence.java`

**P3 · language · after-boundary**

Bind other in the existing negated instanceof guard and remove its immediately following cast.

Evidence: [line 81](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/durability/Persistence.java#L81)

```java
if (!(obj instanceof Edit))
```

Contract: Keep null op as delete-all sentinel and Objects.hashCode(op).

Required validation: DurableStorageTest and RecoveryTest

<a id="file-125"></a>

### `src/main/java/org/apache/aurora/scheduler/storage/durability/Recovery.java`

**P2 · resource-lifecycle · local**

Bind the source recovery stream in try-with-resources in copy and retain the current filter/forEach batch loop.

Evidence: [line 70](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/durability/Recovery.java#L70)

```java
from.recover()
```

Contract: Keep destination emptiness check, leading-reset suppression, batch boundaries and original exceptions; source close must also run when replay fails.

Required validation: storage.durability.RecoveryTest with close callbacks on successful and failing copy

<a id="file-128"></a>

### `src/main/java/org/apache/aurora/scheduler/storage/durability/TransactionRecorder.java`

**P3 · jdk-api · local**

Use ops.isEmpty() ? null : ops.getLast() for the prior operation.

Evidence: [line 37](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/durability/TransactionRecorder.java#L37)

```java
Op prior = Iterables.getLast(ops, null);
```

Contract: Keep mutable adjacent-operation coalescing and the null-on-empty behavior exactly.

Required validation: TransactionRecorderTest

<a id="file-133"></a>

### `src/main/java/org/apache/aurora/scheduler/storage/log/LogPersistence.java`

**P3 · language · after-boundary**

Return a switch expression from the flatMap lambda using a block/yield for snapshot handling and the existing default exception.

Evidence: [line 93](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/log/LogPersistence.java#L93)

```java
switch (entry.getSetField()) {
```

Contract: Keep leading deleteAll before snapshot operations, sequential transaction order and lazy exception timing.

Required validation: LogPersistenceTest

<a id="file-143"></a>

### `src/main/java/org/apache/aurora/scheduler/storage/mem/MemAttributeStore.java`

**P3 · collections · local**

Replace the FluentIterable validation with attributes.getAttributes().stream().allMatch(a -> !a.getValues().isEmpty()).

Evidence: [line 56](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/mem/MemAttributeStore.java#L56)

```java
FluentIterable.from(attributes.getAttributes()).allMatch(a -> !a.getValues().isEmpty()));
```

Contract: Preserve short-circuiting, null behavior and the mode assertion before storing attributes.

Required validation: MemAttributeStoreTest

<a id="file-146"></a>

### `src/main/java/org/apache/aurora/scheduler/storage/mem/MemJobUpdateStore.java`

**P3 · jdk-api · after-boundary**

Use explicit isEmpty guards plus List.getFirst/getLast for update events and getLast for instance events in synthesizeUpdateState.

Evidence: [line 171](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/mem/MemJobUpdateStore.java#L171)

```java
JobUpdateEvent firstEvent = Iterables.getFirst(update.getUpdateEvents(), null);
```

Contract: Keep created/status defaults for empty lists, stable timestamp sorting and max-of-last timestamps.

Required validation: MemJobUpdateStoreTest

<a id="file-148"></a>

### `src/main/java/org/apache/aurora/scheduler/storage/mem/MemSchedulerStore.java`

**P3 · jdk-api · local**

Construct new AtomicReference<>() directly and remove the Guava Atomics import.

Evidence: [line 27](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/mem/MemSchedulerStore.java#L27)

```java
private final AtomicReference<String> frameworkId = Atomics.newReference();
```

Contract: Keep nullable initial ID and atomic publication; do not use a nonvolatile plain field.

Required validation: MemSchedulerStoreTest

<a id="file-151"></a>

### `src/main/java/org/apache/aurora/scheduler/storage/mem/MemTaskStore.java`

**P3 · language · after-boundary**

Bind other in the existing negated instanceof guard and remove its immediately following cast.

Evidence: [line 282](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/storage/mem/MemTaskStore.java#L282)

```java
if (!(o instanceof Task))
```

Contract: Keep storedTask.equals/hashCode and all interner/index behavior.

Required validation: MemTaskStoreTest

<a id="file-156"></a>

### `src/main/java/org/apache/aurora/scheduler/updater/JobDiff.java`

**P3 · language · after-boundary**

Bind other in the existing negated instanceof guard and remove its immediately following cast.

Evidence: [line 114](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/updater/JobDiff.java#L114)

```java
if (!(o instanceof JobDiff))
```

Contract: Keep current field/getter comparisons and collection aliasing; do not introduce copies as part of this edit.

Required validation: JobDiffTest

<a id="file-157"></a>

### `src/main/java/org/apache/aurora/scheduler/updater/JobUpdateController.java`

**P3 · language · after-boundary**

Bind other in the existing negated instanceof guard and remove its immediately following cast.

Evidence: [line 69](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/updater/JobUpdateController.java#L69)

```java
if (!(obj instanceof AuditData))
```

Contract: Keep the 1024-character message constraint and Objects.hash(user,message).

Required validation: JobUpdaterIT

<a id="file-158"></a>

### `src/main/java/org/apache/aurora/scheduler/updater/JobUpdateControllerImpl.java`

**P3 · language · after-boundary**

Convert private PulseState to a record with status, pulseTimeoutMs and lastPulseMs, retaining the status non-null check and isBlocked method.

Evidence: [line 955](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/updater/JobUpdateControllerImpl.java#L955)

```java
private static class PulseState {
```

Contract: Keep synchronized PulseHandler methods and put returning the previous PulseState; do not switch to in-place mutation or alter timeout >= behavior.

Required validation: JobUpdaterIT coordinated pulse/pause/recovery cases

<a id="file-161"></a>

### `src/main/java/org/apache/aurora/scheduler/updater/OneWayJobUpdater.java`

**P3 · language · local**

Use if (!(obj instanceof EvaluationResult<?> other)) return false and delete the unchecked cast/suppression.

Evidence: [line 294](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/updater/OneWayJobUpdater.java#L294)

```java
EvaluationResult<K> other = (EvaluationResult<K>) obj;
```

Contract: Keep equality across differing erased generic parameters, existing side-effect equality and hash formula.

Required validation: OneWayJobUpdaterTest

<a id="file-162"></a>

### `src/main/java/org/apache/aurora/scheduler/updater/SideEffect.java`

**P3 · language · after-boundary**

Bind other in the existing negated instanceof guard and remove its immediately following cast.

Evidence: [line 99](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/updater/SideEffect.java#L99)

```java
if (!(o instanceof SideEffect))
```

Contract: Failure is deliberately excluded from equals/hashCode; do not use generated record equality.

Required validation: OneWayJobUpdaterTest

<a id="file-163"></a>

### `src/main/java/org/apache/aurora/scheduler/updater/SlaKillController.java`

**P3 · language · after-boundary**

Use a switch expression mapping ROLLING_BACK and ROLLING_FORWARD to their current JobUpdateAction values.

Evidence: [line 331](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/updater/SlaKillController.java#L331)

```java
switch (status) {
```

Contract: Retain the same RuntimeException and message for every other status.

Required validation: SlaKillControllerTest
