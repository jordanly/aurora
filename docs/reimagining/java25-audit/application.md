# Application, API and security

[Audit overview and ordering](../JAVA25_FILE_AUDIT.md).

151 files: 40 local-change candidates, 7 deferred, 104 retained.

Source baseline: `6cf7f0ea07c6355af62ceafb029fe086f55b8c65`. Findings describe proposed work; validation is not yet executed.

Retain means no separate improvement prioritized in this pass. A local candidate still needs
the stated contract checks. Framework findings are grouped migrations, not separate PRs per file.

| Source file | Assessment | Recommendation or retain rationale |
| --- | --- | --- |
| [src/main/java/org/apache/aurora/GuavaUtils.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/GuavaUtils.java#L63) | [change](#file-1) | Delegate the duplicate list collector to ImmutableList.toImmutableList; review set/map collector characteristics separately before replacing them. |
| [src/main/java/org/apache/aurora/GuiceUtils.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/GuiceUtils.java#L1) | retain | Keep the Guice matcher and try/finally context-classloader restoration; changing the JNI boundary belongs with the execution adapter, not a syntax rewrite. |
| [src/main/java/org/apache/aurora/Protobufs.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/Protobufs.java#L1) | retain | Keep the small protobuf debug-format adapter; java.lang formatting does not replace protobuf-aware rendering. |
| [src/main/java/org/apache/aurora/codec/ThriftBinaryCodec.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/codec/ThriftBinaryCodec.java#L157) | [change](#file-4) | Give the explicitly constructed Deflater a Java 25 try-with-resources lifetime, and close the transport/compression layer before reading compressed bytes. |
| [src/main/java/org/apache/aurora/scheduler/AppStartup.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/AppStartup.java#L1) | retain | Keep the runtime Guice qualifier and its injection targets; it is metadata rather than a data class. |
| [src/main/java/org/apache/aurora/scheduler/BatchWorker.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/BatchWorker.java#L141) | [change](#file-6) | Make the retry executor an explicitly owned service resource with shutdown and a defined outcome for queued/retrying futures. |
| [src/main/java/org/apache/aurora/scheduler/SchedulerLifecycle.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/SchedulerLifecycle.java#L1) | retain | Keep explicit lifecycle transitions and delayed actions; typed durations and executor ownership must be coordinated with leadership, timeout and driver-join behavior. |
| [src/main/java/org/apache/aurora/scheduler/SchedulerModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/SchedulerModule.java#L1) | retain | Keep qualifier-specific queue and active-service wiring; changing executor or queue types changes scheduler behavior and belongs in the owning service review. |
| [src/main/java/org/apache/aurora/scheduler/SchedulerServicesModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/SchedulerServicesModule.java#L1) | retain | Keep explicit direct listener execution and service multibindings; these preserve the original lifecycle ordering under Guice 6. |
| [src/main/java/org/apache/aurora/scheduler/TaskIdGenerator.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/TaskIdGenerator.java#L50) | [change](#file-10) | Consider simple concatenation for the task ID and remove the unused Clock dependency only after reviewing constructor callers. |
| [src/main/java/org/apache/aurora/scheduler/TaskStatusHandler.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/TaskStatusHandler.java#L1) | retain | Keep the status callback contract until the Mesos-neutral observation interface is extracted. |
| [src/main/java/org/apache/aurora/scheduler/TaskStatusHandlerImpl.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/TaskStatusHandlerImpl.java#L192) | [defer](#file-12) | Express reason-to-message selection with a switch expression and Optional.or while retaining the special unregistered-executor suppression. |
| [src/main/java/org/apache/aurora/scheduler/TaskVars.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/TaskVars.java#L165) | [change](#file-13) | Use Optional.ifPresent for the rack counter update where the optional is read once and only the present branch has work. |
| [src/main/java/org/apache/aurora/scheduler/TierInfo.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/TierInfo.java#L92) | [change](#file-14) | Use an instanceof binding in equals; separately consider a record after JSON tooling is modernized. |
| [src/main/java/org/apache/aurora/scheduler/TierManager.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/TierManager.java#L1) | retain | Keep TierConfig constructor validation, defensive map copy and explicit default JSON property; a record conversion needs the serializer/configuration migration. |
| [src/main/java/org/apache/aurora/scheduler/TierModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/TierModule.java#L1) | retain | Keep explicit file/resource loading and malformed-tier diagnostics; qualify modern Jackson together with TierConfig rather than changing the DTO alone. |
| [src/main/java/org/apache/aurora/scheduler/app/AppModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/app/AppModule.java#L1) | retain | Keep original filter, clock, storage and JNI bindings; component changes should be made at their owning boundary, not by making the module a value type. |
| [src/main/java/org/apache/aurora/scheduler/app/LifecycleModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/app/LifecycleModule.java#L1) | retain | Keep shutdown-registry integration with the Guava service lifecycle; lexical resource scopes do not model the whole application lifetime. |
| [src/main/java/org/apache/aurora/scheduler/app/MoreModules.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/app/MoreModules.java#L55) | [change](#file-19) | Replace Class.newInstance with explicit constructor lookup/invocation and use Class<? extends Module> through the dynamic-module API. |
| [src/main/java/org/apache/aurora/scheduler/app/SchedulerMain.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/app/SchedulerMain.java#L1) | retain | Keep the public main entrypoint and explicit service start, leadership and shutdown phases; compact source files and instance main add no benefit here. |
| [src/main/java/org/apache/aurora/scheduler/app/ServiceGroupMonitor.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/app/ServiceGroupMonitor.java#L1) | retain | Keep the narrow Supplier/Closeable contract and checked start failure; monitor lifetime spans server requests, not a method-local resource scope. |
| [src/main/java/org/apache/aurora/scheduler/app/VolumeConverter.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/app/VolumeConverter.java#L46) | [change](#file-22) | Normalize the volume mode using Locale.ROOT. |
| [src/main/java/org/apache/aurora/scheduler/async/AsyncModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/async/AsyncModule.java#L1) | retain | Keep the executor wrapper that defers callbacks until the storage transaction boundary; generic virtual-thread replacement would lose this contract. |
| [src/main/java/org/apache/aurora/scheduler/base/AsyncUtil.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/base/AsyncUtil.java#L150) | [change](#file-24) | Use instanceof Future<?> future to remove the raw type and cast in afterExecute inspection. |
| [src/main/java/org/apache/aurora/scheduler/base/Conversions.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/base/Conversions.java#L1) | retain | Keep the explicit Mesos state/attribute/maintenance translation until its neutral adapter is extracted; generic enum-name conversion would lose mappings. |
| [src/main/java/org/apache/aurora/scheduler/base/InstanceKeys.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/base/InstanceKeys.java#L1) | retain | Keep the small canonical identity helpers and their existing delimiter semantics; no material Java 25 language change is needed. |
| [src/main/java/org/apache/aurora/scheduler/base/JobKeys.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/base/JobKeys.java#L1) | retain | Keep identifier validation, parse diagnostics and query-scope detection together; records and string splitting would not replace these contracts. |
| [src/main/java/org/apache/aurora/scheduler/base/Jobs.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/base/Jobs.java#L1) | retain | Keep the explicit active/finished/failed/pending counts and Guava multiset; a stream rewrite would not simplify the domain calculation. |
| [src/main/java/org/apache/aurora/scheduler/base/Numbers.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/base/Numbers.java#L1) | retain | Keep Guava ranges and discrete-domain conversion; Java 25 has no drop-in range-set replacement with the same open/closed and overflow behavior. |
| [src/main/java/org/apache/aurora/scheduler/base/Query.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/base/Query.java#L153) | [change](#file-30) | Bind Builder in the instanceof expression used by equals. |
| [src/main/java/org/apache/aurora/scheduler/base/SchedulerException.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/base/SchedulerException.java#L1) | retain | Keep the domain exception and cause-preserving constructors; changing exception hierarchy is unrelated to Java 25 idioms. |
| [src/main/java/org/apache/aurora/scheduler/base/TaskGroupKey.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/base/TaskGroupKey.java#L59) | [change](#file-32) | Use an instanceof binding in TaskGroupKey.equals. |
| [src/main/java/org/apache/aurora/scheduler/base/Tasks.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/base/Tasks.java#L1) | retain | Keep the shared state sets, query helpers and deterministic latest-task/event ordering; preserve Guava collection contracts until callers are migrated together. |
| [src/main/java/org/apache/aurora/scheduler/base/UserProvidedStrings.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/base/UserProvidedStrings.java#L1) | retain | Keep the compiled identifier pattern and explicit nullable input guard; isBlank would change the supported identifier language. |
| [src/main/java/org/apache/aurora/scheduler/config/CliOptions.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/config/CliOptions.java#L1) | retain | Keep mutable JCommander-populated options; introduce immutable runtime configuration only after parsing rather than converting these fields directly to records. |
| [src/main/java/org/apache/aurora/scheduler/config/CommandLine.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/config/CommandLine.java#L187) | [defer](#file-36) | Replace reflective registration of option fields with an explicit typed registry once custom-option extension points are inventoried. |
| [src/main/java/org/apache/aurora/scheduler/config/converters/ClassConverter.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/config/converters/ClassConverter.java#L1) | retain | Keep aliases, Class.forName and ParameterException translation; narrowing to a particular extension type belongs at each typed caller. |
| [src/main/java/org/apache/aurora/scheduler/config/converters/DataAmountConverter.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/config/converters/DataAmountConverter.java#L47) | [change](#file-38) | Use Optional.map/orElseThrow to construct DataAmount and centralize the missing-unit error. |
| [src/main/java/org/apache/aurora/scheduler/config/converters/DockerParameterConverter.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/config/converters/DockerParameterConverter.java#L1) | retain | Keep the first-equals split and empty-name/value rejection; String.split would alter values containing equals signs. |
| [src/main/java/org/apache/aurora/scheduler/config/converters/InetSocketAddressConverter.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/config/converters/InetSocketAddressConverter.java#L1) | retain | Keep delegation to the shared address parser so IPv6, unresolved hosts and port validation remain consistent. |
| [src/main/java/org/apache/aurora/scheduler/config/converters/TimeAmountConverter.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/config/converters/TimeAmountConverter.java#L47) | [change](#file-41) | Use Optional.map/orElseThrow to construct TimeAmount and centralize the missing-unit error. |
| [src/main/java/org/apache/aurora/scheduler/config/splitters/CommaSplitter.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/config/splitters/CommaSplitter.java#L1) | retain | Keep the explicit empty and quoted-empty handling; Arrays.asList returns a fixed-size, element-replaceable list unlike List.of. |
| [src/main/java/org/apache/aurora/scheduler/config/types/DataAmount.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/config/types/DataAmount.java#L1) | retain | Keep the typed Amount subclass and its int truncation/overflow limit; a record cannot extend Amount and a unit-type migration requires caller tests. |
| [src/main/java/org/apache/aurora/scheduler/config/types/TimeAmount.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/config/types/TimeAmount.java#L1) | retain | Keep duration-unit conversion and long scaling behavior until the shared clock/quantity boundary is migrated to Duration. |
| [src/main/java/org/apache/aurora/scheduler/config/validators/NotEmptyIterable.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/config/validators/NotEmptyIterable.java#L1) | retain | Keep validation against Iterable rather than assuming Collection; checking emptiness must not traverse or materialize an arbitrary iterable. |
| [src/main/java/org/apache/aurora/scheduler/config/validators/NotEmptyString.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/config/validators/NotEmptyString.java#L1) | retain | Keep isEmpty because this validator rejects empty values, not all whitespace; replacing it with isBlank changes accepted configuration. |
| [src/main/java/org/apache/aurora/scheduler/config/validators/NotNegativeAmount.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/config/validators/NotNegativeAmount.java#L1) | retain | Keep the current longValue-based validation of non-negative Amount values; fractional and overflow policy needs a deliberate contract decision, not a syntax rewrite. |
| [src/main/java/org/apache/aurora/scheduler/config/validators/NotNegativeNumber.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/config/validators/NotNegativeNumber.java#L1) | retain | Keep the current longValue-based validation of non-negative Number values; fractional and overflow policy needs a deliberate contract decision, not a syntax rewrite. |
| [src/main/java/org/apache/aurora/scheduler/config/validators/PositiveAmount.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/config/validators/PositiveAmount.java#L1) | retain | Keep the current longValue-based validation of positive Amount values; fractional and overflow policy needs a deliberate contract decision, not a syntax rewrite. |
| [src/main/java/org/apache/aurora/scheduler/config/validators/PositiveNumber.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/config/validators/PositiveNumber.java#L1) | retain | Keep the current longValue-based validation of positive Number values; fractional and overflow policy needs a deliberate contract decision, not a syntax rewrite. |
| [src/main/java/org/apache/aurora/scheduler/config/validators/ReadableFile.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/config/validators/ReadableFile.java#L1) | retain | Keep the configured File validation while the CLI is File-based; moving to Path must preserve missing/unreadable-file diagnostics. |
| [src/main/java/org/apache/aurora/scheduler/configuration/ConfigurationManager.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/configuration/ConfigurationManager.java#L1) | retain | Keep ordered validation/default population and the original task configuration model; Optional/record changes must preserve which validation error wins. |
| [src/main/java/org/apache/aurora/scheduler/configuration/SanitizedConfiguration.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/configuration/SanitizedConfiguration.java#L82) | [change](#file-53) | Use an instanceof binding for equality and the JDK Objects.equals helper. |
| [src/main/java/org/apache/aurora/scheduler/configuration/executor/ExecutorConfig.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/configuration/executor/ExecutorConfig.java#L58) | [defer](#file-54) | Use an instanceof binding in executor configuration equality. |
| [src/main/java/org/apache/aurora/scheduler/configuration/executor/ExecutorModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/configuration/executor/ExecutorModule.java#L1) | retain | Keep option-to-Mesos executor construction until the execution adapter is extracted; new records must not replace its resolved-config wire contract. |
| [src/main/java/org/apache/aurora/scheduler/configuration/executor/ExecutorSettings.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/configuration/executor/ExecutorSettings.java#L1) | retain | Keep executor lookup, overhead and discovery policy together while the backend boundary changes; immutable snapshots require explicit collection-ownership review. |
| [src/main/java/org/apache/aurora/scheduler/configuration/executor/ExecutorSettingsLoader.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/configuration/executor/ExecutorSettingsLoader.java#L1) | retain | Keep schema/default validation and caller-owned Readable handling; close only streams opened here and modernize JSON mapping with its schema DTOs. |
| [src/main/java/org/apache/aurora/scheduler/configuration/executor/Executors.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/configuration/executor/Executors.java#L1) | retain | Keep the single placeholder Mesos executor ID constant until adapter extraction removes its usage. |
| [src/main/java/org/apache/aurora/scheduler/discovery/CuratorServiceDiscoveryModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/discovery/CuratorServiceDiscoveryModule.java#L1) | retain | Keep Curator session, ACL, retry and service bindings as a coordinated dependency boundary; JDK executors are not substitutes for ZooKeeper semantics. |
| [src/main/java/org/apache/aurora/scheduler/discovery/CuratorServiceGroupMonitor.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/discovery/CuratorServiceGroupMonitor.java#L97) | [change](#file-60) | Use Optional.stream when flattening successfully decoded child data if it makes the get() pipeline clearer. |
| [src/main/java/org/apache/aurora/scheduler/discovery/CuratorSingletonService.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/discovery/CuratorSingletonService.java#L1) | retain | Keep leadership callbacks, advertiser and closer ownership; compare session-loss and relinquish ordering before changing lock or resource structure. |
| [src/main/java/org/apache/aurora/scheduler/discovery/Encoding.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/discovery/Encoding.java#L41) | [change](#file-62) | Use StandardCharsets.UTF_8 for discovery payload encoding and decoding. |
| [src/main/java/org/apache/aurora/scheduler/discovery/FlaggedZooKeeperConfig.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/discovery/FlaggedZooKeeperConfig.java#L1) | retain | Keep CLI-to-session configuration conversion and explicit optional digest credentials; validate changes with existing ACL and chroot fixtures. |
| [src/main/java/org/apache/aurora/scheduler/discovery/ServiceDiscoveryBindings.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/discovery/ServiceDiscoveryBindings.java#L1) | retain | Keep runtime qualifier annotations and generic Guice Keys; they encode injection identity and are not obsolete functional wrappers. |
| [src/main/java/org/apache/aurora/scheduler/discovery/ServiceDiscoveryModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/discovery/ServiceDiscoveryModule.java#L148) | [change](#file-65) | Handle InterruptedException separately and define cancellation propagation when starting the embedded server. |
| [src/main/java/org/apache/aurora/scheduler/discovery/ServiceInstance.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/discovery/ServiceInstance.java#L60) | [change](#file-66) | Use pattern-bound equality locally; postpone record conversion of ServiceInstance/Endpoint until Gson migration. |
| [src/main/java/org/apache/aurora/scheduler/discovery/ZooKeeperConfig.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/discovery/ZooKeeperConfig.java#L61) | [defer](#file-67) | Adopt Duration for connection/session timeout values at a future typed configuration boundary. |
| [src/main/java/org/apache/aurora/scheduler/events/EventSink.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/events/EventSink.java#L1) | retain | Keep the named event-posting boundary; Consumer alone would obscure its domain role and does not provide delivery/transaction guarantees. |
| [src/main/java/org/apache/aurora/scheduler/events/NotifyingSchedulingFilter.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/events/NotifyingSchedulingFilter.java#L1) | retain | Keep the delegate filter and veto notification order; parallel streams or async event dispatch would change observability of placement decisions. |
| [src/main/java/org/apache/aurora/scheduler/events/PubsubEvent.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/events/PubsubEvent.java#L60) | [change](#file-70) | Use pattern bindings in event equality methods; keep the explicit TaskStateChange JSON envelope. |
| [src/main/java/org/apache/aurora/scheduler/events/PubsubEventModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/events/PubsubEventModule.java#L1) | retain | Keep subscriber registration, exception reporting and dispatch ordering; replacing EventBus is a typed-event architecture change with transaction implications. |
| [src/main/java/org/apache/aurora/scheduler/events/Webhook.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/events/Webhook.java#L73) | [change](#file-72) | Use optional.map(statuses -> statuses.contains(status)).orElse(true) for the whitelist predicate. |
| [src/main/java/org/apache/aurora/scheduler/events/WebhookInfo.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/events/WebhookInfo.java#L83) | [change](#file-73) | Replace repeated Optional.ofNullable checks with statuses == null &#124;&#124; statuses.contains("*") and collect statuses directly into the intended immutable collection. |
| [src/main/java/org/apache/aurora/scheduler/events/WebhookModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/events/WebhookModule.java#L1) | retain | Keep optional enablement and lifecycle ownership of the single async HTTP client; virtual-thread or HTTP-client changes need request cancellation and shutdown tests. |
| [src/main/java/org/apache/aurora/scheduler/filter/AttributeAggregate.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/filter/AttributeAggregate.java#L160) | [change](#file-75) | Use a pattern binding in equality without changing the lazily initialized multiset. |
| [src/main/java/org/apache/aurora/scheduler/filter/AttributeFilter.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/filter/AttributeFilter.java#L1) | retain | Keep explicit attribute cardinality/set matching; Java collections do not replace Guava multiset semantics. |
| [src/main/java/org/apache/aurora/scheduler/filter/ConstraintMatcher.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/filter/ConstraintMatcher.java#L1) | retain | Keep checked absence and constraint-specific veto messages; simplify optional branches only after preserving missing-attribute behavior. |
| [src/main/java/org/apache/aurora/scheduler/filter/SchedulingFilter.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/filter/SchedulingFilter.java#L229) | [change](#file-78) | Use pattern bindings in value-holder equality; consider internal records only after resource/request DTO boundaries stabilize. |
| [src/main/java/org/apache/aurora/scheduler/filter/SchedulingFilterImpl.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/filter/SchedulingFilterImpl.java#L1) | retain | Keep fail-fast veto precedence and constraint ordering; consolidating optional branches or parallelizing predicates risks changing the returned reason. |
| [src/main/java/org/apache/aurora/scheduler/http/AbortCallback.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/AbortCallback.java#L1) | retain | Keep explicit Runtime.halt semantics for the administrative abort hook; graceful shutdown would change the endpoint contract. |
| [src/main/java/org/apache/aurora/scheduler/http/AbstractFilter.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/AbstractFilter.java#L1) | retain | Keep servlet request/response narrowing and lifecycle hooks until the coordinated Servlet/Jakarta upgrade; pattern tests would need a defined non-HTTP failure contract. |
| [src/main/java/org/apache/aurora/scheduler/http/Agents.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/Agents.java#L1) | retain | Keep the template-facing Agent bean getters and attribute formatter until the template layer changes; records would not preserve bean introspection automatically. |
| [src/main/java/org/apache/aurora/scheduler/http/CorsFilter.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/CorsFilter.java#L1) | retain | Keep header names and filter chain order; header policy changes are HTTP/security behavior rather than Java language modernization. |
| [src/main/java/org/apache/aurora/scheduler/http/Cron.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/Cron.java#L1) | retain | Keep the simple scheduled-job response projection and canonical job keys; the loop is already clear and preserves immutable output. |
| [src/main/java/org/apache/aurora/scheduler/http/HttpService.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/HttpService.java#L1) | retain | Keep the minimal bound-address contract; replacing HostAndPort requires coordinated URI/IPv6 caller changes. |
| [src/main/java/org/apache/aurora/scheduler/http/HttpStatsFilter.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/HttpStatsFilter.java#L1) | retain | Keep the mutable HttpServletResponseWrapper used to record final response status; records and value semantics do not apply. |
| [src/main/java/org/apache/aurora/scheduler/http/JerseyTemplateServlet.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/JerseyTemplateServlet.java#L1) | retain | Keep StringWriter rendering and TemplateException-to-WebApplicationException translation; StringWriter has no external resource to release. |
| [src/main/java/org/apache/aurora/scheduler/http/JettyServerModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/JettyServerModule.java#L101) | [defer](#file-88) | Modernize the HTTP server, Servlet/JAX-RS stack and executor configuration as one integration slice before evaluating request virtual threads. |
| [src/main/java/org/apache/aurora/scheduler/http/LeaderHealth.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/LeaderHealth.java#L1) | retain | Keep leader readiness/status-to-response mapping; replacing the control flow must preserve the existing status codes for each lifecycle state. |
| [src/main/java/org/apache/aurora/scheduler/http/LeaderRedirect.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/LeaderRedirect.java#L88) | [change](#file-90) | Use Optional.map for the simple leader-instance-to-HTTP-address projection. |
| [src/main/java/org/apache/aurora/scheduler/http/LeaderRedirectFilter.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/LeaderRedirectFilter.java#L1) | retain | Keep explicit missing-leader, redirect and pass-through branches; their different response bodies/statuses are clearer than a nested Optional pipeline. |
| [src/main/java/org/apache/aurora/scheduler/http/LogConfig.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/LogConfig.java#L1) | retain | Keep the template-facing LoggerConfig getters and dynamic logger behavior; a record would require template introspection and equality review. |
| [src/main/java/org/apache/aurora/scheduler/http/Maintenance.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/Maintenance.java#L1) | retain | Keep the host-maintenance response projection and storage read scope; streams/records add no clear benefit to the endpoint adapter. |
| [src/main/java/org/apache/aurora/scheduler/http/Mname.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/Mname.java#L1) | retain | Keep redirect method/path/query and missing-port cases; the repeated HTTP method adapters expose a deliberate public route contract. |
| [src/main/java/org/apache/aurora/scheduler/http/Offers.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/Offers.java#L1) | retain | Keep the configured protobuf-aware mapper and offer response representation until the neutral resource API replaces Mesos. |
| [src/main/java/org/apache/aurora/scheduler/http/PendingTasks.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/PendingTasks.java#L1) | retain | Keep the pending-veto projection and storage read boundaries; changing collectors must retain key/duplicate behavior and JSON shape. |
| [src/main/java/org/apache/aurora/scheduler/http/QuitCallback.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/QuitCallback.java#L1) | retain | Keep the injected Lifecycle graceful shutdown call; it deliberately differs from the immediate abort endpoint. |
| [src/main/java/org/apache/aurora/scheduler/http/Quotas.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/Quotas.java#L1) | retain | Keep the ResourceAggregateBean getter names consumed by the HTTP representation; record conversion must wait for serializer compatibility. |
| [src/main/java/org/apache/aurora/scheduler/http/Services.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/Services.java#L1) | retain | Keep Guava service state reporting and string status names; modern thread primitives do not replace service-health semantics. |
| [src/main/java/org/apache/aurora/scheduler/http/State.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/State.java#L1) | retain | Keep normalization/deduplication of task configurations before serializing cluster state; DTO changes require state response goldens. |
| [src/main/java/org/apache/aurora/scheduler/http/StructDump.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/StructDump.java#L1) | retain | Keep the intentional Thrift-field exclusion and template representation; public JSON fields must not change through generic reflection cleanup. |
| [src/main/java/org/apache/aurora/scheduler/http/Tiers.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/Tiers.java#L1) | retain | Keep the compact explicit default/tiers response map and constructor validation; no substantial Java 25 change is needed. |
| [src/main/java/org/apache/aurora/scheduler/http/Utilization.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/Utilization.java#L100) | [change](#file-103) | Use pattern bindings in display equality while retaining the bean-facing display class hierarchy. |
| [src/main/java/org/apache/aurora/scheduler/http/api/ApiBeta.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/api/ApiBeta.java#L1) | retain | Keep reflected generated method names/parameter metadata and streamed JSON/error responses; replacement dispatch needs generated contract tests, not a generic switch rewrite. |
| [src/main/java/org/apache/aurora/scheduler/http/api/ApiModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/api/ApiModule.java#L1) | retain | Keep explicit protocol factories, MIME mappings and paths until the HTTP stack migration; Java 25 does not replace Thrift content negotiation. |
| [src/main/java/org/apache/aurora/scheduler/http/api/GsonMessageBodyHandler.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/api/GsonMessageBodyHandler.java#L173) | [change](#file-106) | Use explicit union constructor invocation and pattern-bound Class<?> inspection; express the small TType mapping as a switch expression. |
| [src/main/java/org/apache/aurora/scheduler/http/api/TContentAwareServlet.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/api/TContentAwareServlet.java#L1) | retain | Keep content factories and accepted MIME fallback rules; record conversion of internal holders must preserve constructors and servlet transport lifetime. |
| [src/main/java/org/apache/aurora/scheduler/http/api/security/AuthorizeHeaderToken.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/api/security/AuthorizeHeaderToken.java#L1) | retain | Keep the Shiro AuthenticationToken interface and parsed header byte representation; a record would expose additional default methods/secret rendering semantics. |
| [src/main/java/org/apache/aurora/scheduler/http/api/security/AuthorizingParam.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/api/security/AuthorizingParam.java#L1) | retain | Keep the runtime method-parameter annotation and schema contract; annotations cannot be replaced by records or pattern matching. |
| [src/main/java/org/apache/aurora/scheduler/http/api/security/FieldGetter.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/api/security/FieldGetter.java#L1) | retain | Keep type metadata alongside the functional accessor; it is more than Function and cannot be reduced to a lambda everywhere. |
| [src/main/java/org/apache/aurora/scheduler/http/api/security/FieldGetters.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/api/security/FieldGetters.java#L41) | [change](#file-111) | Compose optional field access with parent.apply(input).flatMap(child::apply). |
| [src/main/java/org/apache/aurora/scheduler/http/api/security/HttpSecurityModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/api/security/HttpSecurityModule.java#L70) | [defer](#file-112) | Modernize Shiro, servlet bindings and request-scoped Subject injection as a coordinated security stack change. |
| [src/main/java/org/apache/aurora/scheduler/http/api/security/IniShiroRealmModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/api/security/IniShiroRealmModule.java#L1) | retain | Keep explicit optional realm configuration and credentials matcher bindings; convert to new Shiro APIs together with the security module. |
| [src/main/java/org/apache/aurora/scheduler/http/api/security/Kerberos5Realm.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/api/security/Kerberos5Realm.java#L67) | [change](#file-114) | Dispose the per-authentication GSSContext reliably on success and every failure path. |
| [src/main/java/org/apache/aurora/scheduler/http/api/security/Kerberos5ShiroRealmModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/api/security/Kerberos5ShiroRealmModule.java#L168) | [change](#file-115) | Replace deprecated Subject.doAs with Subject.callAs and explicitly preserve credential-context and exception translation. |
| [src/main/java/org/apache/aurora/scheduler/http/api/security/KerberosPrincipalConverter.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/api/security/KerberosPrincipalConverter.java#L1) | retain | Keep constructor-based principal validation and ParameterException wrapping; a direct factory abstraction adds no benefit. |
| [src/main/java/org/apache/aurora/scheduler/http/api/security/ShiroAuthenticatingThriftInterceptor.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/api/security/ShiroAuthenticatingThriftInterceptor.java#L1) | retain | Keep the two-phase injected Subject provider and volatile publication guard until AOP construction is migrated; ordinary constructor injection may conflict with interceptor registration. |
| [src/main/java/org/apache/aurora/scheduler/http/api/security/ShiroAuthorizingInterceptor.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/api/security/ShiroAuthorizingInterceptor.java#L1) | retain | Keep the explicit domain permission check and failure counter; redesign two-phase injection only with the Guice/Shiro interceptor binding lifecycle. |
| [src/main/java/org/apache/aurora/scheduler/http/api/security/ShiroAuthorizingParamInterceptor.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/api/security/ShiroAuthorizingParamInterceptor.java#L85) | [change](#file-119) | Consider a private record for the index/accessor descriptor after confirming identity/equality are not consumed; retain the cached typed extraction pipeline. |
| [src/main/java/org/apache/aurora/scheduler/http/api/security/ShiroIniConverter.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/api/security/ShiroIniConverter.java#L1) | retain | Keep section-specific validation and exception distinctions; text blocks or parser substitutions must not change accepted INI configuration. |
| [src/main/java/org/apache/aurora/scheduler/http/api/security/ShiroKerberosAuthenticationFilter.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/api/security/ShiroKerberosAuthenticationFilter.java#L1) | retain | Keep malformed-header, failed-login and unauthenticated branches explicit; Optional pipelines should not obscure response and chain-control semantics. |
| [src/main/java/org/apache/aurora/scheduler/http/api/security/ShiroKerberosPermissiveAuthenticationFilter.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/api/security/ShiroKerberosPermissiveAuthenticationFilter.java#L1) | retain | Keep the specific UnauthenticatedException interception; broader exception catches or unconditional login attempts would change permissive behavior. |
| [src/main/java/org/apache/aurora/scheduler/http/api/security/ShiroUtils.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/api/security/ShiroUtils.java#L1) | retain | Keep the small typed realm multibinding helper; it encapsulates a framework contract rather than obsolete Java boilerplate. |
| [src/main/java/org/apache/aurora/scheduler/http/api/security/ThriftFieldGetter.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/api/security/ThriftFieldGetter.java#L43) | [change](#file-124) | Use a pattern binding after an explicit argument guard for StructMetaData instead of a separated type test and cast. |
| [src/main/java/org/apache/aurora/scheduler/metadata/MetadataModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/metadata/MetadataModule.java#L1) | retain | Keep NearestFit singleton/event subscriber registration; lifecycle and metadata cache policy belong to the subscriber. |
| [src/main/java/org/apache/aurora/scheduler/metadata/NearestFit.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/metadata/NearestFit.java#L135) | [change](#file-126) | Use Map.entry for the temporary non-null key/reason-list pair if callers do not mutate entries. |
| [src/main/java/org/apache/aurora/scheduler/pruning/JobUpdateHistoryPruner.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/pruning/JobUpdateHistoryPruner.java#L65) | [change](#file-127) | Consider an internal settings record retaining explicit current Amount units; migrate to Duration at the timing boundary separately. |
| [src/main/java/org/apache/aurora/scheduler/pruning/PruningModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/pruning/PruningModule.java#L1) | retain | Keep independently configured task/job-update retention and scheduler binding; changing timing representation requires both pruner owners. |
| [src/main/java/org/apache/aurora/scheduler/pruning/TaskHistoryPruner.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/pruning/TaskHistoryPruner.java#L82) | [change](#file-129) | Consider an internal record for immutable retention settings with explicit time units. |
| [src/main/java/org/apache/aurora/scheduler/spi/Permissions.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/spi/Permissions.java#L1) | retain | Keep named permission classes extending Shiro WildcardPermission and the exact RPC/job key encoding; records cannot preserve the superclass contract. |
| [src/main/java/org/apache/aurora/scheduler/spi/package-info.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/spi/package-info.java#L1) | retain | Keep package-level non-null defaults for the extension API; nullness framework changes need a coherent annotation policy. |
| [src/main/java/org/apache/aurora/scheduler/stats/AsyncStatsModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/stats/AsyncStatsModule.java#L1) | retain | Keep separate periodic statistics services and OfferAdapter translation; virtual threads offer no automatic benefit for these serialized sampling loops. |
| [src/main/java/org/apache/aurora/scheduler/stats/CachedCounters.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/stats/CachedCounters.java#L37) | [change](#file-133) | Replace the one-method anonymous CacheLoader with CacheLoader.from(stats::makeCounter). |
| [src/main/java/org/apache/aurora/scheduler/stats/ResourceCounter.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/stats/ResourceCounter.java#L196) | [change](#file-134) | Use a pattern binding in Metric.equals without turning the mutable accumulating Metric into a record. |
| [src/main/java/org/apache/aurora/scheduler/stats/SlotSizeCounter.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/stats/SlotSizeCounter.java#L74) | [change](#file-135) | Consider a small internal record for MachineResource after checking subclasses, getter callers and equality/hash expectations. |
| [src/main/java/org/apache/aurora/scheduler/stats/StatsModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/stats/StatsModule.java#L1) | retain | Keep shared stats registry and time-series service lifetime; lifecycle cleanup must be made in the owning repository implementation. |
| [src/main/java/org/apache/aurora/scheduler/stats/TaskStatCalculator.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/stats/TaskStatCalculator.java#L49) | [change](#file-137) | Use Locale.ROOT when constructing exported metric names. |
| [src/main/java/org/apache/aurora/scheduler/testing/FakeStatsProvider.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/testing/FakeStatsProvider.java#L1) | retain | Keep the mutable deterministic test registry and supplied gauges; immutable records/collections would prevent tests from observing evolving counters. |
| [src/main/java/org/apache/aurora/scheduler/thrift/AuditMessages.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/thrift/AuditMessages.java#L1) | retain | Keep existing Optional.map chains and explicit insecure-user fallback; newer string APIs add little and must not alter audit text. |
| [src/main/java/org/apache/aurora/scheduler/thrift/ReadOnlySchedulerImpl.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/thrift/ReadOnlySchedulerImpl.java#L266) | [defer](#file-140) | Normalize cron prediction to Instant at an internal boundary after the CronPredictor API is migrated. |
| [src/main/java/org/apache/aurora/scheduler/thrift/Responses.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/thrift/Responses.java#L1) | retain | Keep the shared mutable Thrift response builders and message ordering; immutable records would change the public RPC assembly contract. |
| [src/main/java/org/apache/aurora/scheduler/thrift/SchedulerThriftInterface.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/thrift/SchedulerThriftInterface.java#L1) | retain | Keep the original API implementation and transactional mutation boundaries; extract typed internal request/value helpers only with preserved RPC validation and response tests. |
| [src/main/java/org/apache/aurora/scheduler/thrift/Thresholds.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/thrift/Thresholds.java#L21) | [change](#file-143) | Consider a small immutable configuration record once constructor/getter callers and any subclass usage are inventoried. |
| [src/main/java/org/apache/aurora/scheduler/thrift/ThriftModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/thrift/ThriftModule.java#L1) | retain | Keep the separate read-only/admin bindings and original request lifetime; making this a singleton or record would change injection semantics. |
| [src/main/java/org/apache/aurora/scheduler/thrift/aop/AnnotatedAuroraAdmin.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/thrift/aop/AnnotatedAuroraAdmin.java#L1) | retain | Keep the method-specific authorization and workload annotations on the generated API extension; they are consumed reflectively. |
| [src/main/java/org/apache/aurora/scheduler/thrift/aop/AopModule.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/thrift/aop/AopModule.java#L1) | retain | Keep decorator order and annotation/interface matching; functional rewrites must not reorder logging, authorization and server-info decoration. |
| [src/main/java/org/apache/aurora/scheduler/thrift/aop/LoggingInterceptor.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/thrift/aop/LoggingInterceptor.java#L1) | retain | Keep explicit response/error translation, sensitive configuration handling and metrics; replacing its caches/exception branches requires audit and security goldens. |
| [src/main/java/org/apache/aurora/scheduler/thrift/aop/ServerInfoInterceptor.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/thrift/aop/ServerInfoInterceptor.java#L1) | retain | Keep response decoration and framework member injection until interceptor construction is migrated as a whole. |
| [src/main/java/org/apache/aurora/scheduler/thrift/aop/ThriftStatsExporterInterceptor.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/thrift/aop/ThriftStatsExporterInterceptor.java#L79) | [change](#file-149) | Use explicit constructor invocation for workload counters and decide whether to cache constructor metadata, not counter instances. |
| [src/main/java/org/apache/aurora/scheduler/thrift/aop/ThriftWorkload.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/thrift/aop/ThriftWorkload.java#L1) | retain | Keep the explicit generated-result union inspection and default zero count; a switch rewrite must preserve unset/unsupported result behavior. |
| [src/main/java/org/apache/aurora/scheduler/thrift/auth/DecoratedThrift.java](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/thrift/auth/DecoratedThrift.java#L1) | retain | Keep the runtime decorator marker and its exact targets; it is part of AOP binding identity. |

## Findings and required validation

<a id="file-1"></a>

### `src/main/java/org/apache/aurora/GuavaUtils.java`

**P2 · collections · local**

Delegate the duplicate list collector to ImmutableList.toImmutableList; review set/map collector characteristics separately before replacing them.

Evidence: [line 63](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/GuavaUtils.java#L63)

```java
public static <T> Collector<T, ?, ImmutableSet<T>> toImmutableSet() {
```

Contract: The custom set/map collectors declare UNORDERED; compare characteristics, encounter order, null rejection and duplicate-key errors before delegation.

Required validation: GuavaUtilsTest plus parallel collector ordering and duplicate/null inputs.

<a id="file-4"></a>

### `src/main/java/org/apache/aurora/codec/ThriftBinaryCodec.java`

**P1 · resource-lifecycle · local**

Give the explicitly constructed Deflater a Java 25 try-with-resources lifetime, and close the transport/compression layer before reading compressed bytes.

Evidence: [line 157](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/codec/ThriftBinaryCodec.java#L157)

```java
new DeflaterOutputStream(outBytes, new Deflater(DEFLATE_LEVEL), DEFLATER_BUFFER_SIZE),
```

Contract: DeflaterOutputStream does not own an explicitly supplied Deflater; preserve finish-before-toByteArray and CodingException/suppressed-error behavior.

Required validation: ThriftBinaryCodecTest, corrupted/partial streams, repeated compression and historical compressed fixtures.

<a id="file-6"></a>

### `src/main/java/org/apache/aurora/scheduler/BatchWorker.java`

**P1 · resource-lifecycle · local**

Make the retry executor an explicitly owned service resource with shutdown and a defined outcome for queued/retrying futures.

Evidence: [line 141](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/BatchWorker.java#L141)

```java
scheduledExecutor = AsyncUtil.singleThreadLoggingScheduledExecutor(serviceName() + "-%d", LOG);
```

Contract: Do not replace this serialized transaction worker with virtual threads or move future completion outside/inside transactions as incidental cleanup.

Required validation: BatchWorkerTest plus stop during retry, stop with queued work and failed storage writes.

**P3 · collections · local**

Use an ArrayList for the append-and-iterate batch after measuring realistic batch sizes.

Evidence: [line 193](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/BatchWorker.java#L193)

```java
List<WorkItem<T>> batch = new LinkedList<>();
```

Contract: Preserve the current poll/requeue/drain order and maxBatchSize; repairing that order is a separate behavior change.

Required validation: BatchWorkerTest, batch order characterization and allocation comparison.

<a id="file-10"></a>

### `src/main/java/org/apache/aurora/scheduler/TaskIdGenerator.java`

**P3 · language · local**

Consider simple concatenation for the task ID and remove the unused Clock dependency only after reviewing constructor callers.

Evidence: [line 50](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/TaskIdGenerator.java#L50)

```java
return new StringBuilder()
```

Contract: Preserve UUID generation, separators and the exact invalid-character replacement; task IDs are external identities.

Required validation: Add task-ID format and constructor-callsite coverage; run TaskGroupsTest and StateManagerImplTest.

<a id="file-12"></a>

### `src/main/java/org/apache/aurora/scheduler/TaskStatusHandlerImpl.java`

**P2 · language · after-boundary**

Express reason-to-message selection with a switch expression and Optional.or while retaining the special unregistered-executor suppression.

Evidence: [line 192](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/TaskStatusHandlerImpl.java#L192)

```java
switch (status.getReason()) {
```

Contract: Do not change acknowledgements after storage.write, batch order, or explicit message precedence.

Required validation: TaskStatusHandlerImplTest for explicit, missing and suppressed messages.

<a id="file-13"></a>

### `src/main/java/org/apache/aurora/scheduler/TaskVars.java`

**P3 · jdk-api · local**

Use Optional.ifPresent for the rack counter update where the optional is read once and only the present branch has work.

Evidence: [line 165](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/TaskVars.java#L165)

```java
if (rack.isPresent()) {
```

Contract: Keep counter creation, increment order and missing-rack semantics; do not replace atomic counters with adders without checking reads.

Required validation: TaskVarsTest rack and state-change counter cases.

<a id="file-14"></a>

### `src/main/java/org/apache/aurora/scheduler/TierInfo.java`

**P2 · language · local**

Use an instanceof binding in equals; separately consider a record after JSON tooling is modernized.

Evidence: [line 92](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/TierInfo.java#L92)

```java
if (!(obj instanceof TierInfo)) {
```

Contract: Keep isPreemptible/isRevocable/isProduction, JSON names, current hashCode and toString; current Jackson is not a record-migration baseline.

Required validation: TierManagerTest, TierModuleTest and TiersTest; add equality/hash and tier JSON round-trip cases.

<a id="file-19"></a>

### `src/main/java/org/apache/aurora/scheduler/app/MoreModules.java`

**P1 · jdk-api · local**

Replace Class.newInstance with explicit constructor lookup/invocation and use Class<? extends Module> through the dynamic-module API.

Evidence: [line 55](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/app/MoreModules.java#L55)

```java
return (Module) moduleClass.newInstance();
```

Contract: Preserve constructor accessibility and CliOptions preference. Unwrap InvocationTargetException only in the fallback to match Class.newInstance; retain the existing CliOptions-constructor error wrapping.

Required validation: MoreModulesTest plus absent/inaccessible/throwing constructor and wrong-type cases.

<a id="file-22"></a>

### `src/main/java/org/apache/aurora/scheduler/app/VolumeConverter.java`

**P2 · jdk-api · local**

Normalize the volume mode using Locale.ROOT.

Evidence: [line 46](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/app/VolumeConverter.java#L46)

```java
mode = Mode.valueOf(split[2].toUpperCase());
```

Contract: This deliberately stabilizes parsing across host locales; preserve accepted mode names, colon splitting and ParameterException messages.

Required validation: VolumeConverterTest under default and Turkish locales.

<a id="file-24"></a>

### `src/main/java/org/apache/aurora/scheduler/base/AsyncUtil.java`

**P2 · language · local**

Use instanceof Future<?> future to remove the raw type and cast in afterExecute inspection.

Evidence: [line 150](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/base/AsyncUtil.java#L150)

```java
if (runnable instanceof Future) {
```

Contract: Keep isDone before get, interruption restoration and exception metrics; cancellation behavior needs its own characterization.

Required validation: AsyncUtilTest including success, failure, interruption and cancellation.

<a id="file-30"></a>

### `src/main/java/org/apache/aurora/scheduler/base/Query.java`

**P2 · language · local**

Bind Builder in the instanceof expression used by equals.

Evidence: [line 153](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/base/Query.java#L153)

```java
return that instanceof Builder && get().equals(((Builder) that).get());
```

Contract: Keep equality based on get(), not builder identity or mutable internal state; retain the existing hashCode.

Required validation: SchedulingFilterImplTest and AbstractTaskStoreTest; add focused query-builder equality and composition cases.

<a id="file-32"></a>

### `src/main/java/org/apache/aurora/scheduler/base/TaskGroupKey.java`

**P2 · language · local**

Use an instanceof binding in TaskGroupKey.equals.

Evidence: [line 59](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/base/TaskGroupKey.java#L59)

```java
if (!(o instanceof TaskGroupKey)) {
```

Contract: Preserve canonical task equality, Objects.hash and the job-only toString; a default record changes observable methods.

Required validation: TaskGroupsTest and NearestFitTest; add focused TaskGroupKey equality/hash cases.

<a id="file-36"></a>

### `src/main/java/org/apache/aurora/scheduler/config/CommandLine.java`

**P2 · boundary · framework**

Replace reflective registration of option fields with an explicit typed registry once custom-option extension points are inventoried.

Evidence: [line 187](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/config/CommandLine.java#L187)

```java
for (Field field : CliOptions.class.getDeclaredFields()) {
```

Contract: Preserve external custom options, registration order and legacy static option access until their callers migrate.

Required validation: CommandLine tests plus custom-option and duplicate-registration cases.

<a id="file-38"></a>

### `src/main/java/org/apache/aurora/scheduler/config/converters/DataAmountConverter.java`

**P3 · jdk-api · local**

Use Optional.map/orElseThrow to construct DataAmount and centralize the missing-unit error.

Evidence: [line 47](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/config/converters/DataAmountConverter.java#L47)

```java
if (unit.isPresent()) {
```

Contract: Preserve case-sensitive unit matching, numeric overflow behavior and the current diagnostics.

Required validation: CommandLineTest; add direct converter cases for units, unknown units and numeric overflow.

<a id="file-41"></a>

### `src/main/java/org/apache/aurora/scheduler/config/converters/TimeAmountConverter.java`

**P3 · jdk-api · local**

Use Optional.map/orElseThrow to construct TimeAmount and centralize the missing-unit error.

Evidence: [line 47](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/config/converters/TimeAmountConverter.java#L47)

```java
if (unit.isPresent()) {
```

Contract: Preserve case-sensitive unit matching, numeric overflow behavior and the current diagnostics.

Required validation: CommandLineTest; add direct converter cases for units, unknown units and numeric overflow.

<a id="file-53"></a>

### `src/main/java/org/apache/aurora/scheduler/configuration/SanitizedConfiguration.java`

**P2 · language · local**

Use an instanceof binding for equality and the JDK Objects.equals helper.

Evidence: [line 82](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/configuration/SanitizedConfiguration.java#L82)

```java
if (!(o instanceof SanitizedConfiguration)) {
```

Contract: Equality deliberately ignores derived instanceIds; keep lazy ContiguousSet ranges and the existing hashCode.

Required validation: ConfigurationManagerTest and SchedulerThriftInterfaceTest; add sanitized-value equality and derived-range cases.

<a id="file-54"></a>

### `src/main/java/org/apache/aurora/scheduler/configuration/executor/ExecutorConfig.java`

**P3 · language · after-boundary**

Use an instanceof binding in executor configuration equality.

Evidence: [line 58](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/configuration/executor/ExecutorConfig.java#L58)

```java
if (!(obj instanceof ExecutorConfig)) {
```

Contract: Keep ExecutorInfo, ordered volume mounts and taskPrefix semantics; defer a record/neutral DTO until Mesos types are extracted.

Required validation: ExecutorSettingsLoaderTest and executor configuration equality cases.

<a id="file-60"></a>

### `src/main/java/org/apache/aurora/scheduler/discovery/CuratorServiceGroupMonitor.java`

**P3 · jdk-api · local**

Use Optional.stream when flattening successfully decoded child data if it makes the get() pipeline clearer.

Evidence: [line 97](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/discovery/CuratorServiceGroupMonitor.java#L97)

```java
private Optional<ServiceInstance> extractServiceInstance(ChildData data) {
```

Contract: Preserve malformed-entry logging/skipping and immutable set deduplication; do not let one bad child fail the full snapshot.

Required validation: CuratorServiceGroupMonitorTest and BaseCuratorDiscoveryTest with malformed child payloads.

<a id="file-62"></a>

### `src/main/java/org/apache/aurora/scheduler/discovery/Encoding.java`

**P3 · jdk-api · local**

Use StandardCharsets.UTF_8 for discovery payload encoding and decoding.

Evidence: [line 41](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/discovery/Encoding.java#L41)

```java
return GSON.toJson(serviceInstance).getBytes(Charsets.UTF_8);
```

Contract: The charset must remain UTF-8 and legacy status/required-field behavior must not change.

Required validation: EncodingTest byte fixtures, missing fields and malformed JSON.

<a id="file-65"></a>

### `src/main/java/org/apache/aurora/scheduler/discovery/ServiceDiscoveryModule.java`

**P2 · resource-lifecycle · local**

Handle InterruptedException separately and define cancellation propagation when starting the embedded server.

Evidence: [line 148](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/discovery/ServiceDiscoveryModule.java#L148)

```java
} catch (IOException | InterruptedException e) {
```

Contract: Restoring the interrupt flag is a behavioral correction; retain IOException wrapping and do not use a blocking close to erase delayed client-before-server shutdown.

Required validation: Embedded discovery tests interrupted during startup and ordered shutdown.

<a id="file-66"></a>

### `src/main/java/org/apache/aurora/scheduler/discovery/ServiceInstance.java`

**P3 · language · local**

Use pattern-bound equality locally; postpone record conversion of ServiceInstance/Endpoint until Gson migration.

Evidence: [line 60](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/discovery/ServiceInstance.java#L60)

```java
if (!(obj instanceof ServiceInstance)) {
```

Contract: The no-arg constructor, nullable endpoint during deserialization, legacy ALIVE field, map ownership and bean getters are compatibility contracts.

Required validation: EncodingTest and CuratorDiscoveryModuleTest with historical payloads.

<a id="file-67"></a>

### `src/main/java/org/apache/aurora/scheduler/discovery/ZooKeeperConfig.java`

**P2 · jdk-api · framework**

Adopt Duration for connection/session timeout values at a future typed configuration boundary.

Evidence: [line 61](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/discovery/ZooKeeperConfig.java#L61)

```java
private final Amount<Integer, Time> sessionTimeout;
```

Contract: Preserve exact integer-millisecond conversion/overflow and CLI Amount parsing; do not treat server Iterable as an immutable snapshot without checking callers.

Required validation: ZooKeeperConfigTest and connection/session timeout integration tests.

<a id="file-70"></a>

### `src/main/java/org/apache/aurora/scheduler/events/PubsubEvent.java`

**P3 · language · local**

Use pattern bindings in event equality methods; keep the explicit TaskStateChange JSON envelope.

Evidence: [line 60](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/events/PubsubEvent.java#L60)

```java
if (!(o instanceof TasksDeleted)) {
```

Contract: Do not make the event interface sealed until external implementations/subscribers are inventoried; record defaults change equality/hash and serialized fields.

Required validation: PubsubEventModuleTest, WebhookTest and TaskStateChangeTest golden JSON.

<a id="file-72"></a>

### `src/main/java/org/apache/aurora/scheduler/events/Webhook.java`

**P3 · jdk-api · local**

Use optional.map(statuses -> statuses.contains(status)).orElse(true) for the whitelist predicate.

Evidence: [line 73](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/events/Webhook.java#L73)

```java
this.isWhitelisted = status -> !webhookInfo.getWhitelistedStatuses().isPresent()
```

Contract: Missing statuses means all statuses; an explicitly empty list means none. Preserve request/error metrics and callback lifetime.

Required validation: WebhookTest wildcard, absent/empty whitelist, rejected responses and callback failures.

<a id="file-73"></a>

### `src/main/java/org/apache/aurora/scheduler/events/WebhookInfo.java`

**P2 · jdk-api · local**

Replace repeated Optional.ofNullable checks with statuses == null || statuses.contains("*") and collect statuses directly into the intended immutable collection.

Evidence: [line 83](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/events/WebhookInfo.java#L83)

```java
!Optional.ofNullable(statuses).isPresent()
```

Contract: Preserve null/wildcard/empty distinctions, case-sensitive ScheduleStatus parsing and duplicate/order semantics.

Required validation: WebhookTest configuration and whitelist cases.

<a id="file-75"></a>

### `src/main/java/org/apache/aurora/scheduler/filter/AttributeAggregate.java`

**P3 · language · local**

Use a pattern binding in equality without changing the lazily initialized multiset.

Evidence: [line 160](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/filter/AttributeAggregate.java#L160)

```java
if (!(o instanceof AttributeAggregate)) {
```

Contract: Do not convert the aggregate into a record or eager snapshot; cache initialization and update visibility affect filtering.

Required validation: AttributeAggregateTest including missing attributes and incremental updates.

<a id="file-78"></a>

### `src/main/java/org/apache/aurora/scheduler/filter/SchedulingFilter.java`

**P3 · language · local**

Use pattern bindings in value-holder equality; consider internal records only after resource/request DTO boundaries stabilize.

Evidence: [line 229](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/filter/SchedulingFilter.java#L229)

```java
if (!(o instanceof Veto)) {
```

Contract: Retain Veto scoring/reason identity, lazy attribute aggregates and exposed getters; generated Thrift status/resource types remain boundary contracts.

Required validation: SchedulingFilterImplTest, Veto equality and resource-request tests.

<a id="file-88"></a>

### `src/main/java/org/apache/aurora/scheduler/http/JettyServerModule.java`

**P1 · boundary · framework**

Modernize the HTTP server, Servlet/JAX-RS stack and executor configuration as one integration slice before evaluating request virtual threads.

Evidence: [line 101](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/JettyServerModule.java#L101)

```java
public class JettyServerModule extends AbstractModule {
```

Contract: Retain endpoint/filter order, auth context, overload bounds, graceful shutdown and main/recovery packaging; virtual threads do not remove admission limits.

Required validation: ApiIT, HttpSecurityIT, ServletFilterTest, server startup/shutdown and load/cancellation tests.

<a id="file-90"></a>

### `src/main/java/org/apache/aurora/scheduler/http/LeaderRedirect.java`

**P3 · jdk-api · local**

Use Optional.map for the simple leader-instance-to-HTTP-address projection.

Evidence: [line 88](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/LeaderRedirect.java#L88)

```java
if (leadingScheduler.isPresent()) {
```

Contract: Do not collapse distinct missing-leader/missing-local-address states or change redirect host/query construction.

Required validation: LeaderRedirectTest and ServletFilterTest.

<a id="file-103"></a>

### `src/main/java/org/apache/aurora/scheduler/http/Utilization.java`

**P3 · language · local**

Use pattern bindings in display equality while retaining the bean-facing display class hierarchy.

Evidence: [line 100](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/Utilization.java#L100)

```java
if (!(o instanceof  Display)) {
```

Contract: DisplayMetric extends mutable Metric; a record cannot preserve that inheritance or template getter behavior.

Required validation: ResourceCounterTest; add utilization route/template and display equality coverage.

<a id="file-106"></a>

### `src/main/java/org/apache/aurora/scheduler/http/api/GsonMessageBodyHandler.java`

**P1 · jdk-api · local**

Use explicit union constructor invocation and pattern-bound Class<?> inspection; express the small TType mapping as a switch expression.

Evidence: [line 173](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/api/GsonMessageBodyHandler.java#L173)

```java
TUnion union = (TUnion) unionType.newInstance();
```

Contract: Preserve exactly-one-field union validation, numeric deserialization and current reflection error wrapping; add missing direct handler cases.

Required validation: ApiBetaTest plus union numeric/string/struct round trips and constructor failures.

**P2 · resource-lifecycle · local**

Revisit the old FindBugs workaround and use try-with-resources for the reader once the modern quality tool is wired.

Evidence: [line 82](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/api/GsonMessageBodyHandler.java#L82)

```java
InputStreamReader streamReader = null;
```

Contract: The existing code closes the incoming stream; retain ownership semantics and characterize primary versus suppressed exceptions.

Required validation: Direct readFrom close/failure tests and original HTTP JSON integration.

<a id="file-111"></a>

### `src/main/java/org/apache/aurora/scheduler/http/api/security/FieldGetters.java`

**P2 · jdk-api · local**

Compose optional field access with parent.apply(input).flatMap(child::apply).

Evidence: [line 41](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/api/security/FieldGetters.java#L41)

```java
Optional<C> parentValue = parent.apply(input);
```

Contract: Preserve empty-parent short circuit and exposed struct/value class metadata; check that child never returns a null Optional.

Required validation: ShiroAuthorizingParamInterceptorTest nested and absent job keys.

<a id="file-112"></a>

### `src/main/java/org/apache/aurora/scheduler/http/api/security/HttpSecurityModule.java`

**P1 · boundary · framework**

Modernize Shiro, servlet bindings and request-scoped Subject injection as a coordinated security stack change.

Evidence: [line 70](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/api/security/HttpSecurityModule.java#L70)

```java
public class HttpSecurityModule extends ServletModule {
```

Contract: ScopedValue is final in Java 25 but is not a drop-in replacement for Shiro thread/request context or cross-executor propagation.

Required validation: HttpSecurityIT, authentication/authorization filters and real credential/context isolation tests.

<a id="file-114"></a>

### `src/main/java/org/apache/aurora/scheduler/http/api/security/Kerberos5Realm.java`

**P1 · resource-lifecycle · local**

Dispose the per-authentication GSSContext reliably on success and every failure path.

Evidence: [line 67](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/api/security/Kerberos5Realm.java#L67)

```java
context = gssManager.createContext(serverCredential);
```

Contract: GSSContext is not AutoCloseable; use explicit finally or a small owned adapter and preserve the primary AuthenticationException when disposal fails.

Required validation: Kerberos5ShiroRealmModuleTest and ShiroKerberosAuthenticationFilterTest; add per-context disposal tests and disposable-KDC handshakes.

<a id="file-115"></a>

### `src/main/java/org/apache/aurora/scheduler/http/api/security/Kerberos5ShiroRealmModule.java`

**P1 · jdk-api · local**

Replace deprecated Subject.doAs with Subject.callAs and explicitly preserve credential-context and exception translation.

Evidence: [line 168](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/api/security/Kerberos5ShiroRealmModule.java#L168)

```java
serverCredential = Subject.doAs(
```

Contract: callAs uses CompletionException wrapping; separately define LoginContext logout and GSSCredential disposal at service shutdown, not immediately after binding.

Required validation: Kerberos5ShiroRealmModuleTest plus real SPNEGO/KDC and shutdown cleanup.

<a id="file-119"></a>

### `src/main/java/org/apache/aurora/scheduler/http/api/security/ShiroAuthorizingParamInterceptor.java`

**P3 · language · local**

Consider a private record for the index/accessor descriptor after confirming identity/equality are not consumed; retain the cached typed extraction pipeline.

Evidence: [line 85](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/api/security/ShiroAuthorizingParamInterceptor.java#L85)

```java
private static class JobKeyGetter {
```

Contract: Do not seal or replace the externally annotated Thrift interface; preserve absent/multiple job-key validation and method-cache behavior.

Required validation: ShiroAuthorizingParamInterceptorTest all permission extraction and malformed annotation cases.

<a id="file-124"></a>

### `src/main/java/org/apache/aurora/scheduler/http/api/security/ThriftFieldGetter.java`

**P3 · language · local**

Use a pattern binding after an explicit argument guard for StructMetaData instead of a separated type test and cast.

Evidence: [line 43](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/http/api/security/ThriftFieldGetter.java#L43)

```java
checkArgument(fieldValueMetaData instanceof StructMetaData);
```

Contract: Preserve IllegalArgumentException and metadata diagnostics; Java flow scoping does not infer a pattern guard through checkArgument.

Required validation: ShiroAuthorizingParamInterceptorTest metadata mismatches and unset fields.

<a id="file-126"></a>

### `src/main/java/org/apache/aurora/scheduler/metadata/NearestFit.java`

**P3 · collections · local**

Use Map.entry for the temporary non-null key/reason-list pair if callers do not mutate entries.

Evidence: [line 135](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/metadata/NearestFit.java#L135)

```java
return new HashMap.SimpleEntry<>(t.getKey(), reasons);
```

Contract: Map.entry rejects null and is unmodifiable; retain synchronized snapshot creation and ordering.

Required validation: NearestFitTest pending-reason aggregation and concurrent reads.

<a id="file-127"></a>

### `src/main/java/org/apache/aurora/scheduler/pruning/JobUpdateHistoryPruner.java`

**P3 · language · local**

Consider an internal settings record retaining explicit current Amount units; migrate to Duration at the timing boundary separately.

Evidence: [line 65](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/pruning/JobUpdateHistoryPruner.java#L65)

```java
static class HistoryPrunerSettings {
```

Contract: Validate constructor invariants, identity use and retention cutoff units; records are shallow and must not alter store deletion policy.

Required validation: JobUpdateHistoryPrunerTest cutoff and retained-history cases.

<a id="file-129"></a>

### `src/main/java/org/apache/aurora/scheduler/pruning/TaskHistoryPruner.java`

**P3 · language · local**

Consider an internal record for immutable retention settings with explicit time units.

Evidence: [line 82](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/pruning/TaskHistoryPruner.java#L82)

```java
static class HistoryPrunerSettings {
```

Contract: Keep per-job history thresholds, delayed cleanup order and constructor behavior; do not change prune scheduling as style cleanup.

Required validation: TaskHistoryPrunerTest delayed expiry and history limits.

<a id="file-133"></a>

### `src/main/java/org/apache/aurora/scheduler/stats/CachedCounters.java`

**P3 · language · local**

Replace the one-method anonymous CacheLoader with CacheLoader.from(stats::makeCounter).

Evidence: [line 37](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/stats/CachedCounters.java#L37)

```java
new CacheLoader<String, AtomicLong>() {
```

Contract: Keep cache-owned one-counter-per-name behavior and getUnchecked failure semantics; ConcurrentHashMap would be a separate contract change.

Required validation: CachedCounters/TaskStatCalculator tests and concurrent first access.

<a id="file-134"></a>

### `src/main/java/org/apache/aurora/scheduler/stats/ResourceCounter.java`

**P3 · language · local**

Use a pattern binding in Metric.equals without turning the mutable accumulating Metric into a record.

Evidence: [line 196](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/stats/ResourceCounter.java#L196)

```java
if (!(o instanceof Metric)) {
```

Contract: Keep resource accumulation, subclass behavior and hash calculation; stream reductions need algebra/rounding tests.

Required validation: ResourceCounterTest; add utilization route/template rendering coverage.

<a id="file-135"></a>

### `src/main/java/org/apache/aurora/scheduler/stats/SlotSizeCounter.java`

**P3 · language · local**

Consider a small internal record for MachineResource after checking subclasses, getter callers and equality/hash expectations.

Evidence: [line 74](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/stats/SlotSizeCounter.java#L74)

```java
static class MachineResource {
```

Contract: Retain getSize/isDedicated/isRevocable, null validation and the explicit Objects.hash formula; record-generated hash differs. Preserve slot minimum/truncation and serialized accounting.

Required validation: SlotSizeCounterTest and resource accounting cases.

<a id="file-137"></a>

### `src/main/java/org/apache/aurora/scheduler/stats/TaskStatCalculator.java`

**P2 · jdk-api · local**

Use Locale.ROOT when constructing exported metric names.

Evidence: [line 49](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/stats/TaskStatCalculator.java#L49)

```java
Joiner.on("_").join(prefix, type.getAuroraName(), type.getAuroraStatUnit()).toLowerCase();
```

Contract: Treat this as a deliberate cross-locale naming stabilization; assess existing dashboards and role-name metrics before rollout.

Required validation: ResourceCounterTest and SlotSizeCounterTest; add direct calculator metric-name fixtures under English and Turkish locales.

<a id="file-140"></a>

### `src/main/java/org/apache/aurora/scheduler/thrift/ReadOnlySchedulerImpl.java`

**P2 · jdk-api · after-boundary**

Normalize cron prediction to Instant at an internal boundary after the CronPredictor API is migrated.

Evidence: [line 266](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/thrift/ReadOnlySchedulerImpl.java#L266)

```java
Optional<Date> nextRun = cronPredictor.predictNextRun(crontabEntry);
```

Contract: Keep epoch units, timezone and missing-next-run behavior in public Thrift responses; do not change RPC fields.

Required validation: ReadOnlySchedulerImplTest and CronPredictorImplTest around timezone/DST cases.

<a id="file-143"></a>

### `src/main/java/org/apache/aurora/scheduler/thrift/Thresholds.java`

**P3 · language · local**

Consider a small immutable configuration record once constructor/getter callers and any subclass usage are inventoried.

Evidence: [line 21](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/thrift/Thresholds.java#L21)

```java
public class Thresholds {
```

Contract: The current class has identity equality and a custom toString; adopting value equality is an explicit API decision, not mechanical boilerplate deletion.

Required validation: Thresholds callsite compilation plus SchedulerThriftInterfaceTest threshold cases.

<a id="file-149"></a>

### `src/main/java/org/apache/aurora/scheduler/thrift/aop/ThriftStatsExporterInterceptor.java`

**P1 · jdk-api · local**

Use explicit constructor invocation for workload counters and decide whether to cache constructor metadata, not counter instances.

Evidence: [line 79](https://github.com/jordanly/aurora/blob/6cf7f0ea07c6355af62ceafb029fe086f55b8c65/src/main/java/org/apache/aurora/scheduler/thrift/aop/ThriftStatsExporterInterceptor.java#L79)

```java
.newInstance();
```

Contract: Preserve a fresh counter per successful annotated call, timing in finally and constructor failure propagation.

Required validation: ThriftStatsExporterInterceptorTest plus inaccessible/throwing constructors.
