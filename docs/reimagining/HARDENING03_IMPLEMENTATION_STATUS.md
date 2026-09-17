# Bounded execution history, enforced processes and Jakarta modernization

These three slices continue the original `SchedulerMain` application on Java 25.
They preserve the scheduler policy, Thrift API and UI; there is no second scheduler.

## Bounded execution history

The scheduler assigns a durable, monotonically increasing ticket to each agent
attempt. After terminal cleanup, command and supervisor acknowledgments, it
coordinates retirement with that agent. Compact retired-ticket intervals remain
durable after command bodies, observation receipts, supervisor journals and log
directories are deleted. A delayed launch cannot recreate a retired attempt.
Long-lived services do not prevent newer completed attempts from retiring.
Definite capacity, capability and socket refusals become eligible after command
acknowledgment because they create no execution or terminal observation. A stopped
attempt still awaits confirmed terminal cleanup.

The default completed-history window is 64 attempts per node. The agent bounds
retained tickets and retired intervals at 1,024 each. A durable garbage queue
records deletion work after the replay fence commits; failed collection blocks
new admission while authenticated Stop delivery remains available. Recovery pins
the original runtime directory and retries collection there. Filesystem cleanup
uses confined paths and confirms isolation mount cleanup before deletion.

Child supervisors retain at most 256 queued execution events and discard only
acknowledged prefixes. At the node observation threshold of 8,192, readiness-only
changes coalesce to the latest durable value. Terminal/control events remain
ordered and retained; admission bounds provide finite headroom for cleanup.
Routine SQLite transactions retain one replaceable outcome receipt, with uncertain
commits blocking subsequent writes. Explicit caller receipts remain replay-safe
and have a 4,096-entry admission cap. Existing legacy receipts are preserved.

Schema version 4 adds tickets, retirement intervals, observation watermarks and
the automatic outcome receipt. Complete version-3 backups still restore without
changing their source bytes; opening the destination performs migration. An
existing agent needs a one-time drain of legacy work before ticket mode activates.
See [storage](../operations/storage.md), [agent operation](../../agent/README.md)
and [log retention](../operations/task-logs.md).

These are execution-record and log bounds. They do not impose a total installation
disk quota or shrink previously allocated database files. Existing task/update
history and backup retention policies still apply; journal compaction is offline.

## Enforced simple processes

The opt-in Linux profile places each process in its cgroup before opening its
launch gate. It enforces CPU quota, memory and swap limits, and process counts.
Each attempt receives a distinct reserved UID/GID, a read-only trusted rootfs,
bounded writable tmpfs, private mount/IPC/UTS namespaces, no capabilities and
no-new-privileges. Agent journals, credentials and logs remain outside the task
filesystem. Cleanup includes descendants that changed sessions and retains
reservations until their cgroup is empty and work mount is removed.

Prerequisites are checked at startup; missing controllers or privileges fail
closed. The existing trusted-process Docker lab retains its nonroot, capability-
free configuration. The enforced profile has a separate disposable ARM64 KVM
qualification because this Pi booted with memory cgroups disabled. The guest
requires no host reboot, Docker socket or broadly privileged container.

The kernel suite passed filesystem/UID boundaries, memory OOM enforcement, CPU
throttling with restart recovery, TCP health failure and escaped-descendant
cleanup. The final postcheck found no remaining task cgroups, processes or mounts.
See the [profile contract](../operations/native-process-isolation.md) and
[reproducible kernel recipe](../operations/isolation-testing.md).

Network and PID namespaces remain shared. This profile supports simple argv
processes; the process-graph runner needs a separate persistent-volume contract.
Task scratch does not survive reboot. These limits are explicit rather than
claims of complete hostile-workload or container-image support.

## Application framework migration

The maintained HTTP, dependency-injection and security graph now uses Jakarta
APIs. Jackson 1 bindings and the obsolete RESTEasy Guice extension are replaced
with Jackson 2 and a small Guice-to-RESTEasy resource bridge. Jetty resource
servlets, compression and request logging use the current APIs. Integration tests
use Jakarta REST clients and retain the original Thrift MIME and authorization
assertions, including exact generated-browser-client resource checks.

| Component | Before this slice | After |
| --- | --- | --- |
| Jetty / Servlet | 9.3 / 3.1 | 12.1.13 / Jakarta Servlet 6.1 |
| RESTEasy / REST API | 3.1 / JAX-RS 2.0 | 7.0.4 / Jakarta REST 4.0 |
| Guice / Shiro | 6.0 / 1.4 | 7.0 / 3.0.1 |
| Jackson | 1.x provider and 2.5 core | 2.21.6 LTS |
| Guava / Gson | 31.0.1 / 2.3.1 | 33.7.1 / 2.14.0 |
| Async HTTP client / Netty 4 | 2.0.37 / 4.0.52 | 3.0.13 / 4.2.18 |
| SLF4J / Logback | 1.7.25 / 1.2.3 | 2.0.19 / 1.6.3 |

The migration follows the upstream [Jetty guide](https://jetty.org/docs/jetty/12.1/programming-guide/migration/12.0-to-12.1.html),
[RESTEasy API](https://docs.resteasy.dev/7.0/userguide/),
[Guice Jakarta transition](https://github.com/google/guice/wiki/Guice700),
[Shiro Jakarta support](https://shiro.apache.org/jakarta-ee.html), and
[Jackson 2.21 release line](https://github.com/FasterXML/jackson/wiki/Jackson-Release-2.21).
Dependency pins are in `build.gradle`. The Thrift wire/compiler version, optional
ZooKeeper/Curator discovery stack, Quartz, StringTemplate and older test frameworks
remain separate modernization work. HA remains last.

## Qualification

Final Java gates and fresh-cluster acceptance are in progress. This document will
record source-bound results and limitations when those runs finish.
