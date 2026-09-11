<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Bounded composed task execution

`task-v1alpha1` is resolved JSON for an ordinary trusted, preinstalled
`aurora-agent execute-task --manifest-json CANONICAL --state-dir PRIVATE_DIR`
Run. Outer Job/Run assignment admission still owns aggregate resources, ports,
credentials and the surviving supervisor. Child process/run events are local
`task.journal` records, not a new remote process API.

Use `Decode` to reject unknown fields, duplicate keys, nulls and unsupported
versions and to sort processes/dependencies and fill defaults. The canonical
manifest is limited to **4096 bytes** so it fits one unchanged outer argv item.
This intentionally limits the initial cohort to small process graphs. The
manifest and resolved export types in `manifest.go` and `convert.go` specify the
accepted fields. `maxRuns` is mandatory, 1..10000, across all ordinary and finalizer
launches; maxConcurrency=0 means at most the finite process count. Each stream
retains its first logBytes bytes and discards subsequent bytes while continuing
to drain; retention is at most `2 * maxRuns * logBytes` plus journal overhead.

`native-v1` requires all required processes to succeed; optional and ephemeral
processes do not hold completion open. The default failed-run budget is one;
`unlimitedFailures:true` explicitly requests retry until maxRuns. `thermos-v1`
retains independent process failed-run budgets (zero unlimited) and task failed-
process tolerance (zero unlimited); a positive threshold fails the task even
while another process is running. Failed predecessors never release successors,
and a blocked required DAG fails even under unlimited task tolerance. Dependency
cycles, dependencies on daemons, ordinary dependencies on ephemeral processes,
and cross-finalizer dependencies are rejected. Daemon success restarts; ephemeral
failure exhaustion is `finished`, not successful. A pure ephemeral task is
already complete and need not launch any process. Delay is measured after exit.
Unlike legacy Thermos, an exhausted ephemeral predecessor does not release its
ephemeral successors: `afterSuccess` always requires successful execution.
Lost outcomes count toward maxRuns, not the failed-run budget in the pure planner;
the actual runner fails closed on uncertain outcomes and never resumes them.

The dedicated task runner invokes its same-binary `task-child` boundary using
private inherited gate/spec/readiness descriptors, an empty ambient environment,
no_new_privs and PDEATHSIG. A locked launch OS thread stays alive through the
single exact exec.Cmd.Wait. Children inherit the outer group/session. The task
runner is a Linux child subreaper; descendants left by an exiting process are
pidfd-signaled and reaped before further starts. Such descendants conservatively
fail the primary result and prevent retry overlap. Descendants left by finalizers
fail finalization while preserving the already-recorded primary result. There is no hostile tenant
isolation or cgroup enforcement in this package. The outer supervisor independently
checks containment/cleanup before releasing the aggregate reservation.

Cleanup kills remaining primary processes immediately, then finalizers use the
remaining shared finalizationWaitMillis (0..300000). A context deadline can only
shorten that budget. Finalizer launch handshakes and gate release also check the
budget. At expiry children are killed; exit confirmation has a separate bounded
one-second wait, never additional finalizer execution time. primaryResult remains
independent of finalizationResult (`succeeded`, `failed`, `skipped`, `timeout`).
Finalizers are best effort and cannot be promised after runner/host death.

The private state directory is used once. An exclusive durable `.task-owner`
marker detects a missing execution journal and an interrupted initialization.
An exclusive new task.journal is created and its directory synced; the manifest digest, each consumed start
intent, child PID/start identity before gate release, exact exit/signal, and final
results are appended and fsynced. Any existing journal, even empty or corrupt,
refuses all replay/relaunch. Journal failure before gate release kills the helper;
uncertainty prevents retries. This is conservative recovery, not resume-by-PID.
The surviving outer supervisor preserves the task runner across agent daemon
crashes. Runner loss needs outer cleanup and a new scheduler attempt, with a new
state directory. Filesystem loss/rollback still needs external fencing; ordinary
fsync tests are not power-loss experiments.

The offline converter accepts only `thermos-resolved-v1` artifacts with explicit
trustedOfflineExport/defaultsApplied assertions, SHA-256 sourceDigests, bindings
and resolved cmdline/environment/dependencies. Every exported field must be
present, including false booleans and zero numeric values, so omitted defaults
cannot silently become unlimited retry budgets. These assertions are provenance
metadata, not cryptographic proof of trust. The caller must trust its offline
exporter. Bindings are not evaluated. Commands retain explicit `/bin/bash -c`
argv; no Python, imports, profile sourcing, interpolation, custom executor,
container, GPU, health or other unmapped field is accepted. Conversion returns
retained/changed/rejected findings and requires an operator-selected maxRuns.

Tests include source-derived legacy planner traces, real subprocess DAG/retry,
environment exclusion, bounded logs, failed finalizers and startup, shared timeout,
orphan cleanup, a journal failure before gate release, runner crash and no replay.
Race testing on this ARM64 host is unavailable because ThreadSanitizer rejects its
47-bit VMA layout; this is not a passing race result.
