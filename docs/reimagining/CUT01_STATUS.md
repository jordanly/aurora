# CUT-01: packaged native cluster baseline

The native scheduler and Go agents now have a standalone image build and cluster
qualification lane. Java 8 and Gradle 4.10.2 remain the baseline for the next Java
modernization slice. The legacy root build, Mesos adapter and Python worker tree
are isolated from these native distributions.

## Reproduce

Use [the native build guide](../../build-support/native/README.md). The two gates are:

```sh
build-support/native/native-build --output /absolute/new/bundle --cache /absolute/cache
build-support/native/native-qualify --bundle /absolute/new/bundle --run-root /absolute/new/lab
```

The second command runs three recovery/restore rounds and a 600-second mixed
workload test, then removes only the lab's recorded containers and networks.
The bundle and its matching source checkout remain available for another lab.
This is the Pi's Linux ARM64 profile; it does not claim multi-host or production
HA qualification.

## Clean-checkout build evidence

The successful build used clean commit
`67cfb055fa7b51890ee0e1b4b7c4294ebafae55b` at
`.cache/cut01-fresh-source-2`. That checkout contains no `.pi-tools`. Tool archives
were verified and freshly extracted; application binaries and JARs were rebuilt
into `.cache/cut01-fresh-bundle-2`. The build used an explicit warmed dependency
cache with `--offline`. An earlier build populated that new cache from verified
tool archive seeds and downloaded Maven/Go dependencies; no previous application
build output was copied into the fresh build.

The bundle manifest is `.cache/cut01-fresh-bundle-2/bundle.json`, SHA-256
`fd14356ef3e4ced0ecd7ec6744e1ece827a866c27f5310900197b0ae26123863`.
It records clean source status, source hashes, all five immutable image IDs,
129 payload artifact hashes and 57 report hashes. Independent review rehashed
all of them and verified actual Docker container images/mounts.

Executed checks:

- 34 Java tests: 13 scheduler engine, 14 SQL store and 7 protocol tests; all 19
  isolated Gradle tasks executed in the fresh build.
- Go tests and vet passed for the three agent packages and both fixture modules;
  module verification and built-binary dependency metadata matched the pins.
- 101 Python regressions: 68 lab and 33 packaging/boundary tests.
- Installed scheduler runtime: exactly two application JARs plus seven external
  JARs pinned by SHA-256, with class/resource/reference checks and no production
  `NativeStoreTool` CLI.
- All five exported images matched their exact Aurora payload and 3,772 pinned
  Debian base nondirectory entries. Only Docker's generated hostname/hosts/resolver
  files are exceptions. The product images have no keeper or workload fixtures.
- Real product entrypoints passed: agent `version`, and scheduler fresh-store
  inspection including SQLite JNI startup under the packaged JRE.
- All eight temporary image-export/entrypoint probe containers were removed using
  verified ownership records. No shared image pruning or registry publishing ran.

| Image role | Immutable image ID prefix | Docker reported size (bytes) |
| --- | --- | ---: |
| Scheduler | `f522e8a30b62` | 341764208 |
| Agent | `aeaca46894f4` | 120756876 |
| Scheduler lab | `93f2b7a6bf48` | 358369403 |
| Agent lab | `9f8cb70ac981` | 137362071 |
| Tools | `722e9a9b7cbb` | 104127316 |

Sizes are Docker's local image sizes, not unique disk use or compressed registry
transfer sizes. Image layers are shared. Builds record content provenance; they
do not claim identical image IDs across repeated builds.

## Cluster qualification

The complete image-backed acceptance run **passed on 2026-09-10** at
`.pi-lab/cut01-qualification`, run identity `288d89fdab40cdf1`.
Detailed case evidence is
`.pi-lab/cut01-qualification/evidence/c1789078447-result.json`.
All 23 cases passed: authentication rejection, three complete recovery and
isolated backup/restore rounds, and a 600.724-second mixed workload with 20
single-launch batches and two service replicas. The final state contains 40
terminal attempts with cleanup complete and no retained reservations.
`qualification.json` reports success and zero remaining containers/networks;
an independent Docker query confirmed that cleanup and zero packaging probes.
The original MVP still has both agents reachable and both physical HTTP probes
passing, with its original three attempts retained.

The committed [evidence ledger](cut01-evidence.json) records case results, full
image IDs, source/report hashes and the paths/hashes of the complete private
local evidence. The qualified code is commit `67cfb055f`; this report and ledger
are subsequent documentation and do not change its build/runtime sources.

Independent runtime inspection confirmed that the scheduler, both agents and
both proxies use their recorded bundle image IDs with read-only root filesystems.
They bind only their private configuration, TLS, state, work, evidence and control
paths. There are no executable, JRE or JAR bind mounts. The same rule applies to
the three isolated restore schedulers.

## Review and retained limits

Luna handled bounded image scaffolding and archive tests; Astra reviewed the Java
runtime boundary, bundle runner and bootstrap/cleanup behavior. Parent review
corrected actual archive/link handling and strengthened the archive comparison
against the pinned base. Real integration found Docker's repository digest
normalization and BuildKit's inability to use a bare config ID in `FROM`; the
final build verifies a unique local parent reference before and after building,
and uses immutable image IDs for all runtime containers.

The original `.pi-lab/cluster-mvp` remains separate. A node exchange briefly timed
out during concurrent image building; it recovered without intervention, with
both original service HTTP probes passing and the original attempts retained.
No MVP container was restarted or replaced. Home Assistant was not changed.

Local master, fork master and upstream master were verified at
`11ebaeeb071cb182c388a40755e84f60dda32260` on 2026-09-10. Implementation remains on
`codex/standalone-foundations`; it was not pushed onto master.

The [MVP limits](CLUSTER_MVP.md#scope) still apply: one scheduler, two statically
enrolled agents, bounded history, no arbitrary container-loss adoption or rolling
updates, and reservation-only memory accounting on this Pi. Runtime isolation
from Mesos is enforced; repository-wide retirement/migration is a later gate.
CUT-01 is complete. JAVA-01 can now modernize the native Java/Gradle build while
rerunning these exact runtime and cluster checks.
