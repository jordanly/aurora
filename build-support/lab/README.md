# Aurora container lab support

`labctl` provides bounded scaffolding for the Raspberry Pi container lab. It does not start services, build images, generate certificates, or change host configuration.

```sh
build-support/lab/labctl preflight
build-support/lab/labctl init --root "$PWD/.pi-lab" --run-id review
build-support/lab/labctl render "$PWD/.pi-lab/review"
build-support/lab/labctl inspect "$PWD/.pi-lab/review"
```

`init` creates a private, marked run root and records the blueprint hash, selected UID/GID, and project name. Generated files explicitly say `unconfigured`; they are scaffolding and contain no credentials. `render` emits an absolute, run-scoped Compose file with an explicit project name after checking the source hash and ownership markers.

`preflight` reports host page size, Docker and Compose client/server availability, cgroup controllers, `memoryControllerAvailable`, and the conservative `hardMemoryEnforcementVerified` field (always false until a later verified enforcement probe). The CLI targets Python 3.8 or newer.

`destroy --confirm` is intentionally unavailable in this slice. It refuses when Docker state cannot be queried and otherwise exits with an unimplemented status without removing anything. `up`, `build`, and scenario execution are not implemented and must not be treated as successful.

Run the daemon-free checks with:

```sh
python3 -m unittest discover -s build-support/lab/tests -v
```

Seven checks cover the CLI and, when the Docker CLI/Compose plugin is installed,
actual Compose parsing for both service profiles, literal paths and project/network
boundaries. The parser check needs no daemon and skips if the client/plugin is absent.
