# Native workload fixture

This is a one-process, stdlib-only fixture for the native lab. It has no daemon
mode and accepts no ambient secret or filesystem path. Batch and service runs need an
explicit absolute `--evidence` path; launch evidence is appended as JSON and
fsynced to that private regular, non-symlink file and its parent directory.
The evidence file is capped at 1 MiB and locked during append. Its parent
directories must be trusted and must not be changed concurrently. This Linux
fixture checks paths and uses no-follow/nonblocking opens; it is not a general
filesystem sandbox.

Batch example:

```sh
native-workload --mode batch --identity batch-a --evidence /run/fixture/evidence.jsonl --exit-code 0
```

Service mode binds the requested IPv4 address and exact port. It never falls
back to another port. `/ready` returns 503 until `--ready-delay-ms` elapses;
`/identity` returns the explicit identity. `--refuse-sigterm` is for bounded
escalation tests. Every received TERM/INT appends a signal record before it is
handled; refused TERM still permits subsequent INT. Count launches by records
whose `event` is `launch`, rather than by total evidence lines.

Probe mode performs bounded HTTP requests to an already running service and
prints only `{identity,ready}` JSON. It requires `--address` and `--port`, has
no evidence side effect, and treats either 200 or 503 from `/ready` as valid:

```sh
native-workload --mode probe --address 127.0.0.1 --port 20100
```

`--mode port-check --port 20100` briefly binds and closes that exact TCP4 socket,
emitting `{"available":true,"port":20100}` only on success. The container harness
uses this physical check after Stop; an HTTP request failure alone does not prove
that the socket was released.
