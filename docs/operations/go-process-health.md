# Go process health

The original scheduler's `go-process` executor accepts optional `health` in its
`aurora-process-v1` executor data:

```json
{
  "version": "aurora-process-v1",
  "argv": ["/opt/application/bin/server", "--port", "8080"],
  "env": {},
  "graceMillis": 1000,
  "health": {
    "kind": "tcp",
    "port": 8080,
    "network": "agent-container",
    "intervalMillis": 1000,
    "timeoutMillis": 100,
    "startupTimeoutMillis": 30000,
    "failureThreshold": 3
  }
}
```

All health fields are required when `health` is supplied. This initial subset
probes `127.0.0.1` and requires an IPv4 listener owned by the launched process
group. The application may bind `127.0.0.1` or `0.0.0.0` on the specified port;
`network` must equal the agent's
`--network` domain. The check establishes connectivity, not HTTP response or
application-specific correctness. General dynamically assigned Thrift named ports
remain unsupported in this process profile.

Bounds are: port 1–65535, interval 1–60000 ms, per-probe timeout 1–250 ms, startup
allowance 1–600000 ms, and 1–100 consecutive failed probes after first readiness.
An omitted health object preserves immediate readiness after launch. Existing
native TCP readiness definitions without the two health policy fields preserve
readiness-only behavior.

A configured task reaches scheduler RUNNING only after its first successful
probe. Startup timeout or the configured consecutive failure threshold initiates
process-group termination using `graceMillis`, with escalation and cleanup before
the scheduler receives FAILED. Task events identify `health-startup-timeout` or
`health-check-failed`. A process that exits before its first successful probe fails
with `health-exited-before-ready`, including a zero exit code. This failure enters the existing service replacement and
update failure/rollback policy. Transient failures below the threshold reset on a
successful probe. When the agent runs with `--supervise`, health monitoring
continues in the per-attempt supervisor across daemon loss and recovery. The
Docker lab enables this mode; it is not the CLI default. Graceful daemon shutdown
drains workloads.

Placement reserves the configured socket per agent through both pending launch
acknowledgment and terminal process cleanup. Conflicting tasks try another agent
or remain pending with a health-port reservation veto. Update affinity waits for
the prior attempt's cleanup before reusing its socket. Different network domains
are distinct reservations, but each agent accepts only its configured domain.
