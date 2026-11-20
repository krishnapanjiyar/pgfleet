# pgfleet — PostgreSQL fleet provisioning & verification CLI

A small, dependency-free Go CLI that provisions, configures and **verifies** a fleet of containerised
PostgreSQL instances on Linux: replication setup, snapshot backups, WAL integrity checks, restore
validation and failover-readiness (split-brain / replica-lag) checks. Every command runs its per-node
work in parallel on a goroutine worker pool and emits structured JSON logs.

```
go install github.com/krishnapanjiyar/pgfleet/cmd/pgfleet@latest
```

## Why this exists

"Is my database fleet actually recoverable?" is usually answered by hope. Backups that were never
restored, replicas that silently detached, and a second node that quietly became writable are the
failures that turn an outage into data loss. `pgfleet` turns each of those into a command with an exit
code, so it can run from cron, CI, or a pre-maintenance runbook:

| Question | Command |
|---|---|
| Can I stand the cluster up from nothing? | `pgfleet provision` |
| Is every node up, correctly configured, and in its intended role? | `pgfleet verify [--wal]` |
| Do I have a backup that passes its manifest checksums? | `pgfleet backup` |
| Does that backup actually boot? | `pgfleet restore --snapshot <name>` |
| If the primary died now, could I promote a replica safely? | `pgfleet failover-check` |

## Install

Requires Go 1.22+ and, for the real driver, Docker on Linux.

```sh
git clone https://github.com/krishnapanjiyar/pgfleet && cd pgfleet
make build          # -> bin/pgfleet
make test           # unit + mock-cluster integration tests (race detector on)
```

## Core commands

All commands read `fleet.json` (see [`examples/fleet.json`](examples/fleet.json)). Exit status: `0` all
checks passed, `1` a check failed, `2` usage/config error. Human table on stdout (`-json` for machines),
JSON logs on stderr.

```sh
export PGFLEET_PASSWORD=...                 # superuser + replication password
pgfleet provision      -config fleet.json   # primary first, then replicas cloned via pg_basebackup -R
pgfleet verify         -config fleet.json -wal
pgfleet backup         -config fleet.json   # prints snapshot=snap-20260102T030405Z
pgfleet restore        -config fleet.json -snapshot snap-20260102T030405Z
pgfleet failover-check -config fleet.json -max-lag-bytes 16777216
```

| Command | What it does |
|---|---|
| `provision` | Creates a Docker network, starts the primary with `wal_level=replica`, `archive_mode=on`, replication slots; creates the `replicator` role + `pg_hba` entry; clones replicas in parallel with `pg_basebackup -R -X stream`. |
| `verify` | Per node: `pg_isready`, `SELECT 1`, `wal_level`, and that `pg_is_in_recovery()` matches the configured role. `-wal` additionally decodes every WAL record from the last checkpoint's REDO location to the current flush/receive LSN with `pg_waldump`. |
| `backup` | `pg_basebackup -X stream` on the primary into `backup_dir`, then `pg_verifybackup` against the manifest checksums. |
| `restore` | Boots a throw-away server from the snapshot, waits for readiness, runs `SELECT 1`, asserts recovery finished, and removes the container (`-keep` to inspect it). Names containing `/` are rejected. |
| `failover-check` | Detects **split-brain** (more than one writable node), no primary, detached replicas (`pg_stat_replication`), replay lag in bytes (LSN arithmetic across the 4 GiB boundary), and names the best **promotion candidate**. |

Drivers: `-driver docker` (default) or `-driver mock`, an in-memory cluster that understands exactly the
commands `pgfleet` issues — useful for demos and CI without Docker:

```sh
pgfleet verify -config examples/fleet.json -driver mock -wal
```

## Architecture

```
cmd/pgfleet            flag parsing, table/JSON output, exit codes
internal/fleet         config validation + operations (provision, verify, backup, restore, failover-check)
internal/pool          generic ordered worker pool: Map[T,R](ctx, workers, items, fn)
internal/runner        Runner interface:  Docker(args...)  /  Exec(container, argv...)
    ├── DockerRunner   shells out to the docker CLI
    └── MockRunner     in-memory cluster with fault injection (crash, lag, split-brain, corrupt WAL/backup)
internal/pg            LSN parsing/formatting/lag, pg_controldata parsing
tools/                 Python helpers: summarize_logs.py (latency stats from JSON logs), gen_fleet.py
```

**Worker pool design.** `pool.Map` starts `min(workers, len(items))` goroutines that pull indices from an
unbuffered channel and write into a pre-sized result slice, so results come back in input order without
locks, concurrency is hard-bounded, and `Map` only returns after every worker has exited (no leaks).
`verify` and `failover-check` fan out one task per node; `provision` fans out replica cloning. Within a
node, checks run sequentially and short-circuit (no point running `SELECT 1` on a node that isn't ready).

**Structured logging.** Every check emits one `slog` JSON line (`node`, `check`, `ok`, `latency_ms`,
`detail`) and every command ends with a `command_done` line:

```json
{"time":"...","level":"INFO","msg":"check","node":"pg-replica-1","check":"replay_lag","ok":true,"latency_ms":1.13,"detail":"4096 bytes behind"}
```

`python3 tools/summarize_logs.py run.log` turns a log into per-check p50/p95/max.

**Shell integration.** Plain exit codes and `-json` make it composable:
`pgfleet failover-check -json | jq -r .candidate`, or `pgfleet verify || page-someone`.

## Benchmark notes

`make bench` runs `pgfleet bench`, which drives a synthetic **32-node mock cluster** with **8 workers**
and **1 ms of simulated latency per `docker exec`**. Measured in a 2-core sandbox (20 iterations):

| Command | p50 | p95 | max |
|---|---|---|---|
| `backup` | 2.2 ms | 2.2 ms | 2.2 ms |
| `failover-check` | 15.8 ms | 20.3 ms | 20.3 ms |
| `verify` | 17.9 ms | 18.8 ms | 18.8 ms |
| `verify -wal` | 31.4 ms | 35.0 ms | 35.0 ms |

The integration test `TestCommandLatencyUnder50msOnMockCluster` asserts every command stays under 50 ms
on a 16-node mock cluster. **What this does and doesn't show:** it measures the CLI's own orchestration
overhead (scheduling, parsing, logging) given a 1 ms per-command backend — it validates the concurrency
design, not PostgreSQL or Docker performance. Against real containers, latency is dominated by
`docker exec` (typically tens of ms each), so expect real-cluster numbers to be higher.

## Status and caveats

- The mock-cluster logic is covered by tests (`go test -race ./...`: config validation, provision→verify,
  fault detection, backup/restore round-trip incl. corrupt/unknown snapshots, split-brain, lag, detached
  replicas, worker-pool ordering/bounds, LSN math).
- The **Docker driver has not been exercised against a live Docker daemon** in the environment this was
  generated in — the container scripts (`provision` replica bootstrap, `restore`) follow the official
  `postgres` image conventions (`gosu`, `PGDATA`) but treat a first real run as a smoke test.
- `backup_dir` is bind-mounted into containers; it must be writable by the container's `postgres` user (uid 999 in the official image).
- Replication is physical streaming with one slot per replica; no automatic promotion is performed — `failover-check` only reports readiness.

## License

MIT
