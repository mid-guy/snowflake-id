# snowflake-id

Twitter-style Snowflake ID generator in Go, with a Redis-lease worker-ID assigner and an HTTP server.

## Layout

```
.
├── cmd/snowflake-server/   # HTTP service exposing /id, /decode, /healthz
├── snowflake/              # Core 64-bit ID generator (thread-safe)
├── workerid/               # Redis SETNX + TTL lease for safe worker IDs
└── Makefile
```

## ID layout

```
| 1 sign | 41 timestamp (ms since epoch) | 10 worker | 12 sequence |
```

- Epoch: `2024-01-01 00:00:00 UTC` (overridable via `snowflake.WithEpoch`)
- Worker IDs: `0..1023`
- Sequence: `0..4095` per millisecond per worker

## Quick start

```bash
make test                      # unit + race tests
make build && ./bin/snowflake-server --redis localhost:6379
curl localhost:8080/id
curl 'localhost:8080/decode?id=<id-from-above>'
```

Static worker ID (skip Redis):

```bash
./bin/snowflake-server --worker-id 7
```

## Why a Redis lease instead of `INCR % 1024`?

`INCR` is monotonic. After 1024 deploys/restarts, `INCR % 1024` wraps and can hand the same worker ID to two live nodes — duplicate IDs follow.

The lease approach in [workerid/](workerid/) instead does:

1. Scan keys `snowflake:worker:0..1023`, claim the first free slot via `SET NX EX`.
2. A background goroutine refreshes the TTL (CAS via Lua, only if we still own it).
3. On shutdown, delete the key (CAS via Lua) so the slot returns to the pool.
4. If the process dies, the TTL expires and the slot is reusable — no permanent loss, no collisions.

## Notes on the original snippet

This project applies the fixes from the code review of the original single-file version:

- Redis `INCR % 1024` replaced with a TTL lease.
- Clock-backwards: small drift (≤ 5ms) is waited through; only large drift returns an error.
- Concurrency: the test suite actually exercises the mutex with parallel goroutines and a uniqueness check.
- Adds `Decode()` for debugging and an HTTP surface for real use.
