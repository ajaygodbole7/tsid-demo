# Benchmarks

Numbers from `./bench.sh` against the local demo stack.

- **Host/runtime**: Apple Silicon, local Podman stack.
- **Database versions**: Postgres 18 and MongoDB 8.2.7.
- **Container config**: default `infra/docker-compose.yml`.
- **Database tuning**: no explicit Postgres buffer/cache tuning and no explicit MongoDB WiredTiger cache tuning.
- **Podman resources**: host default Podman machine resources.
- **Scale**: N rows per id type from 100K to 1M in 100K increments.
- **Measurements**: insert time, storage, and 100,000 random PK reads per id type per N after 10,000 discarded warmup reads.
- **Bias control**: phase order rotates per N so no id type always faces a cold cache.
- **Tables**: each section shows 100K, 500K, 1M, plus the median across all ten levels.
- **Runtime**: rerunning the benchmark takes about 30 minutes.
- **Stability**: read percentiles are stable within roughly 5%; absolute insert times vary by roughly 30% because of host noise.

> Tables show 100K / 500K / 1M. Each "Median across N" is the median across all 10 levels.

## Summary

- **TSID has smaller indexes in this benchmark**: it uses an 8-byte integer key. UUID keys are 16 bytes.
- **Postgres UUIDv7 indexes were larger in this run**: time ordering helps write locality, but the index is still 1.40x the TSID index.
- **UUIDv4 produced the largest indexes in these measurements**: random 16-byte keys produce the largest indexes in both databases.
- **MongoDB ObjectId remains compact**: ObjectId beats TSID on `_id` index size because it is native, ordered, and 12 bytes.
- **Median read latency was close across tested ID types**: the read results are similar across most row counts.
- **The 1M-row Postgres P99 spike is local-run evidence**: it is one data point, not a universal threshold.
- **Generation throughput is unlikely to determine ID choice for typical request-path usage**: both TSID and `UUID.randomUUID()` are fast enough for this use case.
- **TSID fits workloads that prioritize compact indexes and short external identifiers**: it provides compact SQL and MongoDB indexes plus 13-character external IDs.
- **UUIDv7 fits workloads where avoiding node-id assignment is more important than index size**: UUIDv7 avoids node-id assignment at the cost of larger indexes.

## Postgres

Insert time (ms, batched 1,000 per `executeBatch()`):

| N | TSID | UUIDv4 | UUIDv7 |
|---|---|---|---|
| 100K | 633 | 687 | 719 |
| 500K | 2,797 | 3,539 | 3,313 |
| 1M | 5,843 | 7,605 | 6,462 |

Median across N: TSID 3,113 ms, UUIDv4 4,031 ms (+29%), UUIDv7 3,632 ms (+17%).

Index size:

| N | TSID idx | UUIDv4 idx | UUIDv7 idx | v4 / TSID | v7 / TSID |
|---|---|---|---|---|---|
| 100K | 2,208 KB | 4,312 KB | 3,104 KB | 1.95x | 1.41x |
| 500K | 10,992 KB | 19,648 KB | 15,416 KB | 1.79x | 1.40x |
| 1M | 21,960 KB | 38,688 KB | 30,824 KB | 1.76x | 1.40x |

Median ratio across N (TSID = 1.00x): UUIDv4 1.85x, UUIDv7 1.40x (consistent across tested row counts).

Read P95 / P99 (ms, random PK lookups):

| N | TSID P95 | TSID P99 | UUIDv4 P95 | UUIDv4 P99 | UUIDv7 P95 | UUIDv7 P99 |
|---|---|---|---|---|---|---|
| 100K | 0.39 | 0.67 | 0.41 | 0.64 | 0.41 | 0.64 |
| 500K | 0.38 | 0.57 | 0.39 | 0.60 | 0.40 | 0.59 |
| 1M | 0.35 | 0.60 | 1.54 | 4.35 | 1.57 | 4.45 |

Median across N:

- TSID: P95 0.388 ms / P99 0.585 ms.
- UUIDv4: P95 0.394 ms / P99 0.556 ms.
- UUIDv7: P95 0.398 ms / P99 0.568 ms.

Notes:

- Median read latency clusters within roughly 5%.
- In this local run, at 1M rows UUIDv4 and UUIDv7 P99 spike to about 4.4 ms while TSID stays at 0.60 ms.
- Larger UUID indexes may contribute, but this benchmark does not isolate causality.
- The exact threshold depends on memory, cache, container, and host settings.

## MongoDB

Insert time (ms, batched 1,000 per `insertMany`):

| N | TSID | UUIDv4 | UUIDv7 | ObjectId |
|---|---|---|---|---|
| 100K | 856 | 792 | 697 | 900 |
| 500K | 3,620 | 4,571 | 3,450 | 3,461 |
| 1M | 7,778 | 9,592 | 8,227 | 7,439 |

Median across N: TSID 4,003 ms, UUIDv4 5,219 ms (+30%), UUIDv7 4,221 ms (+5%), ObjectId 4,015 ms (about 0%).

Index size:

| N | TSID idx | UUIDv4 idx | UUIDv7 idx | OID idx | v4 / TSID | v7 / TSID | OID / TSID |
|---|---|---|---|---|---|---|---|
| 100K | 1,172 KB | 2,464 KB | 1,892 KB | 976 KB | 2.10x | 1.61x | 0.83x |
| 500K | 6,028 KB | 13,224 KB | 9,672 KB | 5,104 KB | 2.19x | 1.60x | 0.85x |
| 1M | 12,320 KB | 27,760 KB | 19,668 KB | 10,492 KB | 2.25x | 1.60x | 0.85x |

Median ratio across N (TSID = 1.00x): UUIDv4 2.22x, UUIDv7 1.60x, ObjectId 0.85x.

Read P95 / P99 (ms, random `_id` lookups):

| N | TSID P95 | TSID P99 | UUIDv4 P95 | UUIDv4 P99 | UUIDv7 P95 | UUIDv7 P99 | OID P95 | OID P99 |
|---|---|---|---|---|---|---|---|---|
| 100K | 1.03 | 2.09 | 1.89 | 5.06 | 0.70 | 1.27 | 0.58 | 0.92 |
| 500K | 0.48 | 0.61 | 0.49 | 0.74 | 0.53 | 0.90 | 0.50 | 0.65 |
| 1M | 0.47 | 0.62 | 0.49 | 0.62 | 0.48 | 0.60 | 0.48 | 0.56 |

Median across N (ms): TSID P95 0.485 / P99 0.616; UUIDv4 P95 0.504 / P99 0.711; UUIDv7 P95 0.499 / P99 0.669; ObjectId P95 0.493 / P99 0.637.

## Concurrent id-generation throughput

From `./run.sh` section 11.

- 1,000,000 ids split across N virtual threads.
- Threads are released simultaneously.
- TSID's per-node counter ceiling is 4,096 ids/ms.

**TSID** (lock-protected counter):

| Threads | Throughput | P50 | P95 | P99 | Collisions |
|---|---|---|---|---|---|
| 1 | 13.3 M/s | 42 ns | 84 ns | 125 ns | 0 |
| 16 | 5.4 M/s | 42 ns | 459 ns | 43,959 ns | 0 |
| 64 | 5.5 M/s | 42 ns | 4,500 ns | 243,667 ns | 0 |
| 256 | 4.9 M/s | 42 ns | 627,625 ns | 974,250 ns | 0 |

**UUID.randomUUID()** (UUIDv4, `SecureRandom`-backed):

| Threads | Throughput | P50 | P95 | P99 | Collisions |
|---|---|---|---|---|---|
| 1 | 4.5 M/s | 125 ns | 208 ns | 375 ns | 0 |
| 16 | 3.0 M/s | 334 ns | 1,625 ns | 42,875 ns | 0 |
| 64 | 3.1 M/s | 334 ns | 1,584 ns | 7,834 ns | 0 |
| 256 | 3.0 M/s | 334 ns | 1,708 ns | 10,584 ns | 0 |

Notes:

- TSID throughput is about 3x higher in this run.
- TSID pays a tail-latency cost at high contention because of the locked counter.
- At 256 threads, TSID P99 is about 1 ms.
- UUID's tail stays flatter. At 256 threads, UUID P99 is about 10 us.
- A 10K req/s service at 50 ms/request holds about 50 ids in flight. Both generators are well within that budget.

## Criteria comparison

| Criterion | TSID | UUIDv4 | UUIDv7 | ObjectId |
|---|---|---|---|---|
| Bytes per id | 8 | 16 | 16 | 12 |
| Postgres index size (vs TSID) | 1.00x | 1.85x | 1.40x | n/a (stored as 12-byte opaque blob) |
| Mongo `_id` index size (vs TSID) | 1.00x | 2.22x | 1.60x | 0.85x |
| Built-in human form | Crockford Base32, 13 chars | hyphenated hex, 36 chars | hyphenated hex, 36 chars | hex, 24 chars |
| Type prefixing (e.g. `ORD-...`) | yes (`format()`) | no | no | no |
| Reversible from human form | yes (`unformat()`) | no | no | no |
| Standardization | library convention | RFC 9562 | RFC 9562 | BSON spec |
| Coordination-free generation | no (needs per-pod node ID) | yes | yes | yes |
| Time ordering | sequential within and across ms (per node) | random | sequential across ms; random within ms | sequential across seconds |
