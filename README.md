# tsid-demo

*A demo for [hypersistence-tsid](https://github.com/vladmihalcea/hypersistence-tsid) by Vlad Mihalcea.*

A TSID is a 64-bit time-sortable identifier:

- **Layout**: 42-bit millisecond timestamp, 10-bit node ID, 12-bit counter
- **Wire form**: 13-character Crockford Base32 string
- **Storage**: fits in a Postgres `BIGINT` or a MongoDB `NumberLong _id`
- **Ordering**: time-sortable and sequential per node. Across nodes in the same millisecond, order is deterministic but not causal.
- **Index behavior**: sequential keys concentrate inserts on the right edge of the B-tree, reducing random-page churn

This repo has two parts:

- **Walkthrough** (`./run.sh`): twelve sections; see the Sections table below.
- **Scaling benchmark** (`./bench.sh`): inserts 100K to 1M rows per id type and measures insert time, index size, and read latency. Numbers in [benchmark.md](./benchmark.md).

## Sections

| # | Section |
|---|---------|
| 1 | TSID: 8 bytes, time-sortable, fits in `BIGINT` |
| 2 | Human form: 13-character Base32, prefixable, reversible (vs UUID/ObjectId side-by-side) |
| 3 | Sequential B-tree inserts: pages append at the right edge, reduced random-page splits |
| 4 | Why TSID uses Base32 on the wire: JSON cannot safely represent all 64-bit integers in JavaScript Number |
| 5 | Time-range queries on the TSID PK: no `created_at` column, no second index |
| 6 | Cursor pagination on the TSID primary key: keyset, no `OFFSET`, no second index |
| 7 | Postgres insert + storage comparison: TSID vs UUIDv7 |
| 8 | MongoDB time-range queries on the TSID `_id` (parallel to section 5) |
| 9 | MongoDB insert + storage comparison: TSID vs ObjectId vs UUIDv7 |
| 10 | Idempotent event ingestion via SHA-256 content hash |
| 11 | Concurrent id-generation throughput: TSID vs `UUID.randomUUID()` |
| 12 | Node ID resolution on Kubernetes: from pod name to `withNode(N)` |

## Requirements

- **Java 26** (uses `UUID.ofEpochMillis(long)` for UUIDv7)
- **Maven** on `$PATH`
- **Docker or Podman** running; the stack launches **Postgres 18** (native `uuidv7()` as column DEFAULT) and **MongoDB 8.2** (WiredTiger with Snappy compression)

## Run

```bash
# 1. Start Postgres + MongoDB (Docker or Podman, auto-detected)
./infra/start.sh

# 2. Run the walkthrough (~1 min). Use PAUSE=1 ./run.sh to inspect DB state between sections.
./run.sh

# 3. Run the scaling benchmark (100K to 1M in 100K steps; ~30 min)
./bench.sh

# 4. Tear down
./infra/stop.sh
```

## Credentials

- **Postgres:** `localhost:5432`, db `tsiddemo`, user `demouser`, password `demopass`
- **MongoDB:** `localhost:27017`, root user `demouser`, password `demopass`, `authSource=admin`

## Node ID assignment on Kubernetes

Kubernetes node-id patterns and caveats are covered in [kubernetes.md](./kubernetes.md).

## Benchmarks

Full benchmark results are in [benchmark.md](./benchmark.md). Selected results:

- **Postgres index size** (TSID = 1.00x): UUIDv7 1.40x, UUIDv4 1.85x.
- **Mongo `_id` index size** (TSID = 1.00x): ObjectId 0.85x, UUIDv7 1.60x, UUIDv4 2.22x.
- **Read latency**: medians are within ~5% across id types up to ~500K rows in the local run. At 1M rows, Postgres UUID P99 increased while TSID P99 stayed near lower-N measurements.
- **Concurrent generation**: TSID ~13M ids/s on one thread, ~5M at 256 threads; `UUID.randomUUID()` ~3M at every thread count tested.
