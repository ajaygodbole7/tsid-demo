# Node ID Assignment on Kubernetes

## Kubernetes Context

Kubernetes runs application instances as pods. A pod can be deleted, rescheduled, or replaced during rollouts, failures, evictions, and scaling events.

Most Java services run pods through one of two controllers:

- **Deployment.** Creates interchangeable pods. The common default for stateless services. Pod names include random suffixes — `orders-7d8c9bfb59-abc12` — and are not stable identities.
- **StatefulSet.** Creates pods with stable ordinal identity. A StatefulSet named `orders` creates pods named `orders-0`, `orders-1`, and so on.

AWS EKS workloads use Deployments by default. The rest of this document is structured around that case, with StatefulSet treated as the cleaner alternative when it's available.

---

## TSID Requirement

TSID reserves 10 bits for a node ID. Every pod that generates IDs needs a unique number between 0 and 1023.

The node ID does not need to be permanent. It needs to be unique across pods that may write IDs to the same table or collection at the same time.

The primary operational risk is uncoordinated assignment. If two live pods use the same node ID, they share the same TSID counter space — which creates a chance of duplicate IDs under concurrent writes.

---

## Where Uniqueness Matters

Node IDs only need to be unique among generators writing to the **same** table, collection, event stream, or global ID namespace.

In a distributed ecommerce system, `orders`, `payments`, and `shipping` microservices each write to their own tables.

In that setup, `orders-3` and `payments-3` can both use node ID `3`. They're not writing IDs into the same destination.

That changes if multiple services write into a shared table, collection, event stream, or global namespace. Then node IDs must be unique across all of them.

For shared destinations, don't rely on hashing. Use an explicit assignment scheme.

A common pattern is to partition the 10-bit field — some bits for service ID, the rest for pod ordinal:

- 4 bits for service ID → up to 16 services
- 6 bits for pod ordinal → up to 64 pods per service

```java
if (serviceId < 0 || serviceId >= 16 || podOrdinal < 0 || podOrdinal >= 64) {
    throw new IllegalArgumentException("TSID node partition out of range");
}
int nodeId = (serviceId << 6) | podOrdinal;
```

The rest of this document covers the much more common case: each service writes to its own tables, so node IDs only need to be unique within a single service's pod fleet.

---

## Recommended Pattern for EKS Deployments: Pod UID Hash + Retry

The approach that works without adding any infrastructure: take the pod UID and hash it down into the 10-bit node space.

Kubernetes assigns a pod UID to every pod. It's a full 128-bit UUID, regenerated for every new pod including replacements. Hashing it into `[0, 1023]` gives a stable per-pod node ID for the pod's lifetime.

If two pods happen to hash to the same node ID — which will happen at scale — the database catches the rare collision and the application retries.

No coordination service. No admission webhook. No DynamoDB. No pre-created Kubernetes objects.

### Why It Works: Collision Math

For two pods to produce the same TSID, three things have to happen at once:

1. They share a node ID.
2. They generate in the same millisecond.
3. Their counters land on the same value within that millisecond.

The first is a birthday probability — likely at scale. The second and third depend on write rate.

#### Step 1 — How often do two pods get the same node ID?

| Pods per service | P(duplicate node ID) |
| ---------------: | -------------------: |
|               10 |                4.4 % |
|               20 |                 17 % |
|               40 |                 54 % |
|               50 |                 71 % |
|              100 |                 99 % |

These numbers look alarming. At 50 pods you're almost guaranteed to have at least one duplicate node ID somewhere in the fleet.

But this isn't the number that matters. It's just the first of three things that need to coincide.

#### Step 2 — Given a duplicate, how often do collisions actually happen?

Two pods sharing a node ID, each generating at rate *R* per second, hit the same millisecond with probability roughly `R² / 1000` per second.

Within that same millisecond, the 12-bit counter gives 4,096 different values. The chance they also pick the same counter is `1 / 4096`.

Multiplying those gives the actual collision rate:

| Writes/sec per pod | Collisions per second | Mean time between collisions |
| -----------------: | --------------------: | ---------------------------: |
|                 10 |           2.4 × 10⁻⁵  |                    ~11.5 hr  |
|                100 |           2.4 × 10⁻³  |                    ~7 min   |
|                500 |                  0.06 |                    ~16 sec   |
|              1,000 |                  0.24 |                    ~4 sec    |
|              5,000 |                   6.1 |                  continuous  |

These rates are per *pair* of pods that share a node ID. Not across the whole fleet.

Realistic write rates for most microservices sit in the 10–100/sec range per pod. At those rates, a collision is a once-a-day event. Nowhere near anything that affects request latency.

> **The whole pattern in one sentence.** Duplicate node IDs are likely. Duplicate TSIDs are rare. The database catches what slips through. The retry succeeds because the next `generate()` call advances the counter.

### Example Implementation: Deployment Manifest

The Kubernetes Downward API injects the pod UID and pod name into environment variables.

No init containers. No sidecars. No extra volumes. No special ServiceAccounts.

Here's a sample Helm chart - deployment template:

```yaml
env:
  - name: POD_NAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
  - name: CONTAINER_ID         # this is actually the pod UID
    valueFrom:
      fieldRef:
        fieldPath: metadata.uid
```

If you're writing the chart from scratch, the full Deployment looks like this:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: orders-service
spec:
  replicas: 8
  template:
    spec:
      containers:
        - name: orders-service
          image: your-service:latest
          env:
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: CONTAINER_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.uid
```

### Implementation: Java Configuration

```java
@Configuration
public class TsidConfig {

    /**
     * Creates the TSID factory used to generate primary keys for this service.
     *
     * TSID is a 64-bit identifier with three parts:
     *     [42-bit timestamp][10-bit node ID][12-bit counter]
     *
     * The node ID must be unique across pods writing to the same table at the
     * same time. We get it by hashing the pod's UUID down into 10 bits (0–1023).
     *
     * The two environment variables below come from the Kubernetes Downward API,
     * configured in the Helm chart's deployment template. Kubernetes injects
     * them into the container at pod startup.
     *
     * @param podUid   Sourced from metadata.uid — the Kubernetes-assigned pod UUID.
     *                 NOTE: the env var is named CONTAINER_ID in our platform's
     *                 Helm chart, but the underlying value is actually the pod
     *                 UID (the Kubernetes metadata.uid field), not a container ID.
     *                 The name is a platform-team naming choice we've inherited.
     * @param podName  Sourced from metadata.name — used only as a fallback if
     *                 the pod UID is somehow missing.
     */
    @Bean
    public TSID.Factory tsidFactory(
            @Value("${CONTAINER_ID:}") String podUid,
            @Value("${POD_NAME:}") String podName) {

        // Prefer the pod UID. It's a full 128-bit UUID — plenty of entropy to
        // hash into 10 bits without producing predictable collisions.
        //
        // The pod name (e.g. "orders-7d8c9bfb59-abc12") only varies in the
        // 5-character suffix, giving roughly 14 bits of entropy. Usable as
        // a fallback, but a worse hash source.
        String source = !podUid.isBlank() ? podUid : podName;

        // If both are missing, the Downward API isn't wired up correctly in
        // the Helm chart. Fail fast at startup — generating IDs with a default
        // node value would cause silent collisions across pods.
        if (source.isBlank()) {
            throw new IllegalStateException(
                "Neither CONTAINER_ID nor POD_NAME is set. " +
                "Check the Downward API configuration in the Helm chart.");
        }

        // Use Math.floorMod, not the % operator. Java's % can return a negative
        // value when hashCode() returns a negative int — which happens routinely
        // for UUID strings. A negative node ID would crash TSID.Factory.
        // floorMod always returns a non-negative result for a positive divisor.
        int nodeId = Math.floorMod(source.hashCode(), 1024);

        log.info("TSID node ID {} derived from pod identifier {}", nodeId, source);

        // Build the TSID factory bound to this pod's node ID. The factory is
        // thread-safe and used as a singleton across the application.
        return TSID.Factory.builder().withNode(nodeId).build();
    }
}
```

### Implementation: Retry on Collision

The retry is safe for three reasons.

The insert is the first side effect of the operation. No partial state to clean up.

Generating a new TSID takes microseconds.

The next call to `generate()` advances the counter. The retry produces a different ID. The duplicate-key error doesn't come back.

```java
@Repository
public class OrderRepository {

    private static final int MAX_RETRIES = 3;

    public Order insert(Order order) {
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            long id = tsidFactory.generate().toLong();
            order.setId(id);
            try {
                return jdbcTemplate.insert(order);
            } catch (DuplicateKeyException e) {
                meterRegistry.counter("tsid.collision.retry").increment();
                if (attempt == MAX_RETRIES - 1) {
                    log.error("TSID collision after {} retries — investigate node ID assignment",
                        MAX_RETRIES);
                    throw e;
                }
                // Next generate() advances the counter; the retry produces a different ID
            }
        }
        throw new IllegalStateException("unreachable");
    }
}
```

> **What to watch.** Emit `tsid.collision.retry` to your metrics system. At normal write volumes you'll see a handful of retries per day per service — that's healthy. If the rate climbs above roughly 1 per 10,000 inserts and stays there, something has changed. Treat it as a prompt to revisit node assignment, not as a routine page.

---

## StatefulSet Alternative

If a service already runs as a StatefulSet, you get a much simpler answer.

Kubernetes assigns stable, sequential ordinals to StatefulSet pods — `orders-0`, `orders-1`, and so on.

You can use that number directly as the node ID. Zero collision risk. No hashing. No retry needed.

```yaml
env:
  - name: POD_NAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
```

```java
String podName = System.getenv("POD_NAME");  // "orders-3"
int nodeId = Integer.parseInt(
    podName.substring(podName.lastIndexOf('-') + 1));
if (nodeId < 0 || nodeId > 1023) {
    throw new IllegalArgumentException("TSID node ID out of range: " + nodeId);
}
TSID.Factory factory = TSID.Factory.builder().withNode(nodeId).build();
```

Default StatefulSet ordinals work directly when they're in `0..1023`.

If `.spec.ordinals.start` is configured or if replicas can exceed 1024, map the ordinal into an assigned node range. Fail fast when the mapped value is outside `0..1023`.

StatefulSets fit when a service has other reasons to need stable identity — persistent volumes attached to specific pods, ordered rollouts, sticky network identity. For those services, just use the ordinal directly.

---

## What Not To Do: Static Environment Variable

A single static env var assigned in the Deployment template is not a solution:

```yaml
env:
  - name: TSID_NODE
    value: "7"
```

Every replica in that Deployment would get node ID `7`. Every pod would share counter space. Duplicate IDs would be guaranteed under any concurrent write load.

The node ID assignment must vary per pod. It can't live in a static pod template field.

---

## Other Coordinated Deployment Patterns

For most services, pod UID hash + retry is sufficient. For services that need stricter guarantees — write rates in the thousands per second per pod, or shared global namespaces — coordinated assignment is the alternative.

Valid patterns that assign a distinct node ID per pod outside the static Deployment template:

- A platform controller injects `TSID_NODE` during pod creation and doesn't reuse it while the old pod can still generate IDs.
- A mutating admission webhook assigns `TSID_NODE` from an approved range tracked against a 1,024-slot pool.
- Multiple single-replica Deployments, each with an explicit, non-overlapping `TSID_NODE`.

All three add infrastructure or operational overhead. They're appropriate when the write profile or namespace structure makes hash + retry inadequate.

The constraint is the same as everywhere else: do not assign the same node ID to live pods that may write to the same destination at the same time.

---

## Beyond 1024 Pods

Beyond 1024 concurrent generators writing to the same destination, the 10-bit node field is exhausted.

The fix is to create separate uniqueness domains whose keys include the tenant, service, or region shard. Each domain has its own 1,024-slot space.

Don't merge those domains later on TSID alone. If all IDs must share one global namespace and exceed 1024 concurrent writers, the 10-bit field is the limit.

---

## When to Use UUIDv7 Instead

TSID with pod UID hash + retry fits most microservices. It doesn't fit everything.

Reach for UUIDv7 when:

- The service sustains thousands of writes per second per pod, where the same-millisecond collision rate becomes visible in latency
- The service runs in the hundreds of pods, where the birthday probability of duplicate node IDs approaches certainty even within one service
- Node ID coordination — for any reason — isn't acceptable

UUIDv7 has no node field and no coordination requirement. The tradeoff is 8 extra bytes per ID and partial sequencing (74 random bits within each millisecond) instead of strict sequencing.

The decision is per service, not global. TSID where it fits, UUIDv7 where it doesn't.