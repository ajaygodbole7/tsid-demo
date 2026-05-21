# Node ID Assignment on Kubernetes

## Kubernetes Context

Kubernetes runs application instances as pods. A pod can be deleted, rescheduled, or replaced during rollouts, failures, evictions, and scaling events.

Most Java services run pods through one of two controllers:

- **Deployment.** Creates interchangeable pods. The common default for stateless services. Pod names include random suffixes — `orders-7d8c9bfb59-abc12` — and are not stable identities.
- **StatefulSet.** Creates pods with stable ordinal identity. A StatefulSet named `orders` creates pods named `orders-0`, `orders-1`, and so on.

AWS EKS workloads use Deployments by default. The rest of this document is structured around that case, with StatefulSet covered as an alternative where it applies.

---

## TSID Requirement

TSID reserves 10 bits for a node ID. Every pod that generates IDs needs a unique number between 0 and 1023.

The node ID does not need to be permanent. It needs to be unique across pods that may write IDs to the same table or collection at the same time.

The primary operational risk is uncoordinated assignment. Two live pods using the same node ID share TSID counter space, which permits duplicate IDs under concurrent writes.

---

## Where Uniqueness Matters

Node IDs only need to be unique among generators writing to the **same** table, collection, event stream, or global ID namespace.

In a distributed ecommerce system, `orders`, `payments`, and `shipping` microservices each write to their own tables.

In that setup, `orders-3` and `payments-3` can both use node ID `3`. They are not writing IDs into the same destination.

That changes when multiple services write into a shared table, collection, event stream, or global namespace. Node IDs must then be unique across all contributing pods.

For shared destinations, hashing is not reliable. Use an explicit assignment scheme.

A common pattern is to partition the 10-bit field — some bits for service ID, the rest for pod ordinal:

- 4 bits for service ID → up to 16 services
- 6 bits for pod ordinal → up to 64 pods per service

```java
if (serviceId < 0 || serviceId >= 16 || podOrdinal < 0 || podOrdinal >= 64) {
    throw new IllegalArgumentException("TSID node partition out of range");
}
int nodeId = (serviceId << 6) | podOrdinal;
```

The rest of this document covers the common case: each service writes to its own tables, so node IDs only need to be unique within a single service's pod fleet.

---

## Recommended Pattern for EKS Deployments: Pod UID Hash + Retry

The recommended approach for EKS Deployments: hash the pod UID into the 10-bit node space.

Kubernetes assigns a pod UID to every pod — a 128-bit UUID regenerated for every new pod, including replacements. Hashing it into `[0, 1023]` produces a stable per-pod node ID for the pod's lifetime.

Node ID duplicates are an expected outcome of this approach at scale and are handled by design. When a duplicate produces an actual TSID collision, the database rejects the insert via the primary key constraint, and the application retries with a fresh TSID.

This requires no coordination service, no admission webhook, and no external state.

### Why It Works: Collision Math

A duplicate TSID requires three simultaneous conditions, not one:

1. Two pods share a node ID.
2. They generate an ID in the same millisecond.
3. Their per-millisecond counters land on the same value.

The first condition depends on pod count and follows a birthday distribution. The second and third depend on write rate. All three must coincide for the database to see a duplicate.

#### Step 1 — Pod count and node ID duplication

Node IDs are drawn from a 1,024-slot space by hashing pod UUIDs. The birthday probability of any duplicate across N pods:

| Pods per service | P(duplicate node ID) |
| ---------------: | -------------------: |
|               10 |                4.4 % |
|               20 |                 17 % |
|               40 |                 54 % |
|               50 |                 71 % |
|              100 |                 99 % |

These numbers describe how often the first condition is satisfied. On its own, a duplicate node ID is harmless — it becomes a TSID collision only when the second and third conditions also hold.

#### Step 2 — Write rate and TSID collision

Two pods sharing a node ID, each generating at rate *R* per second, hit the same millisecond with probability approximately `R² / 1000` per second.

Within that millisecond, the 12-bit counter provides 4,096 distinct values. The probability they also collide on counter is `1 / 4096`.

Combined collision rate per pair of duplicate-node pods per second of concurrent generation:

| Writes/sec per pod | Collisions per second | Mean time between collisions |
| -----------------: | --------------------: | ---------------------------: |
|                 10 |           2.4 × 10⁻⁵  |                    ~11.5 hr |
|                100 |           2.4 × 10⁻³  |                    ~7 min   |
|                500 |                  0.06 |                    ~16 sec  |
|              1,000 |                  0.24 |                    ~4 sec   |
|              5,000 |                   6.1 |                  continuous |

These rates apply per pair of pods sharing a node ID, not across the entire fleet. At 10–100 writes per second per pod, the mean time between collisions is hours per duplicate-node pair. The database rejects the rare collision via the primary key constraint; the next `generate()` call advances the counter, so the retry produces a different ID.

### Implementation: Deployment Manifest

The Kubernetes Downward API injects the pod UID and pod name into environment variables. No init containers, sidecars, additional volumes, or ServiceAccounts are required.

The platform Helm chart already provides these. The relevant snippet from the deployment template:

```yaml
env:
  - name: POD_NAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
  - name: CONTAINER_ID         # platform naming — value is the pod UID
    valueFrom:
      fieldRef:
        fieldPath: metadata.uid
```

`CONTAINER_ID` is a platform naming convention; the underlying value is `metadata.uid` — the Kubernetes-assigned pod UID, not a container identifier.

A complete Deployment manifest:

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
     * Creates the TSID factory used to generate primary keys.
     *
     * TSID layout: [42-bit timestamp][10-bit node ID][12-bit counter].
     *
     * The node ID must be unique across pods writing to the same table
     * simultaneously. This implementation derives it by hashing the pod
     * UUID down to 10 bits.
     *
     * Both environment variables are injected by the Kubernetes Downward
     * API, configured in the Helm chart deployment template.
     *
     * @param podUid   metadata.uid — the Kubernetes-assigned pod UUID.
     *                 The environment variable is named CONTAINER_ID by
     *                 platform convention; the underlying value is the
     *                 pod UID, not a container identifier.
     * @param podName  metadata.name — fallback if the pod UID is unavailable.
     */
    @Bean
    public TSID.Factory tsidFactory(
            @Value("${CONTAINER_ID:}") String podUid,
            @Value("${POD_NAME:}") String podName) {

        // The pod UID provides 128 bits of entropy. The pod name varies
        // only in its 5-character suffix (~14 bits) and serves as a fallback.
        String source = !podUid.isBlank() ? podUid : podName;

        // Missing both env vars indicates a Downward API misconfiguration.
        // Fail fast rather than generate IDs from a default node value.
        if (source.isBlank()) {
            throw new IllegalStateException(
                "Neither CONTAINER_ID nor POD_NAME is set. " +
                "Check the Downward API configuration in the Helm chart.");
        }

        // hashCode() can return negative integers for UUID strings.
        // Math.floorMod returns a non-negative result for a positive divisor;
        // the % operator preserves the sign and would produce an invalid
        // negative node ID.
        int nodeId = Math.floorMod(source.hashCode(), 1024);

        log.info("TSID node ID {} derived from pod identifier {}", nodeId, source);

        return TSID.Factory.builder().withNode(nodeId).build();
    }
}
```

### Implementation: Retry on Collision

The retry is safe for three reasons:

1. The insert is the first side effect of the operation. There is no partial state to roll back.
2. TSID generation completes in microseconds.
3. The next `generate()` call advances the counter, so the retry produces a different ID and the duplicate-key error does not recur.

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

> Emit `tsid.collision.retry` to a metrics system. A sustained rate above approximately 1 retry per 10,000 inserts indicates that write volume has increased substantially or the service has outgrown the pattern. Investigate node assignment in either case.

---

## StatefulSet Alternative

Services running as StatefulSets have a simpler option: use the pod ordinal directly.

Kubernetes assigns stable, sequential ordinals to StatefulSet pods — `orders-0`, `orders-1`, and so on. Each ordinal is unique within the StatefulSet and stable for the pod's lifetime.

Using the ordinal as the node ID eliminates the need for hashing or retry.

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

Default StatefulSet ordinals work directly when they are within `[0, 1023]`.

When `.spec.ordinals.start` is configured or replicas can exceed 1024, map the ordinal into an assigned node range. Validate the mapped value against `[0, 1023]` at startup.

StatefulSets are appropriate when a service requires stable identity for other reasons — persistent volumes attached to specific pods, ordered rollouts, sticky network identity. The simpler node ID assignment is a secondary benefit, not a sufficient reason to adopt StatefulSets.

---

## What Not To Do: Static Environment Variable

A static environment variable in the Deployment template is not a valid solution:

```yaml
env:
  - name: TSID_NODE
    value: "7"
```

Every replica receives node ID `7`. All pods share counter space. Duplicate IDs are guaranteed under concurrent write load.

Node ID assignment must vary per pod. It cannot be specified in a static field of the pod template.

---

## Other Coordinated Deployment Patterns

For most services, pod UID hash + retry is sufficient. Services with stricter requirements — sustained write rates in the thousands per second per pod, or shared global namespaces — require coordinated node assignment.

Valid coordinated patterns:

- A platform controller injects `TSID_NODE` during pod creation and tracks live assignments to prevent reuse while an old pod can still generate IDs.
- A mutating admission webhook assigns `TSID_NODE` from an approved range tracked against a 1,024-slot pool.
- Multiple single-replica Deployments, each with an explicit non-overlapping `TSID_NODE`.

All three add infrastructure or operational overhead. They are appropriate when write profile or namespace structure makes hash + retry unsuitable.

The constraint is the same as everywhere: live pods writing to the same destination must not share node IDs.

---

## Beyond 1024 Pods

When more than 1024 concurrent generators write to the same destination, the 10-bit node field is exhausted.

Partition the namespace by tenant, service, or region shard. Each partition has its own 1,024-slot space.

Partitions cannot be merged retroactively. When all IDs must occupy a single global namespace and concurrent writers exceed 1024, the 10-bit field is the upper limit.

---

## When to Use UUIDv7 Instead

TSID with pod UID hash + retry is appropriate for most microservices. It is not appropriate for all.

Use UUIDv7 when:

- The service sustains thousands of writes per second per pod, where same-millisecond collision rates become latency-visible
- The service runs hundreds of pods, where the birthday probability of duplicate node IDs approaches certainty within a single service
- Node ID coordination is unacceptable for organizational or operational reasons

UUIDv7 has no node field and no coordination requirement. The tradeoffs are 8 additional bytes per ID and partial sequencing — 74 random bits within each millisecond instead of strict counter monotonicity.

The decision is per service. TSID is appropriate where the math supports it; UUIDv7 is appropriate where it does not.
