# Node ID assignment on Kubernetes

## Kubernetes context

Kubernetes runs application instances as pods.

A pod can be deleted, rescheduled, or replaced during rollouts, failures, evictions, and scaling events.

Most Java services run pods through one of these controllers:

- **Deployment**: creates interchangeable pods. This is the common default for stateless services.
  Pod names include generated suffixes and are not stable identities.
- **StatefulSet**: creates pods with stable ordinal identity. A StatefulSet named `orders`
  creates pods named `orders-0`, `orders-1`, and so on.

## TSID requirement

TSID is viable on Kubernetes when each live pod writing to the same table or collection has a unique node ID.

The node field is 10 bits, so valid node IDs are 0 to 1023.

The node ID does not need to be permanent.

It needs to be unique across pods that may write IDs to the same table or collection at the same time.

The primary operational risk is uncoordinated node-id assignment. If two live pods use the same node ID, they share the same TSID counter space.

Recommended choices:

- Prefer a StatefulSet ordinal when pod identity can be stable.
- Use Deployments only when the platform assigns a distinct node ID per pod before startup.
- Use UUIDv7 when node-id coordination is not acceptable.

## Where uniqueness matters

Node IDs only need to be unique among generators that write to the same table or collection.

For example, in a distributed ecommerce system, `orders`, `payments`, and `shipping` microservices may each write to their own tables or collections.

In that setup, `orders-3` and `payments-3` can both use node ID `3`. They are not writing IDs into the same table or collection.

That changes if multiple services write IDs into the same table, collection, event stream, or global ID namespace.

Then their node IDs must be unique across all of those services.

For shared tables, collections, streams, or namespaces, do not rely on hashing the full hostname. Use an explicit assignment scheme instead.

One common pattern is to reserve part of the node field for the service and part for the pod ordinal.

Example with 10 node bits:

- 4 bits for service ID: up to 16 services.
- 6 bits for pod ordinal: up to 64 pods per service.

```java
if (serviceId < 0 || serviceId >= 16 || podOrdinal < 0 || podOrdinal >= 64) {
    throw new IllegalArgumentException("TSID node partition out of range");
}
int nodeId = (serviceId << 6) | podOrdinal;
```

## Preferred path: StatefulSet ordinal

A StatefulSet is the lowest-coordination TSID setup on Kubernetes for workloads that can use stable pod identity.

For a StatefulSet named `orders`, pod names are `orders-0`, `orders-1`, through `orders-99`.

Kubernetes sets the container hostname to the pod name by default. In Java, prefer injecting the pod name explicitly with the Downward API:

```yaml
env:
  - name: POD_NAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
```

Parse the trailing ordinal:

```java
String podName = System.getenv("POD_NAME"); // "orders-3"
int nodeId = Integer.parseInt(podName.substring(podName.lastIndexOf('-') + 1));
if (nodeId < 0 || nodeId > 1023) {
    throw new IllegalArgumentException("TSID node id out of range");
}
TSID.Factory factory = TSID.Factory.builder().withNode(nodeId).build();
```

With default StatefulSet ordinals, the ordinal can be used directly when it is in `0..1023`.

If `.spec.ordinals.start` is configured, or if replicas can exceed 1024, map the ordinal into an assigned node range.

Fail fast when the mapped value is outside `0..1023`.

## Deployment option: platform-assigned node IDs

Deployments do not provide a stable ordinal. Pod names have random suffixes such as `orders-7d8c9bfb59-abc12`.

TSID can still work with Deployments, but this is a platform-supported pattern, not a plain Deployment feature.

Something outside the static Deployment pod template must assign a distinct node ID to each pod before the JVM starts.

A single static environment variable is not enough:

```yaml
env:
  - name: TSID_NODE
    value: "7"
```

Every replica in that Deployment would get node ID `7`.

Valid Deployment patterns assign a different value per pod outside the Deployment's static pod template:

- A platform controller injects `TSID_NODE` during pod creation and does not reuse it while the old pod can still generate IDs.
- A mutating webhook assigns `TSID_NODE` from an approved range.
- Multiple single-replica workloads each receive an explicit, non-overlapping `TSID_NODE`.

The constraint is the same: do not assign the same node ID to live pods that may write to the same table, collection, stream, or namespace at the same time.

With this setup, ID generation remains local and coordination-free after startup.

Node-id assignment happens before the application starts and must remain exclusive until the pod can no longer generate or persist IDs.

## Deployment pitfall: hashing generated hostnames

Hashing `HOSTNAME` into 1024 slots is unsafe for write-heavy production systems because duplicate node IDs become likely.

At 100 pods, the birthday probability of at least one duplicate node ID is roughly 99%. That means duplicate node IDs are expected, not exceptional.

A duplicate node ID does not automatically mean a duplicate TSID.

A duplicate TSID requires all three: duplicate node ID, same millisecond, and same counter value.

TSID can generate millions of IDs per second. That throughput assumes node IDs are coordinated.

With duplicate node IDs, high throughput increases the chance that two pods generate inside the same millisecond.

The retry loop below is a last-resort mitigation for low-write systems:

```java
String hostname = System.getenv("HOSTNAME");
int nodeId = Math.floorMod(hostname.hashCode(), 1024);
TSID.Factory factory = TSID.Factory.builder().withNode(nodeId).build();

// A same-(node, ms, counter) collision is possible. Catch it and retry.
for (int attempt = 0; attempt < 3; attempt++) {
    long id = factory.generate().toLong();
    try {
        repository.insert(id, order);
        return id;
    } catch (DuplicateKeyException dup) {
        // Another pod hit the same TSID. The next generate() bumps the counter; retry.
    }
}
throw new IllegalStateException("3 TSID collisions in a row. Investigate node-id assignment.");
```

Why hostname hashing is not recommended by default:

- Duplicate node IDs are likely at normal pod counts.
- Duplicate node IDs create a collision risk under concurrent writes.
- Retry-on-conflict is safe only when the insert is the first side effect, the operation is idempotent, and retries include bounded backoff.
- Retry-on-conflict reduces visible failures. It does not make hashed node IDs safe.
- TSID is appropriate on Kubernetes when node IDs are coordinated.
- If node IDs cannot be coordinated, use UUIDv7.

## Beyond 1024 pods

Beyond 1024 concurrent generators, create separate uniqueness domains whose keys include the tenant, service, or region shard.

Do not merge those domains later on TSID alone. If all IDs must share one global namespace, TSID's 10-bit node field is the limit.

If that coordination cost is unacceptable, use UUIDv7.

UUIDv7 has no node field and no node-id coordination requirement.
The tradeoff is 8 extra bytes per id. See [benchmark.md](./benchmark.md) for storage cost.
