---
title: Configuration
sidebar_position: 3
---

# Configuration

kawa is configured with a single YAML file, loaded at startup:

```bash
java -jar kawa-server.jar --config /path/to/config.yaml   # default: ./config.yaml
```

Unknown properties are ignored. Every field is optional unless stated otherwise.

## Full example

```yaml
name: kawa-gateway

listeners:
  - host: 0.0.0.0
    port: 9092

clusters:
  default:
    name: default
    bootstrapServers:
      - kafka:9092

auth:
  mechanisms:
    - PLAIN
  users:
    alice:
      password: "${ALICE_PASSWORD}"
    bob:
      mechanism: SCRAM-SHA-256
      password: "${BOB_PASSWORD}"
  brokerAuth:
    mechanism: PLAIN
    username: kafka
    password: "${KAFKA_PASSWORD}"

rbac:
  roles:
    producer:
      acls:
        - resource:
            type: TOPIC
            pattern: orders
          operation: WRITE
    admin:
      acls:
        - resource:
            type: CLUSTER
          operation: CREATE
  groups:
    producers:
      members: [alice]
      roles: [producer]
    admins:
      members: [bob]
      roles: [admin]

virtualTopics:
  orders.eu:
    topic: orders-v2
    filter:
      type: headerEquals
      header: region
      value: eu
    exposePhysicalTopic: false

advertised:
  nodeId: 1
  host: localhost
  port: 9092

metrics:
  enabled: true
  prometheusPort: 9404
```

## Reference

### `name`

Gateway name used in logs and metrics.

| | |
|---|---|
| Type | string |
| Default | `kafka-gateway` |

### `listeners[]`

Client-facing listeners. The gateway accepts client connections here.

| Field | Type | Default | Description |
|---|---|---|---|
| `host` | string | `0.0.0.0` | Bind address |
| `port` | int | `9092` | Bind port; `0` binds an ephemeral port |

The first listener is the default and determines the advertised port when
[`advertised.port`](#advertised) is unset.

### `clusters`

Map of cluster key → upstream Kafka cluster. The **first entry is the default
cluster** that traffic is forwarded to (milestone 1 supports a single cluster).

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | map key | Cluster name |
| `bootstrapServers` | string list | *(required)* | `host:port` list used for the initial broker connection |

```yaml
clusters:
  default:
    bootstrapServers:
      - broker1.internal:9092
      - broker2.internal:9092
```

### `virtualTopics`

Map of logical name → virtual topic definition. The map key is the topic name clients
see and use.

| Field | Type | Default | Description |
|---|---|---|---|
| `topic` | string | *(required)* | Physical topic name on the upstream cluster |
| `filter` | object | none | Optional server-side consume filter |
| `exposePhysicalTopic` | bool | `false` | When `true`, the physical topic stays visible in Metadata responses next to its logical name |

#### `filter` (`headerEquals`)

Keeps only records whose record header matches during Fetch. Non-matching records are
dropped by the gateway while preserving offsets of surviving records.

| Field | Type | Description |
|---|---|---|
| `type` | string | Must be `headerEquals` |
| `header` | string | Header key to compare |
| `value` | string | Required header value |

#### `filter` (`cel`)

Keeps only records for which a [CEL](https://cel.dev) (Common Expression Language)
expression evaluates to `true`. The expression is compiled once at first use and
evaluated per record, so it is cheap even on high-throughput topics.

| Field | Type | Description |
|---|---|---|
| `type` | string | Must be `cel` |
| `expression` | string | CEL expression returning a boolean |

The expression has access to these record bindings:

| Binding | Type | Notes |
|---|---|---|
| `key` | string | `""` when the record has no key |
| `value` | string | `""` when the record has no value |
| `headers` | map of string to string | A missing header resolves to `""` (falsy, not an error) |
| `timestamp` | int | Record timestamp in milliseconds |

```yaml
virtualTopics:
  orders.eu:
    topic: orders-v2
    filter:
      type: cel
      expression: headers.region == "eu" && value.contains("error")
```

See [Virtual topics](/docs/concepts/virtual-topics) for behaviour details.

### `auth`

Client SASL authentication configuration. When configured, clients must authenticate
using the standard Kafka SASL handshake (`SaslHandshake` + `SaslAuthenticate`).

| Field | Type | Default | Description |
|---|---|---|---|
| `mechanisms` | string list | *(empty)* | SASL mechanisms advertised to clients |
| `users` | map | *(empty)* | User credentials |

Each user entry requires at minimum a `password`. If `mechanism` is omitted, the user
inherits the first mechanism from the global `mechanisms` list. If no global mechanism
is configured, an error is raised at startup.

```yaml
auth:
  mechanisms:
    - PLAIN
    - SCRAM-SHA-256
  users:
    alice:                          # inherits PLAIN
      password: "${ALICE_PASSWORD}"
    bob:
      mechanism: SCRAM-SHA-256      # explicit override
      password: "${BOB_PASSWORD}"
```

See [Authentication](/docs/concepts/authentication) for the full authentication model.

#### `auth.brokerAuth`

Upstream broker SASL authentication. When set, the gateway authenticates to the
Kafka cluster using these credentials instead of connecting in plaintext.

| Field | Type | Required | Description |
|---|---|---|---|
| `mechanism` | string | yes | SASL mechanism (currently `PLAIN` only) |
| `username` | string | yes | Broker SASL username |
| `password` | string | yes | Plain-text or `${VAR}` / `${VAR:-default}` |

```yaml
auth:
  mechanisms:
    - PLAIN
  users:
    alice:
      password: s3cret
  brokerAuth:
    mechanism: PLAIN
    username: kafka
    password: "${KAFKA_PASSWORD}"
```

The gateway authenticates to the broker during the initial connection handshake
(`SaslHandshake` + `SaslAuthenticate`), before forwarding any client requests.
This is transparent to clients — they authenticate to the gateway independently.

### `rbac`

Role-based access control. kawa checks each request against the principal's ACLs before
forwarding it to the cluster. RBAC is **always enforced** — there is no way to disable it,
and a gateway with no roles or groups configured denies every request from every client.
RBAC is **default-deny**: a request is only allowed if at least one matching ACL grants it,
and any matching deny wins.

| Field | Type | Default | Description |
|---|---|---|---|
| `roles` | map | *(empty)* | Named roles, each a list of ACLs |
| `groups` | map | *(empty)* | Named groups, each a member list plus the roles those members inherit |

A user's effective ACLs are the union of every role referenced by every group they belong
to.

```yaml
rbac:
  roles:
    producer:
      acls:
        - resource:
            type: TOPIC
            pattern: orders
          operation: WRITE
    admin:
      acls:
        - resource:
            type: CLUSTER
          operation: CREATE
  groups:
    producers:
      members: [alice]
      roles: [producer]
    admins:
      members: [bob]
      roles: [admin]
```

#### `rbac.roles.<name>.acls[]`

Each ACL grants or denies one operation on one resource.

| Field | Type | Default | Description |
|---|---|---|---|
| `resource` | object | *(required)* | The resource the ACL applies to |
| `operation` | string | *(required)* | `WRITE`, `READ`, `CREATE`, `DELETE`, `ALL`, ... |
| `permission` | string | `ALLOW` | `ALLOW` or `DENY`; a matching `DENY` wins over any `ALLOW` |

#### `rbac.roles.<name>.acls[].resource`

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | string | *(required)* | `TOPIC`, `GROUP` or `CLUSTER` |
| `pattern` | string | *(required for TOPIC/GROUP)* | Resource name; ignored for `CLUSTER`. May be empty (`""`) when `patternType` is `PREFIXED` to match any resource of that type |
| `patternType` | string | `LITERAL` | `LITERAL` (exact match) or `PREFIXED` (name prefix) |

#### `rbac.groups.<name>`

| Field | Type | Description |
|---|---|---|
| `members` | string list | Authenticated usernames in this group |
| `roles` | string list | Roles whose ACLs every member inherits |

See [Access control (RBAC)](/docs/concepts/rbac) for the full model, what is enforced
today, and how unauthenticated requests are handled.

### `advertised`

The endpoint kawa advertises to clients in rewritten Metadata and FindCoordinator
responses — effectively "the broker" every client will connect to.

| Field | Type | Default | Description |
|---|---|---|---|
| `nodeId` | int | `1` | Broker node id advertised to clients |
| `host` | string | `localhost` | Host clients connect to |
| `port` | int | first listener's port | Port clients connect to; `0` means "use the bound listener port" |

:::warning Choose the host from the client's point of view
All broker endpoints are rewritten to this value, so `advertised.host` must resolve
**from the client's perspective**. Inside Docker Compose the gateway config uses
`localhost` because the port is published to the host where the clients run.
:::

### `metrics`

Observability settings.

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | `false` | Whether metrics are collected at all |
| `prometheusPort` | int | disabled | Port for a Prometheus text-format HTTP endpoint |

When enabled, kawa exports Micrometer metrics — see [Metrics](/docs/reference/metrics)
for the full list.
