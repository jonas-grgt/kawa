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

The file is the **static bootstrap**: it carries the startup-only configuration
(`listeners`, `clusters`, `advertised`, `admin`, `auth.brokerAuth`, `configTopic`).
Virtual topics, RBAC, client authentication and governance are **dynamic** and are read
from the config topic (see [Dynamic config](#dynamic-config)); an empty config topic on
first boot is valid and the gateway boots with no virtual topics, default-deny RBAC, no
client auth, and no governance rules.

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

admin:
  enabled: true
  host: 0.0.0.0
  port: 8080
  cors:
    allowedOrigins:
      - http://localhost:8080

governance:
  topicRules:
    min-replication:
      message: replication factor must be at least 3
      expression: topic.replicationFactor >= 3
    min-partitions:
      message: must have at least 1 partition
      expression: topic.partitions >= 1
  exemptions:
    ops-internal:
      principal: "ops-.*"
      topicPattern: ".*-changelog"
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

Map of virtual name → virtual topic definition. The map key is the topic name clients
see and use.

| Field | Type | Default | Description |
|---|---|---|---|
| `topic` | string | *(required)* | Physical topic name on the upstream cluster |
| `filter` | object | none | Optional server-side consume filter |
| `exposePhysicalTopic` | bool | `false` | When `true`, the physical topic stays visible in Metadata responses next to its virtual name |

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

### `admin`

The admin HTTP surface exposing gateway state (e.g. `GET /topics`) to a UI.

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | `false` | Whether the admin HTTP server is started |
| `host` | string | `0.0.0.0` | Bind address |
| `port` | int | `8080` | Bind port; `0` binds an ephemeral port |
| `cors` | object | *(disabled)* | CORS configuration for browser-based UIs served from another host/port |

```yaml
admin:
  enabled: true
  host: 0.0.0.0
  port: 8080
  cors:
    allowedOrigins:
      - http://localhost:8080
```

#### `admin.cors`

| Field | Type | Default | Description |
|---|---|---|---|
| `allowedOrigins` | string list | `["*"]` | Origins allowed to call the admin API; `["*"]` allows any origin |
| `allowedMethods` | string list | `["GET", "POST", "PUT", "DELETE", "OPTIONS"]` | HTTP methods allowed in preflight responses |
| `allowedHeaders` | string list | *(empty)* | Request headers allowed in preflight responses |
| `allowCredentials` | bool | `false` | Whether credentialed requests (cookies, auth headers) are allowed |
| `maxAge` | int | *(omitted)* | How long preflight results may be cached, in seconds |

CORS is disabled entirely when `admin.cors` is omitted. When `allowCredentials` is `true`
with a wildcard origin, kawa echoes the request origin (with a `Vary` header) instead of
returning `*`, as required by the CORS spec.

### `governance`

Topic governance: named CEL rules that new topics must satisfy, and named exemptions
that skip evaluation for matching principal + topic pairs. Governance is **dynamic** —
it is read from the config topic like virtual topics and RBAC. When no rules are
configured, every topic creation is admitted.

| Field | Type | Default | Description |
|---|---|---|---|
| `topicRules` | map | *(empty)* | Named rules, each a message plus a CEL expression |
| `exemptions` | map | *(empty)* | Named exemptions, each a principal regex plus a topic pattern regex |

```yaml
governance:
  topicRules:
    min-replication:
      message: replication factor must be at least 3
      expression: topic.replicationFactor >= 3
  exemptions:
    ops-internal:
      principal: "ops-.*"
      topicPattern: ".*-changelog"
```

#### `governance.topicRules.<name>`

| Field | Type | Description |
|---|---|---|
| `message` | string | Human-readable description shown when this rule rejects a topic |
| `expression` | string | CEL expression that must evaluate to `true` for the topic to be compliant |

The expression is compiled eagerly when the config is applied — a bad expression fails
the config load instead of the first request. It has access to these bindings:

| Binding | Type | Notes |
|---|---|---|
| `principal` | string | The principal requesting the topic |
| `service` | string | The service the topic is created on |
| `topic.name` | string | Topic name |
| `topic.partitions` | int | Requested partitions, or `-1` for the broker default |
| `topic.replicationFactor` | int | Requested replication factor, or `-1` for the broker default |
| `topic.configs` | map of string to string | Topic configs; a missing key resolves to `""` (falsy, not an error) |

```yaml
governance:
  topicRules:
    min-replication:
      message: replication factor must be at least 3
      expression: topic.replicationFactor >= 3
    no-compact:
      message: compaction is not allowed
      expression: !("cleanup.policy" in topic.configs) || topic.configs["cleanup.policy"] != "compact"
```

#### `governance.exemptions.<name>`

| Field | Type | Description |
|---|---|---|
| `principal` | string | Regex matched against the requesting principal |
| `topicPattern` | string | Regex matched against the topic name |

Both patterns must match for the exemption to apply. Two patterns rather than a
name-only one: a topic-only exemption would be a bypass, because anyone could name a
topic `...-changelog` to skip enforcement.

## Dynamic config

Virtual topics, RBAC, client authentication and governance are **dynamic**: they are
read from the config topic (default `__kawa`) and update live while the gateway runs.
The static YAML file does **not** carry them - any `virtualTopics`, `rbac`,
`auth.users`/`auth.mechanisms` or `governance` in the file are ignored.

Each message in the config topic is a full JSON snapshot of the dynamic subset of
[`GatewayConfig`](#reference). The topic is expected to have a single partition and
`cleanup.policy=compact`; the last snapshot wins.

An **empty config topic on first boot is valid**: the gateway boots with no virtual topics,
default-deny RBAC, no client auth and no governance rules, and picks up the config as soon
as the first snapshot is written. This is the normal first-boot flow - there is no "seed
the config topic before starting" requirement.

The startup-only configuration (`listeners`, `clusters`, `advertised`, `admin`,
`auth.brokerAuth`, `configTopic`) always comes from the static YAML file and cannot be
changed live.

## Admin API

When `admin.enabled` is `true`, the admin HTTP server exposes gateway state and the
dynamic config. All endpoints return JSON.

### `GET /topics`

Lists the virtual and physical topics known to the gateway (see [Admin](#admin)). Virtual
topics are listed even when their physical backing topic does not exist yet.

### `POST /topics`

The unified topic creation surface. The request body carries a `type` discriminator plus
the fields of one topic kind:

```json
{"type": "physical", "name": "orders", "partitions": 3, "replicationFactor": 3, "configs": {"cleanup.policy": "compact"}}
{"type": "virtual", "name": "orders", "topic": "orders-v2", "filter": {"kind": "header", "header": "tenant", "value": "acme"}}
```

A `physical` topic runs the governance admission check and then creates the topic on the
broker: an exempt principal + topic pair is admitted without evaluation, otherwise every
rule must pass. A compliant topic returns `201` with the stored spec; a topic that violates
one or more rules returns `403` with the violation messages; a topic that already exists
returns `409`. A `virtual` topic writes the virtual topic config (no governance applies)
and returns `201` with the stored config. An invalid body, unknown `type`, or a virtual
topic without a physical `topic` returns `400`.

The admin HTTP layer has no authentication yet, so the requesting principal is a
placeholder (`admin`) until real admin auth lands.

### `PUT /topics/{name}`

Adds or replaces the virtual topic config for `name`. The body uses the same `type`
discriminator as `POST /topics` — only `"type": "virtual"` is accepted
(`{"type": "virtual", "topic": "orders-v2", "filter": {...}}`). Returns `200` with the
stored config; an invalid body, a `"type": "physical"` body, or a virtual body without a
physical `topic` returns `400`. Physical topic alteration is not supported.

### `DELETE /topics/{name}`

Removes a topic. When `name` is a virtual topic, its config is removed (no broker
operation). Otherwise the physical topic is deleted on the broker. Returns `204`; a name
that is neither virtual nor physical returns `404`. A name that is both resolves to the
virtual config removal.

### Config endpoints

The `/rbac/...`, `/auth/...` and `/governance` endpoints read and write the dynamic config
(RBAC, client auth and governance). `GET` lists a section, `PUT /{name}` upserts one entry,
`DELETE /{name}` removes it. Writes persist a full snapshot to the config topic and are
applied live.

| Endpoint | Method | Description |
|---|---|---|
| `/rbac/roles` | GET | List roles |
| `/rbac/roles/{name}` | PUT | Add or replace a role |
| `/rbac/roles/{name}` | DELETE | Remove a role |
| `/rbac/groups` | GET | List groups |
| `/rbac/groups/{name}` | PUT | Add or replace a group |
| `/rbac/groups/{name}` | DELETE | Remove a group |
| `/auth/users` | GET | List users |
| `/auth/users/{name}` | PUT | Add or replace a user |
| `/auth/users/{name}` | DELETE | Remove a user |
| `/governance` | GET | List the governance section (rules + exemptions) |
| `/governance` | PUT | Replace the whole governance section |

The request body for a `PUT` is the entry's JSON object, using the same fields as the
YAML reference above — e.g. `{"acls": [...]}` for a role, `{"members": [...], "roles": [...]}`
for a group, `{"mechanism": "PLAIN", "password": "..."}` for a user, or the full
`{"topicRules": {...}, "exemptions": {...}}` object for governance.

Adding a user via `PUT /auth/users/{name}` auto-expands the advertised SASL
mechanisms to include the user's mechanism, so the first user can be added to an empty
config. The user's `mechanism` is required — a `PUT` without it is rejected with `400`.

Unlike the per-entry sections, `PUT /governance` replaces the **whole**
governance section in one write. Every rule expression is validated before the snapshot
is persisted — a `PUT` containing an invalid CEL expression is rejected with `400` and
nothing is written.

`PUT` returns `200` with the stored entry, `DELETE` returns `204`, a missing entry on
`DELETE` returns `404`, and an invalid body returns `400`.
