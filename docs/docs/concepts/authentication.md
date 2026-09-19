---
title: Authentication
sidebar_position: 3
---

# Authentication

kawa terminates client authentication itself. Clients authenticate to kawa
directly via the standard Kafka SASL handshake; kawa authenticates to the
upstream cluster separately as one dedicated service identity. Adding a kawa
client never requires provisioning a matching Kafka principal.

## Protocol

The same `SaslHandshake` + `SaslAuthenticate` flow every Kafka client already
implements. No custom client configuration — point any client at kawa with
`security.protocol=SASL_PLAINTEXT`.

## Configuration

```yaml
auth:
  mechanisms:
    - PLAIN
  salt: "a-stable-static-salt"
  clients:
    john:
      password: doe
    alice:
      password: "${ALICE_PASSWORD}"
```

| Field | Type | Required | Description |
|---|---|---|---|
| `mechanisms` | string list | yes | Advertised in `SaslHandshake` responses. Must include every mechanism any client needs. |
| `salt` | string | no | Static value used when hashing client passwords persisted to the `__kawa` config topic. Keep it stable across gateway restarts. |
| `clients.<name>.password` | string | yes | Plain-text or `${VAR}` / `${VAR:-default}` for env interpolation. |
| `clients.<name>.mechanism` | string | no | Per-client override. Inherits `mechanisms[0]` when omitted. |

### Per-client mechanism override

When a client needs a different mechanism than the global default:

```yaml
auth:
  mechanisms:
    - PLAIN
    - SCRAM-SHA-256
  clients:
    john:
      password: doe                    # inherits PLAIN
    alice:
      mechanism: SCRAM-SHA-256         # explicit override
      password: "${ALICE_PASSWORD}"
```

Every client mechanism must appear in the `mechanisms` list — the gateway
advertises this list during handshake, so a mechanism not listed will be
rejected before authentication is even attempted.

### Environment variable interpolation

Passwords support `${VAR}` and `${VAR:-default}` syntax. Missing variables
without a default cause a startup error.

```yaml
auth:
  mechanisms:
    - PLAIN
  clients:
    alice:
      password: "${ALICE_PASSWORD}"           # required at startup
    bob:
      password: "${BOB_PASSWORD:-changeme}"   # falls back to default
```

### Validation

kawa validates auth config at startup:

- A client without `mechanism` + no global `mechanisms` → error
- A client with `mechanism` not in the `mechanisms` list → error
- Blank or missing password → error

Client passwords are stored as PBKDF2 hashes in the `__kawa` config topic, never
as plain text. The salt is optional; when configured, it must be supplied
consistently by the static YAML configuration.

## Upstream broker authentication

kawa can authenticate to the upstream Kafka cluster when the broker requires SASL.
Configure `auth.brokerAuth` with the credentials kawa uses as a client:

```yaml
auth:
  brokerAuth:
    mechanism: PLAIN
    username: kafka
    password: "${KAFKA_PASSWORD}"
```

| Field | Type | Required | Description |
|---|---|---|---|
| `mechanism` | string | yes | SASL mechanism (`PLAIN` for now) |
| `username` | string | yes | Broker SASL username |
| `password` | string | yes | Plain-text or `${VAR}` / `${VAR:-default}` |

The gateway authenticates during connection setup — `SaslHandshake` + `SaslAuthenticate`
— before any client traffic is forwarded. This is transparent to clients: they
authenticate to the gateway independently.

:::note
Only `PLAIN` is supported for upstream broker authentication. SCRAM would require
the `javax.security.sasl.Sasl` API and is not yet implemented.
:::
