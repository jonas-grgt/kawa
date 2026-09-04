---
title: Authentication
sidebar_position: 3
---

# Authentication

kawa terminates client authentication itself. Clients authenticate to kawa
directly via the standard Kafka SASL handshake; kawa authenticates to the
upstream cluster separately as one dedicated service identity. Adding a kawa
user never requires provisioning a matching Kafka principal.

## Protocol

The same `SaslHandshake` + `SaslAuthenticate` flow every Kafka client already
implements. No custom client configuration — point any client at kawa with
`security.protocol=SASL_PLAINTEXT`.

## Configuration

```yaml
auth:
  mechanisms:
    - PLAIN
  users:
    john:
      password: doe
    alice:
      password: "${ALICE_PASSWORD}"
```

| Field | Type | Required | Description |
|---|---|---|---|
| `mechanisms` | string list | yes | Advertised in `SaslHandshake` responses. Must include every mechanism any user needs. |
| `users.<name>.password` | string | yes | Plain-text or `${VAR}` / `${VAR:-default}` for env interpolation. |
| `users.<name>.mechanism` | string | no | Per-user override. Inherits `mechanisms[0]` when omitted. |

### Per-user mechanism override

When a user needs a different mechanism than the global default:

```yaml
auth:
  mechanisms:
    - PLAIN
    - SCRAM-SHA-256
  users:
    john:
      password: doe                    # inherits PLAIN
    alice:
      mechanism: SCRAM-SHA-256         # explicit override
      password: "${ALICE_PASSWORD}"
```

Every user mechanism must appear in the `mechanisms` list — the gateway
advertises this list during handshake, so a mechanism not listed will be
rejected before authentication is even attempted.

### Environment variable interpolation

Passwords support `${VAR}` and `${VAR:-default}` syntax. Missing variables
without a default cause a startup error.

```yaml
auth:
  mechanisms:
    - PLAIN
  users:
    alice:
      password: "${ALICE_PASSWORD}"           # required at startup
    bob:
      password: "${BOB_PASSWORD:-changeme}"   # falls back to default
```

### Validation

kawa validates auth config at startup:

- A user without `mechanism` + no global `mechanisms` → error
- A user with `mechanism` not in the `mechanisms` list → error
- Blank or missing password → error

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
