---
title: Getting started
sidebar_position: 2
---

# Getting started

The fastest way to run kawa is the bundled Docker Compose setup, which starts a Kafka
broker and a gateway configured with one virtual topic.

## Prerequisites

- Docker (with Compose v2)

## Run the demo stack

```bash
make up
```

This always rebuilds the gateway image and force-recreates the containers, so code and
config changes are picked up reliably. (A plain `docker compose up` can silently keep
running a stale image or container.) The SASL-enabled variant is `make up-sasl`; see
`make help` for the full list of targets.

This starts three services:

| Service | Image | Port |
|---|---|---|
| `kafka` | `apache/kafka-native:4.3.1` | `19092` on the host (inspection only) |
| `kafka-init` | `apache/kafka:4.3.1` | creates the physical topic `orders-v2`, then exits |
| `gateway` | built from this repo's Dockerfile | `9092` — **connect your clients here** |

The gateway is mounted with [`docker/gateway.yaml`](https://GITHUB_URL/blob/main/docker/gateway.yaml),
which defines:

- a listener on `0.0.0.0:9092`
- an upstream cluster pointing at `kafka:9092`
- a virtual topic `orders.eu` mapped to physical topic `orders-v2`
- an advertised listener at `localhost:9092`

## Produce and consume through the gateway

Point any Kafka client at `localhost:9092` and use the logical topic name:

```bash
# Produce to the LOGICAL topic via the gateway
docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 \
  --topic orders.eu <<'EOF'
{"orderId": "1", "region": "eu"}
EOF
```

```bash
# Consume from the logical topic via the host-published broker port,
# bypassing the gateway, to see that only the PHYSICAL topic exists
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --list
# -> orders-v2   (orders.eu never existed on the cluster)
```

Because of the `headerEquals` filter in the default config (`header: region`,
`value: eu`), consumers of `orders.eu` only receive records whose `region` header
equals `eu`. Records are filtered server-side by the gateway; offsets stay intact.

## Run the gateway without Docker

Build the shaded fat jar and point it at a config file:

```bash
./mvnw -pl kawa-server -am package
java -jar kawa-server/target/kawa-server-*.jar --config path/to/config.yaml
```

If `--config` is omitted, kawa looks for `config.yaml` in the working directory.

## Verify it works

With the stack running, connect any Kafka tool to `localhost:9092`. Metadata requests
return a single broker — the gateway itself — advertising `localhost:9092`, so every
subsequent connection also lands on kawa instead of the real cluster.
