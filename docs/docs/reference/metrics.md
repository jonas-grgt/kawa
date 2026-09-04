---
title: Metrics
sidebar_position: 2
---

# Metrics

kawa instruments itself with [Micrometer](https://micrometer.io). Enable collection
in the config:

```yaml
metrics:
  enabled: true
  prometheusPort: 9404   # optional; omit to skip the HTTP endpoint
```

When `prometheusPort` is set, a Prometheus text-format endpoint is served at
`http://<host>:<port>/` for scraping.

## Exported metrics

| Metric | Type | Tags | Description |
|---|---|---|---|
| `gateway.requests.total` | counter | `api`, `result` | Requests received from clients |
| `gateway.responses.total` | counter | `api`, `result` | Responses sent back to clients |
| `gateway.request.latency` | timer | `api` | Request→response latency |
| `gateway.bytes.in` | counter | — | Bytes received from clients |
| `gateway.bytes.out` | counter | — | Bytes sent to clients |
| `gateway.connections.client.active` | gauge | — | Currently open client connections |
| `gateway.connections.broker.active` | gauge | — | Currently open upstream broker connections |
| `gateway.virtual_topic.hits` | counter | `direction`, `logical`, `physical` | Virtual topic rewrites |

### Tag values

- **`api`** — Kafka API name, e.g. `Produce`, `Fetch`, `Metadata`.
- **`result`** — request/response outcome as classified by the transport layer.
- **`direction`** — rewrite direction: `request` (client→broker, logical→physical) or
  `response` (broker→client, physical→logical).
- **`logical` / `physical`** — virtual topic names involved in the rewrite.

## Example PromQL

```promql
# Request rate by API
rate(gateway_requests_total[5m])

# p99 produce latency
histogram_quantile(0.99,
  rate(gateway_request_latency_seconds_bucket{api="Produce"}[5m]))

# Rewrites per logical topic
sum by (logical) (rate(gateway_virtual_topic_hits_total[5m]))
```
