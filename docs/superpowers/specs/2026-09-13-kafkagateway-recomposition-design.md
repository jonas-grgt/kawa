# KafkaGateway Recomposition — Design

Date: 2026-09-13
Branch: `feat/recomposition`

## Problem

`KafkaGateway.start()` (`kawa-server/.../server/KafkaGateway.java`) is a ~95-line
composition root that constructs ~20 objects in one method, mixing six concerns:

- Netty transport (3 event loop groups, `ServerBootstrap`, `KafkaServerInitializer`)
- Dynamic config (`VirtualTopicManager`, `RbacAuthorizer`, `SaslAuthenticator`,
  `GovernancePolicy`, `DynamicConfigManager`)
- Protocol (`KafkaApiRegistry`, `KafkaBodyCodec`, `ApiVersionsResponseBuilder`)
- Interceptors (`FetchSessionRegistry`, `AuthorizationInterceptor`,
  `VirtualTopicInterceptor`, `InterceptorPipeline`)
- Broker side (`BrokerClientPool`, `MetadataClient`, `LeaderRouter`)
- Dispatcher + admin (`KafkaClientRequestHandler`, `KafkaTopicAdmin`, `AdminHttpServer`)

The class holds 12 mutable fields, most set only inside `start()`. `stop()` mirrors
this with 8 null-checked shutdown steps. Ordering constraints are implicit:

1. The listener binds **before** the dispatcher exists (port-0 advertised
   resolution) — `DispatcherHolder` closes every connection until the dispatcher is
   installed.
2. `MetadataClient.start()` must complete before the dispatcher is installed.
3. `DynamicConfigManager` must catch up from the config topic before anything serves.

## Goal

Decompose the wiring into four focused components, each owning one bounded
responsibility and its own lifecycle. `KafkaGateway` stays a thin composition root.
Manual wiring is preserved — **no DI framework, no new dependencies, no new Maven
modules**. Zero behavior change on the success path.

## Components (all new classes in `kawa-server`)

### `DynamicGatewayState` (`io.jonasg.kawa.server`)

Owns the four dynamically-reloaded consumers and the config-topic manager that feeds
them. Created empty, then `start()` blocks until the config topic has been caught up.

- Constructor: `(String bootstrapServers, String topic, Properties extraProps)`
- `start()` — starts `DynamicConfigManager`, awaits initial load
- Accessors: `virtualTopics()`, `authorizer()`, `saslAuthenticator()`,
  `governance()`, `configManager()`
- `close()` — closes the `DynamicConfigManager`
- Absorbs the `configTopicProps(...)` helper from `KafkaGateway`

### `KafkaListener` (`io.jonasg.kawa.server.netty`)

Owns the client-facing Netty listener: `parentGroup`, `childGroup`, `serverChannel`,
`DispatcherHolder`, `KafkaServerInitializer`.

- Constructor: `(GatewayMetrics metrics, KafkaBodyCodec codec)`
- `bind(String host, int port)` → `int boundPort`
- `installDispatcher(KafkaClientRequestHandler dispatcher)`
- `boundPort()`
- `close()` — closes `serverChannel`, shuts down both groups

### `ClusterConnections` (`io.jonasg.kawa.server.broker`)

Owns the gateway's own connections to the cluster: `brokerGroup`, `BrokerClientPool`,
`MetadataClient`, `LeaderRouter`.

- Constructor: `(KafkaBodyCodec codec, InterceptorPipeline pipeline,
  GatewayMetrics metrics, MetadataCache cache, String host, int port,
  BrokerAuthConfig brokerAuth)`
- `start()` — starts the `MetadataClient`
- Accessors: `pool()`, `metadataClient()`, `router()`
- `close()` — `metadataClient.stop()`, `pool.closeAll()`, group shutdown
  (preserves current stop order)

### `AdminSurface` (`io.jonasg.kawa.server`)

Owns the admin HTTP surface: `KafkaTopicAdmin` + `AdminHttpServer`.

- Constructor: `(AdminConfig config, DynamicGatewayState state, MetadataCache cache,
  String bootstrapServers, BrokerAuthConfig brokerAuth)`
- `start()`, `boundPort()`, `close()`

## `KafkaGateway` after

Fields: `config`, `cache` (final), `metrics`, `codec`, `apiVersionsBuilder`,
`dynamicState`, `listener`, `connections`, `admin`, `running`.

`start()` becomes a readable sequence of labeled steps:

1. Validate cluster, parse bootstrap
2. Create shared infrastructure: `metrics`, `codec`, `apiVersionsBuilder`
3. `dynamicState.start()` — load config topic
4. `listener.bind(...)` → bound port; resolve advertised
5. `buildPipeline(...)` — private method returning
   `record RequestPipeline(InterceptorPipeline pipeline, FetchSessionRegistry fetchSessions)`
6. `connections.start()` — metadata client
7. Build `KafkaClientRequestHandler`, `listener.installDispatcher(...)`
8. `admin` (conditional on `config.admin().enabled()`)

`stop()` collapses to four component closes in the current order:
`admin` → `dynamicState` → `connections` → `listener`.

Public API unchanged: `KafkaGateway(GatewayConfig)`, `start()`, `stop()`,
`boundPort()`, `adminBoundPort()`.

## Ordering constraints preserved

- Listener binds before dispatcher exists; connections closed until installed
- Metadata client started before dispatcher install
- Config topic caught up before serving
- `stop()` order: admin → dynamic config → metadata client → broker pool →
  server channel → event loop groups

## Testing

- Existing unit tests (`DynamicConfigManagerTest`, `KafkaClientRequestHandlerTest`,
  `LeaderRouterTest`, …) are untouched — component constructors do not change.
- New: `KafkaListenerTest` — bind/close lifecycle; connections are closed before a
  dispatcher is installed (currently untested, subtle behavior).
- `kawa-integration-tests` are the end-to-end verification; all ITs must stay green.
- Verification: `./mvnw -pl kawa-server -am test`, then the full IT suite
  (`./mvnw -pl kawa-integration-tests -am verify`).

## Out of scope

- `MetricsConfig` is still ignored (always `SimpleMeterRegistry`) — unchanged.
- Failure-path cleanup in `start()` (closing partially-created components when a
  later step throws) — deliberately **not** included to keep the refactor strictly
  behavior-preserving; noted as a possible follow-up.
- No changes to the constructors of existing component classes.