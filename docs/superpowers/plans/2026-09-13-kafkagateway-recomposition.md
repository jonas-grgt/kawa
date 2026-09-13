# KafkaGateway Recomposition — Implementation Plan

Date: 2026-09-13
Branch: `feat/recomposition`
Spec: `docs/superpowers/specs/2026-09-13-kafkagateway-recomposition-design.md`

## Overview

Five TDD tasks. Tasks 1–4 each add one new component plus its failing test
(presented before implementation). Task 5 rewires `KafkaGateway` to the four
components. Bottom-up order: each component is independently testable, and
`KafkaGateway` is only touched once all four exist.

Per task: write failing test → present → get go → implement → verify → commit.

## Task 1 — `DynamicGatewayState` + `DynamicGatewayStateTest`

### Files
- New: `kawa-server/src/main/java/io/jonasg/kawa/server/DynamicGatewayState.java`
- New: `kawa-server/src/test/java/io/jonasg/kawa/server/DynamicGatewayStateTest.java`

### Failing test first

`DynamicGatewayStateTest.applyReachesAllConsumers` — verifies the wiring seam:
`configManager().apply(config)` (package-private on `DynamicConfigManager`, same
package) reaches all four consumers. No broker needed: `ConfigTopicConsumer` /
`ConfigTopicRepository` constructors are lazy (no connection until `start()` /
`send()`), and `close()` on a never-started consumer/producer is safe.

```java
package io.jonasg.kawa.server;

class DynamicGatewayStateTest {

    @Test
    void applyReachesAllConsumers() {
        // given
        var state = new DynamicGatewayState("localhost:9092", "__kawa", null);
        var config = GatewayConfig.empty()
                .putVirtualTopic("orders", new VirtualTopicConfig("orders-physical"))
                .updateRbac(new RbacConfig(
                        Map.of("reader", new RoleConfig(List.of(new AclConfig(
                                new ResourceConfig(ResourceType.TOPIC, "orders-physical"),
                                AclOperation.READ)))),
                        Map.of("readers", new GroupConfig(List.of("alice"), List.of("reader")))))
                .updateAuth(new AuthConfig(Set.of("PLAIN"),
                        Map.of("alice", new UserConfig("PLAIN", "secret")), null))
                .updateGovernance(new GovernanceConfig(
                        Map.of("no-delete", new GovernanceRuleConfig(
                                "must not delete", "topic.name != 'deleted'")), Map.of()));

        // when
        state.configManager().apply(config);

        // then
        assertThat(state.virtualTopics().virtualTopics()).containsEntry("orders", "orders-physical");
        assertThat(state.authorizer().hasAnyAcls()).isTrue();
        assertThat(state.authorizer().isAuthorized(
                "alice", ResourceType.TOPIC, "orders-physical", AclOperation.READ)).isTrue();
        assertThat(state.saslAuthenticator().handleAuthenticate(
                new SaslAuthenticateRequestData().setAuthBytes("\u0000alice\u0000secret".getBytes(UTF_8))))
                .isInstanceOf(AuthenticationResult.Success.class);
        assertThat(state.governance().evaluate("alice", "svc",
                new TopicSpec("deleted", 1, 1, Map.of()))).isNotEmpty();

        state.close();
    }
}
```

### Implementation

```java
public final class DynamicGatewayState implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DynamicGatewayState.class);

    private final String bootstrapServers;
    private final String topic;
    private final VirtualTopicManager virtualTopics;
    private final RbacAuthorizer authorizer;
    private final SaslAuthenticator saslAuthenticator;
    private final GovernancePolicy governance;
    private final DynamicConfigManager configManager;

    public DynamicGatewayState(String bootstrapServers, String topic, BrokerAuthConfig brokerAuth) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        virtualTopics = new VirtualTopicManager(Map.of());
        authorizer = new RbacAuthorizer(new RbacConfig(Map.of(), Map.of()));
        saslAuthenticator = new SaslAuthenticator(Set.of());
        governance = new GovernancePolicy(new GovernanceConfig(null, null));
        configManager = new DynamicConfigManager(
                bootstrapServers, topic, "kawa-config-" + UUID.randomUUID(),
                configTopicProps(brokerAuth),
                virtualTopics, authorizer, saslAuthenticator, governance);
    }

    public void start() throws InterruptedException {
        configManager.start();
        log.info("waiting for initial config load from topic '{}' on {}", topic, bootstrapServers);
        configManager.awaitInitialLoad();
    }

    public VirtualTopicManager virtualTopics() { return virtualTopics; }
    public RbacAuthorizer authorizer() { return authorizer; }
    public SaslAuthenticator saslAuthenticator() { return saslAuthenticator; }
    public GovernancePolicy governance() { return governance; }
    public DynamicConfigManager configManager() { return configManager; }

    @Override
    public void close() {
        configManager.close();
    }

    /// Consumer properties for the config topic. The gateway's own broker connection is
    /// PLAIN-only (see `BrokerSaslAuthenticator`), so the config-topic consumer uses the
    /// same credentials when the broker requires SASL.
    private static Properties configTopicProps(BrokerAuthConfig brokerAuth) {
        Properties props = new Properties();
        if (brokerAuth != null) {
            props.put("security.protocol", "SASL_PLAINTEXT");
            props.put("sasl.mechanism", brokerAuth.mechanism());
            props.put("sasl.jaas.config",
                    "org.apache.kafka.common.security.plain.PlainLoginModule required "
                            + "username=\"" + brokerAuth.username() + "\" password=\"" + brokerAuth.password() + "\";");
        }
        return props;
    }
}
```

### Verify
`./mvnw -pl kawa-server -am test -Dtest=DynamicGatewayStateTest -Dsurefire.failIfNoSpecifiedTests=false`

### Commit
`feat(server): extract DynamicGatewayState`

## Task 2 — `KafkaListener` + `KafkaListenerTest`

### Files
- New: `kawa-server/src/main/java/io/jonasg/kawa/server/netty/KafkaListener.java`
- New: `kawa-server/src/test/java/io/jonasg/kawa/server/netty/KafkaListenerTest.java`

### Failing tests first

Two tests. Use the `"127.0.0.1"` literal (not `localhost`) to avoid
multi-address flakiness (AGENTS.md).

1. `bindReturnsBoundPortAndCloseIsSafe` — bind, assert port > 0 and
   `boundPort()` matches, close.
2. `connectionsAreClosedUntilDispatcherIsInstalled` — the subtle behavior the
   spec calls out:
   - bind; raw-socket connect → `read()` returns `-1` (server closed it)
   - install a real dispatcher (built with the `KafkaClientRequestHandlerTest`
     pattern: real codec, `ApiVersionsResponseBuilder`, empty pipeline,
     `BrokerClientPool` + `MetadataClient` on a throwaway `NioEventLoopGroup`,
     `LeaderRouter`, `FetchSessionRegistry`, `SaslAuthenticator`)
   - raw-socket connect again with `setSoTimeout(500)` → `read()` throws
     `SocketTimeoutException` (connection stays open)

```java
package io.jonasg.kawa.server.netty;

class KafkaListenerTest {

    private final KafkaApiRegistry registry = KafkaApiRegistry.create();
    private final KafkaBodyCodec codec = new KafkaBodyCodec(registry);
    private final GatewayMetrics metrics = new GatewayMetrics(new SimpleMeterRegistry());

    @Test
    void bindReturnsBoundPortAndCloseIsSafe() throws Exception {
        // given
        var listener = new KafkaListener(metrics, codec);

        // when
        int boundPort = listener.bind("127.0.0.1", 0);

        // then
        assertThat(boundPort).isGreaterThan(0);
        assertThat(listener.boundPort()).isEqualTo(boundPort);

        // when
        listener.close();
    }

    @Test
    void connectionsAreClosedUntilDispatcherIsInstalled() throws Exception {
        // given
        var listener = new KafkaListener(metrics, codec);
        int boundPort = listener.bind("127.0.0.1", 0);

        // when — connect before a dispatcher exists
        try (var socket = new Socket("127.0.0.1", boundPort)) {
            // then — the server closes the connection immediately
            assertThat(socket.getInputStream().read()).isEqualTo(-1);
        }

        // when — a dispatcher is installed
        listener.installDispatcher(dispatcher());

        // then — a new connection stays open (read times out instead of EOF)
        try (var socket = new Socket("127.0.0.1", boundPort)) {
            socket.setSoTimeout(500);
            assertThatThrownBy(() -> socket.getInputStream().read())
                    .isInstanceOf(SocketTimeoutException.class);
        }

        listener.close();
    }

    private KafkaClientRequestHandler dispatcher() {
        var group = new NioEventLoopGroup(1);
        var cache = new MetadataCache();
        var pipeline = new InterceptorPipeline(List.of());
        var brokerPool = new BrokerClientPool(group, codec, pipeline, metrics, cache, "localhost", 9092);
        var metadataClient = new MetadataClient("localhost", 9092, group, codec, cache, brokerPool, metrics);
        return new KafkaClientRequestHandler(
                codec, new ApiVersionsResponseBuilder(SupportedVersions.from(registry)), pipeline,
                new LeaderRouter(cache), brokerPool, metadataClient, metrics, new FetchSessionRegistry(),
                new SaslAuthenticator(Set.of()));
    }
}
```

### Implementation

```java
public final class KafkaListener implements AutoCloseable {

    private final EventLoopGroup parentGroup = new NioEventLoopGroup(1);
    private final EventLoopGroup childGroup = new NioEventLoopGroup();
    private final DispatcherHolder dispatcherHolder = new DispatcherHolder();
    private final GatewayMetrics metrics;
    private final KafkaBodyCodec codec;

    private Channel serverChannel;

    public KafkaListener(GatewayMetrics metrics, KafkaBodyCodec codec) {
        this.metrics = metrics;
        this.codec = codec;
    }

    public int bind(String host, int port) throws InterruptedException {
        var server = new ServerBootstrap();
        server.group(parentGroup, childGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new KafkaServerInitializer(dispatcherHolder, metrics, codec));
        serverChannel = server.bind(host, port).sync().channel();
        return ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    public void installDispatcher(KafkaClientRequestHandler dispatcher) {
        dispatcherHolder.set(dispatcher);
    }

    public int boundPort() {
        return ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    @Override
    public void close() {
        if (serverChannel != null) {
            serverChannel.close();
        }
        parentGroup.shutdownGracefully();
        childGroup.shutdownGracefully();
    }
}
```

### Verify
`./mvnw -pl kawa-server -am test -Dtest=KafkaListenerTest -Dsurefire.failIfNoSpecifiedTests=false`

### Commit
`feat(server): extract KafkaListener`

## Task 3 — `ClusterConnections` + `ClusterConnectionsTest`

### Files
- New: `kawa-server/src/main/java/io/jonasg/kawa/server/broker/ClusterConnections.java`
- New: `kawa-server/src/test/java/io/jonasg/kawa/server/broker/ClusterConnectionsTest.java`

### Failing test first

`closeIsSafeWithoutStart` — construct with a dummy host/port, close without
starting, assert accessors are non-null and no exception. `MetadataClient.stop()`
and `BrokerClientPool.closeAll()` are both safe when never started (verified:
`channel` is null until `connect()`).

```java
package io.jonasg.kawa.server.broker;

class ClusterConnectionsTest {

    @Test
    void closeIsSafeWithoutStart() {
        // given
        var metrics = new GatewayMetrics(new SimpleMeterRegistry());
        var codec = new KafkaBodyCodec(KafkaApiRegistry.create());
        var pipeline = new InterceptorPipeline(List.of());
        var cache = new MetadataCache();
        var connections = new ClusterConnections(codec, pipeline, metrics, cache, "localhost", 9092, null);

        // when
        connections.close();

        // then
        assertThat(connections.pool()).isNotNull();
        assertThat(connections.metadataClient()).isNotNull();
        assertThat(connections.router()).isNotNull();
    }
}
```

### Implementation

```java
public final class ClusterConnections implements AutoCloseable {

    private final EventLoopGroup brokerGroup = new NioEventLoopGroup();
    private final BrokerClientPool pool;
    private final MetadataClient metadataClient;
    private final LeaderRouter router;

    public ClusterConnections(KafkaBodyCodec codec, InterceptorPipeline pipeline,
                              GatewayMetrics metrics, MetadataCache cache,
                              String host, int port, BrokerAuthConfig brokerAuth) {
        pool = new BrokerClientPool(brokerGroup, codec, pipeline, metrics, cache, host, port, brokerAuth);
        metadataClient = new MetadataClient(host, port, brokerGroup, codec, cache, pool, metrics, brokerAuth);
        router = new LeaderRouter(cache);
    }

    public void start() throws Exception {
        metadataClient.start();
    }

    public BrokerClientPool pool() { return pool; }
    public MetadataClient metadataClient() { return metadataClient; }
    public LeaderRouter router() { return router; }

    @Override
    public void close() {
        metadataClient.stop();
        pool.closeAll();
        brokerGroup.shutdownGracefully();
    }
}
```

### Verify
`./mvnw -pl kawa-server -am test -Dtest=ClusterConnectionsTest -Dsurefire.failIfNoSpecifiedTests=false`

### Commit
`feat(server): extract ClusterConnections`

## Task 4 — `AdminSurface` + `AdminSurfaceTest`

### Files
- New: `kawa-server/src/main/java/io/jonasg/kawa/server/AdminSurface.java`
- New: `kawa-server/src/test/java/io/jonasg/kawa/server/AdminSurfaceTest.java`

### Failing test first

`startBindsAndCloseStops` — no broker needed: `KafkaTopicAdmin` wraps a lazy
`AdminClient` (no connection until a call).

```java
package io.jonasg.kawa.server;

class AdminSurfaceTest {

    @Test
    void startBindsAndCloseStops() throws Exception {
        // given
        var state = new DynamicGatewayState("localhost:9092", "__kawa", null);
        var cache = new MetadataCache();
        var admin = new AdminSurface(
                new AdminConfig(true, "127.0.0.1", 0, null), state, cache, "localhost:9092", null);

        // when
        admin.start();

        // then
        assertThat(admin.boundPort()).isGreaterThan(0);

        // when
        admin.close();
    }
}
```

### Implementation

```java
public final class AdminSurface implements AutoCloseable {

    private final AdminHttpServer server;

    public AdminSurface(AdminConfig config, DynamicGatewayState state, MetadataCache cache,
                        String bootstrapServers, BrokerAuthConfig brokerAuth) {
        var topicAdmin = new KafkaTopicAdmin(bootstrapServers, brokerAuth);
        server = new AdminHttpServer(config, state.virtualTopics(), cache, state.configManager(),
                state.governance(), topicAdmin);
    }

    public void start() throws InterruptedException {
        server.start();
    }

    public int boundPort() {
        return server.boundPort();
    }

    @Override
    public void close() {
        server.stop(); // AdminHttpServer.stop() also closes the TopicAdmin
    }
}
```

### Verify
`./mvnw -pl kawa-server -am test -Dtest=AdminSurfaceTest -Dsurefire.failIfNoSpecifiedTests=false`

### Commit
`feat(server): extract AdminSurface`

## Task 5 — Recompose `KafkaGateway`

### Files
- Modified: `kawa-server/src/main/java/io/jonasg/kawa/server/KafkaGateway.java`

### Implementation

Per spec. Fields become: `config`, `cache` (final), `metrics`, `codec`,
`apiVersionsBuilder`, `dynamicState`, `listener`, `connections`, `admin`,
`running`. `start()` is a readable sequence of labeled steps; `stop()` collapses
to `admin → dynamicState → connections → listener`; `configTopicProps` is
removed (moved into `DynamicGatewayState`); `resolveAdvertised` and
`parseBootstrap` stay as private statics.

```java
@Override
public void start() throws Exception {
    if (running) {
        log.warn("Cannot start Gateway because it is already running");
        return;
    }

    // 1. Validate cluster, parse bootstrap
    ClusterConfig cluster = config.defaultCluster();
    if (cluster == null || cluster.bootstrapServers().isEmpty()) {
        throw new IllegalStateException("No cluster configured");
    }
    String bootstrapServers = cluster.bootstrapServers().get(0);
    InetSocketAddress bootstrap = parseBootstrap(bootstrapServers);

    // 2. Shared infrastructure
    metrics = new GatewayMetrics(new SimpleMeterRegistry());
    KafkaApiRegistry registry = KafkaApiRegistry.create();
    codec = new KafkaBodyCodec(registry);
    apiVersionsBuilder = new ApiVersionsResponseBuilder(SupportedVersions.from(registry));

    // 3. Dynamic config from the config topic (blocks until caught up)
    dynamicState = new DynamicGatewayState(bootstrapServers, config.configTopic(), config.auth().brokerAuth());
    dynamicState.start();

    // 4. Bind the client listener; resolve the advertised endpoint
    ListenerConfig listenerConfig = config.listeners().getFirst();
    listener = new KafkaListener(metrics, codec);
    int boundPort = listener.bind(listenerConfig.host(), listenerConfig.port());
    AdvertisedListener advertised = resolveAdvertised(config.advertised(), boundPort);

    // 5. Interceptor pipeline
    RequestPipeline requestPipeline = buildPipeline(dynamicState, advertised);

    // 6. Broker connections (metadata client)
    connections = new ClusterConnections(codec, requestPipeline.pipeline(), metrics, cache,
            bootstrap.getHostString(), bootstrap.getPort(), config.auth().brokerAuth());
    connections.start();

    // 7. Dispatcher — installed only after the initial metadata fetch, so the listener
    //    (bound earlier for advertised-port resolution) closes connections until then
    var dispatcher = new KafkaClientRequestHandler(
            codec, apiVersionsBuilder, requestPipeline.pipeline(),
            connections.router(), connections.pool(), connections.metadataClient(),
            metrics, requestPipeline.fetchSessions(), dynamicState.saslAuthenticator());
    listener.installDispatcher(dispatcher);

    // 8. Admin surface
    if (config.admin().enabled()) {
        admin = new AdminSurface(config.admin(), dynamicState, cache, bootstrapServers, config.auth().brokerAuth());
        admin.start();
    }
    running = true;
    log.info("kawa gateway listening on {}:{} (advertised as {}:{})",
            listenerConfig.host(), boundPort, advertised.host(), advertised.port());
}

@Override
public void stop() {
    if (!running) {
        return;
    }
    running = false;
    if (admin != null) {
        admin.close();
    }
    if (dynamicState != null) {
        dynamicState.close();
    }
    if (connections != null) {
        connections.close();
    }
    if (listener != null) {
        listener.close();
    }
}

public int boundPort() {
    return listener.boundPort();
}

public int adminBoundPort() {
    return admin == null ? -1 : admin.boundPort();
}

private RequestPipeline buildPipeline(DynamicGatewayState state, AdvertisedListener advertised) {
    var fetchSessions = new FetchSessionRegistry();
    var interceptors = new ArrayList<Interceptor>();
    interceptors.add(new AuthorizationInterceptor(state.authorizer(), state.virtualTopics()));
    if (!state.authorizer().hasAnyAcls()) {
        log.warn("RBAC has no roles or groups configured - every request will be denied");
    }
    interceptors.add(new VirtualTopicInterceptor(state.virtualTopics(), advertised, fetchSessions));
    return new RequestPipeline(new InterceptorPipeline(interceptors), fetchSessions);
}

private record RequestPipeline(InterceptorPipeline pipeline, FetchSessionRegistry fetchSessions) {
}
```

Ordering notes (behavior-preserving):
- Listener binds before the dispatcher exists (port-0 advertised resolution).
- `connections.start()` (metadata client) completes before `installDispatcher`.
- Config topic caught up (step 3) before anything serves.
- `metadataClient.start()` now runs before the dispatcher is *constructed*
  instead of after — no functional difference (the dispatcher ctor only stores
  references; `start()` doesn't depend on the dispatcher).
- `stop()` order preserved: admin → dynamic config → metadata client → broker
  pool → server channel → event loop groups.

### Verify
1. `./mvnw -pl kawa-server -am test` — all unit tests green.
2. Full IT suite (Docker required): `./mvnw clean verify -pl kawa-integration-tests -am`.

### Commit
`refactor(server): recompose KafkaGateway around extracted components`

## Final verification

- `./mvnw -pl kawa-server -am test`
- `./mvnw clean verify -pl kawa-integration-tests -am`
- Review `git log --oneline` and `git status` (only intended files).

## Out of scope (unchanged)

- `MetricsConfig` still ignored (always `SimpleMeterRegistry`).
- Failure-path cleanup in `start()` deliberately not added.
- No changes to existing component constructors.