package io.jonasg.kawa.server;

import io.jonasg.kawa.config.AdvertisedListener;
import io.jonasg.kawa.config.BrokerAuthConfig;
import io.jonasg.kawa.config.ClusterConfig;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.ListenerConfig;
import io.jonasg.kawa.config.RbacConfig;
import io.jonasg.kawa.core.Gateway;
import io.jonasg.kawa.core.Interceptor;
import io.jonasg.kawa.core.InterceptorPipeline;
import io.jonasg.kawa.core.VirtualTopicManager;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.metrics.GatewayMetrics;
import io.jonasg.kawa.http.AdminHttpServer;
import io.jonasg.kawa.protocol.kafka.ApiVersionsResponseBuilder;
import io.jonasg.kawa.protocol.kafka.KafkaApiRegistry;
import io.jonasg.kawa.protocol.kafka.KafkaBodyCodec;
import io.jonasg.kawa.protocol.kafka.SupportedVersions;
import io.jonasg.kawa.rbac.AuthorizationInterceptor;
import io.jonasg.kawa.rbac.RbacAuthorizer;
import io.jonasg.kawa.server.auth.SaslAuthenticator;
import io.jonasg.kawa.server.broker.BrokerClientPool;
import io.jonasg.kawa.server.broker.MetadataClient;
import io.jonasg.kawa.server.netty.KafkaServerInitializer;
import io.jonasg.kawa.virtualtopic.FetchSessionRegistry;
import io.jonasg.kawa.virtualtopic.VirtualTopicInterceptor;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/// Netty-based Kafka gateway: accepts client connections, rewrites virtual topics and
/// forwards traffic to the physical cluster.
///
/// Milestone 1 scope: a single cluster, plaintext only, one broker connection per
/// physical broker, requests routed by the first partition's leader (clients converge
/// across brokers via NOT_LEADER retries).
///
/// The constructor takes the static bootstrap config: cluster bootstrap servers, broker
/// credentials, config topic name, listeners, advertised endpoint and admin. Virtual topics,
/// RBAC and client auth are read from the config topic and keep updating live via
/// [DynamicConfigManager]; clusters, listeners, admin and advertised stay startup-only.
/// An empty config topic on first boot is valid - the gateway boots with no virtual topics,
/// default-deny RBAC and no client auth.
public final class KafkaGateway implements Gateway {

    private static final Logger log = LoggerFactory.getLogger(KafkaGateway.class);

    private final GatewayConfig config;
    private final MetadataCache cache = new MetadataCache();

    private EventLoopGroup parentGroup;
    private EventLoopGroup childGroup;
    private EventLoopGroup brokerGroup;
    private Channel serverChannel;
    private MetadataClient metadataClient;
    private BrokerClientPool brokerPool;
    private GatewayMetrics metrics;
    private AdminHttpServer adminServer;
    private DynamicConfigManager dynamicConfig;

    private volatile boolean running;

    public KafkaGateway(GatewayConfig config) {
        this.config = config;
    }

    @Override
    public void start() throws Exception {
        if (running) {
            log.warn("Cannot start Gateway because it is already running");
            return;
        }

        ClusterConfig cluster = config.defaultCluster();
        if (cluster == null || cluster.bootstrapServers().isEmpty()) {
            throw new IllegalStateException("No cluster configured");
        }

        String bootstrapServers = cluster.bootstrapServers().get(0);
        InetSocketAddress bootstrap = parseBootstrap(bootstrapServers);

        parentGroup = new NioEventLoopGroup(1);
        childGroup = new NioEventLoopGroup();
        brokerGroup = new NioEventLoopGroup();
        metrics = new GatewayMetrics(new SimpleMeterRegistry());

        // The mutable consumers start empty; the config topic populates them before the
        // gateway serves anything. An empty topic on first boot is valid - the gateway boots
        // with no virtual topics, default-deny RBAC and no client auth.
        var virtualTopics = new VirtualTopicManager(Map.of());
        var authorizer = new RbacAuthorizer(new RbacConfig(Map.of(), Map.of()));
        var saslAuthenticator = new SaslAuthenticator(Set.of());

        // A fresh consumer group per boot: each gateway instance must read the full config
        // topic, and the catch-up model re-reads from the earliest offset anyway.
        dynamicConfig = new DynamicConfigManager(
                bootstrapServers,
                config.configTopic(),
                "kawa-config-" + UUID.randomUUID(),
                configTopicProps(config.auth().brokerAuth()),
                virtualTopics, authorizer, saslAuthenticator);
        dynamicConfig.start();
        log.info("waiting for initial config load from topic '{}' on {}", config.configTopic(), bootstrapServers);
        dynamicConfig.awaitInitialLoad();

        // Startup-only config (listeners, advertised, admin) comes from the static file;
        // only virtual topics, RBAC and client auth are dynamic from the config topic.
        ListenerConfig listener = config.listeners().getFirst();

        KafkaApiRegistry registry = KafkaApiRegistry.create();
        var codec = new KafkaBodyCodec(registry);

        KafkaServerInitializer.DispatcherHolder holder = new KafkaServerInitializer.DispatcherHolder();
        var server = new ServerBootstrap();
        server.group(parentGroup, childGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new KafkaServerInitializer(holder, metrics, codec));
        serverChannel = server.bind(listener.host(), listener.port()).sync().channel();
        int boundPort = ((InetSocketAddress) serverChannel.localAddress()).getPort();

        AdvertisedListener advertised = resolveAdvertised(config.advertised(), boundPort);
        var fetchSessions = new FetchSessionRegistry();
        var interceptors = new ArrayList<Interceptor>();
        interceptors.add(new AuthorizationInterceptor(authorizer, virtualTopics));
        if (!authorizer.hasAnyAcls()) {
            log.warn("RBAC has no roles or groups configured - every request will be denied");
        }
        interceptors.add(new VirtualTopicInterceptor(virtualTopics, advertised, fetchSessions));
        var pipeline = new InterceptorPipeline(interceptors);

        ApiVersionsResponseBuilder apiVersionsBuilder =
                new ApiVersionsResponseBuilder(SupportedVersions.from(registry));

        // The broker pool and metadata client connect to the static bootstrap - the same
        // cluster that hosts the config topic. The dynamic snapshot's `clusters` map is
        // carried in the message but not used for routing in this milestone.
        brokerPool = new BrokerClientPool(brokerGroup, codec, pipeline, metrics, cache,
                bootstrap.getHostString(), bootstrap.getPort(), config.auth().brokerAuth());
        metadataClient = new MetadataClient(bootstrap.getHostString(), bootstrap.getPort(),
                brokerGroup, codec, cache, brokerPool, metrics, config.auth().brokerAuth());

        var dispatcher = new KafkaClientRequestHandler(
                codec, apiVersionsBuilder, pipeline,
                new LeaderRouter(cache), brokerPool, metadataClient, metrics, fetchSessions,
                saslAuthenticator);
        metadataClient.start();
        // The dispatcher is only installed after the initial metadata fetch, so the Kafka
        // listener (bound earlier for advertised-port resolution) closes every connection
        // until the gateway can actually route.
        holder.set(dispatcher);
        if (config.admin().enabled()) {
            adminServer = new AdminHttpServer(config.admin(), virtualTopics, cache, dynamicConfig);
            adminServer.start();
        }
        running = true;
        log.info("kawa gateway listening on {}:{} (advertised as {}:{})",
                listener.host(), boundPort, advertised.host(), advertised.port());
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (adminServer != null) {
            adminServer.stop();
        }
        if (dynamicConfig != null) {
            dynamicConfig.close();
        }
        if (metadataClient != null) {
            metadataClient.stop();
        }
        if (brokerPool != null) {
            brokerPool.closeAll();
        }
        if (serverChannel != null) {
            serverChannel.close();
        }
        if (parentGroup != null) {
            parentGroup.shutdownGracefully();
            childGroup.shutdownGracefully();
            brokerGroup.shutdownGracefully();
        }
    }

    public int boundPort() {
        return ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    /// The bound port of the admin HTTP server, or `-1` when the admin surface is disabled.
    public int adminBoundPort() {
        return adminServer == null ? -1 : adminServer.boundPort();
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

    private static AdvertisedListener resolveAdvertised(
            AdvertisedListener advertised,
            int boundPort
    ) {
        Integer port = advertised.port() == null || advertised.port() == 0 ? boundPort : advertised.port();
        return AdvertisedListener.of(advertised.nodeId(), advertised.host(), port);
    }

    private static InetSocketAddress parseBootstrap(String server) {
        int scheme = server.indexOf("://");
        String hostPort = scheme >= 0 ? server.substring(scheme + 3) : server;
        int colon = hostPort.lastIndexOf(':');
        if (colon < 0) {
            throw new IllegalArgumentException("Invalid bootstrap server: " + server);
        }
        String host = hostPort.substring(0, colon);
        int port = Integer.parseInt(hostPort.substring(colon + 1));
        return new InetSocketAddress(host, port);
    }
}
