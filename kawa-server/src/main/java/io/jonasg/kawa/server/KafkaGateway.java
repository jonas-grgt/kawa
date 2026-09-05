package io.jonasg.kawa.server;

import io.jonasg.kawa.config.AdvertisedListener;
import io.jonasg.kawa.config.ClusterConfig;
import io.jonasg.kawa.config.GatewayConfig;
import io.jonasg.kawa.config.ListenerConfig;
import io.jonasg.kawa.core.VirtualTopicManager;
import io.jonasg.kawa.core.Gateway;
import io.jonasg.kawa.core.Interceptor;
import io.jonasg.kawa.core.InterceptorPipeline;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.metrics.GatewayMetrics;
import io.jonasg.kawa.http.AdminHttpServer;
import io.jonasg.kawa.protocol.kafka.ApiVersionsResponseBuilder;
import io.jonasg.kawa.protocol.kafka.KafkaApiRegistry;
import io.jonasg.kawa.protocol.kafka.KafkaBodyCodec;
import io.jonasg.kawa.protocol.kafka.SupportedVersions;
import io.jonasg.kawa.virtualtopic.FetchSessionRegistry;
import io.jonasg.kawa.virtualtopic.VirtualTopicInterceptor;
import io.jonasg.kawa.rbac.AuthorizationInterceptor;
import io.jonasg.kawa.rbac.RbacAuthorizer;
import io.jonasg.kawa.server.auth.SaslAuthenticator;
import io.jonasg.kawa.server.broker.BrokerClientPool;
import io.jonasg.kawa.server.broker.MetadataClient;
import io.jonasg.kawa.server.netty.KafkaServerInitializer;
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
import java.util.List;

/// Netty-based Kafka gateway: accepts client connections, rewrites virtual topics and
/// forwards traffic to the physical cluster.
///
/// Milestone 1 scope: a single cluster, plaintext only, one broker connection per
/// physical broker, requests routed by the first partition's leader (clients converge
/// across brokers via NOT_LEADER retries).
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

        InetSocketAddress bootstrap = parseBootstrap(cluster.bootstrapServers().get(0));
        ListenerConfig listener = config.listeners().getFirst();

        parentGroup = new NioEventLoopGroup(1);
        childGroup = new NioEventLoopGroup();
        brokerGroup = new NioEventLoopGroup();
        metrics = new GatewayMetrics(new SimpleMeterRegistry());
        var virtualTopics = new VirtualTopicManager(config.virtualTopics());

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
        var authorizer = new RbacAuthorizer(config.rbac());
        var interceptors = new ArrayList<Interceptor>();
        interceptors.add(new AuthorizationInterceptor(authorizer, virtualTopics));
        if (config.rbac().roles().isEmpty() && config.rbac().groups().isEmpty()) {
            log.warn("RBAC has no roles or groups configured - every request will be denied");
        }
        interceptors.add(new VirtualTopicInterceptor(virtualTopics, advertised, fetchSessions));
        var pipeline = new InterceptorPipeline(interceptors);
        var authConfig = config.auth();
        var saslAuthenticator = new SaslAuthenticator(authConfig.mechanisms(), authConfig.users());

        ApiVersionsResponseBuilder apiVersionsBuilder =
                new ApiVersionsResponseBuilder(SupportedVersions.from(registry));

        brokerPool = new BrokerClientPool(brokerGroup, codec, pipeline, metrics, cache,
                bootstrap.getHostString(), bootstrap.getPort(), authConfig.brokerAuth());
        metadataClient = new MetadataClient(bootstrap.getHostString(), bootstrap.getPort(),
                brokerGroup, codec, cache, brokerPool, metrics, authConfig.brokerAuth());

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
            adminServer = new AdminHttpServer(config.admin(), virtualTopics, cache);
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
