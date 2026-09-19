package io.jonasg.kawa.server.broker;

import io.jonasg.kawa.config.BrokerAuthConfig;
import io.jonasg.kawa.core.InterceptorPipeline;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.jonasg.kawa.core.metrics.GatewayMetrics;
import io.jonasg.kawa.protocol.kafka.KafkaBodyCodec;
import io.jonasg.kawa.server.LeaderRouter;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

/// The gateway's own connections to the cluster: the broker client pool, the metadata client
/// that refreshes the [MetadataCache] and captures the broker's ApiVersions ranges, and the
/// [LeaderRouter] that routes requests to the leader of a topic's first partition.
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

    /// Starts the metadata client, which blocks until the initial metadata fetch completes.
    public void start() throws Exception {
        metadataClient.start();
    }

    public BrokerClientPool pool() {
        return pool;
    }

    public MetadataClient metadataClient() {
        return metadataClient;
    }

    public LeaderRouter router() {
        return router;
    }

    @Override
    public void close() {
        metadataClient.stop();
        pool.closeAll();
        brokerGroup.shutdownGracefully();
    }
}
