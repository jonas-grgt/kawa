package io.jonasg.kawa.http;

import io.jonasg.kawa.config.AdminConfig;
import io.jonasg.kawa.config.GatewayConfigRepository;
import io.jonasg.kawa.core.VirtualTopicManager;
import io.jonasg.kawa.core.cluster.MetadataCache;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.cors.CorsHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/// Netty HTTP server exposing the gateway's admin/UI surface: `GET /topics` reads the
/// [VirtualTopicManager] and [MetadataCache], and the `/config/...` endpoints read and write
/// the dynamic config through the [GatewayConfigRepository]. It never talks to the broker
/// directly - config writes go to the config topic and are applied by the consumer.
public final class AdminHttpServer {

    private static final Logger log = LoggerFactory.getLogger(AdminHttpServer.class);

    private final AdminConfig config;
    private final VirtualTopicManager virtualTopics;
    private final MetadataCache cache;
    private final Router router;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private Channel serverChannel;

    public AdminHttpServer(
            AdminConfig config,
            VirtualTopicManager virtualTopics,
            MetadataCache cache,
            GatewayConfigRepository configRepository
    ) {
        this.config = config;
        this.virtualTopics = virtualTopics;
        this.cache = cache;
        this.router = new Router()
                .get("/topics", new GetTopicsHandler(virtualTopics, cache))
                .get("/config/virtual-topics", new VirtualTopicsConfigHandler(configRepository))
                .put("/config/virtual-topics/{name}", new VirtualTopicsConfigHandler(configRepository))
                .delete("/config/virtual-topics/{name}", new VirtualTopicsConfigHandler(configRepository))
                .get("/config/rbac/roles", new RbacRolesConfigHandler(configRepository))
                .put("/config/rbac/roles/{name}", new RbacRolesConfigHandler(configRepository))
                .delete("/config/rbac/roles/{name}", new RbacRolesConfigHandler(configRepository))
                .get("/config/rbac/groups", new RbacGroupsConfigHandler(configRepository))
                .put("/config/rbac/groups/{name}", new RbacGroupsConfigHandler(configRepository))
                .delete("/config/rbac/groups/{name}", new RbacGroupsConfigHandler(configRepository))
                .get("/config/auth/users", new AuthUsersConfigHandler(configRepository))
                .put("/config/auth/users/{name}", new AuthUsersConfigHandler(configRepository))
                .delete("/config/auth/users/{name}", new AuthUsersConfigHandler(configRepository));
    }

    public void start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        var bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 128)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast("httpCodec", new HttpServerCodec());
                        ch.pipeline().addLast("httpAggregator", new HttpObjectAggregator(65536));
                        if (config.cors() != null) {
                            ch.pipeline().addLast("cors", new CorsHandler(CorsConfigFactory.from(config.cors())));
                        }
                        ch.pipeline().addLast("httpRouter", new HttpRouterHandler(router));
                    }
                });
        serverChannel = bootstrap.bind(config.host(), config.port()).sync().channel();
        int boundPort = ((InetSocketAddress) serverChannel.localAddress()).getPort();
        log.info("Admin HTTP server listening on {}:{}", config.host(), boundPort);
    }

    public void stop() {
        if (serverChannel != null) {
            serverChannel.close();
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public int boundPort() {
        return ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }
}
