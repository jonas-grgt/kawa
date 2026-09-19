package io.jonasg.kawa.server.netty;

import io.jonasg.kawa.core.metrics.GatewayMetrics;
import io.jonasg.kawa.protocol.kafka.KafkaBodyCodec;
import io.jonasg.kawa.server.KafkaClientRequestHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;

/// The client-facing Netty listener. Binds the server socket and owns the
/// [KafkaServerInitializer.DispatcherHolder], so connections are closed until a dispatcher is
/// installed - the listener is bound before the dispatcher exists (port-0 advertised
/// resolution), and the dispatcher is only installed after the initial metadata fetch.
public final class KafkaListener implements AutoCloseable {

    private final EventLoopGroup parentGroup = new NioEventLoopGroup(1);
    private final EventLoopGroup childGroup = new NioEventLoopGroup();
    private final KafkaServerInitializer.DispatcherHolder dispatcherHolder =
            new KafkaServerInitializer.DispatcherHolder();
    private final GatewayMetrics metrics;
    private final KafkaBodyCodec codec;

    private Channel serverChannel;

    public KafkaListener(GatewayMetrics metrics, KafkaBodyCodec codec) {
        this.metrics = metrics;
        this.codec = codec;
    }

    /// Binds the server socket and returns the bound port (useful when `port` is 0).
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

    /// Installs the request dispatcher; connections accepted before this point are closed.
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
