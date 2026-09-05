package io.jonasg.kawa.server.broker;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;

import static org.assertj.core.api.Assertions.assertThat;

class MetadataClientTest {

    @Test
    void connectWithRetryWaitsForBrokerToComeUp() throws Exception {
        // given — a free port that is not yet listening
        int port;
        try (var probe = new ServerSocket(0)) {
            port = probe.getLocalPort();
        }
        var group = new NioEventLoopGroup(1);
        var bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                    }
                });
        var client = new MetadataClient("127.0.0.1", port, group, null, null, null, null);

        // when — the listener comes up after the first connect is refused
        var serverThread = new Thread(() -> {
            try {
                Thread.sleep(500);
                try (var server = new ServerSocket(port)) {
                    Thread.sleep(2000);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        serverThread.start();

        Channel channel = client.connectWithRetry(bootstrap);

        // then
        assertThat(channel.isActive()).isTrue();
        channel.close().sync();
        serverThread.join();
        group.shutdownGracefully().sync();
    }
}
