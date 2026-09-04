package io.jonasg.kawa.server.netty;

import io.jonasg.kawa.core.metrics.GatewayMetrics;
import io.jonasg.kawa.protocol.kafka.KafkaBodyCodec;
import io.jonasg.kawa.protocol.kafka.KafkaFrameDecoder;
import io.jonasg.kawa.protocol.kafka.KafkaFrameEncoder;
import io.jonasg.kawa.server.KafkaClientRequestHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

public final class KafkaServerInitializer extends ChannelInitializer<SocketChannel> {

    private final DispatcherHolder dispatcherHolder;
    private final GatewayMetrics metrics;
    private final KafkaBodyCodec codec;

    public KafkaServerInitializer(
            DispatcherHolder dispatcherHolder,
            GatewayMetrics metrics,
            KafkaBodyCodec codec
    ) {
        this.dispatcherHolder = dispatcherHolder;
        this.metrics = metrics;
        this.codec = codec;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        KafkaClientRequestHandler dispatcher = dispatcherHolder.get();
        if (dispatcher == null) {
            ch.close();
            return;
        }
        var session = new ClientSession(ch);
        ch.pipeline().addLast("frameDecoder", new KafkaFrameDecoder());
        ch.pipeline().addLast("frameEncoder", new KafkaFrameEncoder());
        ch.pipeline().addLast("handler", new KafkaClientConnectionHandler(dispatcher, session, codec));
        metrics.clientConnectionOpened();
        ch.closeFuture().addListener(_ -> metrics.clientConnectionClosed());
    }

    /// Holds the dispatcher, which is created only after the listener is bound (port 0 resolution).
    public static final class DispatcherHolder {

        private volatile KafkaClientRequestHandler dispatcher;

        public void set(KafkaClientRequestHandler dispatcher) {
            this.dispatcher = dispatcher;
        }

        public KafkaClientRequestHandler get() {
            return dispatcher;
        }
    }
}
