package io.jonasg.kawa.server.broker;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Inbound handler for a broker connection: hands each decoded frame to its
/// [BrokerClient] for correlation-id lookup and response dispatch.
final class BrokerResponseHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(BrokerResponseHandler.class);

    private final BrokerClient client;

    BrokerResponseHandler(BrokerClient client) {
        this.client = client;
    }

    @Override
    public void channelRead(
            ChannelHandlerContext ctx,
            Object msg
    ) {
        ByteBuf buf = (ByteBuf) msg;
        log.debug("Broker channel read {} bytes (correlation {})", buf.readableBytes(),
                buf.readableBytes() >= 4 ? buf.getInt(buf.readerIndex()) : -1);
        client.handleBrokerResponse(buf);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        client.onBrokerDisconnected();
        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(
            ChannelHandlerContext ctx,
            Throwable cause
    ) {
        log.warn("Broker connection error", cause);
        ctx.close();
    }
}
