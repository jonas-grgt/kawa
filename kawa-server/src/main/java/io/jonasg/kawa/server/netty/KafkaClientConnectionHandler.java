package io.jonasg.kawa.server.netty;

import io.jonasg.kawa.protocol.kafka.KafkaBodyCodec;
import io.jonasg.kawa.protocol.kafka.KafkaClientRequest;
import io.jonasg.kawa.protocol.kafka.KafkaHeader;
import io.jonasg.kawa.protocol.kafka.RequestHeaderCodec;
import io.jonasg.kawa.server.KafkaClientRequestHandler;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/// Inbound handler for kafka client connections: forwards each decoded request frame to the
/// dispatcher and notifies it when the connection is torn down.
public final class KafkaClientConnectionHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(KafkaClientConnectionHandler.class);

    private final KafkaClientRequestHandler kafkaClientRequestHandler;
    private final ClientSession session;
    private final KafkaBodyCodec codec;
    private final RequestHeaderCodec requestHeaderCodec = new RequestHeaderCodec();

    public KafkaClientConnectionHandler(
            KafkaClientRequestHandler kafkaClientRequestHandler,
            ClientSession session,
            KafkaBodyCodec codec
    ) {
        this.kafkaClientRequestHandler = kafkaClientRequestHandler;
        this.session = session;
        this.codec = codec;
    }

    @Override
    public void channelRead(
            ChannelHandlerContext ctx,
            Object msg
    ) {
        ByteBuf frame = (ByteBuf) msg;

        try {
            KafkaHeader header = requestHeaderCodec.decode(frame);
            Object body = codec.decodeRequest(header.apiKey(), header.apiVersion(), frame);
            ByteBuf rawBody = null;
            if (body != null) {
                frame.release();
            } else {
                rawBody = frame;
            }
            var req = new KafkaClientRequest(header, body, rawBody);

            kafkaClientRequestHandler.handleRequest(session, req);
        } catch (Exception e) {
            if (log.isDebugEnabled() && frame != null && frame.refCnt() > 0) {
                var hex = new StringBuilder();
                int start = frame.readerIndex();
                for (int i = 0; i < frame.capacity(); i++) {
                    hex.append(String.format("%02x ", frame.getByte(i)));
                }
                log.debug("Failing frame (readerIndex={}, capacity={}): {}", start, frame.capacity(), hex);
            }
            log.warn("Error dispatching request; closing client connection", e);
            if (frame != null && frame.refCnt() > 0) {
                frame.release();
            }
            session.close();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        kafkaClientRequestHandler.sessionClosed(session);
        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(
            ChannelHandlerContext ctx,
            Throwable cause
    ) {
        log.warn("Client connection error: {}", cause.toString());
        if (log.isDebugEnabled()) {
            log.debug("Client connection error stack", cause);
        }
        ctx.close();
    }
}
