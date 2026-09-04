package io.jonasg.kawa.protocol.kafka;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public final class KafkaFrameEncoder extends MessageToByteEncoder<ByteBuf> {

    @Override
    protected void encode(
            ChannelHandlerContext ctx,
            ByteBuf msg,
            ByteBuf out
    ) {
        out.writeInt(msg.readableBytes());
        out.writeBytes(msg);
    }
}
