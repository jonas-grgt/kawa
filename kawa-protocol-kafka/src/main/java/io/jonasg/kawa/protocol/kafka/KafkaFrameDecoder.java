package io.jonasg.kawa.protocol.kafka;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;

import java.util.List;

/// Splits a Kafka stream into length-prefixed frames. Each emitted `ByteBuf` is the
/// frame payload (without the 4-byte length prefix) and must be released by the consumer.
public final class KafkaFrameDecoder extends ByteToMessageDecoder {

    public static final int DEFAULT_MAX_FRAME_SIZE = 100 * 1024 * 1024;

    private final int maxFrameSize;

    public KafkaFrameDecoder() {
        this(DEFAULT_MAX_FRAME_SIZE);
    }

    public KafkaFrameDecoder(int maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
    }

    @Override
    protected void decode(
            ChannelHandlerContext ctx,
            ByteBuf in,
            List<Object> out
    ) {
        if (in.readableBytes() < Integer.BYTES) {
            return;
        }
        in.markReaderIndex();
        int length = in.readInt();
        if (length < 0 || length > maxFrameSize) {
            throw new CorruptedFrameException("Invalid frame length: " + length);
        }
        if (in.readableBytes() < length) {
            in.resetReaderIndex();
            return;
        }
        out.add(in.readRetainedSlice(length));
    }
}
