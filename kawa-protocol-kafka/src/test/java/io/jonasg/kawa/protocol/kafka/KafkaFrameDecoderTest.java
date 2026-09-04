package io.jonasg.kawa.protocol.kafka;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.CorruptedFrameException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaFrameDecoderTest {

    @Test
    void emitsFullFrames() {
        var channel = new EmbeddedChannel(new KafkaFrameDecoder(1024));

        ByteBuf input = Unpooled.buffer();
        input.writeInt(4);
        input.writeBytes(new byte[]{1, 2, 3, 4});
        input.writeInt(2);
        input.writeBytes(new byte[]{9, 8});

        assertThat(channel.writeInbound(input)).isTrue();

        ByteBuf first = channel.readInbound();
        assertThat(first.readableBytes()).isEqualTo(4);
        assertThat(first.getByte(0)).isEqualTo((byte) 1);
        first.release();

        ByteBuf second = channel.readInbound();
        assertThat(second.readableBytes()).isEqualTo(2);
        second.release();

        assertThat(channel.finish()).isFalse();
    }

    @Test
    void buffersPartialFramesAcrossReads() {
        var channel = new EmbeddedChannel(new KafkaFrameDecoder(1024));

        ByteBuf firstChunk = Unpooled.buffer();
        firstChunk.writeInt(6);
        firstChunk.writeBytes(new byte[]{1, 2});
        assertThat(channel.writeInbound(firstChunk)).isFalse();

        ByteBuf secondChunk = Unpooled.buffer();
        secondChunk.writeBytes(new byte[]{3, 4, 5, 6});
        assertThat(channel.writeInbound(secondChunk)).isTrue();

        ByteBuf frame = channel.readInbound();
        assertThat(frame.readableBytes()).isEqualTo(6);
        assertThat(frame.getByte(5)).isEqualTo((byte) 6);
        frame.release();
    }

    @Test
    void rejectsOversizedFrames() {
        var channel = new EmbeddedChannel(new KafkaFrameDecoder(100));

        ByteBuf input = Unpooled.buffer();
        input.writeInt(101);

        assertThatThrownBy(() -> channel.writeInbound(input))
                .isInstanceOf(CorruptedFrameException.class);
    }

    @Test
    void rejectsNegativeLength() {
        var channel = new EmbeddedChannel(new KafkaFrameDecoder(100));

        ByteBuf input = Unpooled.buffer();
        input.writeInt(-1);

        assertThatThrownBy(() -> channel.writeInbound(input))
                .isInstanceOf(CorruptedFrameException.class);
    }
}
