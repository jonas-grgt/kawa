package io.jonasg.kawa.protocol.kafka;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RequestHeaderCodecTest {

    private final RequestHeaderCodec codec = new RequestHeaderCodec();

    @Test
    void roundTripsClassicHeader() {
        KafkaHeader header = KafkaHeader.of((short) 3, (short) 8, 1234, "test-client");
        assertThat(header.requestHeaderVersion()).isEqualTo((short) 1);

        ByteBuf buf = Unpooled.buffer();
        codec.encode(buf, header);
        buf.writeBytes(new byte[]{1, 2, 3, 4});

        KafkaHeader decoded = codec.decode(buf);
        assertThat(decoded).isEqualTo(header);
        assertThat(buf.readableBytes()).isEqualTo(4);
    }

    @Test
    void roundTripsNullClientIdClassicHeader() {
        KafkaHeader header = KafkaHeader.of((short) 3, (short) 8, 1, null);

        ByteBuf buf = Unpooled.buffer();
        codec.encode(buf, header);
        codec.encode(buf, header);

        assertThat(codec.decode(buf)).isEqualTo(header);
        assertThat(codec.decode(buf)).isEqualTo(header);
    }

    @Test
    void roundTripsFlexibleHeader() {
        // Metadata v9 is flexible -> request header v2 (classic client id + tag buffer)
        KafkaHeader header = KafkaHeader.of((short) 3, (short) 9, 99, "flex-client");
        assertThat(header.requestHeaderVersion()).isEqualTo((short) 2);

        ByteBuf buf = Unpooled.buffer();
        codec.encode(buf, header);
        buf.writeBytes(new byte[]{9, 9, 9});

        KafkaHeader decoded = codec.decode(buf);
        assertThat(decoded).isEqualTo(header);
        assertThat(buf.readableBytes()).isEqualTo(3);
    }

    @Test
    void headerSizeMatchesEncodedSize() {
        KafkaHeader header = KafkaHeader.of((short) 18, (short) 3, 5, "abc");

        ByteBuf buf = Unpooled.buffer();
        codec.encode(buf, header);

        assertThat(codec.headerSize(header)).isEqualTo(buf.readableBytes());
    }

    @Test
    void decodesRealKafkaClientApiVersionsV3Header() {
        // Bytes captured from a real kafka-clients 3.8 ApiVersions v3 request:
        // apiKey=18, version=3, correlationId=1, clientId="producer-1" (int16 length),
        // empty tag buffer.
        ByteBuf buf = Unpooled.wrappedBuffer(new byte[]{
                0x00, 0x12, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01,
                0x00, 0x0a, 'p', 'r', 'o', 'd', 'u', 'c', 'e', 'r', '-', '1',
                0x00,
                0x00, 0x12, 'x'
        });

        KafkaHeader decoded = codec.decode(buf);

        assertThat(decoded.apiKey()).isEqualTo((short) 18);
        assertThat(decoded.apiVersion()).isEqualTo((short) 3);
        assertThat(decoded.correlationId()).isEqualTo(1);
        assertThat(decoded.clientId()).isEqualTo("producer-1");
        assertThat(buf.readableBytes()).isEqualTo(3);
    }
}
