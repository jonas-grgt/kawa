package io.jonasg.kawa.protocol.kafka;

import io.netty.buffer.ByteBuf;

public final class RequestHeaderCodec {

    /// Decodes a request header from the start of a frame. On return the frame's reader index
    /// points at the start of the request body.
    public KafkaHeader decode(ByteBuf frame) {
        int apiKey = frame.readShort();
        short apiVersion = frame.readShort();
        short headerVersion = KafkaHeader.of((short) apiKey, apiVersion, 0, null).requestHeaderVersion();
        int correlationId = frame.readInt();
        String clientId = Wire.readNullableString(frame);
        if (headerVersion >= 2) {
            Wire.skipTagBuffer(frame);
        }
        return new KafkaHeader((short) apiKey, apiVersion, correlationId, clientId);
    }

    public int headerSize(KafkaHeader header) {
        int size = 2 + 2 + 4;
        size += Wire.sizeOfNullableString(header.clientId());
        if (header.requestHeaderVersion() >= 2) {
            size += Wire.tagBufferSize();
        }
        return size;
    }

    public void encode(
            ByteBuf out,
            KafkaHeader header
    ) {
        out.writeShort(header.apiKey());
        out.writeShort(header.apiVersion());
        out.writeInt(header.correlationId());
        Wire.writeNullableString(out, header.clientId());
        if (header.requestHeaderVersion() >= 2) {
            Wire.writeTagBuffer(out);
        }
    }
}
