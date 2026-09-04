package io.jonasg.kawa.protocol.kafka;

import io.netty.buffer.ByteBuf;

public final class ResponseHeaderCodec {

    /// Reads the correlation id and advances past any tag buffer. On return the frame's reader
    /// index points at the start of the response body.
    public int decodeCorrelationId(
            ByteBuf frame,
            short headerVersion
    ) {
        int correlationId = frame.readInt();
        if (headerVersion >= 1) {
            Wire.skipTagBuffer(frame);
        }
        return correlationId;
    }

    public int headerSize(short headerVersion) {
        return headerVersion >= 1 ? 4 + Wire.tagBufferSize() : 4;
    }

    public void encode(
            ByteBuf out,
            short headerVersion,
            int correlationId
    ) {
        out.writeInt(correlationId);
        if (headerVersion >= 1) {
            Wire.writeTagBuffer(out);
        }
    }
}
