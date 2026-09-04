package io.jonasg.kawa.protocol.kafka;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

/// Low-level Kafka wire primitives (nullable/compact strings, unsigned varints, tag buffers).
/// Package-private on purpose; not part of the public API.
final class Wire {

    private Wire() {
    }

    static String readNullableString(ByteBuf in) {
        int length = in.readShort();
        if (length < 0) {
            return null;
        }
        return in.readCharSequence(length, StandardCharsets.UTF_8).toString();
    }

    static void writeNullableString(
            ByteBuf out,
            String value
    ) {
        if (value == null) {
            out.writeShort(-1);
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeShort(bytes.length);
        out.writeBytes(bytes);
    }

    static int sizeOfNullableString(String value) {
        if (value == null) {
            return 2;
        }
        return 2 + value.getBytes(StandardCharsets.UTF_8).length;
    }

    static String readCompactNullableString(ByteBuf in) {
        int n = readUnsignedVarInt(in);
        if (n == 0) {
            return null;
        }
        return in.readCharSequence(n - 1, StandardCharsets.UTF_8).toString();
    }

    static void writeCompactNullableString(
            ByteBuf out,
            String value
    ) {
        if (value == null) {
            writeUnsignedVarInt(out, 0);
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        writeUnsignedVarInt(out, bytes.length + 1);
        out.writeBytes(bytes);
    }

    static int sizeOfCompactNullableString(String value) {
        if (value == null) {
            return varIntSize(0);
        }
        return varIntSize(value.getBytes(StandardCharsets.UTF_8).length + 1)
                + value.getBytes(StandardCharsets.UTF_8).length;
    }

    static int readUnsignedVarInt(ByteBuf in) {
        int value = 0;
        int shift = 0;
        while (true) {
            int b = in.readUnsignedByte();
            value |= (b & 0x7f) << shift;
            if ((b & 0x80) == 0) {
                return value;
            }
            shift += 7;
            if (shift > 35) {
                throw new IllegalArgumentException("Malformed varint");
            }
        }
    }

    static void writeUnsignedVarInt(
            ByteBuf out,
            int value
    ) {
        while ((value & ~0x7f) != 0) {
            out.writeByte((value & 0x7f) | 0x80);
            value >>>= 7;
        }
        out.writeByte(value);
    }

    static int varIntSize(int value) {
        int size = 1;
        while ((value & ~0x7f) != 0) {
            size++;
            value >>>= 7;
        }
        return size;
    }

    static void skipTagBuffer(ByteBuf in) {
        int count = readUnsignedVarInt(in);
        for (int i = 0; i < count; i++) {
            readUnsignedVarInt(in);
            int size = readUnsignedVarInt(in);
            in.skipBytes(size);
        }
    }

    static void writeTagBuffer(ByteBuf out) {
        writeUnsignedVarInt(out, 0);
    }

    static int tagBufferSize() {
        return 1;
    }
}
