package io.jonasg.kawa.protocol.kafka;

import io.netty.buffer.ByteBuf;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.utils.ByteUtils;

import java.nio.ByteBuffer;

/// Adapter exposing a Netty `ByteBuf` through kafka-clients' `Readable`. After
/// the message has been read, call `consumed()` and advance the buffer's reader index
/// by that amount.
public final class ByteBufReadable implements Readable {

    private final ByteBuffer buffer;

    public ByteBufReadable(ByteBuf buf) {
        this.buffer = buf.nioBuffer(buf.readerIndex(), buf.readableBytes());
    }

    private ByteBufReadable(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public Readable slice() {
        return new ByteBufReadable(buffer.slice());
    }

    @Override
    public byte readByte() {
        return buffer.get();
    }

    @Override
    public short readShort() {
        return buffer.getShort();
    }

    @Override
    public int readInt() {
        return buffer.getInt();
    }

    @Override
    public long readLong() {
        return buffer.getLong();
    }

    @Override
    public double readDouble() {
        return buffer.getDouble();
    }

    @Override
    public byte[] readArray(int size) {
        byte[] array = new byte[size];
        buffer.get(array);
        return array;
    }

    @Override
    public int readUnsignedVarint() {
        return ByteUtils.readUnsignedVarint(buffer);
    }

    @Override
    public ByteBuffer readByteBuffer(int size) {
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        return ByteBuffer.wrap(bytes);
    }

    @Override
    public int readVarint() {
        return ByteUtils.readVarint(buffer);
    }

    @Override
    public long readVarlong() {
        return ByteUtils.readVarlong(buffer);
    }

    @Override
    public int remaining() {
        return buffer.remaining();
    }

    public int consumed() {
        return buffer.position();
    }
}
