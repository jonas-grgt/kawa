package io.jonasg.kawa.protocol.kafka;

import io.netty.buffer.ByteBuf;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.utils.ByteUtils;

import java.nio.ByteBuffer;

public final class ByteBufWritable implements Writable {

    private final ByteBuf out;

    public ByteBufWritable(ByteBuf out) {
        this.out = out;
    }

    @Override
    public void writeByte(byte value) {
        out.writeByte(value);
    }

    @Override
    public void writeShort(short value) {
        out.writeShort(value);
    }

    @Override
    public void writeInt(int value) {
        out.writeInt(value);
    }

    @Override
    public void writeLong(long value) {
        out.writeLong(value);
    }

    @Override
    public void writeDouble(double value) {
        out.writeDouble(value);
    }

    @Override
    public void writeByteArray(byte[] value) {
        out.writeBytes(value);
    }

    @Override
    public void writeUnsignedVarint(int value) {
        ByteBuffer tmp = ByteBuffer.allocate(5);
        ByteUtils.writeUnsignedVarint(value, tmp);
        tmp.flip();
        out.writeBytes(tmp);
    }

    @Override
    public void writeByteBuffer(ByteBuffer value) {
        out.writeBytes(value);
    }

    @Override
    public void writeVarint(int value) {
        ByteBuffer tmp = ByteBuffer.allocate(5);
        ByteUtils.writeVarint(value, tmp);
        tmp.flip();
        out.writeBytes(tmp);
    }

    @Override
    public void writeVarlong(long value) {
        ByteBuffer tmp = ByteBuffer.allocate(10);
        ByteUtils.writeVarlong(value, tmp);
        tmp.flip();
        out.writeBytes(tmp);
    }
}
