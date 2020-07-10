package org.apache.kafka.common.protocol;

import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.ByteUtils;

import java.nio.ByteBuffer;

public class RecordsReader implements Readable {
    private final ByteBuffer buf;

    public RecordsReader(ByteBuffer buf) {
        this.buf = buf;
    }

    @Override
    public byte readByte() {
        return buf.get();
    }

    @Override
    public short readShort() {
        return buf.getShort();
    }

    @Override
    public int readInt() {
        return buf.getInt();
    }

    @Override
    public long readLong() {
        return buf.getLong();
    }

    @Override
    public double readDouble() {
        return ByteUtils.readDouble(buf);
    }

    @Override
    public void readArray(byte[] arr) {
        buf.get(arr);
    }

    @Override
    public int readUnsignedVarint() {
        return ByteUtils.readUnsignedVarint(buf);
    }

    @Override
    public ByteBuffer readByteBuffer(int length) {
        ByteBuffer res = buf.slice();
        res.limit(length);

        buf.position(buf.position() + length);

        return res;
    }

    @Override
    public BaseRecords readRecords(int length) {
        if (length < 0) {
            // no records
            return null;
        } else {
            ByteBuffer recordsBuffer = readByteBuffer(length);
            return new MemoryRecords(recordsBuffer);
        }
    }
}
