package org.apache.kafka.common.protocol;

import org.apache.kafka.common.utils.ByteUtils;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DataInputStreamReadable implements Readable, Closeable {
    protected final DataInputStream input;

    public DataInputStreamReadable(DataInputStream input) {
        this.input = input;
    }

    @Override
    public byte readByte() {
        try {
            return input.readByte();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public short readShort() {
        try {
            return input.readShort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int readInt() {
        try {
            return input.readInt();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long readLong() {
        try {
            return input.readLong();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public double readDouble() {
        try {
            return input.readDouble();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void readArray(byte[] arr) {
        try {
            input.readFully(arr);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int readUnsignedVarint() {
        try {
            return ByteUtils.readUnsignedVarint(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ByteBuffer readByteBuffer(int length) {
        byte[] arr = new byte[length];
        readArray(arr);
        return ByteBuffer.wrap(arr);
    }

    @Override
    public int readVarint() {
        try {
            return ByteUtils.readVarint(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long readVarlong() {
        try {
            return ByteUtils.readVarlong(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            input.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
