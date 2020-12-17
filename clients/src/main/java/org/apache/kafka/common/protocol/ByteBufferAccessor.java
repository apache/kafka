/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.protocol;

import org.apache.kafka.common.utils.ByteUtils;

import java.nio.ByteBuffer;

public class ByteBufferAccessor implements Readable, Writable {
    private final ByteBuffer buf;

    public ByteBufferAccessor(ByteBuffer buf) {
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
    public void writeByte(byte val) {
        buf.put(val);
    }

    @Override
    public void writeShort(short val) {
        buf.putShort(val);
    }

    @Override
    public void writeInt(int val) {
        buf.putInt(val);
    }

    @Override
    public void writeLong(long val) {
        buf.putLong(val);
    }

    @Override
    public void writeDouble(double val) {
        ByteUtils.writeDouble(val, buf);
    }

    @Override
    public void writeByteArray(byte[] arr) {
        buf.put(arr);
    }

    @Override
    public void writeUnsignedVarint(int i) {
        ByteUtils.writeUnsignedVarint(i, buf);
    }

    @Override
    public void writeByteBuffer(ByteBuffer src) {
        buf.put(src.duplicate());
    }

    @Override
    public void writeVarint(int i) {
        ByteUtils.writeVarint(i, buf);
    }

    @Override
    public void writeVarlong(long i) {
        ByteUtils.writeVarlong(i, buf);
    }

    @Override
    public int readVarint() {
        return ByteUtils.readVarint(buf);
    }

    @Override
    public long readVarlong() {
        return ByteUtils.readVarlong(buf);
    }

    public void flip() {
        buf.flip();
    }

    public ByteBuffer buffer() {
        return buf;
    }
}
