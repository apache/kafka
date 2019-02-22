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
    public void readArray(byte[] arr) {
        buf.get(arr);
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
    public void writeArray(byte[] arr) {
        buf.put(arr);
    }
}
