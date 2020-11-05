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
package org.apache.kafka.common.network;

import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * This class provides a way to build {@link Send} objects for network
 * transmission from generated {@link org.apache.kafka.common.protocol.ApiMessage}
 * types. Its main advantage over direct {@link ByteBuffer} allocation based on
 * {@link org.apache.kafka.common.protocol.ApiMessage#size(ObjectSerializationCache, short)}
 * is that it avoids copying "bytes" fields. The downside is that it is up to the caller
 * to allocate a buffer which accounts only for the additional request overhead.
 *
 * See {@link org.apache.kafka.common.requests.EnvelopeRequest#toSend(String, RequestHeader)}
 * for example usage.
 */
public class SendBuilder implements Writable {
    private final List<ByteBuffer> buffers = new ArrayList<>();
    private final ByteBuffer buffer;

    public SendBuilder(ByteBuffer buffer) {
        this.buffer = buffer;
        this.buffer.mark();
    }

    private void maybeCloseBlock() {
        int latestPosition = buffer.position();
        buffer.reset();

        if (latestPosition > buffer.position()) {
            ByteBuffer duplicate = buffer.duplicate();
            duplicate.limit(latestPosition);
            buffers.add(duplicate);
            buffer.position(latestPosition);
            buffer.mark();
        }
    }

    @Override
    public void writeByte(byte val) {
        buffer.put(val);
    }

    @Override
    public void writeShort(short val) {
        buffer.putShort(val);
    }

    @Override
    public void writeInt(int val) {
        buffer.putInt(val);
    }

    @Override
    public void writeLong(long val) {
        buffer.putLong(val);
    }

    @Override
    public void writeDouble(double val) {
        buffer.putDouble(val);
    }

    @Override
    public void writeByteArray(byte[] arr) {
        maybeCloseBlock();
        buffers.add(ByteBuffer.wrap(arr));
    }

    @Override
    public void writeUnsignedVarint(int i) {
        ByteUtils.writeUnsignedVarint(i, buffer);
    }

    @Override
    public void writeByteBuffer(ByteBuffer buf) {
        maybeCloseBlock();
        buffers.add(buf);
    }

    @Override
    public void writeVarint(int i) {
        ByteUtils.writeVarint(i, buffer);
    }

    @Override
    public void writeVarlong(long i) {
        ByteUtils.writeVarlong(i, buffer);
    }

    public Send toSend(String destination) {
        maybeCloseBlock();
        ByteBuffer[] buffers = this.buffers.toArray(new ByteBuffer[0]);
        return new ByteBufferSend(destination, buffers);
    }

}
