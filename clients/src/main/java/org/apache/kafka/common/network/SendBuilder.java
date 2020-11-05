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

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.MessageSize;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.MultiRecordsSend;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;

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
    private final Queue<Send> sends = new ArrayDeque<>();
    private final ByteBuffer buffer;
    private final String destinationId;

    private SendBuilder(String destinationId, int overheadSize) {
        this.destinationId = destinationId;
        this.buffer = ByteBuffer.allocate(overheadSize);
        this.buffer.mark();
    }

    private void maybeCloseBlock() {
        int latestPosition = buffer.position();
        buffer.reset();

        if (latestPosition > buffer.position()) {
            ByteBuffer duplicate = buffer.duplicate();
            duplicate.limit(latestPosition);
            addByteBufferSend(duplicate);
            buffer.position(latestPosition);
            buffer.mark();
        }
    }

    private void addByteBufferSend(ByteBuffer buffer) {
        sends.add(new ByteBufferSend(destinationId, buffer));
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
        buffer.put(arr);
    }

    @Override
    public void writeUnsignedVarint(int i) {
        ByteUtils.writeUnsignedVarint(i, buffer);
    }

    @Override
    public void writeByteBuffer(ByteBuffer buf) {
        maybeCloseBlock();
        addByteBufferSend(buf);
    }

    @Override
    public void writeVarint(int i) {
        ByteUtils.writeVarint(i, buffer);
    }

    @Override
    public void writeVarlong(long i) {
        ByteUtils.writeVarlong(i, buffer);
    }

    @Override
    public void writeRecords(BaseRecords records) {
        maybeCloseBlock();
        sends.add(records.toSend(destinationId));
    }

    public Send toSend(String destination) {
        maybeCloseBlock();
        return new MultiRecordsSend(destination, sends);
    }

    public static Send buildRequestSend(
        String destination,
        RequestHeader header,
        ApiMessage apiRequest
    ) {
        return buildSend(
            destination,
            header.data(),
            header.headerVersion(),
            apiRequest,
            header.apiVersion()
        );
    }

    public static Send buildResponseSend(
        String destination,
        ResponseHeader header,
        ApiMessage apiResponse,
        short apiVersion
    ) {
        return buildSend(
            destination,
            header.data(),
            header.headerVersion(),
            apiResponse,
            apiVersion
        );
    }

    private static Send buildSend(
        String destination,
        ApiMessage header,
        short headerVersion,
        ApiMessage apiMessage,
        short apiVersion
    ) {
        ObjectSerializationCache serializationCache = new ObjectSerializationCache();
        MessageSize messageSize = apiMessage.messageSize(serializationCache, apiVersion);

        int totalSize = header.size(serializationCache, headerVersion) + messageSize.totalSize();
        int sizeExcludingZeroCopyFields = totalSize - messageSize.zeroCopySize();

        SendBuilder builder = new SendBuilder(destination, sizeExcludingZeroCopyFields + 4);
        builder.writeInt(totalSize);
        builder.writeApiMessage(header, serializationCache, headerVersion);
        builder.writeApiMessage(apiMessage, serializationCache, apiVersion);
        return builder.toSend(destination);
    }

}
