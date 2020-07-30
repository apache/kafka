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

import org.apache.kafka.common.network.ByteBufferSend;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.utils.ByteUtils;

import java.io.DataOutput;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

/**
 * Implementation of Writable which produces a sequence of {@link Send} objects. This allows for deferring the transfer
 * of data from a record-set's file channel to the eventual socket channel.
 *
 * Excepting {@link #writeRecords(BaseRecords)}, calls to the write methods on this class will append to a byte array
 * according to the format specified in {@link DataOutput}. When a call is made to writeRecords, any previously written
 * bytes will be flushed as a new {@link ByteBufferSend} to the given Send consumer. After flushing the pending bytes,
 * another Send is passed to the consumer which wraps the underlying record-set's transfer logic.
 *
 * For example,
 *
 * <pre>
 *     recordsWritable.writeInt(10);
 *     recordsWritable.writeRecords(records1);
 *     recordsWritable.writeInt(20);
 *     recordsWritable.writeRecords(records2);
 *     recordsWritable.writeInt(30);
 *     recordsWritable.flush();
 * </pre>
 *
 * Will pass 5 Send objects to the consumer given in the constructor. Care must be taken by callers to flush any
 * pending bytes at the end of the writing sequence to ensure everything is flushed to the consumer. This class is
 * intended to be used with {@link org.apache.kafka.common.record.MultiRecordsSend}.
 *
 * @see org.apache.kafka.common.requests.FetchResponse
 */
public class RecordsWritable implements Writable {
    private final String dest;
    private final Consumer<Send> sendConsumer;
    private final ByteBuffer buffer;
    private int mark;

    public RecordsWritable(String dest, int messageSizeExcludingRecords, Consumer<Send> sendConsumer) {
        this.dest = dest;
        this.sendConsumer = sendConsumer;
        this.buffer = ByteBuffer.allocate(messageSizeExcludingRecords);
        this.mark = 0;
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
        ByteUtils.writeDouble(val, buffer);
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
    public void writeByteBuffer(ByteBuffer src) {
        buffer.put(src);
    }

    public void writeRecords(BaseRecords records) {
        flush();
        sendConsumer.accept(records.toSend(dest));
    }

    /**
     * Flush any pending bytes as a ByteBufferSend
     */
    public void flush() {
        int end = buffer.position();
        int len = end - mark;

        if (len > 0) {
            int limit = buffer.limit();

            // Set the desired absolute position and limit before slicing
            buffer.position(mark);
            buffer.limit(end);
            ByteBuffer slice = buffer.slice();

            // Restore absolute position and limit on original buffer
            buffer.limit(limit);
            buffer.position(end);

            // Update the mark to the end of slice we just took
            mark = end;

            ByteBufferSend send = new ByteBufferSend(dest, slice);
            sendConsumer.accept(send);
        }
    }
}
