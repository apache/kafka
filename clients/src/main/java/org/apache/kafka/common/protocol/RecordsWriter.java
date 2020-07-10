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

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
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
public class RecordsWriter implements Writable {
    private final String dest;
    private final Consumer<Send> sendConsumer;
    private final ByteArrayOutputStream byteArrayOutputStream;
    private final DataOutput output;

    public RecordsWriter(String dest, Consumer<Send> sendConsumer) {
        this.dest = dest;
        this.sendConsumer = sendConsumer;
        this.byteArrayOutputStream = new ByteArrayOutputStream();
        this.output = new DataOutputStream(this.byteArrayOutputStream);
    }

    @Override
    public void writeByte(byte val) {
        writeQuietly(() -> output.writeByte(val));
    }

    @Override
    public void writeShort(short val) {
        writeQuietly(() -> output.writeShort(val));
    }

    @Override
    public void writeInt(int val) {
        writeQuietly(() -> output.writeInt(val));
    }

    @Override
    public void writeLong(long val) {
        writeQuietly(() -> output.writeLong(val));

    }

    @Override
    public void writeDouble(double val) {
        writeQuietly(() -> ByteUtils.writeDouble(val, output));

    }

    @Override
    public void writeByteArray(byte[] arr) {
        writeQuietly(() -> output.write(arr));
    }

    @Override
    public void writeUnsignedVarint(int i) {
        writeQuietly(() -> ByteUtils.writeUnsignedVarint(i, output));
    }

    @Override
    public void writeByteBuffer(ByteBuffer src) {
        writeQuietly(() -> output.write(src.array(), src.position(), src.remaining()));
    }

    @FunctionalInterface
    private interface IOExceptionThrowingRunnable {
        void run() throws IOException;
    }

    private void writeQuietly(IOExceptionThrowingRunnable runnable) {
        try {
            runnable.run();
        } catch (IOException e) {
            throw new RuntimeException("Writable encountered an IO error", e);
        }
    }

    @Override
    public void writeRecords(BaseRecords records) {
        flush();
        sendConsumer.accept(records.toSend(dest));
    }

    /**
     * Flush any pending bytes as a ByteBufferSend and reset the buffer
     */
    public void flush() {
        ByteBufferSend send = new ByteBufferSend(dest,
                ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
        sendConsumer.accept(send);
        byteArrayOutputStream.reset();
    }
}
