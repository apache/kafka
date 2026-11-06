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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.common.network.TransferableChannel;
import org.apache.kafka.common.record.internal.BaseRecords;
import org.apache.kafka.common.record.internal.DefaultRecordsSend;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.RecordsSend;
import org.apache.kafka.common.record.internal.TransferableRecords;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 * A record set whose bytes are spread over a list of {@link ByteBuffer}s, each read over its
 * {@code [position, limit)}. It lets the producer's chunked write path send its chunks without
 * first copying them into one buffer.
 * <p>
 * It is send-only: it does not expose batches or records. A caller that needs to read the records
 * (the producer when splitting a batch that is too large) uses {@link #flatten()}.
 */
public class CompositeMemoryRecords implements TransferableRecords {

    public static final CompositeMemoryRecords EMPTY = new CompositeMemoryRecords(List.of());

    private final ByteBuffer[] buffers;
    private final int sizeInBytes;

    public CompositeMemoryRecords(List<ByteBuffer> buffers) {
        this.buffers = buffers.toArray(new ByteBuffer[0]);
        int size = 0;
        for (ByteBuffer buffer : this.buffers) {
            size += buffer.remaining();
        }
        this.sizeInBytes = size;
    }

    /**
     * Duplicates of the backing buffers, in order, so callers can't disturb their positions.
     */
    public List<ByteBuffer> buffers() {
        return Arrays.asList(duplicates());
    }

    private ByteBuffer[] duplicates() {
        ByteBuffer[] duplicates = new ByteBuffer[buffers.length];
        for (int i = 0; i < buffers.length; i++) {
            duplicates[i] = buffers[i].duplicate();
        }
        return duplicates;
    }

    @Override
    public int sizeInBytes() {
        return sizeInBytes;
    }

    @Override
    public int writeTo(TransferableChannel channel, int position, int length) throws IOException {
        if (((long) position) + length > sizeInBytes) {
            throw new IllegalArgumentException("position+length should not be greater than sizeInBytes, position: "
                    + position + ", length: " + length + ", sizeInBytes: " + sizeInBytes);
        }

        // The whole record set: every buffer is written as is, so nothing needs slicing.
        if (position == 0 && length == sizeInBytes) {
            return (int) channel.write(duplicates());
        }

        ByteBuffer[] views = new ByteBuffer[buffers.length];
        int count = 0;
        int skip = position;
        int remaining = length;
        for (ByteBuffer buffer : buffers) {
            if (remaining == 0) {
                break;
            }
            if (skip >= buffer.remaining()) {
                skip -= buffer.remaining();
                continue;
            }
            ByteBuffer view = buffer.duplicate();
            view.position(view.position() + skip);
            int n = Math.min(view.remaining(), remaining);
            view.limit(view.position() + n);
            views[count++] = view;
            remaining -= n;
            skip = 0;
        }
        return (int) channel.write(views, 0, count);
    }

    @Override
    public RecordsSend<? extends BaseRecords> toSend() {
        return new DefaultRecordsSend<>(this);
    }

    /**
     * Copy the bytes into a single new buffer, to read them back with the {@link MemoryRecords}
     * read path.
     */
    public MemoryRecords flatten() {
        ByteBuffer flattened = ByteBuffer.allocate(sizeInBytes);
        for (ByteBuffer buffer : buffers) {
            flattened.put(buffer.duplicate());
        }
        flattened.flip();
        return MemoryRecords.readableRecords(flattened);
    }

    @Override
    public String toString() {
        return "CompositeMemoryRecords(size=" + sizeInBytes + ", buffers=" + buffers.length + ")";
    }
}
