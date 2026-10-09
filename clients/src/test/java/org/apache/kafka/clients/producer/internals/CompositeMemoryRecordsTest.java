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

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.network.TransferableChannel;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.RecordBatch;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that a {@link CompositeMemoryRecords} built from a chunk list sends exactly the bytes of
 * the {@link MemoryRecords} it represents, and flattens back into it. The chunk boundaries fall
 * inside the batch header and inside record bodies.
 */
public class CompositeMemoryRecordsTest {

    private MemoryRecords buildRecords() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE, TimestampType.CREATE_TIME, 0L);
        builder.append(10L, "key1".getBytes(), "value-one".getBytes());
        builder.append(11L, "key2".getBytes(), "the second, longer value".getBytes());
        builder.append(12L, null, "third".getBytes());
        return builder.build();
    }

    /** Split {@code records}' bytes into chunks of {@code partSizes}, with any leftover in a last chunk. */
    private CompositeMemoryRecords composite(MemoryRecords records, int... partSizes) {
        ByteBuffer data = records.buffer();
        List<ByteBuffer> chunks = new ArrayList<>();
        for (int size : partSizes) {
            ByteBuffer chunk = data.duplicate();
            chunk.limit(chunk.position() + Math.min(size, data.remaining()));
            chunks.add(chunk.slice());
            data.position(chunk.limit());
        }
        if (data.hasRemaining()) {
            chunks.add(data.slice());
        }
        return new CompositeMemoryRecords(chunks);
    }

    @Test
    public void testSizeInBytesMatchesFlattened() {
        MemoryRecords flat = buildRecords();
        CompositeMemoryRecords composite = composite(flat, 5, 20, 8, 40);
        assertEquals(flat.sizeInBytes(), composite.sizeInBytes());
    }

    @Test
    public void testFlattenMatchesOriginal() {
        MemoryRecords flat = buildRecords();
        CompositeMemoryRecords composite = composite(flat, 5, 20, 8, 40);
        assertEquals(flat, composite.flatten());
    }

    @Test
    public void testWriteToProducesIdenticalWireBytes() throws IOException {
        MemoryRecords flat = buildRecords();
        CompositeMemoryRecords composite = composite(flat, 4, 13, 6, 30);

        CappedChannel channel = new CappedChannel(composite.sizeInBytes(), Integer.MAX_VALUE);
        int written = composite.writeTo(channel, 0, composite.sizeInBytes());

        assertEquals(flat.sizeInBytes(), written);
        assertArrayEquals(readable(flat.buffer()), channel.written());
    }

    @Test
    public void testWriteToResumesAfterPartialWrites() throws IOException {
        MemoryRecords flat = buildRecords();
        CompositeMemoryRecords composite = composite(flat, 9, 9, 9, 9, 9);

        // A channel that accepts at most 7 bytes per call, forcing writeTo to return short and the
        // caller to resume from the new position — the non-blocking-socket path.
        CappedChannel channel = new CappedChannel(composite.sizeInBytes(), 7);
        int total = composite.sizeInBytes();
        int position = 0;
        int guard = 0;
        while (position < total) {
            int n = composite.writeTo(channel, position, total - position);
            assertTrue(n >= 0);
            position += n;
            assertTrue(guard++ < total + 10, "writeTo made no progress");
        }
        assertEquals(total, position);
        assertArrayEquals(readable(flat.buffer()), channel.written());
    }

    private static byte[] readable(ByteBuffer buffer) {
        ByteBuffer dup = buffer.duplicate();
        byte[] out = new byte[dup.remaining()];
        dup.get(out);
        return out;
    }

    /**
     * A {@link TransferableChannel} that accepts at most {@code cap} bytes per call, stopping at the
     * first buffer it can't fully take, like a socket with a full send buffer.
     */
    private static final class CappedChannel implements TransferableChannel {
        private final ByteBuffer buf;
        private final int cap;
        private boolean closed;

        CappedChannel(long size, int cap) {
            this.buf = ByteBuffer.allocate((int) size);
            this.cap = cap;
        }

        byte[] written() {
            ByteBuffer dup = buf.duplicate();
            dup.flip();
            byte[] out = new byte[dup.remaining()];
            dup.get(out);
            return out;
        }

        @Override
        public int write(ByteBuffer src) {
            int n = Math.min(cap, src.remaining());
            ByteBuffer slice = src.duplicate();
            slice.limit(slice.position() + n);
            buf.put(slice);
            src.position(src.position() + n);
            return n;
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) {
            long total = 0;
            for (int i = offset; i < offset + length && total < cap; i++) {
                int n = Math.min(cap - (int) total, srcs[i].remaining());
                ByteBuffer slice = srcs[i].duplicate();
                slice.limit(slice.position() + n);
                buf.put(slice);
                srcs[i].position(srcs[i].position() + n);
                total += n;
            }
            return total;
        }

        @Override
        public long write(ByteBuffer[] srcs) {
            return write(srcs, 0, srcs.length);
        }

        @Override
        public boolean isOpen() {
            return !closed;
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public boolean hasPendingWrites() {
            return false;
        }

        @Override
        public long transferFrom(FileChannel fileChannel, long position, long count) {
            throw new UnsupportedOperationException();
        }
    }
}
