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
package org.apache.kafka.raft.internals;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.apache.kafka.common.protocol.DataInputStreamReadable;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.server.common.serialization.RecordSerde;

public final class RecordsIterator<T> implements Iterator<Batch<T>>, AutoCloseable {
    private final Records records;
    private final RecordSerde<T> serde;
    private final BufferSupplier bufferSupplier;
    private final int batchSize;

    private Iterator<MutableRecordBatch> nextBatches = Collections.emptyIterator();
    private Optional<Batch<T>> nextBatch = Optional.empty();
    // Buffer used as the backing store for nextBatches if needed
    private Optional<ByteBuffer> allocatedBuffer = Optional.empty();
    // Number of bytes from records read up to now
    private int bytesRead = 0;
    private boolean isClosed = false;

    public RecordsIterator(
        Records records,
        RecordSerde<T> serde,
        BufferSupplier bufferSupplier,
        int batchSize
    ) {
        this.records = records;
        this.serde = serde;
        this.bufferSupplier = bufferSupplier;
        this.batchSize = Math.max(batchSize, Records.HEADER_SIZE_UP_TO_MAGIC);
    }

    @Override
    public boolean hasNext() {
        ensureOpen();

        if (!nextBatch.isPresent()) {
            nextBatch = nextBatch();
        }

        return nextBatch.isPresent();
    }

    @Override
    public Batch<T> next() {
        if (!hasNext()) {
            throw new NoSuchElementException("Batch iterator doesn't have any more elements");
        }

        Batch<T> batch = nextBatch.get();
        nextBatch = Optional.empty();

        return batch;
    }

    @Override
    public void close() {
        isClosed = true;
        allocatedBuffer.ifPresent(bufferSupplier::release);
        allocatedBuffer = Optional.empty();
    }

    private void ensureOpen() {
        if (isClosed) {
            throw new IllegalStateException("Serde record batch itererator was closed");
        }
    }

    private MemoryRecords readFileRecords(FileRecords fileRecords, ByteBuffer buffer) {
        int start = buffer.position();
        try {
            fileRecords.readInto(buffer, bytesRead);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read records into memory", e);
        }

        bytesRead += buffer.limit() - start;
        return MemoryRecords.readableRecords(buffer.slice());
    }

    private MemoryRecords createMemoryRecords(FileRecords fileRecords) {
        final ByteBuffer buffer;
        if (allocatedBuffer.isPresent()) {
            buffer = allocatedBuffer.get();
            buffer.compact();
        } else {
            buffer = bufferSupplier.get(Math.min(batchSize, records.sizeInBytes()));
            allocatedBuffer = Optional.of(buffer);
        }

        MemoryRecords memoryRecords = readFileRecords(fileRecords, buffer);

        // firstBatchSize() is always non-null because the minimum buffer is HEADER_SIZE_UP_TO_MAGIC.
        if (memoryRecords.firstBatchSize() <= buffer.remaining()) {
            return memoryRecords;
        } else {
            // Not enough bytes read; create a bigger buffer
            ByteBuffer newBuffer = bufferSupplier.get(memoryRecords.firstBatchSize());
            allocatedBuffer = Optional.of(newBuffer);

            newBuffer.put(buffer);
            bufferSupplier.release(buffer);

            return readFileRecords(fileRecords, newBuffer);
        }
    }

    private Iterator<MutableRecordBatch> nextBatches() {
        int recordSize = records.sizeInBytes();
        if (bytesRead < recordSize) {
            final MemoryRecords memoryRecords;
            if (records instanceof MemoryRecords) {
                bytesRead = recordSize;
                memoryRecords = (MemoryRecords) records;
            } else if (records instanceof FileRecords) {
                memoryRecords = createMemoryRecords((FileRecords) records);
            } else {
                throw new IllegalStateException(String.format("Unexpected Records type %s", records.getClass()));
            }

            return memoryRecords.batchIterator();
        }

        return Collections.emptyIterator();
    }

    private Optional<Batch<T>> nextBatch() {
        if (!nextBatches.hasNext()) {
            nextBatches = nextBatches();
        }

        if (nextBatches.hasNext()) {
            MutableRecordBatch nextBatch = nextBatches.next();

            // Update the buffer position to reflect the read batch
            allocatedBuffer.ifPresent(buffer -> buffer.position(buffer.position() + nextBatch.sizeInBytes()));

            if (!(nextBatch instanceof DefaultRecordBatch)) {
                throw new IllegalStateException(
                    String.format("DefaultRecordBatch expected by record type was %s", nextBatch.getClass())
                );
            }

            return Optional.of(readBatch((DefaultRecordBatch) nextBatch));
        }

        return Optional.empty();
    }

    private Batch<T> readBatch(DefaultRecordBatch batch) {
        final Batch<T> result;
        if (batch.isControlBatch()) {
            result = Batch.control(
                batch.baseOffset(),
                batch.partitionLeaderEpoch(),
                batch.maxTimestamp(),
                batch.sizeInBytes(),
                batch.lastOffset()
            );
        } else {
            Integer numRecords = batch.countOrNull();
            if (numRecords == null) {
                throw new IllegalStateException("Expected a record count for the records batch");
            }

            List<T> records = new ArrayList<>(numRecords);
            try (DataInputStreamReadable input = new DataInputStreamReadable(batch.recordInputStream(bufferSupplier))) {
                for (int i = 0; i < numRecords; i++) {
                    T record = readRecord(input);
                    records.add(record);
                }
            }

            result = Batch.data(
                batch.baseOffset(),
                batch.partitionLeaderEpoch(),
                batch.maxTimestamp(),
                batch.sizeInBytes(),
                records
            );
        }

        return result;
    }

    private T readRecord(Readable input) {
        // Read size of body in bytes
        input.readVarint();

        // Read unused attributes
        input.readByte();

        long timestampDelta = input.readVarlong();
        if (timestampDelta != 0) {
            throw new IllegalArgumentException();
        }

        // Read offset delta
        input.readVarint();

        int keySize = input.readVarint();
        if (keySize != -1) {
            throw new IllegalArgumentException("Unexpected key size " + keySize);
        }

        int valueSize = input.readVarint();
        if (valueSize < 0) {
            throw new IllegalArgumentException();
        }

        T record = serde.read(input, valueSize);

        int numHeaders = input.readVarint();
        if (numHeaders != 0) {
            throw new IllegalArgumentException();
        }

        return record;
    }
}
