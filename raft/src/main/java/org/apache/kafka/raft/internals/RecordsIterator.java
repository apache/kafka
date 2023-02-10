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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.server.common.serialization.RecordSerde;

public final class RecordsIterator<T> implements Iterator<Batch<T>>, AutoCloseable {
    private final Records records;
    private final RecordSerde<T> serde;
    private final BufferSupplier bufferSupplier;
    private final int batchSize;
    // Setting to true will make the RecordsIterator perform a CRC Validation
    // on the batch header when iterating over them
    private final boolean doCrcValidation;

    private Iterator<MutableRecordBatch> nextBatches = Collections.emptyIterator();
    private Optional<Batch<T>> nextBatch = Optional.empty();
    // Buffer used as the backing store for nextBatches if needed
    private Optional<ByteBuffer> allocatedBuffer = Optional.empty();
    // Number of bytes from records read up to now
    private int bytesRead = 0;
    private boolean isClosed = false;

    /**
     * This class provides an iterator over records retrieved via the raft client or from a snapshot
     * @param records the records
     * @param serde the serde to deserialize records
     * @param bufferSupplier the buffer supplier implementation to allocate buffers when reading records. This must return ByteBuffer allocated on the heap
     * @param batchSize the maximum batch size
     */
    public RecordsIterator(
        Records records,
        RecordSerde<T> serde,
        BufferSupplier bufferSupplier,
        int batchSize,
        boolean doCrcValidation
    ) {
        this.records = records;
        this.serde = serde;
        this.bufferSupplier = bufferSupplier;
        this.batchSize = Math.max(batchSize, Records.HEADER_SIZE_UP_TO_MAGIC);
        this.doCrcValidation = doCrcValidation;
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
            throw new IllegalStateException("Serde record batch iterator was closed");
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
        if (doCrcValidation) {
            // Perform a CRC validity check on this batch
            batch.ensureValid();
        }

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
            DataInputStream input = new DataInputStream(batch.recordInputStream(bufferSupplier));
            try {
                for (int i = 0; i < numRecords; i++) {
                    T record = readRecord(input, batch.sizeInBytes());
                    records.add(record);
                }
            } finally {
                Utils.closeQuietly(input, "DataInputStream");
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

    private T readRecord(DataInputStream stream, int totalBatchSize) {
        // Read size of body in bytes
        int size;
        try {
            size = ByteUtils.readVarint(stream);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to read record size", e);
        }
        if (size <= 0) {
            throw new RuntimeException("Invalid non-positive frame size: " + size);
        }
        if (size > totalBatchSize) {
            throw new RuntimeException("Specified frame size, " + size + ", is larger than the entire size of the " +
                    "batch, which is " + totalBatchSize);
        }
        ByteBuffer buf = bufferSupplier.get(size);

        // The last byte of the buffer is reserved for a varint set to the number of record headers, which
        // must be 0. Therefore, we set the ByteBuffer limit to size - 1.
        buf.limit(size - 1);

        try {
            int bytesRead = stream.read(buf.array(), 0, size);
            if (bytesRead != size) {
                throw new RuntimeException("Unable to read " + size + " bytes, only read " + bytesRead);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read record bytes", e);
        }
        try {
            ByteBufferAccessor input = new ByteBufferAccessor(buf);

            // Read unused attributes
            input.readByte();

            long timestampDelta = input.readVarlong();
            if (timestampDelta != 0) {
                throw new IllegalArgumentException("Got timestamp delta of " + timestampDelta + ", but this is invalid because it " +
                        "is not 0 as expected.");
            }

            // Read offset delta
            input.readVarint();

            int keySize = input.readVarint();
            if (keySize != -1) {
                throw new IllegalArgumentException("Got key size of " + keySize + ", but this is invalid because it " +
                        "is not -1 as expected.");
            }

            int valueSize = input.readVarint();
            if (valueSize < 1) {
                throw new IllegalArgumentException("Got payload size of " + valueSize + ", but this is invalid because " +
                        "it is less than 1.");
            }

            // Read the metadata record body from the file input reader
            T record = serde.read(input, valueSize);

            // Read the number of headers. Currently, this must be a single byte set to 0.
            int numHeaders = buf.array()[size - 1];
            if (numHeaders != 0) {
                throw new IllegalArgumentException("Got numHeaders of " + numHeaders + ", but this is invalid because " +
                        "it is not 0 as expected.");
            }
            return record;
        } finally {
            bufferSupplier.release(buf);
        }
    }
}
