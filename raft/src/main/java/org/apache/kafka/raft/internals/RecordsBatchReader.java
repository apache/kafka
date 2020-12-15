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

import org.apache.kafka.common.protocol.DataInputStreamReadable;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.record.BufferSupplier;
import org.apache.kafka.common.record.DefaultRecordBatch;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.raft.BatchReader;
import org.apache.kafka.raft.RecordSerde;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.OptionalLong;

public class RecordsBatchReader<T> implements BatchReader<T> {
    private final long baseOffset;
    private final Records records;
    private final RecordSerde<T> serde;
    private final BufferSupplier bufferSupplier;
    private final CloseListener<BatchReader<T>> closeListener;

    private Iterator<MutableRecordBatch> batchIterator;
    private long lastReturnedOffset;
    private Batch<T> nextBatch;
    private boolean isClosed = false;
    private ByteBuffer allocatedBuffer = null;

    public RecordsBatchReader(
        long baseOffset,
        Records records,
        RecordSerde<T> serde,
        BufferSupplier bufferSupplier,
        CloseListener<BatchReader<T>> closeListener
    ) {
        this.baseOffset = baseOffset;
        this.records = records;
        this.serde = serde;
        this.bufferSupplier = bufferSupplier;
        this.closeListener = closeListener;
        this.lastReturnedOffset = baseOffset;
    }

    private void materializeIterator() throws IOException {
        if (records instanceof MemoryRecords) {
            batchIterator = ((MemoryRecords) records).batchIterator();
        } else if (records instanceof FileRecords) {
            this.allocatedBuffer = bufferSupplier.get(records.sizeInBytes());
            ((FileRecords) records).readInto(allocatedBuffer, 0);
            MemoryRecords memRecords = MemoryRecords.readableRecords(allocatedBuffer);
            batchIterator = memRecords.batchIterator();
        } else {
            throw new IllegalStateException("Unexpected Records type " + records.getClass());
        }
    }

    private void findNextDataBatch() {
        if (batchIterator == null) {
            try {
                materializeIterator();
            } catch (IOException e) {
                throw new RuntimeException("Failed to read records into memory", e);
            }
        }

        while (batchIterator.hasNext()) {
            MutableRecordBatch nextBatch = batchIterator.next();
            if (!(nextBatch instanceof DefaultRecordBatch)) {
                throw new IllegalStateException();
            }

            DefaultRecordBatch batch = (DefaultRecordBatch) nextBatch;
            if (!batch.isControlBatch()) {
                this.nextBatch = readBatch(batch);
                return;
            } else {
                this.lastReturnedOffset = batch.lastOffset();
            }
        }
    }

    private Batch<T> readBatch(DefaultRecordBatch batch) {
        Integer numRecords = batch.countOrNull();
        if (numRecords == null) {
            throw new IllegalStateException();
        }

        List<T> records = new ArrayList<>(numRecords);
        try (DataInputStreamReadable input = new DataInputStreamReadable(
            batch.recordInputStream(bufferSupplier))) {
            for (int i = 0; i < numRecords; i++) {
                T record = readRecord(input);
                records.add(record);
            }
            return new Batch<>(
                batch.baseOffset(),
                batch.partitionLeaderEpoch(),
                records
            );
        }
    }

    @Override
    public boolean hasNext() {
        if (nextBatch != null) {
            return true;
        } else {
            findNextDataBatch();
            return nextBatch != null;
        }
    }

    @Override
    public Batch<T> next() {
        if (nextBatch != null) {
            Batch<T> res = nextBatch;
            nextBatch = null;
            lastReturnedOffset = res.lastOffset();
            return res;
        } else {
            findNextDataBatch();
            if (nextBatch == null) {
                throw new NoSuchElementException();
            }
            return next();
        }
    }

    @Override
    public long baseOffset() {
        return baseOffset;
    }

    public OptionalLong lastOffset() {
        if (isClosed) {
            return OptionalLong.of(lastReturnedOffset);
        } else {
            return OptionalLong.empty();
        }
    }

    @Override
    public void close() {
        isClosed = true;

        if (allocatedBuffer != null) {
            bufferSupplier.release(allocatedBuffer);
        }

        closeListener.onClose(this);
    }

    public T readRecord(Readable input) {
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
