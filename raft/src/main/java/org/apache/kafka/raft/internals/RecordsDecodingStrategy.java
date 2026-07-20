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

import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.record.internal.DefaultRecordBatch;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.ByteUtils;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.ControlRecord;
import org.apache.kafka.server.common.serialization.RecordSerde;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * Decides which records {@link RecordsIterator} decodes when turning a batch into a {@link Batch}.
 * A batch's records are decoded only when this strategy is interested in them; otherwise the batch
 * is returned as a {@link Batch#skipped} batch carrying only the offset information.
 *
 * <p>Use one of the factory methods to select the behavior:
 * <ul>
 *   <li>{@link #dataAndControl} decodes both the control and data records.</li>
 *   <li>{@link #controlOnly} decodes only the control records and skips the data records. Used by
 *       the internal kraft partition listener, which needs no serde.</li>
 *   <li>{@link #dataOnly} decodes only the data records and skips the control records.</li>
 *   <li>{@link #none} skips both.</li>
 * </ul>
 */
public final class RecordsDecodingStrategy<T> {
    private final boolean decodeControlRecords;
    // When present, data records are decoded with this serde; when empty, they are skipped.
    private final Optional<RecordSerde<T>> serde;

    private RecordsDecodingStrategy(boolean decodeControlRecords, Optional<RecordSerde<T>> serde) {
        this.decodeControlRecords = decodeControlRecords;
        this.serde = serde;
    }

    /**
     * Decodes both the control and data records of a batch.
     */
    public static <T> RecordsDecodingStrategy<T> dataAndControl(RecordSerde<T> serde) {
        return new RecordsDecodingStrategy<>(true, Optional.of(serde));
    }

    /**
     * Decodes only the data records of a batch and skips the control records.
     */
    public static <T> RecordsDecodingStrategy<T> dataOnly(RecordSerde<T> serde) {
        return new RecordsDecodingStrategy<>(false, Optional.of(serde));
    }

    /**
     * Decodes only the control records of a batch and skips the data records.
     */
    public static <T> RecordsDecodingStrategy<T> controlOnly() {
        return new RecordsDecodingStrategy<>(true, Optional.empty());
    }

    /**
     * Skips both the control and data records of a batch.
     */
    public static <T> RecordsDecodingStrategy<T> none() {
        return new RecordsDecodingStrategy<>(false, Optional.empty());
    }

    Batch<T> readBatch(DefaultRecordBatch batch, BufferSupplier bufferSupplier, int numRecords) {
        if (batch.isControlBatch()) {
            return decodeControlRecords ? readControlBatch(batch, bufferSupplier, numRecords) : skippedBatch(batch, numRecords);
        } else {
            return serde
                .map(value -> readDataBatch(batch, value, bufferSupplier, numRecords))
                .orElseGet(() -> skippedBatch(batch, numRecords));
        }
    }

    private static <T> Batch<T> readControlBatch(DefaultRecordBatch batch, BufferSupplier bufferSupplier, int numRecords) {
        InputStream input = batch.recordInputStream(bufferSupplier);
        try {
            List<ControlRecord> records = new ArrayList<>(numRecords);
            for (int i = 0; i < numRecords; i++) {
                records.add(readRecord(input, batch.sizeInBytes(), bufferSupplier, RecordsDecodingStrategy::decodeControlRecord));
            }
            return Batch.control(
                batch.baseOffset(),
                batch.partitionLeaderEpoch(),
                batch.maxTimestamp(),
                batch.sizeInBytes(),
                records
            );
        } finally {
            Utils.closeQuietly(input, "BytesStream for input containing records");
        }
    }

    private static <T> Batch<T> readDataBatch(DefaultRecordBatch batch, RecordSerde<T> serde, BufferSupplier bufferSupplier, int numRecords) {
        InputStream input = batch.recordInputStream(bufferSupplier);
        try {
            List<T> records = new ArrayList<>(numRecords);
            for (int i = 0; i < numRecords; i++) {
                records.add(readRecord(input, batch.sizeInBytes(), bufferSupplier, (key, value) -> decodeDataRecord(key, value, serde)));
            }
            return Batch.data(
                batch.baseOffset(),
                batch.partitionLeaderEpoch(),
                batch.maxTimestamp(),
                batch.sizeInBytes(),
                records
            );
        } finally {
            Utils.closeQuietly(input, "BytesStream for input containing records");
        }
    }

    private static <T> Batch<T> skippedBatch(DefaultRecordBatch batch, int numRecords) {
        return Batch.skipped(
            batch.baseOffset(),
            batch.partitionLeaderEpoch(),
            batch.maxTimestamp(),
            batch.sizeInBytes(),
            numRecords
        );
    }

    @SuppressWarnings("NPathComplexity")
    private static <U> U readRecord(
        InputStream stream,
        int totalBatchSize,
        BufferSupplier bufferSupplier,
        BiFunction<Optional<ByteBuffer>, Optional<ByteBuffer>, U> decoder
    ) {
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

            // Read the key
            int keySize = input.readVarint();
            Optional<ByteBuffer> key = Optional.empty();
            if (keySize >= 0) {
                key = Optional.of(input.readByteBuffer(keySize));
            }

            // Read the value
            int valueSize = input.readVarint();
            Optional<ByteBuffer> value = Optional.empty();
            if (valueSize >= 0) {
                value = Optional.of(input.readByteBuffer(valueSize));
            }

            // Read the record body from the file input reader
            U record = decoder.apply(key, value);

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

    private static <T> T decodeDataRecord(Optional<ByteBuffer> key, Optional<ByteBuffer> value, RecordSerde<T> serde) {
        if (key.isPresent()) {
            throw new IllegalArgumentException("Got key in the record when no key was expected");
        }

        if (value.isEmpty()) {
            throw new IllegalArgumentException("Missing value in the record when a value was expected");
        } else if (value.get().remaining() == 0) {
            throw new IllegalArgumentException("Got an unexpected empty value in the record");
        }

        ByteBuffer valueBuffer = value.get();

        return serde.read(new ByteBufferAccessor(valueBuffer), valueBuffer.remaining());
    }

    private static ControlRecord decodeControlRecord(Optional<ByteBuffer> key, Optional<ByteBuffer> value) {
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Missing key in the record when a key was expected");
        } else if (key.get().remaining() == 0) {
            throw new IllegalArgumentException("Got an unexpected empty key in the record");
        }

        if (value.isEmpty()) {
            throw new IllegalArgumentException("Missing value in the record when a value was expected");
        } else if (value.get().remaining() == 0) {
            throw new IllegalArgumentException("Got an unexpected empty value in the record");
        }

        return ControlRecord.of(key.get(), value.get());
    }
}
