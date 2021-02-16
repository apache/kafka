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

package org.apache.kafka.snapshot;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.raft.RecordSerde;

public final class SnapshotReader<T> implements Closeable, Iterable<List<T>> {
    private final RawSnapshotReader snapshot;
    private final RecordSerde<T> serde;
    private final BufferSupplier bufferSupplier;

    /**
     * A type for reading an immutable snapshot.
     *
     * A snapshot reader can be used to scan through all of the objects T in a snapshot. It
     * is assumed that the content of the snapshot represents all of the objects T for the topic
     * partition from offset 0 up to but not including the end offset in the snapshot id.
     */
    public SnapshotReader(
        RawSnapshotReader snapshot,
        RecordSerde<T> serde,
        BufferSupplier bufferSupplier
    ) {
        this.snapshot = snapshot;
        this.serde = serde;
        this.bufferSupplier = bufferSupplier;
    }

    /**
     * Returns the end offset and epoch for the snapshot.
     */
    public OffsetAndEpoch snapshotId() {
        return snapshot.snapshotId();
    }

    @Override
    public Iterator<List<T>> iterator() {
        return new SnapshotReaderIterator(snapshot.iterator());
    }

    /**
     * Closes the snapshot reader.
     *
     * @throws IOException for any IO error during close
     */
    public void close() throws IOException {
        snapshot.close();
    }


    final class SnapshotReaderIterator implements Iterator<List<T>> {
        private final Iterator<RecordBatch> iterator;
        private List<T> nextBatch;

        SnapshotReaderIterator(Iterator<RecordBatch> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            if (nextBatch == null) {
                nextBatch = findNext();
            }

            return nextBatch != null;
        }

        @Override
        public List<T> next() {
            if (nextBatch == null) {
                nextBatch = findNext();
            }

            if (nextBatch == null) {
                throw new NoSuchElementException(
                    String.format("Snapshot (%s) doesn't have any more elements", snapshotId())
                );
            } else {
                List<T> result = nextBatch;
                nextBatch = null;
                return result;
            }
        }

        private List<T> findNext() {
            RecordBatch batch = null;
            while (iterator.hasNext()) {
                RecordBatch current = iterator.next();
                if (!current.isControlBatch()) {
                    batch = current;
                    break;
                }
            }

            if (batch == null) {
                return null;
            }

            if (batch.countOrNull() == null) {
                throw new IllegalStateException(
                    String.format("Expected a record count the the batch (%s)", batch)
                );
            }
            List<T> data = new ArrayList<>(batch.countOrNull());

            Iterator<Record> records = batch.streamingIterator(bufferSupplier);
            while (records.hasNext()) {
                Record record = records.next();

                if (record.hasValue()) {
                    data.add(
                        serde.read(new ByteBufferAccessor(record.value()), record.value().remaining())
                    );
                } else {
                    throw new IllegalStateException(
                        String.format(
                            "Expected all records in the snapshot (%s) to have a value",
                            snapshotId()
                        )
                    );
                }
            }

            return data;
        }
    }
}
