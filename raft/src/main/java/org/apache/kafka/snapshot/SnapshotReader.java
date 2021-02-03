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
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.record.BufferSupplier.GrowableBufferSupplier;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.raft.RecordSerde;

final public class SnapshotReader<T> implements Closeable, Iterable<List<T>> {
    final private RawSnapshotReader snapshot;
    final private RecordSerde<T> serde;

    /**
     * TODO: write documentation
     */
    public SnapshotReader(
        RawSnapshotReader snapshot,
        RecordSerde<T> serde
    ) {
        this.snapshot = snapshot;
        this.serde = serde;
    }

    /**
     * Returns the end offset and epoch for the snapshot.
     */
    public OffsetAndEpoch snapshotId() {
        return snapshot.snapshotId();
    }

    @Override
    public Iterator<List<T>> iterator() {
        return new SnapshotReaderIterator<>(snapshot.iterator(), serde);
    }

    /**
     * Closes the snapshot reader.
     *
     * @throws IOException for any IO error during close
     */
    public void close() throws IOException {
        snapshot.close();
    }


    final static class SnapshotReaderIterator<T> implements Iterator<List<T>> {
        final Iterator<RecordBatch> iterator;
        final RecordSerde<T> serde;

        SnapshotReaderIterator(Iterator<RecordBatch> iterator, RecordSerde<T> serde) {
            this.iterator = iterator;
            this.serde = serde;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public List<T> next() {
            RecordBatch batch = iterator.next();

            if (batch.countOrNull() == null) {
                throw new IllegalStateException(
                    String.format(
                        "Expected a record count the the batch (%s)",
                        batch
                    )
                );
            }
            List<T> data = new ArrayList<>(batch.countOrNull());

            // TODO: make BufferSupplier configurable
            Iterator<Record> records = batch.streamingIterator(new GrowableBufferSupplier());
            while (records.hasNext()) {
                Record record = records.next();

                // TODO: ignore control records
                // TODO: verify hasValue is true
                data.add(
                    serde.read(new ByteBufferAccessor(record.value()), record.value().remaining())
                );
            }

            return data;
        }
    }
}
