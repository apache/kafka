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
package org.apache.kafka.streams.state.internals;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

public abstract class AbstractTransactionalSegment extends AbstractTransactionalStore<Segment> implements Segment {
    final KeyValueSegment tmpStore;
    final Segment mainStore;
    private final String name;

    AbstractTransactionalSegment(final String segmentName,
                                 final String windowName,
                                 final long segmentId,
                                 final RocksDBMetricsRecorder metricsRecorder) {
        this.mainStore = createMainStore(segmentName, windowName, segmentId, metricsRecorder);
        this.tmpStore = createTmpStore(segmentName, windowName, segmentId, metricsRecorder);
        this.name = PREFIX + mainStore.name();
    }

    abstract Segment createMainStore(final String segmentName,
                                     final String windowName,
                                     final long segmentId,
                                     final RocksDBMetricsRecorder metricsRecorder);

    public abstract void openDB(final Map<String, Object> configs, final File stateDir);

    @Override
    public void addToBatch(final KeyValue<byte[], byte[]> record, final WriteBatch batch)
        throws RocksDBException {
        mainStore.addToBatch(record, batch);
    }

    @Override
    public void write(final WriteBatch batch) throws RocksDBException {
        mainStore.write(batch);
    }

    @Override
    public long id() {
        return mainStore.id();
    }

    @Override
    public void destroy() throws IOException {
        tmpStore.destroy();
        mainStore.destroy();
    }

    @Override
    public void deleteRange(final Bytes keyFrom, final Bytes keyTo) {
        try (KeyValueIterator<Bytes, byte[]> iterator = range(keyFrom, keyTo)) {
            while (iterator.hasNext()) {
                final KeyValue<Bytes, byte[]> next = iterator.next();
                delete(next.key);
            }
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Segment mainStore() {
        return mainStore;
    }

    @Override
    public KeyValueSegment tmpStore() {
        return tmpStore;
    }

    @Override
    public int compareTo(final Segment o) {
        return Long.compare(mainStore.id(), o.id());
    }

    @Override
    public String toString() {
        return "TransactionalKeyValueSegment(id=" + mainStore.id() + ", name=" + name() + ")";
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final AbstractTransactionalSegment segment = (AbstractTransactionalSegment) obj;
        return mainStore.id() == segment.mainStore.id();
    }

    @Override
    public int hashCode() {
        return Objects.hash(mainStore.id());
    }
}
