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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.internals.RocksDBVersionedStore.RocksDBVersionedStoreClient;
import org.apache.kafka.streams.state.internals.RocksDBVersionedStore.VersionedStoreClient;
import org.apache.kafka.streams.state.internals.RocksDBVersionedStore.VersionedStoreSegment;

import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/**
 * A write buffer for use in restoring a {@link RocksDBVersionedStore} from its changelog. This
 * class exposes a {@link VersionedStoreClient} to put records into the write buffer, which may
 * then be flushed to the store via {@link WriteBatch}es, for improved write efficiency during
 * restoration.
 * <p>
 * The structure of the internals of this write buffer mirrors the structure of the
 * {@code RocksDBVersionedStore} itself, i.e., data for the latest value store and each of the
 * segment stores is buffered in a separate object -- specifically, a map.
 */
public class RocksDBVersionedStoreRestoreWriteBuffer {

    private static final Logger log = LoggerFactory.getLogger(RocksDBVersionedStoreRestoreWriteBuffer.class);

    // write buffer for latest value store. value type is Optional in order to track tombstones
    // which must be written to the underlying store.
    private final Map<Bytes, Optional<byte[]>> latestValueWriteBuffer;
    // map from segment id to write buffer. segments are stored in reverse-sorted order,
    // so getReverseSegments() is more efficient
    private final TreeMap<Long, WriteBufferSegmentWithDbFallback> segmentsWriteBuffer;
    private final RocksDBVersionedStoreClient dbClient;
    private final RocksDBVersionedStoreRestoreClient restoreClient;

    /**
     * Creates a new write buffer.
     * @param dbClient client for reading from and writing to the underlying persistent store
     */
    RocksDBVersionedStoreRestoreWriteBuffer(final RocksDBVersionedStoreClient dbClient) {
        this.dbClient = Objects.requireNonNull(dbClient);

        this.latestValueWriteBuffer = new HashMap<>();
        // store in reverse-sorted order, to make getReverseSegments() more efficient
        this.segmentsWriteBuffer = new TreeMap<>((x, y) -> Long.compare(y, x));
        this.restoreClient = new RocksDBVersionedStoreRestoreClient();
    }

    /**
     * @return client for writing to (and reading from) the write buffer
     */
    VersionedStoreClient<?> getClient() {
        return restoreClient;
    }

    /**
     * Flushes the contents of the write buffer into the persistent store, and clears the write
     * buffer in the process.
     * @throws RocksDBException if a failure occurs adding to or writing a {@link WriteBatch}
     */
    void flush() throws RocksDBException {

        // flush segments first, as this is consistent with the store always writing to
        // older segments/stores before later ones
        try (final WriteBatch segmentsBatch = new WriteBatch()) {
            final List<WriteBufferSegmentWithDbFallback> allSegments = restoreClient.reversedSegments(Long.MIN_VALUE);
            if (!allSegments.isEmpty()) {
                // collect entries into write batch
                for (final WriteBufferSegmentWithDbFallback bufferSegment : allSegments) {
                    final LogicalKeyValueSegment dbSegment = bufferSegment.dbSegment();
                    for (final Map.Entry<Bytes, byte[]> segmentEntry : bufferSegment.getAll().entrySet()) {
                        dbSegment.addToBatch(
                            new KeyValue<>(segmentEntry.getKey().get(), segmentEntry.getValue()),
                            segmentsBatch);
                    }
                }

                // write to db. all the logical segments share the same physical store,
                // so we can use any segment to perform the write
                allSegments.get(0).dbSegment().write(segmentsBatch);
            }
        } catch (final RocksDBException e) {
            log.error("Error restoring batch to RocksDBVersionedStore segments store.");
            throw e;
        }
        segmentsWriteBuffer.clear();

        // flush latest value store
        try (final WriteBatch latestValueBatch = new WriteBatch()) {
            // collect entries into write batch
            for (final Map.Entry<Bytes, Optional<byte[]>> latestValueEntry : latestValueWriteBuffer.entrySet()) {
                final byte[] value = latestValueEntry.getValue().orElse(null);
                dbClient.addToLatestValueBatch(
                    new KeyValue<>(latestValueEntry.getKey().get(), value),
                    latestValueBatch);
            }

            // write to db
            dbClient.writeLatestValues(latestValueBatch);
        } catch (final RocksDBException e) {
            log.error("Error restoring batch to RocksDBVersionedStore latest value store.");
            throw e;
        }
        latestValueWriteBuffer.clear();
    }

    /**
     * The object representation of the write buffer corresponding to a single segment store.
     * Contains the write buffer itself (a simple hash map) and also a reference to the underlying
     * persistent segment store.
     */
    private class WriteBufferSegmentWithDbFallback implements VersionedStoreSegment {

        private final long id;
        private final Map<Bytes, byte[]> data;
        private final LogicalKeyValueSegment dbSegment;

        WriteBufferSegmentWithDbFallback(final LogicalKeyValueSegment dbSegment) {
            this.dbSegment = Objects.requireNonNull(dbSegment);
            this.id = dbSegment.id();
            this.data = new HashMap<>();

            // register segment with segments store
            segmentsWriteBuffer.put(id, this);
        }

        LogicalKeyValueSegment dbSegment() {
            return dbSegment;
        }

        @Override
        public long id() {
            return id;
        }

        @Override
        public void put(final Bytes key, final byte[] value) {
            // all writes go to the write buffer
            data.put(key, value);
        }

        @Override
        public byte[] get(final Bytes key) {
            final byte[] bufferValue = data.get(key);
            if (bufferValue != null) {
                return bufferValue;
            }
            return dbSegment.get(key);
        }

        Map<Bytes, byte[]> getAll() {
            return Collections.unmodifiableMap(data);
        }
    }

    /**
     * Client for writing to (and reading from) the write buffer as part of restore.
     */
    private class RocksDBVersionedStoreRestoreClient implements VersionedStoreClient<WriteBufferSegmentWithDbFallback> {

        @Override
        public byte[] latestValue(final Bytes key) {
            final Optional<byte[]> bufferValue = latestValueWriteBuffer.get(key);
            if (bufferValue != null) {
                return bufferValue.orElse(null);
            }
            return dbClient.latestValue(key);
        }

        @Override
        public void putLatestValue(final Bytes key, final byte[] value) {
            // all writes go to write buffer
            latestValueWriteBuffer.put(key, Optional.ofNullable(value));
        }

        @Override
        public void deleteLatestValue(final Bytes key) {
            putLatestValue(key, null);
        }

        @Override
        public WriteBufferSegmentWithDbFallback getOrCreateSegmentIfLive(final long segmentId, final StateStoreContext context, final long streamTime) {
            if (segmentsWriteBuffer.containsKey(segmentId)) {
                return segmentsWriteBuffer.get(segmentId);
            }

            final LogicalKeyValueSegment dbSegment = dbClient.getOrCreateSegmentIfLive(segmentId, context, streamTime);
            if (dbSegment == null) {
                // segment is not live
                return null;
            }
            // creating a new segment automatically registers it with the segments store
            return new WriteBufferSegmentWithDbFallback(dbSegment);
        }

        @Override
        public List<WriteBufferSegmentWithDbFallback> reversedSegments(final long timestampFrom) {
            // head and not tail because the map is sorted in reverse order
            final long segmentFrom = segmentIdForTimestamp(timestampFrom);
            final List<WriteBufferSegmentWithDbFallback> bufferSegments =
                new ArrayList<>(segmentsWriteBuffer.headMap(segmentFrom, true).values());

            final List<LogicalKeyValueSegment> dbSegments = dbClient.reversedSegments(timestampFrom);

            // merge segments from db with segments from write buffer
            final List<WriteBufferSegmentWithDbFallback> allSegments = new ArrayList<>();
            int dbIndex = 0;
            int bufferIndex = 0;
            while (dbIndex < dbSegments.size() && bufferIndex < bufferSegments.size()) {
                final LogicalKeyValueSegment dbSegment = dbSegments.get(dbIndex);
                final WriteBufferSegmentWithDbFallback bufferSegment = bufferSegments.get(bufferIndex);
                final long dbSegmentId = dbSegment.id();
                final long bufferSegmentId = bufferSegment.id();
                if (dbSegmentId > bufferSegmentId) {
                    // creating a new segment automatically registers it with the segments store
                    allSegments.add(new WriteBufferSegmentWithDbFallback(dbSegment));
                    dbIndex++;
                } else if (dbSegmentId < bufferSegmentId) {
                    allSegments.add(bufferSegment);
                    bufferIndex++;
                } else {
                    allSegments.add(bufferSegment);
                    dbIndex++;
                    bufferIndex++;
                }
            }
            while (dbIndex < dbSegments.size()) {
                // creating a new segment automatically registers it with the segments store
                allSegments.add(new WriteBufferSegmentWithDbFallback(dbSegments.get(dbIndex)));
                dbIndex++;
            }
            while (bufferIndex < bufferSegments.size()) {
                allSegments.add(bufferSegments.get(bufferIndex));
                bufferIndex++;
            }
            return allSegments;
        }

        @Override
        public long segmentIdForTimestamp(final long timestamp) {
            return dbClient.segmentIdForTimestamp(timestamp);
        }
    }
}