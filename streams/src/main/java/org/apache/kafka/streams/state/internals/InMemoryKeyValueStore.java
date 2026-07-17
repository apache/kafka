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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.internals.ByteUtils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.ChangelogRecordDeserializationHelper;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.apache.kafka.streams.StreamsConfig.InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED;

public class InMemoryKeyValueStore implements KeyValueStore<Bytes, byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryKeyValueStore.class);

    private final String name;
    private final NavigableMap<Bytes, byte[]> map = new TreeMap<>();
    private final Position position = Position.emptyPosition();
    private volatile boolean open = false;
    private StateStoreContext context;
    private InMemoryTransactionBuffer transactionBuffer;

    public InMemoryKeyValueStore(final String name) {
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void init(final StateStoreContext stateStoreContext,
                     final StateStore root) {
        if (root != null) {
            final boolean consistencyEnabled = StreamsConfig.InternalConfig.getBoolean(
                stateStoreContext.appConfigs(),
                IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED,
                false
            );
            // register the store
            open = true;

            stateStoreContext.register(
                root,
                (RecordBatchingStateRestoreCallback) records -> {
                    synchronized (position) {
                        for (final ConsumerRecord<byte[], byte[]> record : records) {
                            final Bytes key = Bytes.wrap(record.key());
                            putInternal(key, record.value());
                            ChangelogRecordDeserializationHelper.applyChecksAndUpdatePosition(
                                record,
                                consistencyEnabled,
                                position
                            );
                        }
                    }
                }
            );
        }

        open = true;
        this.context = stateStoreContext;
        final boolean transactional = StreamsConfig.InternalConfig.getBoolean(
            stateStoreContext.appConfigs(),
            StreamsConfig.TRANSACTIONAL_STATE_STORES_CONFIG,
            false);
        if (transactional) {
            this.transactionBuffer = new InMemoryTransactionBuffer(map);
        }
    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public Position getPosition() {
        // Mirror RocksDBStore#getPosition: report the uncommitted position (committed + staged) so the
        // changelog consistency vector, which ChangeLogging*BytesStore writes at put() time via
        // getPosition(), reflects the input position of the staged write. Otherwise a transactional
        // store's changelog records carry the stale committed position and the position cannot be
        // rebuilt when the store is restored from its changelog.
        if (transactionBuffer != null) {
            synchronized (position) {
                return position.copy().merge(transactionBuffer.pendingPosition());
            }
        }
        return position;
    }

    @Override
    public <R> QueryResult<R> query(final Query<R> query,
                                    final PositionBound positionBound,
                                    final QueryConfig config) {

        synchronized (position) {
            // Mirror RocksDBStore#query: under READ_UNCOMMITTED, expose the writes staged in the
            // transaction buffer since the last commit by merging the buffer's pending position
            // deltas into a copy of the committed position. READ_COMMITTED (and the
            // non-transactional store) query the committed position directly.
            final Position queryPosition;
            if (transactionBuffer != null && config.getIsolationLevel() == IsolationLevel.READ_UNCOMMITTED) {
                queryPosition = position.copy().merge(transactionBuffer.pendingPosition());
            } else {
                queryPosition = position;
            }
            return StoreQueryUtils.handleBasicQueries(
                query,
                positionBound,
                config,
                this,
                queryPosition,
                context
            );
        }
    }

    @Override
    public byte[] get(final Bytes key) {
        return get(key, IsolationLevel.READ_UNCOMMITTED);
    }

    private byte[] get(final Bytes key, final IsolationLevel isolationLevel) {
        if (transactionBuffer != null) {
            if (isolationLevel == IsolationLevel.READ_UNCOMMITTED) {
                final java.util.Optional<byte[]> staged = transactionBuffer.get(key);
                if (staged != null) {
                    return staged.orElse(null);
                }
            }
            return transactionBuffer.getCommitted(key);
        }
        synchronized (this) {
            return map.get(key);
        }
    }

    @Override
    public synchronized void put(final Bytes key, final byte[] value) {
        if (transactionBuffer != null) {
            transactionBuffer.stage(key, value);
            transactionBuffer.updatePosition(context);
            return;
        }
        putInternal(key, value);
    }

    @Override
    public synchronized byte[] putIfAbsent(final Bytes key, final byte[] value) {
        final byte[] originalValue = get(key);
        if (originalValue == null) {
            put(key, value);
        }
        return originalValue;
    }

    // the unlocked implementation of put method, to avoid multiple lock/unlock cost in `putAll` method
    private void putInternal(final Bytes key, final byte[] value) {
        synchronized (position) {
            if (value == null) {
                map.remove(key);
            } else {
                map.put(key, value);
            }

            StoreQueryUtils.updatePosition(position, context);
        }
    }

    @Override
    public synchronized void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        if (transactionBuffer != null) {
            for (final KeyValue<Bytes, byte[]> entry : entries) {
                transactionBuffer.stage(entry.key, entry.value);
                transactionBuffer.updatePosition(context);
            }
            return;
        }
        for (final KeyValue<Bytes, byte[]> entry : entries) {
            putInternal(entry.key, entry.value);
        }
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefixScan(final P prefix, final PS prefixKeySerializer) {
        return prefixScan(prefix, prefixKeySerializer, IsolationLevel.READ_UNCOMMITTED);
    }

    private <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefixScan(final P prefix, final PS prefixKeySerializer,
                                                                                     final IsolationLevel isolationLevel) {
        final Bytes from = Bytes.wrap(prefixKeySerializer.serialize(null, prefix));
        final Bytes to = ByteUtils.increment(from);

        if (transactionBuffer != null) {
            return transactionBuffer.range(from, to, true, false, isolationLevel);
        }
        synchronized (this) {
            return new InMemoryKeyValueIterator(map.subMap(from, true, to, false).keySet(), true);
        }
    }

    @Override
    public synchronized byte[] delete(final Bytes key) {
        if (transactionBuffer != null) {
            final byte[] oldValue = get(key);
            transactionBuffer.stage(key, null);
            return oldValue;
        }
        return map.remove(key);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
        return range(from, to, true, IsolationLevel.READ_UNCOMMITTED);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from, final Bytes to) {
        return range(from, to, false, IsolationLevel.READ_UNCOMMITTED);
    }

    private KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to, final boolean forward,
                                                  final IsolationLevel isolationLevel) {
        if (from != null && to != null && from.compareTo(to) > 0) {
            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. " +
                    "This may be due to range arguments set in the wrong order, " +
                    "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                    "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }
        if (transactionBuffer != null) {
            return transactionBuffer.range(from, to, forward, true, isolationLevel);
        }
        synchronized (this) {
            if (from == null && to == null) {
                return getKeyValueIterator(map.keySet(), forward);
            } else if (from == null) {
                return getKeyValueIterator(map.headMap(to, true).keySet(), forward);
            } else if (to == null) {
                return getKeyValueIterator(map.tailMap(from, true).keySet(), forward);
            } else {
                return getKeyValueIterator(map.subMap(from, true, to, true).keySet(), forward);
            }
        }
    }

    private KeyValueIterator<Bytes, byte[]> getKeyValueIterator(final Set<Bytes> rangeSet, final boolean forward) {
        return new InMemoryKeyValueIterator(rangeSet, forward);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        return range(null, null, true, IsolationLevel.READ_UNCOMMITTED);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseAll() {
        return range(null, null, false, IsolationLevel.READ_UNCOMMITTED);
    }

    @Override
    public long approximateNumEntries() {
        return map.size();
    }

    @Override
    public ReadOnlyKeyValueStore<Bytes, byte[]> readOnly(final IsolationLevel isolationLevel) {
        Objects.requireNonNull(isolationLevel, "isolationLevel cannot be null");
        return new ReadOnlyView(isolationLevel);
    }

    /**
     * Read-only view of this store bound to an isolation level. Every read delegates to the store's
     * shared read helper with this view's {@code isolationLevel}, so the view and the store's own
     * reads follow one code path; under READ_COMMITTED that path excludes the transaction buffer's
     * staging layer and snapshots the committed base under the buffer's read-lock, so an interactive
     * query cannot race a concurrent commit.
     */
    private final class ReadOnlyView implements ReadOnlyKeyValueStore<Bytes, byte[]> {

        private final IsolationLevel isolationLevel;

        ReadOnlyView(final IsolationLevel isolationLevel) {
            this.isolationLevel = isolationLevel;
        }

        @Override
        public byte[] get(final Bytes key) {
            Objects.requireNonNull(key, "key cannot be null");
            return InMemoryKeyValueStore.this.get(key, isolationLevel);
        }

        @Override
        public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
            return InMemoryKeyValueStore.this.range(from, to, true, isolationLevel);
        }

        @Override
        public KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from, final Bytes to) {
            return InMemoryKeyValueStore.this.range(from, to, false, isolationLevel);
        }

        @Override
        public KeyValueIterator<Bytes, byte[]> all() {
            return InMemoryKeyValueStore.this.range(null, null, true, isolationLevel);
        }

        @Override
        public KeyValueIterator<Bytes, byte[]> reverseAll() {
            return InMemoryKeyValueStore.this.range(null, null, false, isolationLevel);
        }

        @Override
        public <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefixScan(final P prefix,
                                                                                        final PS prefixKeySerializer) {
            Objects.requireNonNull(prefix, "prefix cannot be null");
            Objects.requireNonNull(prefixKeySerializer, "prefixKeySerializer cannot be null");
            return InMemoryKeyValueStore.this.prefixScan(prefix, prefixKeySerializer, isolationLevel);
        }

        @Override
        public long approximateNumEntries() {
            return InMemoryKeyValueStore.this.approximateNumEntries();
        }
    }

    @Override
    public long approximateNumUncommittedBytes() {
        if (transactionBuffer != null) {
            return transactionBuffer.approximateNumUncommittedBytes();
        }
        return 0;
    }

    @Override
    public void commit(final Map<TopicPartition, Long> changelogOffsets) {
        commitStagedWrites();
    }

    void commitStagedWrites() {
        if (transactionBuffer != null) {
            synchronized (position) {
                transactionBuffer.mergePendingPositionInto(position);
                transactionBuffer.commit();
            }
        }
    }

    void rollbackStagedWrites() {
        if (transactionBuffer != null) {
            transactionBuffer.rollback();
        }
    }

    @Override
    public void close() {
        rollbackStagedWrites();
        map.clear();
        open = false;
    }

    private class InMemoryKeyValueIterator implements KeyValueIterator<Bytes, byte[]> {
        private final Iterator<Bytes> iter;
        private Bytes currentKey;
        private Boolean iteratorOpen = true;

        private InMemoryKeyValueIterator(final Set<Bytes> keySet, final boolean forward) {
            if (forward) {
                this.iter = new TreeSet<>(keySet).iterator();
            } else {
                this.iter = new TreeSet<>(keySet).descendingIterator();
            }
        }

        @Override
        public boolean hasNext() {
            if (!iteratorOpen) {
                throw new IllegalStateException(String.format("Iterator for store %s has already been closed.", name));
            }
            if (currentKey != null) {
                if (map.containsKey(currentKey)) {
                    return true;
                } else {
                    currentKey = null;
                    return hasNext();
                }
            }
            if (!iter.hasNext()) {
                return false;
            }
            currentKey = iter.next();
            return hasNext();
        }

        @Override
        public KeyValue<Bytes, byte[]> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final KeyValue<Bytes, byte[]> ret = new KeyValue<>(currentKey, map.get(currentKey));
            currentKey = null;
            return ret;
        }

        @Override
        public void close() {
            iteratorOpen = false;
        }

        @Override
        public Bytes peekNextKey() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return currentKey;
        }
    }
}
