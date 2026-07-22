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
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.ChangelogRecordDeserializationHelper;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextUtils;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;

import static org.apache.kafka.streams.StreamsConfig.InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED;
import static org.apache.kafka.streams.state.internals.WindowKeySchema.extractStoreKeyBytes;
import static org.apache.kafka.streams.state.internals.WindowKeySchema.extractStoreTimestamp;

public class InMemoryWindowStore implements WindowStore<Bytes, byte[]>, WithRetentionPeriod {

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryWindowStore.class);
    private static final int SEQNUM_SIZE = 4;

    private final String name;
    private final String metricScope;
    private final long retentionPeriod;
    private final long windowSize;
    private final boolean retainDuplicates;

    private final ConcurrentNavigableMap<Long, ConcurrentNavigableMap<Bytes, byte[]>> segmentMap = new ConcurrentSkipListMap<>();
    private final Set<InMemoryWindowStoreIteratorWrapper> openIterators = ConcurrentHashMap.newKeySet();
    private final Set<KeyValueIterator<?, ?>> openTransactionalIterators = ConcurrentHashMap.newKeySet();

    private InternalProcessorContext<?, ?> internalProcessorContext;
    private Sensor expiredRecordSensor;
    private int seqnum = 0;
    private long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;

    private volatile boolean open = false;

    private final Position position;
    private InMemoryWindowTransactionBuffer transactionBuffer;

    public InMemoryWindowStore(final String name,
                               final long retentionPeriod,
                               final long windowSize,
                               final boolean retainDuplicates,
                               final String metricScope) {
        this.name = name;
        this.retentionPeriod = retentionPeriod;
        this.windowSize = windowSize;
        this.retainDuplicates = retainDuplicates;
        this.metricScope = metricScope;
        this.position = Position.emptyPosition();
    }

    @Override
    public long retentionPeriod() {
        return retentionPeriod;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void init(final StateStoreContext stateStoreContext,
                     final StateStore root) {
        this.internalProcessorContext = ProcessorContextUtils.asInternalProcessorContext(stateStoreContext);

        final StreamsMetricsImpl metrics = ProcessorContextUtils.metricsImpl(stateStoreContext);
        final String threadId = Thread.currentThread().getName();
        final String taskName = stateStoreContext.taskId().toString();
        expiredRecordSensor = TaskMetrics.droppedRecordsSensor(
            threadId,
            taskName,
            metrics
        );

        if (root != null) {
            final boolean consistencyEnabled = StreamsConfig.InternalConfig.getBoolean(
                stateStoreContext.appConfigs(),
                IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED,
                false
            );
            stateStoreContext.register(
                root,
                (RecordBatchingStateRestoreCallback) records -> {
                    synchronized (position) {
                        long expiredRecords = 0;
                        for (final ConsumerRecord<byte[], byte[]> record : records) {
                            final Bytes key = Bytes.wrap(extractStoreKeyBytes(record.key()));
                            final long windowStartTimestamp = extractStoreTimestamp(record.key());
                            observedStreamTime = Math.max(observedStreamTime, windowStartTimestamp);
                            if (windowStartTimestamp <= observedStreamTime - retentionPeriod) {
                                expiredRecords++;
                            } else {
                                // Write directly to the committed map: restored records are already committed.
                                putInternal(key, record.value(), windowStartTimestamp);
                            }
                            ChangelogRecordDeserializationHelper.applyChecksAndUpdatePosition(
                                record,
                                consistencyEnabled,
                                position
                            );
                        }
                        removeExpiredSegments();
                        if (expiredRecords > 0) {
                            expiredRecordSensor.record(expiredRecords, internalProcessorContext.currentSystemTimeMs());
                            LOG.warn("Skipping {} records for expired segments.", expiredRecords);
                        }
                    }
                }
            );
        }
        open = true;

        final boolean transactional = StreamsConfig.InternalConfig.getBoolean(
            stateStoreContext.appConfigs(),
            StreamsConfig.TRANSACTIONAL_STATE_STORES_CONFIG,
            false);
        if (transactional) {
            this.transactionBuffer = new InMemoryWindowTransactionBuffer(segmentMap, retainDuplicates);
        }
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
    public void put(final Bytes key, final byte[] value, final long windowStartTimestamp) {
        removeExpiredSegments();
        observedStreamTime = Math.max(observedStreamTime, windowStartTimestamp);

        synchronized (position) {
            if (windowStartTimestamp <= observedStreamTime - retentionPeriod) {
                expiredRecordSensor.record(1.0d, internalProcessorContext.currentSystemTimeMs());
                LOG.warn("Skipping record for expired segment.");
            } else if (transactionBuffer != null) {
                if (value != null) {
                    maybeUpdateSeqnumForDups();
                    final Bytes keyBytes = retainDuplicates ? wrapForDups(key, seqnum) : key;
                    transactionBuffer.stage(windowStartTimestamp, keyBytes, value);
                } else if (!retainDuplicates) {
                    // Skip if value is null and duplicates are allowed since this delete is a no-op
                    transactionBuffer.stage(windowStartTimestamp, key, null);
                }
            } else {
                putInternal(key, value, windowStartTimestamp);
            }

            if (transactionBuffer != null) {
                transactionBuffer.updatePosition(internalProcessorContext);
            } else {
                StoreQueryUtils.updatePosition(position, internalProcessorContext);
            }
        }
    }

    private void putInternal(final Bytes key, final byte[] value, final long windowStartTimestamp) {
        if (value != null) {
            maybeUpdateSeqnumForDups();
            final Bytes keyBytes = retainDuplicates ? wrapForDups(key, seqnum) : key;
            segmentMap.computeIfAbsent(windowStartTimestamp, t -> new ConcurrentSkipListMap<>());
            segmentMap.get(windowStartTimestamp).put(keyBytes, value);
        } else if (!retainDuplicates) {
            // Skip if value is null and duplicates are allowed since this delete is a no-op
            segmentMap.computeIfPresent(windowStartTimestamp, (t, kvMap) -> {
                kvMap.remove(key);
                if (kvMap.isEmpty()) {
                    segmentMap.remove(windowStartTimestamp);
                }
                return kvMap;
            });
        }
    }

    @Override
    public byte[] fetch(final Bytes key, final long windowStartTimestamp) {
        return fetch(key, windowStartTimestamp, IsolationLevel.READ_UNCOMMITTED);
    }

    private byte[] fetch(final Bytes key, final long windowStartTimestamp, final IsolationLevel isolationLevel) {
        Objects.requireNonNull(key, "key cannot be null");

        removeExpiredSegments();

        if (windowStartTimestamp <= observedStreamTime - retentionPeriod) {
            return null;
        }

        if (transactionBuffer != null) {
            if (isolationLevel == IsolationLevel.READ_UNCOMMITTED) {
                final Optional<byte[]> staged = transactionBuffer.get(windowStartTimestamp, key);
                if (staged != null) {
                    return staged.orElse(null);
                }
            }
            // Committed read of the base map, taken under the buffer's snapshot read-lock so it
            // cannot race a concurrent commit.
            return transactionBuffer.getCommitted(windowStartTimestamp, key);
        }

        final ConcurrentNavigableMap<Bytes, byte[]> kvMap = segmentMap.get(windowStartTimestamp);
        if (kvMap == null) {
            return null;
        } else {
            return kvMap.get(key);
        }
    }

    @Override
    public WindowStoreIterator<byte[]> fetch(final Bytes key, final long timeFrom, final long timeTo) {
        return fetch(key, timeFrom, timeTo, true);
    }

    @Override
    public WindowStoreIterator<byte[]> backwardFetch(final Bytes key, final long timeFrom, final long timeTo) {
        return fetch(key, timeFrom, timeTo, false);
    }

    WindowStoreIterator<byte[]> fetch(final Bytes key, final long timeFrom, final long timeTo, final boolean forward) {
        return fetch(key, timeFrom, timeTo, forward, IsolationLevel.READ_UNCOMMITTED);
    }

    private WindowStoreIterator<byte[]> fetch(final Bytes key, final long timeFrom, final long timeTo,
                                              final boolean forward, final IsolationLevel isolationLevel) {
        Objects.requireNonNull(key, "key cannot be null");

        removeExpiredSegments();

        // add one b/c records expire exactly retentionPeriod ms after created
        final long minTime = Math.max(timeFrom, observedStreamTime - retentionPeriod + 1);

        if (timeTo < minTime) {
            return WrappedInMemoryWindowStoreIterator.emptyIterator();
        }

        if (transactionBuffer != null) {
            final Bytes keyFrom = retainDuplicates ? wrapForDups(key, 0) : key;
            final Bytes keyTo = retainDuplicates ? wrapForDups(key, Integer.MAX_VALUE) : key;
            return registerTransactional(new TransactionalWindowStoreIterator(
                transactionBuffer, keyFrom, keyTo, minTime, timeTo, forward, retainDuplicates, isolationLevel,
                openTransactionalIterators::remove
            ));
        }

        if (forward) {
            return registerNewWindowStoreIterator(
                key,
                segmentMap.subMap(minTime, true, timeTo, true).entrySet().iterator(),
                true
            );
        } else {
            return registerNewWindowStoreIterator(
                key,
                segmentMap.subMap(minTime, true, timeTo, true).descendingMap().entrySet().iterator(),
                false
            );
        }
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom,
                                                           final Bytes keyTo,
                                                           final long timeFrom,
                                                           final long timeTo) {
        return fetch(keyFrom, keyTo, timeFrom, timeTo, true);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom,
                                                                   final Bytes keyTo,
                                                                   final long timeFrom,
                                                                   final long timeTo) {
        return fetch(keyFrom, keyTo, timeFrom, timeTo, false);
    }

    KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes from,
                                                    final Bytes to,
                                                    final long timeFrom,
                                                    final long timeTo,
                                                    final boolean forward) {
        return fetch(from, to, timeFrom, timeTo, forward, IsolationLevel.READ_UNCOMMITTED);
    }

    private KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes from,
                                                            final Bytes to,
                                                            final long timeFrom,
                                                            final long timeTo,
                                                            final boolean forward,
                                                            final IsolationLevel isolationLevel) {
        removeExpiredSegments();

        if (from != null && to != null && from.compareTo(to) > 0) {
            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. " +
                "This may be due to range arguments set in the wrong order, " +
                "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }

        // add one b/c records expire exactly retentionPeriod ms after created
        final long minTime = Math.max(timeFrom, observedStreamTime - retentionPeriod + 1);

        if (timeTo < minTime) {
            return KeyValueIterators.emptyIterator();
        }

        if (transactionBuffer != null) {
            final Bytes keyFrom = (retainDuplicates && from != null) ? wrapForDups(from, 0) : from;
            final Bytes keyTo = (retainDuplicates && to != null) ? wrapForDups(to, Integer.MAX_VALUE) : to;
            return registerTransactional(new TransactionalWindowedKeyValueIterator(
                transactionBuffer, keyFrom, keyTo, minTime, timeTo, forward, retainDuplicates, windowSize, isolationLevel,
                openTransactionalIterators::remove
            ));
        }

        if (forward) {
            return registerNewWindowedKeyValueIterator(
                from,
                to,
                segmentMap.subMap(minTime, true, timeTo, true).entrySet().iterator(),
                true
            );
        } else {
            return registerNewWindowedKeyValueIterator(
                from,
                to,
                segmentMap.subMap(minTime, true, timeTo, true).descendingMap().entrySet().iterator(),
                false
            );
        }
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final long timeFrom, final long timeTo) {
        return fetchAll(timeFrom, timeTo, true);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(final long timeFrom, final long timeTo) {
        return fetchAll(timeFrom, timeTo, false);
    }

    KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final long timeFrom, final long timeTo, final boolean forward) {
        return fetchAll(timeFrom, timeTo, forward, IsolationLevel.READ_UNCOMMITTED);
    }

    private KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final long timeFrom, final long timeTo,
                                                               final boolean forward,
                                                               final IsolationLevel isolationLevel) {
        removeExpiredSegments();

        // add one b/c records expire exactly retentionPeriod ms after created
        final long minTime = Math.max(timeFrom, observedStreamTime - retentionPeriod + 1);

        if (timeTo < minTime) {
            return KeyValueIterators.emptyIterator();
        }

        if (transactionBuffer != null) {
            return registerTransactional(new TransactionalWindowedKeyValueIterator(
                transactionBuffer, null, null, minTime, timeTo, forward, retainDuplicates, windowSize, isolationLevel,
                openTransactionalIterators::remove
            ));
        }

        if (forward) {
            return registerNewWindowedKeyValueIterator(
                null,
                null,
                segmentMap.subMap(minTime, true, timeTo, true).entrySet().iterator(),
                true
            );
        } else {
            return registerNewWindowedKeyValueIterator(
                null,
                null,
                segmentMap.subMap(minTime, true, timeTo, true).descendingMap().entrySet().iterator(),
                false
            );
        }
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
        return all(true, IsolationLevel.READ_UNCOMMITTED);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardAll() {
        return all(false, IsolationLevel.READ_UNCOMMITTED);
    }

    private KeyValueIterator<Windowed<Bytes>, byte[]> all(final boolean forward,
                                                          final IsolationLevel isolationLevel) {
        removeExpiredSegments();

        final long minTime = observedStreamTime - retentionPeriod;

        if (transactionBuffer != null) {
            return registerTransactional(new TransactionalWindowedKeyValueIterator(
                transactionBuffer, null, null, minTime + 1, Long.MAX_VALUE, forward, retainDuplicates, windowSize, isolationLevel,
                openTransactionalIterators::remove
            ));
        }

        final Iterator<Map.Entry<Long, ConcurrentNavigableMap<Bytes, byte[]>>> segIter = forward
            ? segmentMap.tailMap(minTime, false).entrySet().iterator()
            : segmentMap.tailMap(minTime, false).descendingMap().entrySet().iterator();
        return registerNewWindowedKeyValueIterator(null, null, segIter, forward);
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
                internalProcessorContext
            );
        }
    }

    @Override
    public ReadOnlyWindowStore<Bytes, byte[]> readOnly(final IsolationLevel isolationLevel) {
        Objects.requireNonNull(isolationLevel, "isolationLevel cannot be null");
        return new ReadOnlyView(isolationLevel);
    }

    /**
     * Read-only view of this store. For a transactional store the {@code isolationLevel} is passed
     * down to the transactional read path (which excludes the staging layer under READ_COMMITTED), so
     * both isolation levels share the one path and the read is snapshotted under the buffer's
     * read-lock — an interactive query cannot race a concurrent commit. For a non-transactional store
     * reads hit {@code segmentMap} directly.
     */
    private final class ReadOnlyView implements ReadOnlyWindowStore<Bytes, byte[]> {

        private final IsolationLevel isolationLevel;

        ReadOnlyView(final IsolationLevel isolationLevel) {
            this.isolationLevel = isolationLevel;
        }

        @Override
        public byte[] fetch(final Bytes key, final long time) {
            return InMemoryWindowStore.this.fetch(key, time, isolationLevel);
        }

        @Override
        public WindowStoreIterator<byte[]> fetch(final Bytes key, final Instant timeFrom, final Instant timeTo) {
            return InMemoryWindowStore.this.fetch(key, timeFrom.toEpochMilli(), timeTo.toEpochMilli(), true, isolationLevel);
        }

        @Override
        public WindowStoreIterator<byte[]> backwardFetch(final Bytes key, final Instant timeFrom, final Instant timeTo) {
            return InMemoryWindowStore.this.fetch(key, timeFrom.toEpochMilli(), timeTo.toEpochMilli(), false, isolationLevel);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom, final Bytes keyTo,
                                                               final Instant timeFrom, final Instant timeTo) {
            return InMemoryWindowStore.this.fetch(keyFrom, keyTo, timeFrom.toEpochMilli(), timeTo.toEpochMilli(), true, isolationLevel);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom, final Bytes keyTo,
                                                                        final Instant timeFrom, final Instant timeTo) {
            return InMemoryWindowStore.this.fetch(keyFrom, keyTo, timeFrom.toEpochMilli(), timeTo.toEpochMilli(), false, isolationLevel);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final Instant timeFrom, final Instant timeTo) {
            return InMemoryWindowStore.this.fetchAll(timeFrom.toEpochMilli(), timeTo.toEpochMilli(), true, isolationLevel);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(final Instant timeFrom, final Instant timeTo) {
            return InMemoryWindowStore.this.fetchAll(timeFrom.toEpochMilli(), timeTo.toEpochMilli(), false, isolationLevel);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
            return InMemoryWindowStore.this.all(true, isolationLevel);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> backwardAll() {
            return InMemoryWindowStore.this.all(false, isolationLevel);
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
        if (transactionBuffer != null) {
            synchronized (position) {
                transactionBuffer.mergePendingPositionInto(position);
                transactionBuffer.commit();
            }
        }
    }

    @Override
    public void close() {
        if (transactionBuffer != null) {
            transactionBuffer.rollback();
        }

        final int openCount = openIterators.size() + openTransactionalIterators.size();
        if (openCount != 0) {
            LOG.warn("Closing {} open iterators for store {}", openCount, name);
            for (final InMemoryWindowStoreIteratorWrapper it : openIterators) {
                it.close();
            }
            for (final KeyValueIterator<?, ?> it : openTransactionalIterators) {
                it.close();
            }
        }

        segmentMap.clear();
        openTransactionalIterators.clear();
        open = false;
    }

    private <T extends KeyValueIterator<?, ?>> T registerTransactional(final T iterator) {
        openTransactionalIterators.add(iterator);
        return iterator;
    }

    long numEntries() {
        return segmentMap.values().stream()
            .mapToLong(Map::size)
            .sum();
    }

    private void removeExpiredSegments() {
        long minLiveTime = Math.max(0L, observedStreamTime - retentionPeriod + 1);
        for (final InMemoryWindowStoreIteratorWrapper it : openIterators) {
            minLiveTime = Math.min(minLiveTime, it.minTime());
        }
        segmentMap.headMap(minLiveTime, false).clear();
    }

    private void maybeUpdateSeqnumForDups() {
        if (retainDuplicates) {
            seqnum = (seqnum + 1) & 0x7FFFFFFF;
        }
    }

    private static Bytes wrapForDups(final Bytes key, final int seqnum) {
        final ByteBuffer buf = ByteBuffer.allocate(key.get().length + SEQNUM_SIZE);
        buf.put(key.get());
        buf.putInt(seqnum);

        return Bytes.wrap(buf.array());
    }

    private static Bytes getKey(final Bytes keyBytes) {
        final byte[] bytes = new byte[keyBytes.get().length - SEQNUM_SIZE];
        System.arraycopy(keyBytes.get(), 0, bytes, 0, bytes.length);
        return Bytes.wrap(bytes);
    }

    private WrappedInMemoryWindowStoreIterator registerNewWindowStoreIterator(final Bytes key,
                                                                              final Iterator<Map.Entry<Long, ConcurrentNavigableMap<Bytes, byte[]>>> segmentIterator,
                                                                              final boolean forward) {
        final Bytes keyFrom = retainDuplicates ? wrapForDups(key, 0) : key;
        final Bytes keyTo = retainDuplicates ? wrapForDups(key, Integer.MAX_VALUE) : key;

        final WrappedInMemoryWindowStoreIterator iterator =
            new WrappedInMemoryWindowStoreIterator(keyFrom, keyTo, segmentIterator, openIterators::remove, retainDuplicates, forward);

        openIterators.add(iterator);
        return iterator;
    }

    private WrappedWindowedKeyValueIterator registerNewWindowedKeyValueIterator(final Bytes keyFrom,
                                                                                final Bytes keyTo,
                                                                                final Iterator<Map.Entry<Long, ConcurrentNavigableMap<Bytes, byte[]>>> segmentIterator,
                                                                                final boolean forward) {
        final Bytes from = (retainDuplicates && keyFrom != null) ? wrapForDups(keyFrom, 0) : keyFrom;
        final Bytes to = (retainDuplicates && keyTo != null) ? wrapForDups(keyTo, Integer.MAX_VALUE) : keyTo;

        final WrappedWindowedKeyValueIterator iterator =
            new WrappedWindowedKeyValueIterator(
                from,
                to,
                segmentIterator,
                openIterators::remove,
                retainDuplicates,
                windowSize,
                forward);
        openIterators.add(iterator);
        return iterator;
    }

    private static Bytes lowerBoundKey(final Bytes keyFrom) {
        // The staged composite scan needs a non-null lower bound; empty bytes is the natural minimum.
        return keyFrom != null ? keyFrom : Bytes.wrap(new byte[0]);
    }

    private static Bytes unwrapBound(final Bytes wrappedBound, final boolean retainDuplicates) {
        if (wrappedBound == null) {
            return null;
        }
        return retainDuplicates ? getKey(wrappedBound) : wrappedBound;
    }

    /**
     * Whether a stored (possibly seqnum-wrapped) key falls within the original key range. Needed
     * because the staged composite scan is bounded only by timestamp (and, at the boundary
     * timestamps, by wrapped-key order, which can admit out-of-range keys); the committed base
     * iterator is already correctly bounded.
     */
    private static boolean keyInRange(final Bytes storedKey,
                                      final Bytes unwrappedFrom,
                                      final Bytes unwrappedTo,
                                      final boolean retainDuplicates) {
        final Bytes key = retainDuplicates ? getKey(storedKey) : storedKey;
        return (unwrappedFrom == null || key.compareTo(unwrappedFrom) >= 0)
            && (unwrappedTo == null || key.compareTo(unwrappedTo) <= 0);
    }

    /**
     * A WindowStoreIterator over the transaction buffer's merge scan, exposing each entry's
     * timestamp/value, filtered to the requested key.
     */
    private static class TransactionalWindowStoreIterator implements WindowStoreIterator<byte[]> {
        private final KeyValueIterator<InMemoryWindowTransactionBuffer.WindowEntryKey, byte[]> delegate;
        private final Bytes unwrappedFrom;
        private final Bytes unwrappedTo;
        private final boolean retainDuplicates;
        private final Consumer<KeyValueIterator<?, ?>> deregister;
        private KeyValue<Long, byte[]> prefetched;
        private boolean closed = false;

        TransactionalWindowStoreIterator(
                final InMemoryWindowTransactionBuffer buffer,
                final Bytes keyFrom,
                final Bytes keyTo,
                final long timeFrom,
                final long timeTo,
                final boolean forward,
                final boolean retainDuplicates,
                final IsolationLevel isolationLevel,
                final Consumer<KeyValueIterator<?, ?>> deregister) {
            this.retainDuplicates = retainDuplicates;
            this.deregister = deregister;
            this.unwrappedFrom = unwrapBound(keyFrom, retainDuplicates);
            this.unwrappedTo = unwrapBound(keyTo, retainDuplicates);
            final InMemoryWindowTransactionBuffer.WindowEntryKey from =
                new InMemoryWindowTransactionBuffer.WindowEntryKey(timeFrom, lowerBoundKey(keyFrom));
            final InMemoryWindowTransactionBuffer.WindowEntryKey to =
                new InMemoryWindowTransactionBuffer.WindowEntryKey(timeTo, keyTo);

            this.delegate = buffer.range(from, to, forward, true, isolationLevel);
        }

        @Override
        public Long peekNextKey() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return prefetched.key;
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            if (prefetched != null) {
                return true;
            }
            prefetched = computeNext();
            return prefetched != null;
        }

        @Override
        public KeyValue<Long, byte[]> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final KeyValue<Long, byte[]> result = prefetched;
            prefetched = null;
            return result;
        }

        @Override
        public void close() {
            closed = true;
            prefetched = null;
            try {
                delegate.close();
            } finally {
                deregister.accept(this);
            }
        }

        private KeyValue<Long, byte[]> computeNext() {
            while (delegate.hasNext()) {
                final KeyValue<InMemoryWindowTransactionBuffer.WindowEntryKey, byte[]> entry = delegate.next();
                if (keyInRange(entry.key.key(), unwrappedFrom, unwrappedTo, retainDuplicates)) {
                    return new KeyValue<>(entry.key.timestamp(), entry.value);
                }
            }
            return null;
        }
    }

    /**
     * A Windowed KeyValueIterator over the transaction buffer's merge scan, filtered to the key
     * range and with the stored (possibly seqnum-wrapped) key unwrapped.
     */
    private static class TransactionalWindowedKeyValueIterator implements KeyValueIterator<Windowed<Bytes>, byte[]> {
        private final KeyValueIterator<InMemoryWindowTransactionBuffer.WindowEntryKey, byte[]> delegate;
        private final Bytes unwrappedFrom;
        private final Bytes unwrappedTo;
        private final boolean retainDuplicates;
        private final long windowSize;
        private final Consumer<KeyValueIterator<?, ?>> deregister;
        private KeyValue<Windowed<Bytes>, byte[]> prefetched;
        private boolean closed = false;

        TransactionalWindowedKeyValueIterator(
                final InMemoryWindowTransactionBuffer buffer,
                final Bytes keyFrom,
                final Bytes keyTo,
                final long timeFrom,
                final long timeTo,
                final boolean forward,
                final boolean retainDuplicates,
                final long windowSize,
                final IsolationLevel isolationLevel,
                final Consumer<KeyValueIterator<?, ?>> deregister) {
            this.retainDuplicates = retainDuplicates;
            this.windowSize = windowSize;
            this.deregister = deregister;
            this.unwrappedFrom = unwrapBound(keyFrom, retainDuplicates);
            this.unwrappedTo = unwrapBound(keyTo, retainDuplicates);
            final InMemoryWindowTransactionBuffer.WindowEntryKey from =
                new InMemoryWindowTransactionBuffer.WindowEntryKey(timeFrom, lowerBoundKey(keyFrom));
            final InMemoryWindowTransactionBuffer.WindowEntryKey to =
                new InMemoryWindowTransactionBuffer.WindowEntryKey(timeTo, keyTo);

            this.delegate = buffer.range(from, to, forward, true, isolationLevel);
        }

        @Override
        public Windowed<Bytes> peekNextKey() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return prefetched.key;
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            if (prefetched != null) {
                return true;
            }
            prefetched = computeNext();
            return prefetched != null;
        }

        @Override
        public KeyValue<Windowed<Bytes>, byte[]> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final KeyValue<Windowed<Bytes>, byte[]> result = prefetched;
            prefetched = null;
            return result;
        }

        @Override
        public void close() {
            closed = true;
            prefetched = null;
            try {
                delegate.close();
            } finally {
                deregister.accept(this);
            }
        }

        private KeyValue<Windowed<Bytes>, byte[]> computeNext() {
            while (delegate.hasNext()) {
                final KeyValue<InMemoryWindowTransactionBuffer.WindowEntryKey, byte[]> entry = delegate.next();
                if (keyInRange(entry.key.key(), unwrappedFrom, unwrappedTo, retainDuplicates)) {
                    return new KeyValue<>(toWindowed(entry.key), entry.value);
                }
            }
            return null;
        }

        private Windowed<Bytes> toWindowed(final InMemoryWindowTransactionBuffer.WindowEntryKey entryKey) {
            final Bytes key = retainDuplicates ? getKey(entryKey.key()) : entryKey.key();
            long endTime = entryKey.timestamp() + windowSize;
            if (endTime < 0) {
                LOG.warn("Warning: window end time was truncated to Long.MAX");
                endTime = Long.MAX_VALUE;
            }
            final TimeWindow timeWindow = new TimeWindow(entryKey.timestamp(), endTime);
            return new Windowed<>(key, timeWindow);
        }
    }


    interface ClosingCallback {
        void deregisterIterator(final InMemoryWindowStoreIteratorWrapper iterator);
    }

    private abstract static class InMemoryWindowStoreIteratorWrapper {

        private Iterator<Map.Entry<Bytes, byte[]>> recordIterator;
        private KeyValue<Bytes, byte[]> next;
        private long currentTime;

        private final boolean allKeys;
        private final Bytes keyFrom;
        private final Bytes keyTo;
        private final Iterator<Map.Entry<Long, ConcurrentNavigableMap<Bytes, byte[]>>> segmentIterator;
        private final ClosingCallback callback;
        private final boolean retainDuplicates;
        private final boolean forward;

        InMemoryWindowStoreIteratorWrapper(final Bytes keyFrom,
                                           final Bytes keyTo,
                                           final Iterator<Map.Entry<Long, ConcurrentNavigableMap<Bytes, byte[]>>> segmentIterator,
                                           final ClosingCallback callback,
                                           final boolean retainDuplicates,
                                           final boolean forward) {
            this.keyFrom = keyFrom;
            this.keyTo = keyTo;
            allKeys = (keyFrom == null) && (keyTo == null);
            this.retainDuplicates = retainDuplicates;
            this.forward = forward;

            this.segmentIterator = segmentIterator;
            this.callback = callback;
            recordIterator = segmentIterator == null ? null : setRecordIterator();
        }

        public boolean hasNext() {
            if (next != null) {
                return true;
            }
            if (recordIterator == null || (!recordIterator.hasNext() && !segmentIterator.hasNext())) {
                return false;
            }

            next = getNext();
            if (next == null) {
                return false;
            }

            if (allKeys || !retainDuplicates) {
                return true;
            }

            final Bytes key = getKey(next.key);
            if (isKeyWithinRange(key)) {
                return true;
            } else {
                next = null;
                return hasNext();
            }
        }

        private boolean isKeyWithinRange(final Bytes key) {
            // split all cases for readability and avoid BooleanExpressionComplexity checkstyle warning
            if (keyFrom == null && keyTo == null) {
                // fetch all
                return true;
            } else if (keyFrom == null) {
                // start from the beginning
                return key.compareTo(getKey(keyTo)) <= 0;
            } else if (keyTo == null) {
                // end to the last
                return key.compareTo(getKey(keyFrom)) >= 0;
            } else {
                // key is within the range
                return key.compareTo(getKey(keyFrom)) >= 0 && key.compareTo(getKey(keyTo)) <= 0;
            }
        }

        public void close() {
            next = null;
            recordIterator = null;
            callback.deregisterIterator(this);
        }

        // getNext is only called when either recordIterator or segmentIterator has a next
        // Note this does not guarantee a next record exists as the next segments may not contain any keys in range
        protected KeyValue<Bytes, byte[]> getNext() {
            while (!recordIterator.hasNext()) {
                recordIterator = setRecordIterator();
                if (recordIterator == null) {
                    return null;
                }
            }
            final Map.Entry<Bytes, byte[]> nextRecord = recordIterator.next();
            return new KeyValue<>(nextRecord.getKey(), nextRecord.getValue());
        }

        // Resets recordIterator to point to the next segment and returns null if there are no more segments
        // Note it may not actually point to anything if no keys in range exist in the next segment
        Iterator<Map.Entry<Bytes, byte[]>> setRecordIterator() {
            if (!segmentIterator.hasNext()) {
                return null;
            }

            final Map.Entry<Long, ConcurrentNavigableMap<Bytes, byte[]>> currentSegment = segmentIterator.next();
            currentTime = currentSegment.getKey();

            final ConcurrentNavigableMap<Bytes, byte[]> subMap;
            if (allKeys) { // keyFrom == null && keyTo == null
                subMap = currentSegment.getValue();
            } else if (keyFrom == null) {
                subMap = currentSegment.getValue().headMap(keyTo, true);
            } else if (keyTo == null) {
                subMap = currentSegment.getValue().tailMap(keyFrom, true);
            } else {
                subMap = currentSegment.getValue().subMap(keyFrom, true, keyTo, true);
            }

            if (forward) {
                return subMap.entrySet().iterator();
            } else {
                return subMap.descendingMap().entrySet().iterator();
            }
        }

        Long minTime() {
            return currentTime;
        }
    }

    private static class WrappedInMemoryWindowStoreIterator extends InMemoryWindowStoreIteratorWrapper implements WindowStoreIterator<byte[]> {

        WrappedInMemoryWindowStoreIterator(final Bytes keyFrom,
                                           final Bytes keyTo,
                                           final Iterator<Map.Entry<Long, ConcurrentNavigableMap<Bytes, byte[]>>> segmentIterator,
                                           final ClosingCallback callback,
                                           final boolean retainDuplicates,
                                           final boolean forward) {
            super(keyFrom, keyTo, segmentIterator, callback, retainDuplicates, forward);
        }

        @Override
        public Long peekNextKey() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return super.currentTime;
        }

        @Override
        public KeyValue<Long, byte[]> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            final KeyValue<Long, byte[]> result = new KeyValue<>(super.currentTime, super.next.value);
            super.next = null;
            return result;
        }

        public static WrappedInMemoryWindowStoreIterator emptyIterator() {
            return new WrappedInMemoryWindowStoreIterator(null, null, null, it -> {
            }, false, true);
        }
    }

    private static class WrappedWindowedKeyValueIterator
        extends InMemoryWindowStoreIteratorWrapper
        implements KeyValueIterator<Windowed<Bytes>, byte[]> {

        private final long windowSize;

        WrappedWindowedKeyValueIterator(final Bytes keyFrom,
                                        final Bytes keyTo,
                                        final Iterator<Map.Entry<Long, ConcurrentNavigableMap<Bytes, byte[]>>> segmentIterator,
                                        final ClosingCallback callback,
                                        final boolean retainDuplicates,
                                        final long windowSize,
                                        final boolean forward) {
            super(keyFrom, keyTo, segmentIterator, callback, retainDuplicates, forward);
            this.windowSize = windowSize;
        }

        public Windowed<Bytes> peekNextKey() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return getWindowedKey();
        }

        public KeyValue<Windowed<Bytes>, byte[]> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            final KeyValue<Windowed<Bytes>, byte[]> result = new KeyValue<>(getWindowedKey(), super.next.value);
            super.next = null;
            return result;
        }

        private Windowed<Bytes> getWindowedKey() {
            final Bytes key = super.retainDuplicates ? getKey(super.next.key) : super.next.key;
            long endTime = super.currentTime + windowSize;

            if (endTime < 0) {
                LOG.warn("Warning: window end time was truncated to Long.MAX");
                endTime = Long.MAX_VALUE;
            }

            final TimeWindow timeWindow = new TimeWindow(super.currentTime, endTime);
            return new Windowed<>(key, timeWindow);
        }
    }

    /**
     * Yields each entry as an {@link InMemoryWindowTransactionBuffer.WindowEntryKey} keyed by the
     * stored (possibly seqnum-wrapped) key. Reused by {@link InMemoryWindowTransactionBuffer} as the
     * committed-side base iterator, so it merges in lock-step with the staged composite map; the
     * store's transactional read wrappers unwrap the key afterwards.
     */
    static final class WindowEntryKeyIterator extends InMemoryWindowStoreIteratorWrapper
        implements ManagedKeyValueIterator<InMemoryWindowTransactionBuffer.WindowEntryKey, byte[]> {

        private Runnable closeCallback;

        WindowEntryKeyIterator(final Bytes keyFrom,
                               final Bytes keyTo,
                               final Iterator<Map.Entry<Long, ConcurrentNavigableMap<Bytes, byte[]>>> segmentIterator,
                               final boolean retainDuplicates,
                               final boolean forward) {
            super(keyFrom, keyTo, segmentIterator, ignored -> { }, retainDuplicates, forward);
        }

        @Override
        public InMemoryWindowTransactionBuffer.WindowEntryKey peekNextKey() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return new InMemoryWindowTransactionBuffer.WindowEntryKey(super.currentTime, super.next.key);
        }

        @Override
        public KeyValue<InMemoryWindowTransactionBuffer.WindowEntryKey, byte[]> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            final KeyValue<InMemoryWindowTransactionBuffer.WindowEntryKey, byte[]> result =
                new KeyValue<>(new InMemoryWindowTransactionBuffer.WindowEntryKey(super.currentTime, super.next.key), super.next.value);
            super.next = null;
            return result;
        }

        @Override
        public void onClose(final Runnable closeCallback) {
            this.closeCallback = closeCallback;
        }

        @Override
        public void close() {
            try {
                super.close();
            } finally {
                if (closeCallback != null) {
                    closeCallback.run();
                }
            }
        }
    }
}
