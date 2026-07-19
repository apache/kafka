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
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.ChangelogRecordDeserializationHelper;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.state.SessionStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;

import static org.apache.kafka.streams.StreamsConfig.InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED;

public class InMemorySessionStore implements SessionStore<Bytes, byte[]>, WithRetentionPeriod {

    private static final Logger LOG = LoggerFactory.getLogger(InMemorySessionStore.class);

    private final String name;
    private final String metricScope;
    private Sensor expiredRecordSensor;
    private InternalProcessorContext<?, ?> context;
    private long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;

    private final long retentionPeriod;

    private static final String INVALID_RANGE_WARN_MSG =
        "Returning empty iterator for fetch with invalid key range: from > to. " +
        "This may be due to range arguments set in the wrong order, " +
        "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
        "Note that the built-in numerical serdes do not follow this for negative numbers";

    private final ConcurrentNavigableMap<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>> endTimeMap = new ConcurrentSkipListMap<>();
    private final Set<InMemorySessionStoreIterator> openIterators  = ConcurrentHashMap.newKeySet();
    private final Set<TransactionalSessionIterator> openTransactionalIterators = ConcurrentHashMap.newKeySet();

    private volatile boolean open = false;

    private StateStoreContext stateStoreContext;
    private final Position position;
    private InMemorySessionTransactionBuffer transactionBuffer;

    public InMemorySessionStore(
        final String name,
        final long retentionPeriod,
        final String metricScope
    ) {
        this.name = name;
        this.retentionPeriod = retentionPeriod;
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
        this.stateStoreContext = stateStoreContext;
        final String threadId = Thread.currentThread().getName();
        final String taskName = stateStoreContext.taskId().toString();

        // The provided context is not required to implement InternalProcessorContext,
        // If it doesn't, we can't record this metric.
        if (stateStoreContext instanceof InternalProcessorContext) {
            this.context = (InternalProcessorContext<?, ?>) stateStoreContext;
            final StreamsMetricsImpl metrics = this.context.metrics();
            expiredRecordSensor = TaskMetrics.droppedRecordsSensor(
                threadId,
                taskName,
                metrics
            );
        } else {
            this.context = null;
            expiredRecordSensor = null;
        }

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
                            final Windowed<Bytes> sessionKey = SessionKeySchema.from(Bytes.wrap(record.key()));
                            final long windowEndTimestamp = sessionKey.window().end();
                            observedStreamTime = Math.max(observedStreamTime, windowEndTimestamp);
                            if (windowEndTimestamp <= observedStreamTime - retentionPeriod) {
                                expiredRecords++;
                            } else {
                                // Write directly to the committed map: restored records are already committed.
                                putInternal(sessionKey, record.value());
                            }
                            ChangelogRecordDeserializationHelper.applyChecksAndUpdatePosition(
                                record,
                                consistencyEnabled,
                                position
                            );
                        }
                        removeExpiredSegments();
                        if (expiredRecords > 0) {
                            if (expiredRecordSensor != null && context != null) {
                                expiredRecordSensor.record(expiredRecords, context.currentSystemTimeMs());
                            }
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
            this.transactionBuffer = new InMemorySessionTransactionBuffer(endTimeMap);
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
    public void put(final Windowed<Bytes> sessionKey, final byte[] aggregate) {
        removeExpiredSegments();

        final long windowEndTimestamp = sessionKey.window().end();
        observedStreamTime = Math.max(observedStreamTime, windowEndTimestamp);

        synchronized (position) {
            if (windowEndTimestamp <= observedStreamTime - retentionPeriod) {
                // The provided context is not required to implement InternalProcessorContext,
                // If it doesn't, we can't record this metric (in fact, we wouldn't have even initialized it).
                if (expiredRecordSensor != null && context != null) {
                    expiredRecordSensor.record(1.0d, context.currentSystemTimeMs());
                }
                LOG.warn("Skipping record for expired segment.");
            } else if (transactionBuffer != null) {
                transactionBuffer.stage(sessionKey, aggregate);
            } else {
                putInternal(sessionKey, aggregate);
            }

            if (transactionBuffer != null) {
                transactionBuffer.updatePosition(stateStoreContext);
            } else {
                StoreQueryUtils.updatePosition(position, stateStoreContext);
            }
        }
    }

    private void putInternal(final Windowed<Bytes> sessionKey, final byte[] aggregate) {
        if (aggregate != null) {
            final long windowEndTimestamp = sessionKey.window().end();
            endTimeMap.computeIfAbsent(windowEndTimestamp, t -> new ConcurrentSkipListMap<>());
            final ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>> keyMap = endTimeMap.get(windowEndTimestamp);
            keyMap.computeIfAbsent(sessionKey.key(), t -> new ConcurrentSkipListMap<>());
            keyMap.get(sessionKey.key()).put(sessionKey.window().start(), aggregate);
        } else {
            removeFromBase(sessionKey);
        }
    }

    @Override
    public void remove(final Windowed<Bytes> sessionKey) {
        if (transactionBuffer != null) {
            transactionBuffer.stage(sessionKey, null);
            return;
        }
        removeFromBase(sessionKey);
    }

    private void removeFromBase(final Windowed<Bytes> sessionKey) {
        final ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>> keyMap = endTimeMap.get(sessionKey.window().end());
        if (keyMap == null) {
            return;
        }

        final ConcurrentNavigableMap<Long, byte[]> startTimeMap = keyMap.get(sessionKey.key());
        if (startTimeMap == null) {
            return;
        }

        startTimeMap.remove(sessionKey.window().start());

        if (startTimeMap.isEmpty()) {
            keyMap.remove(sessionKey.key());
            if (keyMap.isEmpty()) {
                endTimeMap.remove(sessionKey.window().end());
            }
        }
    }

    @Override
    public byte[] fetchSession(final Bytes key,
                               final long sessionStartTime,
                               final long sessionEndTime) {
        return fetchSession(key, sessionStartTime, sessionEndTime, IsolationLevel.READ_UNCOMMITTED);
    }

    private byte[] fetchSession(final Bytes key,
                                final long sessionStartTime,
                                final long sessionEndTime,
                                final IsolationLevel isolationLevel) {
        removeExpiredSegments();

        Objects.requireNonNull(key, "key cannot be null");

        // Only need to search if the record hasn't expired yet
        if (sessionEndTime > observedStreamTime - retentionPeriod) {
            if (transactionBuffer != null) {
                if (isolationLevel == IsolationLevel.READ_UNCOMMITTED) {
                    final Optional<byte[]> staged = transactionBuffer.get(key, sessionStartTime, sessionEndTime);
                    if (staged != null) {
                        return staged.orElse(null);
                    }
                }
                // Committed read of the base map, taken under the buffer's snapshot read-lock so it
                // cannot race a concurrent commit.
                return transactionBuffer.getCommitted(key, sessionStartTime, sessionEndTime);
            }

            final ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>> keyMap = endTimeMap.get(sessionEndTime);
            if (keyMap != null) {
                final ConcurrentNavigableMap<Long, byte[]> startTimeMap = keyMap.get(key);
                if (startTimeMap != null) {
                    return startTimeMap.get(sessionStartTime);
                }
            }
        }
        return null;
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final long earliestSessionEndTime,
                                                                  final long latestSessionEndTime) {
        return findSessions(earliestSessionEndTime, latestSessionEndTime, IsolationLevel.READ_UNCOMMITTED);
    }

    private KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final long earliestSessionEndTime,
                                                                   final long latestSessionEndTime,
                                                                   final IsolationLevel isolationLevel) {
        removeExpiredSegments();

        if (transactionBuffer != null) {
            return newTransactionalSessionIterator(
                null, null, Long.MAX_VALUE, earliestSessionEndTime, latestSessionEndTime, true, isolationLevel
            );
        }

        final ConcurrentNavigableMap<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>> endTimSubMap
            = endTimeMap.subMap(earliestSessionEndTime, true, latestSessionEndTime, true);

        return registerNewIterator(null, null, Long.MAX_VALUE, endTimSubMap.entrySet().iterator(), true);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes key,
                                                                  final long earliestSessionEndTime,
                                                                  final long latestSessionStartTime) {
        return findSessionsForKey(key, earliestSessionEndTime, latestSessionStartTime, true, IsolationLevel.READ_UNCOMMITTED);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFindSessions(final Bytes key,
                                                                          final long earliestSessionEndTime,
                                                                          final long latestSessionStartTime) {
        return findSessionsForKey(key, earliestSessionEndTime, latestSessionStartTime, false, IsolationLevel.READ_UNCOMMITTED);
    }

    private KeyValueIterator<Windowed<Bytes>, byte[]> findSessionsForKey(final Bytes key,
                                                                         final long earliestSessionEndTime,
                                                                         final long latestSessionStartTime,
                                                                         final boolean forward,
                                                                         final IsolationLevel isolationLevel) {
        Objects.requireNonNull(key, "key cannot be null");

        removeExpiredSegments();

        if (transactionBuffer != null) {
            return newTransactionalSessionIterator(
                key, key, latestSessionStartTime, earliestSessionEndTime, Long.MAX_VALUE, forward, isolationLevel
            );
        }

        final ConcurrentNavigableMap<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>> endTimeSubMap =
            endTimeMap.tailMap(earliestSessionEndTime, true);
        final Iterator<Entry<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>>> endTimeIter =
            forward
                ? endTimeSubMap.entrySet().iterator()
                : endTimeSubMap.descendingMap().entrySet().iterator();
        return registerNewIterator(key, key, latestSessionStartTime, endTimeIter, forward);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes keyFrom,
                                                                  final Bytes keyTo,
                                                                  final long earliestSessionEndTime,
                                                                  final long latestSessionStartTime) {
        return findSessionsForKeyRange(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime, true, IsolationLevel.READ_UNCOMMITTED);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFindSessions(final Bytes keyFrom,
                                                                          final Bytes keyTo,
                                                                          final long earliestSessionEndTime,
                                                                          final long latestSessionStartTime) {
        return findSessionsForKeyRange(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime, false, IsolationLevel.READ_UNCOMMITTED);
    }

    private KeyValueIterator<Windowed<Bytes>, byte[]> findSessionsForKeyRange(final Bytes keyFrom,
                                                                              final Bytes keyTo,
                                                                              final long earliestSessionEndTime,
                                                                              final long latestSessionStartTime,
                                                                              final boolean forward,
                                                                              final IsolationLevel isolationLevel) {
        removeExpiredSegments();

        if (keyFrom != null && keyTo != null && keyFrom.compareTo(keyTo) > 0) {
            LOG.warn(INVALID_RANGE_WARN_MSG);
            return KeyValueIterators.emptyIterator();
        }

        if (transactionBuffer != null) {
            return newTransactionalSessionIterator(
                keyFrom, keyTo, latestSessionStartTime, earliestSessionEndTime, Long.MAX_VALUE, forward, isolationLevel
            );
        }

        final ConcurrentNavigableMap<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>> endTimeSubMap =
            endTimeMap.tailMap(earliestSessionEndTime, true);
        final Iterator<Entry<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>>> endTimeIter =
            forward
                ? endTimeSubMap.entrySet().iterator()
                : endTimeSubMap.descendingMap().entrySet().iterator();
        return registerNewIterator(keyFrom, keyTo, latestSessionStartTime, endTimeIter, forward);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes key) {
        return fetchForKey(key, true, IsolationLevel.READ_UNCOMMITTED);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes key) {
        return fetchForKey(key, false, IsolationLevel.READ_UNCOMMITTED);
    }

    private KeyValueIterator<Windowed<Bytes>, byte[]> fetchForKey(final Bytes key,
                                                                  final boolean forward,
                                                                  final IsolationLevel isolationLevel) {
        Objects.requireNonNull(key, "key cannot be null");

        removeExpiredSegments();

        if (transactionBuffer != null) {
            return newTransactionalSessionIterator(key, key, Long.MAX_VALUE, 0, Long.MAX_VALUE, forward, isolationLevel);
        }

        final Iterator<Entry<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>>> endTimeIter =
            forward ? endTimeMap.entrySet().iterator() : endTimeMap.descendingMap().entrySet().iterator();
        return registerNewIterator(key, key, Long.MAX_VALUE, endTimeIter, forward);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom, final Bytes keyTo) {
        return fetchForKeyRange(keyFrom, keyTo, true, IsolationLevel.READ_UNCOMMITTED);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom, final Bytes keyTo) {
        return fetchForKeyRange(keyFrom, keyTo, false, IsolationLevel.READ_UNCOMMITTED);
    }

    private KeyValueIterator<Windowed<Bytes>, byte[]> fetchForKeyRange(final Bytes keyFrom,
                                                                       final Bytes keyTo,
                                                                       final boolean forward,
                                                                       final IsolationLevel isolationLevel) {
        removeExpiredSegments();

        if (transactionBuffer != null) {
            return newTransactionalSessionIterator(keyFrom, keyTo, Long.MAX_VALUE, 0, Long.MAX_VALUE, forward, isolationLevel);
        }

        final Iterator<Entry<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>>> endTimeIter =
            forward ? endTimeMap.entrySet().iterator() : endTimeMap.descendingMap().entrySet().iterator();
        return registerNewIterator(keyFrom, keyTo, Long.MAX_VALUE, endTimeIter, forward);
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
                context
            );
        }
    }

    @Override
    public ReadOnlySessionStore<Bytes, byte[]> readOnly(final IsolationLevel isolationLevel) {
        Objects.requireNonNull(isolationLevel, "isolationLevel cannot be null");
        return new ReadOnlyView(isolationLevel);
    }

    /**
     * Read-only view of this store. For a transactional store the {@code isolationLevel} is passed
     * down to the transactional read path (which excludes the staging layer under READ_COMMITTED), so
     * both isolation levels share the one path and the read is snapshotted under the buffer's
     * read-lock — an interactive query cannot race a concurrent commit. For a non-transactional store
     * reads hit {@code endTimeMap} directly.
     */
    private final class ReadOnlyView implements ReadOnlySessionStore<Bytes, byte[]> {

        private final IsolationLevel isolationLevel;

        ReadOnlyView(final IsolationLevel isolationLevel) {
            this.isolationLevel = isolationLevel;
        }

        @Override
        public byte[] fetchSession(final Bytes key, final long startTime, final long endTime) {
            return InMemorySessionStore.this.fetchSession(key, startTime, endTime, isolationLevel);
        }

        @Override
        public byte[] fetchSession(final Bytes key, final Instant startTime, final Instant endTime) {
            return fetchSession(key, startTime.toEpochMilli(), endTime.toEpochMilli());
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes key,
                                                                       final long earliestSessionEndTime,
                                                                       final long latestSessionStartTime) {
            return InMemorySessionStore.this.findSessionsForKey(key, earliestSessionEndTime, latestSessionStartTime, true, isolationLevel);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFindSessions(final Bytes key,
                                                                               final long earliestSessionEndTime,
                                                                               final long latestSessionStartTime) {
            return InMemorySessionStore.this.findSessionsForKey(key, earliestSessionEndTime, latestSessionStartTime, false, isolationLevel);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes keyFrom, final Bytes keyTo,
                                                                       final long earliestSessionEndTime,
                                                                       final long latestSessionStartTime) {
            return InMemorySessionStore.this.findSessionsForKeyRange(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime, true, isolationLevel);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFindSessions(final Bytes keyFrom, final Bytes keyTo,
                                                                               final long earliestSessionEndTime,
                                                                               final long latestSessionStartTime) {
            return InMemorySessionStore.this.findSessionsForKeyRange(keyFrom, keyTo, earliestSessionEndTime, latestSessionStartTime, false, isolationLevel);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes key) {
            return InMemorySessionStore.this.fetchForKey(key, true, isolationLevel);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes key) {
            return InMemorySessionStore.this.fetchForKey(key, false, isolationLevel);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom, final Bytes keyTo) {
            return InMemorySessionStore.this.fetchForKeyRange(keyFrom, keyTo, true, isolationLevel);
        }

        @Override
        public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom, final Bytes keyTo) {
            return InMemorySessionStore.this.fetchForKeyRange(keyFrom, keyTo, false, isolationLevel);
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
            for (final InMemorySessionStoreIterator it : openIterators) {
                it.close();
            }
            for (final TransactionalSessionIterator it : openTransactionalIterators) {
                it.close();
            }
        }

        endTimeMap.clear();
        openIterators.clear();
        openTransactionalIterators.clear();
        open = false;
    }

    long numEntries() {
        return endTimeMap.values().stream()
            .flatMap(keyMap -> keyMap.values().stream())
            .mapToLong(Map::size)
            .sum();
    }

    private void removeExpiredSegments() {
        long minLiveTime = Math.max(0L, observedStreamTime - retentionPeriod + 1);

        for (final InMemorySessionStoreIterator it : openIterators) {
            minLiveTime = Math.min(minLiveTime, it.minTime());
        }

        endTimeMap.headMap(minLiveTime, false).clear();
    }

    private KeyValueIterator<Windowed<Bytes>, byte[]> newTransactionalSessionIterator(
            final Bytes keyFrom,
            final Bytes keyTo,
            final long latestSessionStartTime,
            final long earliestSessionEndTime,
            final long latestSessionEndTime,
            final boolean forward,
            final IsolationLevel isolationLevel) {
        final TransactionalSessionIterator iterator = new TransactionalSessionIterator(
            transactionBuffer, keyFrom, keyTo, latestSessionStartTime,
            earliestSessionEndTime, latestSessionEndTime,
            observedStreamTime - retentionPeriod, forward, isolationLevel, openTransactionalIterators::remove
        );
        openTransactionalIterators.add(iterator);
        return iterator;
    }

    private InMemorySessionStoreIterator registerNewIterator(final Bytes keyFrom,
                                                             final Bytes keyTo,
                                                             final long latestSessionStartTime,
                                                             final Iterator<Entry<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>>> endTimeIterator,
                                                             final boolean forward) {
        final InMemorySessionStoreIterator iterator =
            new InMemorySessionStoreIterator(
                keyFrom,
                keyTo,
                latestSessionStartTime,
                endTimeIterator,
                openIterators::remove,
                forward
            );
        openIterators.add(iterator);
        return iterator;
    }

    /**
     * A session iterator backed by a transactional buffer's merge scan.
     * Converts SessionEntryKey/byte[] pairs into Windowed<Bytes>/byte[] pairs,
     * filtering by latestSessionStartTime.
     */
    private static class TransactionalSessionIterator implements KeyValueIterator<Windowed<Bytes>, byte[]> {
        private final KeyValueIterator<InMemorySessionTransactionBuffer.SessionEntryKey, byte[]> delegate;
        private final Bytes keyFrom;
        private final Bytes keyTo;
        private final long latestSessionStartTime;
        private final long oldestRetainedEndTime;
        private final Consumer<TransactionalSessionIterator> deregister;
        private KeyValue<Windowed<Bytes>, byte[]> prefetched;
        private boolean closed = false;

        TransactionalSessionIterator(
                final InMemorySessionTransactionBuffer buffer,
                final Bytes keyFrom,
                final Bytes keyTo,
                final long latestSessionStartTime,
                final long earliestSessionEndTime,
                final long latestSessionEndTime,
                final long oldestRetainedEndTime,
                final boolean forward,
                final IsolationLevel isolationLevel,
                final Consumer<TransactionalSessionIterator> deregister) {
            this.keyFrom = keyFrom;
            this.keyTo = keyTo;
            this.latestSessionStartTime = latestSessionStartTime;
            this.oldestRetainedEndTime = oldestRetainedEndTime;
            this.deregister = deregister;

            // startTime sorts descending, so the lower bound carries the largest startTime and the
            // upper bound the smallest. A null key leaves the key dimension open (see SessionEntryKey).
            final InMemorySessionTransactionBuffer.SessionEntryKey from =
                new InMemorySessionTransactionBuffer.SessionEntryKey(earliestSessionEndTime, keyFrom, Long.MAX_VALUE);
            final InMemorySessionTransactionBuffer.SessionEntryKey to =
                new InMemorySessionTransactionBuffer.SessionEntryKey(latestSessionEndTime, keyTo, 0);

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
                final KeyValue<InMemorySessionTransactionBuffer.SessionEntryKey, byte[]> entry = delegate.next();
                // The committed side is already pruned by removeExpiredSegments, but the staged side is
                // not; drop expired entries here. The staged scan is also bounded only by endTime, so
                // filter the key range and latestSessionStartTime too.
                if (entry.key.endTime() > oldestRetainedEndTime
                    && entry.key.startTime() <= latestSessionStartTime
                    && keyInRange(entry.key.key())) {
                    final SessionWindow sessionWindow = new SessionWindow(entry.key.startTime(), entry.key.endTime());
                    final Windowed<Bytes> windowedKey = new Windowed<>(entry.key.key(), sessionWindow);
                    return new KeyValue<>(windowedKey, entry.value);
                }
            }
            return null;
        }

        private boolean keyInRange(final Bytes key) {
            return (keyFrom == null || key.compareTo(keyFrom) >= 0)
                && (keyTo == null || key.compareTo(keyTo) <= 0);
        }
    }

    interface ClosingCallback {
        void deregisterIterator(final InMemorySessionStoreIterator iterator);
    }

    static class InMemorySessionStoreIterator implements KeyValueIterator<Windowed<Bytes>, byte[]> {

        private final Iterator<Entry<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>>> endTimeIterator;
        private Iterator<Entry<Bytes, ConcurrentNavigableMap<Long, byte[]>>> keyIterator;
        private Iterator<Entry<Long, byte[]>> recordIterator;

        private KeyValue<Windowed<Bytes>, byte[]> next;
        private Bytes currentKey;
        private long currentEndTime;

        private final Bytes keyFrom;
        private final Bytes keyTo;
        private final long latestSessionStartTime;

        private final ClosingCallback callback;

        private final boolean forward;

        InMemorySessionStoreIterator(final Bytes keyFrom,
                                     final Bytes keyTo,
                                     final long latestSessionStartTime,
                                     final Iterator<Entry<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>>> endTimeIterator,
                                     final ClosingCallback callback,
                                     final boolean forward) {
            this.keyFrom = keyFrom;
            this.keyTo = keyTo;
            this.latestSessionStartTime = latestSessionStartTime;

            this.endTimeIterator = endTimeIterator;
            this.callback = callback;
            this.forward = forward;
            setAllIterators();
        }

        @Override
        public boolean hasNext() {
            if (next != null) {
                return true;
            } else if (recordIterator == null) {
                return false;
            } else {
                next = getNext();
                return next != null;
            }
        }

        @Override
        public Windowed<Bytes> peekNextKey() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return next.key;
        }

        @Override
        public KeyValue<Windowed<Bytes>, byte[]> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            final KeyValue<Windowed<Bytes>, byte[]> ret = next;
            next = null;
            return ret;
        }

        @Override
        public void close() {
            next = null;
            recordIterator = null;
            callback.deregisterIterator(this);
        }

        Long minTime() {
            return currentEndTime;
        }

        // getNext is only called when either recordIterator or segmentIterator has a next
        // Note this does not guarantee a next record exists as the next segments may not contain any keys in range
        private KeyValue<Windowed<Bytes>, byte[]> getNext() {
            if (!recordIterator.hasNext()) {
                getNextIterators();
            }

            if (recordIterator == null) {
                return null;
            }

            final Map.Entry<Long, byte[]> nextRecord = recordIterator.next();
            final SessionWindow sessionWindow = new SessionWindow(nextRecord.getKey(), currentEndTime);
            final Windowed<Bytes> windowedKey = new Windowed<>(currentKey, sessionWindow);

            return new KeyValue<>(windowedKey, nextRecord.getValue());
        }

        // Called when the inner two (key and starttime) iterators are empty to roll to the next endTimestamp
        // Rolls all three iterators forward until recordIterator has a next entry
        // Sets recordIterator to null if there are no records to return
        private void setAllIterators() {
            while (endTimeIterator.hasNext()) {
                final Entry<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>> nextEndTimeEntry = endTimeIterator.next();
                currentEndTime = nextEndTimeEntry.getKey();

                final ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>> subKVMap;
                if (keyFrom == null && keyTo == null) {
                    subKVMap = nextEndTimeEntry.getValue();
                } else if (keyFrom == null) {
                    subKVMap = nextEndTimeEntry.getValue().headMap(keyTo, true);
                } else if (keyTo == null) {
                    subKVMap = nextEndTimeEntry.getValue().tailMap(keyFrom, true);
                } else {
                    subKVMap = nextEndTimeEntry.getValue().subMap(keyFrom, true, keyTo, true);
                }

                if (forward) {
                    keyIterator = subKVMap.entrySet().iterator();
                } else {
                    keyIterator = subKVMap.descendingMap().entrySet().iterator();
                }

                if (setInnerIterators()) {
                    return;
                }
            }
            recordIterator = null;
        }

        // Rolls the inner two iterators (key and record) forward until recordIterators has a next entry
        // Returns false if no more records are found (for the current end time)
        private boolean setInnerIterators() {
            while (keyIterator.hasNext()) {
                final Entry<Bytes, ConcurrentNavigableMap<Long, byte[]>> nextKeyEntry = keyIterator.next();
                currentKey = nextKeyEntry.getKey();

                if (latestSessionStartTime == Long.MAX_VALUE) {
                    if (forward) {
                        recordIterator = nextKeyEntry.getValue().descendingMap().entrySet().iterator();
                    } else {
                        recordIterator = nextKeyEntry.getValue().entrySet().iterator();
                    }
                } else {
                    if (forward) {
                        recordIterator = nextKeyEntry.getValue()
                                                     .headMap(latestSessionStartTime, true)
                                                     .descendingMap()
                                                     .entrySet().iterator();
                    } else {
                        recordIterator = nextKeyEntry.getValue()
                                                     .headMap(latestSessionStartTime, true)
                                                     .entrySet().iterator();
                    }
                }

                if (recordIterator.hasNext()) {
                    return true;
                }
            }
            return false;
        }

        // Called when the current recordIterator has no entries left to roll it to the next valid entry
        // When there are no more records to return, recordIterator will be set to null
        private void getNextIterators() {
            if (setInnerIterators()) {
                return;
            }

            setAllIterators();
        }
    }

}
