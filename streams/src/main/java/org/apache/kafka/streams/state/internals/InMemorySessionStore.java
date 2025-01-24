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
import org.apache.kafka.streams.state.SessionStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.apache.kafka.streams.StreamsConfig.InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED;

public class InMemorySessionStore implements SessionStore<Bytes, byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(InMemorySessionStore.class);

    private final String name;
    private final String metricScope;
    private Sensor expiredRecordSensor;
    private InternalProcessorContext context;
    private long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;

    private final long retentionPeriod;

    private static final String INVALID_RANGE_WARN_MSG =
        "Returning empty iterator for fetch with invalid key range: from > to. " +
        "This may be due to range arguments set in the wrong order, " +
        "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
        "Note that the built-in numerical serdes do not follow this for negative numbers";

    private final ConcurrentNavigableMap<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>> endTimeMap = new ConcurrentSkipListMap<>();
    private final Set<InMemorySessionStoreIterator> openIterators  = ConcurrentHashMap.newKeySet();

    private volatile boolean open = false;

    private StateStoreContext stateStoreContext;
    private final Position position;

    InMemorySessionStore(final String name,
                         final long retentionPeriod,
                         final String metricScope) {
        this.name = name;
        this.retentionPeriod = retentionPeriod;
        this.metricScope = metricScope;
        this.position = Position.emptyPosition();
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
            this.context = (InternalProcessorContext) stateStoreContext;
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
                        for (final ConsumerRecord<byte[], byte[]> record : records) {
                            put(SessionKeySchema.from(Bytes.wrap(record.key())), record.value());
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
    }

    @Override
    public Position getPosition() {
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
            } else {
                if (aggregate != null) {
                    endTimeMap.computeIfAbsent(windowEndTimestamp, t -> new ConcurrentSkipListMap<>());
                    final ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>> keyMap = endTimeMap.get(windowEndTimestamp);
                    keyMap.computeIfAbsent(sessionKey.key(), t -> new ConcurrentSkipListMap<>());
                    keyMap.get(sessionKey.key()).put(sessionKey.window().start(), aggregate);
                } else {
                    remove(sessionKey);
                }
            }

            StoreQueryUtils.updatePosition(position, stateStoreContext);
        }
    }

    @Override
    public void remove(final Windowed<Bytes> sessionKey) {
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
        removeExpiredSegments();

        Objects.requireNonNull(key, "key cannot be null");

        // Only need to search if the record hasn't expired yet
        if (sessionEndTime > observedStreamTime - retentionPeriod) {
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
        removeExpiredSegments();

        final ConcurrentNavigableMap<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>> endTimSubMap
            = endTimeMap.subMap(earliestSessionEndTime, true, latestSessionEndTime, true);

        return registerNewIterator(null, null, Long.MAX_VALUE, endTimSubMap.entrySet().iterator(), true);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes key,
                                                                  final long earliestSessionEndTime,
                                                                  final long latestSessionStartTime) {
        Objects.requireNonNull(key, "key cannot be null");

        removeExpiredSegments();

        return registerNewIterator(key,
                                   key,
                                   latestSessionStartTime,
                                   endTimeMap.tailMap(earliestSessionEndTime, true).entrySet().iterator(),
                                   true);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFindSessions(final Bytes key,
                                                                          final long earliestSessionEndTime,
                                                                          final long latestSessionStartTime) {
        Objects.requireNonNull(key, "key cannot be null");

        removeExpiredSegments();

        return registerNewIterator(
            key,
            key,
            latestSessionStartTime,
            endTimeMap.tailMap(earliestSessionEndTime, true).descendingMap().entrySet().iterator(),
            false
        );
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes keyFrom,
                                                                  final Bytes keyTo,
                                                                  final long earliestSessionEndTime,
                                                                  final long latestSessionStartTime) {
        removeExpiredSegments();

        if (keyFrom != null && keyTo != null && keyFrom.compareTo(keyTo) > 0) {
            LOG.warn(INVALID_RANGE_WARN_MSG);
            return KeyValueIterators.emptyIterator();
        }

        return registerNewIterator(keyFrom,
                                   keyTo,
                                   latestSessionStartTime,
                                   endTimeMap.tailMap(earliestSessionEndTime, true).entrySet().iterator(),
                                   true);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFindSessions(final Bytes keyFrom,
                                                                          final Bytes keyTo,
                                                                          final long earliestSessionEndTime,
                                                                          final long latestSessionStartTime) {
        removeExpiredSegments();

        if (keyFrom != null && keyTo != null && keyFrom.compareTo(keyTo) > 0) {
            LOG.warn(INVALID_RANGE_WARN_MSG);
            return KeyValueIterators.emptyIterator();
        }

        return registerNewIterator(
            keyFrom,
            keyTo,
            latestSessionStartTime,
            endTimeMap.tailMap(earliestSessionEndTime, true).descendingMap().entrySet().iterator(),
            false
        );
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes key) {

        Objects.requireNonNull(key, "key cannot be null");

        removeExpiredSegments();

        return registerNewIterator(key, key, Long.MAX_VALUE, endTimeMap.entrySet().iterator(), true);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes key) {

        Objects.requireNonNull(key, "key cannot be null");

        removeExpiredSegments();

        return registerNewIterator(key, key, Long.MAX_VALUE, endTimeMap.descendingMap().entrySet().iterator(), false);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes keyFrom, final Bytes keyTo) {
        removeExpiredSegments();

        return registerNewIterator(keyFrom, keyTo, Long.MAX_VALUE, endTimeMap.entrySet().iterator(), true);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetch(final Bytes keyFrom, final Bytes keyTo) {
        removeExpiredSegments();

        return registerNewIterator(
            keyFrom, keyTo, Long.MAX_VALUE, endTimeMap.descendingMap().entrySet().iterator(), false);
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

        return StoreQueryUtils.handleBasicQueries(
            query,
            positionBound,
            config,
            this,
            position,
            context
        );
    }

    @Override
    public void flush() {
        // do-nothing since it is in-memory
    }

    @Override
    public void close() {
        if (openIterators.size() != 0) {
            LOG.warn("Closing {} open iterators for store {}", openIterators.size(), name);
            for (final InMemorySessionStoreIterator it : openIterators) {
                it.close();
            }
        }

        endTimeMap.clear();
        openIterators.clear();
        open = false;
    }

    private void removeExpiredSegments() {
        long minLiveTime = Math.max(0L, observedStreamTime - retentionPeriod + 1);

        for (final InMemorySessionStoreIterator it : openIterators) {
            minLiveTime = Math.min(minLiveTime, it.minTime());
        }

        endTimeMap.headMap(minLiveTime, false).clear();
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

    interface ClosingCallback {
        void deregisterIterator(final InMemorySessionStoreIterator iterator);
    }

    private static class InMemorySessionStoreIterator implements KeyValueIterator<Windowed<Bytes>, byte[]> {

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
