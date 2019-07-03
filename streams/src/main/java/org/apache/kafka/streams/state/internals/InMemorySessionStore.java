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

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.EXPIRED_WINDOW_RECORD_DROP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addInvocationRateAndCount;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemorySessionStore implements SessionStore<Bytes, byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(InMemorySessionStore.class);

    private final String name;
    private final String metricScope;
    private Sensor expiredRecordSensor;
    private long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;

    private final long retentionPeriod;

    private final ConcurrentNavigableMap<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>> endTimeMap = new ConcurrentSkipListMap<>();
    private final Set<InMemorySessionStoreIterator> openIterators  = ConcurrentHashMap.newKeySet();

    private volatile boolean open = false;

    InMemorySessionStore(final String name,
                         final long retentionPeriod,
                         final String metricScope) {
        this.name = name;
        this.retentionPeriod = retentionPeriod;
        this.metricScope = metricScope;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        final StreamsMetricsImpl metrics = ((InternalProcessorContext) context).metrics();
        final String taskName = context.taskId().toString();
        expiredRecordSensor = metrics.storeLevelSensor(
            taskName,
            name(),
            EXPIRED_WINDOW_RECORD_DROP,
            Sensor.RecordingLevel.INFO
        );
        addInvocationRateAndCount(
            expiredRecordSensor,
            "stream-" + metricScope + "-metrics",
            metrics.tagMap("task-id", taskName, metricScope + "-id", name()),
            EXPIRED_WINDOW_RECORD_DROP
        );

        if (root != null) {
            context.register(root, (key, value) -> put(SessionKeySchema.from(Bytes.wrap(key)), value));
        }
        open = true;
    }

    @Override
    public void put(final Windowed<Bytes> sessionKey, final byte[] aggregate) {
        removeExpiredSegments();

        final long windowEndTimestamp = sessionKey.window().end();
        observedStreamTime = Math.max(observedStreamTime, windowEndTimestamp);

        if (windowEndTimestamp <= observedStreamTime - retentionPeriod) {
            expiredRecordSensor.record();
            LOG.debug("Skipping record for expired segment.");
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
    }

    @Override
    public void remove(final Windowed<Bytes> sessionKey) {
        final ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>> keyMap = endTimeMap.get(sessionKey.window().end());
        final ConcurrentNavigableMap<Long, byte[]> startTimeMap = keyMap.get(sessionKey.key());
        startTimeMap.remove(sessionKey.window().start());

        if (startTimeMap.isEmpty()) {
            keyMap.remove(sessionKey.key());
            if (keyMap.isEmpty()) {
                endTimeMap.remove(sessionKey.window().end());
            }
        }
    }

    @Override
    public byte[] fetchSession(final Bytes key, final long startTime, final long endTime) {
        removeExpiredSegments();

        Objects.requireNonNull(key, "key cannot be null");

        // Only need to search if the record hasn't expired yet
        if (endTime > observedStreamTime - retentionPeriod) {
            final ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>> keyMap = endTimeMap.get(endTime);
            if (keyMap != null) {
                final ConcurrentNavigableMap<Long, byte[]> startTimeMap = keyMap.get(key);
                if (startTimeMap != null) {
                    return startTimeMap.get(startTime);
                }
            }
        }
        return null;
    }

    @Deprecated
    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes key,
                                                                  final long earliestSessionEndTime,
                                                                  final long latestSessionStartTime) {
        Objects.requireNonNull(key, "key cannot be null");

        removeExpiredSegments();

        return registerNewIterator(key,
                                   key,
                                   latestSessionStartTime,
                                   endTimeMap.tailMap(earliestSessionEndTime, true).entrySet().iterator());
    }

    @Deprecated
    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> findSessions(final Bytes keyFrom,
                                                                  final Bytes keyTo,
                                                                  final long earliestSessionEndTime,
                                                                  final long latestSessionStartTime) {
        Objects.requireNonNull(keyFrom, "from key cannot be null");
        Objects.requireNonNull(keyTo, "to key cannot be null");

        removeExpiredSegments();

        if (keyFrom.compareTo(keyTo) > 0) {
            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. "
                + "This may be due to serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }

        return registerNewIterator(keyFrom,
                                   keyTo,
                                   latestSessionStartTime,
                                   endTimeMap.tailMap(earliestSessionEndTime, true).entrySet().iterator());
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes key) {

        Objects.requireNonNull(key, "key cannot be null");

        removeExpiredSegments();

        return registerNewIterator(key, key, Long.MAX_VALUE, endTimeMap.entrySet().iterator());
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes from, final Bytes to) {

        Objects.requireNonNull(from, "from key cannot be null");
        Objects.requireNonNull(to, "to key cannot be null");

        removeExpiredSegments();


        return registerNewIterator(from, to, Long.MAX_VALUE, endTimeMap.entrySet().iterator());
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
                                                             final Iterator<Entry<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>>> endTimeIterator) {
        final InMemorySessionStoreIterator iterator = new InMemorySessionStoreIterator(keyFrom, keyTo, latestSessionStartTime, endTimeIterator, it -> openIterators.remove(it));
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

        InMemorySessionStoreIterator(final Bytes keyFrom,
                                     final Bytes keyTo,
                                     final long latestSessionStartTime,
                                     final Iterator<Entry<Long, ConcurrentNavigableMap<Bytes, ConcurrentNavigableMap<Long, byte[]>>>> endTimeIterator,
                                     final ClosingCallback callback) {
            this.keyFrom = keyFrom;
            this.keyTo = keyTo;
            this.latestSessionStartTime = latestSessionStartTime;

            this.endTimeIterator = endTimeIterator;
            this.callback = callback;
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
                keyIterator = nextEndTimeEntry.getValue().subMap(keyFrom, true, keyTo, true).entrySet().iterator();

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
                    recordIterator = nextKeyEntry.getValue().entrySet().iterator();
                } else {
                    recordIterator = nextKeyEntry.getValue().headMap(latestSessionStartTime, true).entrySet().iterator();
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
