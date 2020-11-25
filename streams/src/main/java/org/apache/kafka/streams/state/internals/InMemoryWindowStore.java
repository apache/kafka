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
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ProcessorContextUtils;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.apache.kafka.streams.state.internals.WindowKeySchema.extractStoreKeyBytes;
import static org.apache.kafka.streams.state.internals.WindowKeySchema.extractStoreTimestamp;


public class InMemoryWindowStore implements WindowStore<Bytes, byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryWindowStore.class);
    private static final int SEQNUM_SIZE = 4;

    private final String name;
    private final String metricScope;
    private final long retentionPeriod;
    private final long windowSize;
    private final boolean retainDuplicates;

    private final ConcurrentNavigableMap<Long, ConcurrentNavigableMap<Bytes, byte[]>> segmentMap = new ConcurrentSkipListMap<>();
    private final Set<InMemoryWindowStoreIteratorWrapper> openIterators = ConcurrentHashMap.newKeySet();

    private ProcessorContext context;
    private Sensor expiredRecordSensor;
    private int seqnum = 0;
    private long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;

    private volatile boolean open = false;

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
    }

    @Override
    public String name() {
        return name;
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        this.context = context;

        final StreamsMetricsImpl metrics = ProcessorContextUtils.getMetricsImpl(context);
        final String threadId = Thread.currentThread().getName();
        final String taskName = context.taskId().toString();
        expiredRecordSensor = TaskMetrics.droppedRecordsSensorOrExpiredWindowRecordDropSensor(
            threadId,
            taskName,
            metricScope,
            name,
            metrics
        );

        if (root != null) {
            context.register(root, (key, value) ->
                put(Bytes.wrap(extractStoreKeyBytes(key)), value, extractStoreTimestamp(key)));
        }
        open = true;
    }

    @Deprecated
    @Override
    public void put(final Bytes key, final byte[] value) {
        put(key, value, context.timestamp());
    }

    @Override
    public void put(final Bytes key, final byte[] value, final long windowStartTimestamp) {
        removeExpiredSegments();
        observedStreamTime = Math.max(observedStreamTime, windowStartTimestamp);

        if (windowStartTimestamp <= observedStreamTime - retentionPeriod) {
            expiredRecordSensor.record(1.0d, ProcessorContextUtils.getCurrentSystemTime(context));
            LOG.warn("Skipping record for expired segment.");
        } else {
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
    }

    @Override
    public byte[] fetch(final Bytes key, final long windowStartTimestamp) {
        Objects.requireNonNull(key, "key cannot be null");

        removeExpiredSegments();

        if (windowStartTimestamp <= observedStreamTime - retentionPeriod) {
            return null;
        }

        final ConcurrentNavigableMap<Bytes, byte[]> kvMap = segmentMap.get(windowStartTimestamp);
        if (kvMap == null) {
            return null;
        } else {
            return kvMap.get(key);
        }
    }

    @Deprecated
    @Override
    public WindowStoreIterator<byte[]> fetch(final Bytes key, final long timeFrom, final long timeTo) {
        return fetch(key, timeFrom, timeTo, true);
    }

    @Override
    public WindowStoreIterator<byte[]> backwardFetch(final Bytes key, final long timeFrom, final long timeTo) {
        return fetch(key, timeFrom, timeTo, false);
    }

    WindowStoreIterator<byte[]> fetch(final Bytes key, final long timeFrom, final long timeTo, final boolean forward) {
        Objects.requireNonNull(key, "key cannot be null");

        removeExpiredSegments();

        // add one b/c records expire exactly retentionPeriod ms after created
        final long minTime = Math.max(timeFrom, observedStreamTime - retentionPeriod + 1);

        if (timeTo < minTime) {
            return WrappedInMemoryWindowStoreIterator.emptyIterator();
        }

        if (forward) {
            return registerNewWindowStoreIterator(
                key,
                segmentMap.subMap(minTime, true, timeTo, true)
                    .entrySet().iterator(),
                true
            );
        } else {
            return registerNewWindowStoreIterator(
                key,
                segmentMap.subMap(minTime, true, timeTo, true)
                    .descendingMap().entrySet().iterator(),
                false
            );
        }
    }

    @Deprecated
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
        Objects.requireNonNull(from, "from key cannot be null");
        Objects.requireNonNull(to, "to key cannot be null");

        removeExpiredSegments();

        if (from.compareTo(to) > 0) {
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

        if (forward) {
            return registerNewWindowedKeyValueIterator(
                from,
                to,
                segmentMap.subMap(minTime, true, timeTo, true)
                    .entrySet().iterator(),
                true
            );
        } else {
            return registerNewWindowedKeyValueIterator(
                from,
                to,
                segmentMap.subMap(minTime, true, timeTo, true)
                    .descendingMap().entrySet().iterator(),
                false
            );
        }
    }

    @Deprecated
    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final long timeFrom, final long timeTo) {
        return fetchAll(timeFrom, timeTo, true);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardFetchAll(final long timeFrom, final long timeTo) {
        return fetchAll(timeFrom, timeTo, false);
    }

    KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final long timeFrom, final long timeTo, final boolean forward) {
        removeExpiredSegments();

        // add one b/c records expire exactly retentionPeriod ms after created
        final long minTime = Math.max(timeFrom, observedStreamTime - retentionPeriod + 1);

        if (timeTo < minTime) {
            return KeyValueIterators.emptyIterator();
        }

        if (forward) {
            return registerNewWindowedKeyValueIterator(
                null,
                null,
                segmentMap.subMap(minTime, true, timeTo, true)
                    .entrySet().iterator(),
                true
            );
        } else {
            return registerNewWindowedKeyValueIterator(
                null,
                null,
                segmentMap.subMap(minTime, true, timeTo, true)
                    .descendingMap().entrySet().iterator(),
                false
            );
        }
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
        removeExpiredSegments();

        final long minTime = observedStreamTime - retentionPeriod;

        return registerNewWindowedKeyValueIterator(
            null,
            null,
            segmentMap.tailMap(minTime, false).entrySet().iterator(),
            true
        );
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> backwardAll() {
        removeExpiredSegments();

        final long minTime = observedStreamTime - retentionPeriod;

        return registerNewWindowedKeyValueIterator(
            null,
            null,
            segmentMap.tailMap(minTime, false).descendingMap().entrySet().iterator(),
            false
        );
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
            for (final InMemoryWindowStoreIteratorWrapper it : openIterators) {
                it.close();
            }
        }

        segmentMap.clear();
        open = false;
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
            new WrappedWindowedKeyValueIterator(from,
                to,
                segmentIterator,
                openIterators::remove,
                retainDuplicates,
                windowSize,
                forward);
        openIterators.add(iterator);
        return iterator;
    }


    interface ClosingCallback {
        void deregisterIterator(final InMemoryWindowStoreIteratorWrapper iterator);
    }

    private static abstract class InMemoryWindowStoreIteratorWrapper {

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
            if (key.compareTo(getKey(keyFrom)) >= 0 && key.compareTo(getKey(keyTo)) <= 0) {
                return true;
            } else {
                next = null;
                return hasNext();
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

            if (allKeys) {
                return currentSegment.getValue().entrySet().iterator();
            } else {
                return currentSegment.getValue().subMap(keyFrom, true, keyTo, true).entrySet().iterator();
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
}
