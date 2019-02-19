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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addInvocationRateAndCount;
import static org.apache.kafka.streams.state.internals.WindowKeySchema.extractStoreKey;
import static org.apache.kafka.streams.state.internals.WindowKeySchema.extractStoreTimestamp;

public class InMemoryWindowStore<K extends Comparable<K>, V> implements WindowStore<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryWindowStore.class);

    private final String name;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final String metricScope;
    private StateSerdes<K, V> serdes;
    private InternalProcessorContext context;
    private Sensor expiredRecordSensor;
    private int seqnum = 0;
    private long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;

    private final long retentionPeriod;
    private final long windowSize;
    private final boolean retainDuplicates;

    private final NavigableMap<Long, NavigableMap<WrappedK<K>, V>> segmentMap;

    private volatile boolean open = false;

    InMemoryWindowStore(final String name,
                               final Serde<K> keySerde,
                               final Serde<V> valueSerde,
                               final long retentionPeriod,
                               final long windowSize,
                               final boolean retainDuplicates,
                               final String metricScope) {
        this.name = name;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.retentionPeriod = retentionPeriod;
        this.windowSize = windowSize;
        this.retainDuplicates = retainDuplicates;
        this.metricScope = metricScope;

        this.segmentMap = new TreeMap<>();
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(final ProcessorContext context, final StateStore root) {
        this.context = (InternalProcessorContext) context;

        // construct the serde
        this.serdes = new StateSerdes<>(
            ProcessorStateManager.storeChangelogTopic(context.applicationId(), name),
            keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
            valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);

        final StreamsMetricsImpl metrics = this.context.metrics();
        final String taskName = context.taskId().toString();
        expiredRecordSensor = metrics.storeLevelSensor(
            taskName,
            name(),
            "expired-window-record-drop",
            Sensor.RecordingLevel.INFO
        );
        addInvocationRateAndCount(
            expiredRecordSensor,
            "stream-" + metricScope + "-metrics",
            metrics.tagMap("task-id", taskName, metricScope + "-id", name()),
            "expired-window-record-drop"
        );

        if (root != null) {
            context.register(root, (key, value) -> {
                put(extractStoreKey(key, serdes), serdes.valueFrom(value), extractStoreTimestamp(key));
            });
        }
        this.open = true;
    }

    @Override
    public void put(final K key, final V value) {
        put(key, value, context.timestamp());
    }

    @Override
    public void put(final K key, final V value, final long windowStartTimestamp) {
        removeExpiredSegments();
        maybeUpdateSeqnumForDups();
        this.observedStreamTime = Math.max(this.observedStreamTime, windowStartTimestamp);

        if (windowStartTimestamp <= this.observedStreamTime - this.retentionPeriod) {
            expiredRecordSensor.record();
            LOG.debug("Skipping record for expired segment.");
        } else {
            if (value != null) {
                this.segmentMap.computeIfAbsent(windowStartTimestamp, t -> new TreeMap<>());
                this.segmentMap.get(windowStartTimestamp).put(new WrappedK<>(key, seqnum), value);
            } else {
                this.segmentMap.computeIfPresent(windowStartTimestamp, (t, kvMap) -> {
                    kvMap.remove(new WrappedK<>(key, seqnum));
                    return kvMap;
                });
            }
        }
    }

    @Override
    public V fetch(final K key, final long windowStartTimestamp) {
        removeExpiredSegments();

        final NavigableMap<WrappedK<K>, V> kvMap = this.segmentMap.get(windowStartTimestamp);
        if (kvMap == null) {
            return null;
        } else {
            return kvMap.get(new WrappedK<>(key, seqnum));
        }
    }

    @Deprecated
    @Override
    public WindowStoreIterator<V> fetch(final K key, final long timeFrom, final long timeTo) {
        removeExpiredSegments();
        final List<KeyValue<Long, V>> records = retainDuplicates ? fetchWithDuplicates(key, timeFrom, timeTo) : fetchUnique(key, timeFrom, timeTo);

        return new InMemoryWindowStoreIterator<>(records.listIterator());
    }

    @Deprecated
    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K from, final K to, final long timeFrom, final long timeTo) {
        removeExpiredSegments();
        final List<KeyValue<Windowed<K>, V>> returnSet = new LinkedList<>();

        // add one b/c records expire exactly retentionPeriod ms after created
        final long minTime = Math.max(timeFrom, this.observedStreamTime - this.retentionPeriod + 1);
        final WrappedK<K> keyFrom = new WrappedK<>(from, 0);
        final WrappedK<K> keyTo = new WrappedK<>(to, Integer.MAX_VALUE);

        for (final Map.Entry<Long, NavigableMap<WrappedK<K>, V>> segmentMapEntry : this.segmentMap.subMap(minTime, true, timeTo, true).entrySet()) {
            for (final Map.Entry<WrappedK<K>, V> kvMapEntry : segmentMapEntry.getValue().subMap(keyFrom, true, keyTo, true).entrySet()) {
                final WrappedK<K> wrappedKey = kvMapEntry.getKey();
                returnSet.add(getWindowedKeyValue(wrappedKey.getKey(), segmentMapEntry.getKey(), kvMapEntry.getValue()));
            }
        }
        return new InMemoryWindowedKeyValueIterator<>(returnSet.listIterator());
    }

    @Deprecated
    @Override
    public KeyValueIterator<Windowed<K>, V> fetchAll(final long timeFrom, final long timeTo) {
        removeExpiredSegments();
        final List<KeyValue<Windowed<K>, V>> returnSet = new LinkedList<>();

        // add one b/c records expire exactly retentionPeriod ms after created
        final long minTime = Math.max(timeFrom, this.observedStreamTime - this.retentionPeriod + 1);

        for (final Map.Entry<Long, NavigableMap<WrappedK<K>, V>> segmentMapEntry : this.segmentMap.subMap(minTime, true, timeTo, true).entrySet()) {
            for (final Map.Entry<WrappedK<K>, V> kvMapEntry : segmentMapEntry.getValue().entrySet()) {
                final WrappedK<K> wrappedKey = kvMapEntry.getKey();
                returnSet.add(getWindowedKeyValue(wrappedKey.getKey(), segmentMapEntry.getKey(), kvMapEntry.getValue()));
            }
        }
        return new InMemoryWindowedKeyValueIterator<>(returnSet.listIterator());
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> all() {
        removeExpiredSegments();
        final List<KeyValue<Windowed<K>, V>> returnSet = new LinkedList<>();

        for (final Entry<Long, NavigableMap<WrappedK<K>, V>> segmentMapEntry : this.segmentMap.entrySet()) {
            for (final Entry<WrappedK<K>, V> kvMapEntry : segmentMapEntry.getValue().entrySet()) {
                final WrappedK<K> wrappedKey = kvMapEntry.getKey();
                returnSet.add(getWindowedKeyValue(wrappedKey.getKey(), segmentMapEntry.getKey(),
                    kvMapEntry.getValue()));
            }
        }
        return new InMemoryWindowedKeyValueIterator<>(returnSet.listIterator());
    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return this.open;
    }

    @Override
    public void flush() {
        // do-nothing since it is in-memory
    }

    @Override
    public void close() {
        this.segmentMap.clear();
        this.open = false;
    }

    private List<KeyValue<Long, V>> fetchUnique(final K key, final long timeFrom, final long timeTo) {
        final List<KeyValue<Long, V>> returnSet = new LinkedList<>();

        // add one b/c records expire exactly retentionPeriod ms after created
        final long minTime = Math.max(timeFrom, this.observedStreamTime - this.retentionPeriod + 1);

        for (final Map.Entry<Long, NavigableMap<WrappedK<K>, V>> segmentMapEntry : this.segmentMap.subMap(minTime, true, timeTo, true).entrySet()) {
            final V value = segmentMapEntry.getValue().get(new WrappedK<>(key, seqnum));
            if (value != null) {
                returnSet.add(new KeyValue<>(segmentMapEntry.getKey(), value));
            }
        }
        return returnSet;
    }

    private List<KeyValue<Long, V>> fetchWithDuplicates(final K key, final long timeFrom, final long timeTo) {
        final List<KeyValue<Long, V>> returnSet = new LinkedList<>();

        // add one b/c records expire exactly retentionPeriod ms after created
        final long minTime = Math.max(timeFrom, this.observedStreamTime - this.retentionPeriod + 1);
        final WrappedK<K> keyFrom = new WrappedK<>(key, 0);
        final WrappedK<K> keyTo = new WrappedK<>(key, Integer.MAX_VALUE);

        for (final Map.Entry<Long, NavigableMap<WrappedK<K>, V>> segmentMapEntry : this.segmentMap.subMap(minTime, true, timeTo, true).entrySet()) {
            for (final Map.Entry<WrappedK<K>, V> kvMapEntry : segmentMapEntry.getValue().subMap(keyFrom, true, keyTo, true).entrySet()) {
                returnSet.add(new KeyValue<>(segmentMapEntry.getKey(), kvMapEntry.getValue()));
            }
        }
        return returnSet;
    }

    private void removeExpiredSegments() {
        final long minLiveTime = this.observedStreamTime - this.retentionPeriod;
        this.segmentMap.headMap(minLiveTime, true).clear();
    }

    private KeyValue<Windowed<K>, V> getWindowedKeyValue(final K key, final long startTimestamp, final V value) {
        final Windowed<K> windowedK = new Windowed<>(key, new TimeWindow(startTimestamp, startTimestamp + windowSize));
        return new KeyValue<>(windowedK, value);
    }

    private void maybeUpdateSeqnumForDups() {
        if (retainDuplicates) {
            seqnum = (seqnum + 1) & 0x7FFFFFFF;
        }
    }

    private static class WrappedK<K extends Comparable<K>> implements Comparable<WrappedK<K>> {
        private final K key;
        private final int seqnum;

        WrappedK(final K key, final int seqnum) {
            this.key = key;
            this.seqnum = seqnum;
        }

        public K getKey() {
            return this.key;
        }

        public int compareTo(final WrappedK<K> k) {
            final int compareKeys = this.key.compareTo(k.key);
            if (compareKeys == 0) {
                return this.seqnum - k.seqnum;
            } else {
                return compareKeys;
            }
        }
    }

    private static class InMemoryWindowStoreIterator<V> implements WindowStoreIterator<V> {

        private ListIterator<KeyValue<Long, V>> iterator;

        InMemoryWindowStoreIterator(final ListIterator<KeyValue<Long, V>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public KeyValue<Long, V> next() {
            return iterator.next();
        }

        @Override
        public Long peekNextKey() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            } else {
                final long next = iterator.next().key;
                iterator.previous();
                return next;
            }
        }

        @Override
        public void close() {
            iterator = null;
        }
    }

    private static class InMemoryWindowedKeyValueIterator<K, V> implements KeyValueIterator<Windowed<K>, V> {

        ListIterator<KeyValue<Windowed<K>, V>> iterator;

        InMemoryWindowedKeyValueIterator(final ListIterator<KeyValue<Windowed<K>, V>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public KeyValue<Windowed<K>, V> next() {
            return iterator.next();
        }

        @Override
        public Windowed<K> peekNextKey() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            } else {
                final Windowed<K> next = iterator.next().key;
                iterator.previous();
                return next;
            }
        }

        @Override
        public void close() {
            iterator = null;
        }
    }
}



