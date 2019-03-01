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

import java.nio.ByteBuffer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
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
import static org.apache.kafka.streams.state.internals.WindowKeySchema.extractStoreKeyBytes;
import static org.apache.kafka.streams.state.internals.WindowKeySchema.extractStoreTimestamp;

public class InMemoryWindowStore implements WindowStore<Bytes, byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryWindowStore.class);
    private static final int SEQNUM_SIZE = 4;

    private final String name;
    private final String metricScope;
    private InternalProcessorContext context;
    private Sensor expiredRecordSensor;
    private int seqnum = 0;
    private long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;

    private final long retentionPeriod;
    private final long windowSize;
    private final boolean retainDuplicates;

    private final NavigableMap<Long, NavigableMap<Bytes, byte[]>> segmentMap;

    private volatile boolean open = false;

    InMemoryWindowStore(final String name,
                               final long retentionPeriod,
                               final long windowSize,
                               final boolean retainDuplicates,
                               final String metricScope) {
        this.name = name;
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
                put(Bytes.wrap(extractStoreKeyBytes(key)), value, extractStoreTimestamp(key));
            });
        }
        this.open = true;
    }

    @Override
    public void put(final Bytes key, final byte[] value) {
        put(key, value, context.timestamp());
    }

    @Override
    public void put(final Bytes key, final byte[] value, final long windowStartTimestamp) {
        removeExpiredSegments();
        maybeUpdateSeqnumForDups();
        this.observedStreamTime = Math.max(this.observedStreamTime, windowStartTimestamp);

        final Bytes keyBytes = retainDuplicates ? wrapForDups(key, seqnum) : key;

        if (windowStartTimestamp <= this.observedStreamTime - this.retentionPeriod) {
            expiredRecordSensor.record();
            LOG.debug("Skipping record for expired segment.");
        } else {
            if (value != null) {
                this.segmentMap.computeIfAbsent(windowStartTimestamp, t -> new TreeMap<>());
                this.segmentMap.get(windowStartTimestamp).put(keyBytes, value);
            } else {
                this.segmentMap.computeIfPresent(windowStartTimestamp, (t, kvMap) -> {
                    kvMap.remove(keyBytes);
                    return kvMap;
                });
            }
        }
    }

    @Override
    public byte[] fetch(final Bytes key, final long windowStartTimestamp) {
        removeExpiredSegments();

        final NavigableMap<Bytes, byte[]> kvMap = this.segmentMap.get(windowStartTimestamp);
        if (kvMap == null) {
            return null;
        } else {
            return kvMap.get(key);
        }
    }

    @Deprecated
    @Override
    public WindowStoreIterator<byte[]> fetch(final Bytes key, final long timeFrom, final long timeTo) {
        removeExpiredSegments();
        final List<KeyValue<Long, byte[]>> records = retainDuplicates ? fetchWithDuplicates(key, timeFrom, timeTo) : fetchUnique(key, timeFrom, timeTo);

        return new InMemoryWindowStoreIterator(records.listIterator());
    }

    @Deprecated
    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetch(final Bytes from,
                                                           final Bytes to,
                                                           final long timeFrom,
                                                           final long timeTo) {
        removeExpiredSegments();
        final List<KeyValue<Windowed<Bytes>, byte[]>> returnSet = new LinkedList<>();

        // add one b/c records expire exactly retentionPeriod ms after created
        final long minTime = Math.max(timeFrom, this.observedStreamTime - this.retentionPeriod + 1);
        final Bytes keyFrom = retainDuplicates ? wrapForDups(from, 0) : from;
        final Bytes keyTo = retainDuplicates ? wrapForDups(to, Integer.MAX_VALUE) : to;

        for (final Map.Entry<Long, NavigableMap<Bytes, byte[]>> segmentMapEntry : this.segmentMap.subMap(minTime, true, timeTo, true).entrySet()) {
            for (final Map.Entry<Bytes, byte[]> kvMapEntry : segmentMapEntry.getValue().subMap(keyFrom, true, keyTo, true).entrySet()) {
                final Bytes keyBytes = retainDuplicates ? getKey(kvMapEntry.getKey()) : kvMapEntry.getKey();
                returnSet.add(getWindowedKeyValue(keyBytes, segmentMapEntry.getKey(), kvMapEntry.getValue()));
            }
        }
        return new InMemoryWindowedKeyValueIterator(returnSet.listIterator());
    }

    @Deprecated
    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> fetchAll(final long timeFrom, final long timeTo) {
        removeExpiredSegments();
        final List<KeyValue<Windowed<Bytes>, byte[]>> returnSet = new LinkedList<>();

        // add one b/c records expire exactly retentionPeriod ms after created
        final long minTime = Math.max(timeFrom, this.observedStreamTime - this.retentionPeriod + 1);

        for (final Map.Entry<Long, NavigableMap<Bytes, byte[]>> segmentMapEntry : this.segmentMap.subMap(minTime, true, timeTo, true).entrySet()) {
            for (final Map.Entry<Bytes,  byte[]> kvMapEntry : segmentMapEntry.getValue().entrySet()) {
                final Bytes keyBytes = retainDuplicates ? getKey(kvMapEntry.getKey()) : kvMapEntry.getKey();
                returnSet.add(getWindowedKeyValue(keyBytes, segmentMapEntry.getKey(), kvMapEntry.getValue()));
            }
        }
        return new InMemoryWindowedKeyValueIterator(returnSet.listIterator());
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, byte[]> all() {
        removeExpiredSegments();
        final List<KeyValue<Windowed<Bytes>, byte[]>> returnSet = new LinkedList<>();

        for (final Entry<Long, NavigableMap<Bytes, byte[]>> segmentMapEntry : this.segmentMap.entrySet()) {
            for (final Entry<Bytes, byte[]> kvMapEntry : segmentMapEntry.getValue().entrySet()) {
                final Bytes keyBytes = retainDuplicates ? getKey(kvMapEntry.getKey()) : kvMapEntry.getKey();
                returnSet.add(getWindowedKeyValue(keyBytes, segmentMapEntry.getKey(), kvMapEntry.getValue()));
            }
        }
        return new InMemoryWindowedKeyValueIterator(returnSet.listIterator());
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

    private List<KeyValue<Long, byte[]>> fetchUnique(final Bytes key, final long timeFrom, final long timeTo) {
        final List<KeyValue<Long, byte[]>> returnSet = new LinkedList<>();

        // add one b/c records expire exactly retentionPeriod ms after created
        final long minTime = Math.max(timeFrom, this.observedStreamTime - this.retentionPeriod + 1);

        for (final Map.Entry<Long, NavigableMap<Bytes, byte[]>> segmentMapEntry : this.segmentMap.subMap(minTime, true, timeTo, true).entrySet()) {
            final byte[] value = segmentMapEntry.getValue().get(key);
            if (value != null) {
                returnSet.add(new KeyValue<>(segmentMapEntry.getKey(), value));
            }
        }
        return returnSet;
    }

    private List<KeyValue<Long, byte[]>> fetchWithDuplicates(final Bytes key, final long timeFrom, final long timeTo) {
        final List<KeyValue<Long, byte[]>> returnSet = new LinkedList<>();

        // add one b/c records expire exactly retentionPeriod ms after created
        final long minTime = Math.max(timeFrom, this.observedStreamTime - this.retentionPeriod + 1);
        final Bytes keyFrom = wrapForDups(key, 0);
        final Bytes keyTo = wrapForDups(key, Integer.MAX_VALUE);

        for (final Map.Entry<Long, NavigableMap<Bytes, byte[]>> segmentMapEntry : this.segmentMap.subMap(minTime, true, timeTo, true).entrySet()) {
            for (final Map.Entry<Bytes, byte[]> kvMapEntry : segmentMapEntry.getValue().subMap(keyFrom, true, keyTo, true).entrySet()) {
                returnSet.add(new KeyValue<>(segmentMapEntry.getKey(), kvMapEntry.getValue()));
            }
        }
        return returnSet;
    }

    private void removeExpiredSegments() {
        final long minLiveTime = this.observedStreamTime - this.retentionPeriod;
        this.segmentMap.headMap(minLiveTime, true).clear();
    }

    private KeyValue<Windowed<Bytes>, byte[]> getWindowedKeyValue(final Bytes key,
                                                                  final long startTimestamp,
                                                                  final byte[] value) {
        final Windowed<Bytes> windowedK = new Windowed<>(key, new TimeWindow(startTimestamp, startTimestamp + windowSize));
        return new KeyValue<>(windowedK, value);
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
        final byte[] bytes = new byte[keyBytes.get().length  - SEQNUM_SIZE];
        System.arraycopy(keyBytes.get(), 0, bytes, 0, bytes.length);
        return Bytes.wrap(bytes);

    }

    private static class InMemoryWindowStoreIterator implements WindowStoreIterator<byte[]> {

        private ListIterator<KeyValue<Long, byte[]>> iterator;

        InMemoryWindowStoreIterator(final ListIterator<KeyValue<Long, byte[]>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public KeyValue<Long, byte[]> next() {
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

    private static class InMemoryWindowedKeyValueIterator implements KeyValueIterator<Windowed<Bytes>, byte[]> {

        ListIterator<KeyValue<Windowed<Bytes>, byte[]>> iterator;

        InMemoryWindowedKeyValueIterator(final ListIterator<KeyValue<Windowed<Bytes>, byte[]>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public KeyValue<Windowed<Bytes>, byte[]> next() {
            return iterator.next();
        }

        @Override
        public Windowed<Bytes> peekNextKey() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            } else {
                final Windowed<Bytes> next = iterator.next().key;
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



