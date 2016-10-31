/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SimpleTimeZone;
import java.util.concurrent.ConcurrentHashMap;

public class RocksDBWindowStore<K, V> implements WindowStore<K, V> {

    public static final long MIN_SEGMENT_INTERVAL = 60 * 1000; // one minute

    private volatile boolean open = false;

    // use the Bytes wrapper for underlying rocksDB keys since they are used for hashing data structures
    private static class Segment extends RocksDBStore<Bytes, byte[]> {
        public final long id;

        Segment(String segmentName, String windowName, long id) {
            super(segmentName, windowName, WindowStoreUtils.INNER_KEY_SERDE, WindowStoreUtils.INNER_VALUE_SERDE);
            this.id = id;
        }

        public void destroy() {
            Utils.delete(dbDir);
        }

        @Override
        public void openDB(final ProcessorContext context) {
            super.openDB(context);
            open = true;
        }
    }

    private static class RocksDBWindowStoreIterator<V> implements WindowStoreIterator<V> {
        private final StateSerdes<?, V> serdes;
        private final Iterator<Segment> segments;
        private final Bytes from;
        private final Bytes to;
        private KeyValueIterator<Bytes, byte[]> currentIterator;
        private KeyValueStore<Bytes, byte[]> currentSegment;

        RocksDBWindowStoreIterator(StateSerdes<?, V> serdes) {
            this(serdes, null, null, Collections.<Segment>emptyIterator());
        }

        RocksDBWindowStoreIterator(StateSerdes<?, V> serdes, Bytes from, Bytes to, Iterator<Segment> segments) {
            this.serdes = serdes;
            this.from = from;
            this.to = to;
            this.segments = segments;
        }

        @Override
        public boolean hasNext() {
            while ((currentIterator == null || !currentIterator.hasNext() || !currentSegment.isOpen())
                    && segments.hasNext()) {
                close();
                currentSegment = segments.next();
                try {
                    currentIterator = currentSegment.range(from, to);
                } catch (InvalidStateStoreException e) {
                    // segment may have been closed so we ignore it.
                }
            }
            return currentIterator != null && currentIterator.hasNext();
        }

        /**
         * @throws NoSuchElementException if no next element exists
         */
        @Override
        public KeyValue<Long, V> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            KeyValue<Bytes, byte[]> kv = currentIterator.next();
            return new KeyValue<>(WindowStoreUtils.timestampFromBinaryKey(kv.key.get()),
                    serdes.valueFrom(kv.value));
        }

        @Override
        public void remove() {

        }

        @Override
        public void close() {
            if (currentIterator != null) {
                currentIterator.close();
                currentIterator = null;
            }
        }
    }

    private final String name;
    private final int numSegments;
    private final long segmentInterval;
    private final boolean retainDuplicates;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final SimpleDateFormat formatter;
    private final StoreChangeLogger.ValueGetter<Bytes, byte[]> getter;
    private final ConcurrentHashMap<Long, Segment> segments = new ConcurrentHashMap<>();

    private ProcessorContext context;
    private int seqnum = 0;
    private long currentSegmentId = -1L;

    private StateSerdes<K, V> serdes;

    private boolean loggingEnabled = false;
    private StoreChangeLogger<Bytes, byte[]> changeLogger = null;

    public RocksDBWindowStore(String name, long retentionPeriod, int numSegments, boolean retainDuplicates, Serde<K> keySerde, Serde<V> valueSerde) {
        this.name = name;
        this.numSegments = numSegments;

        // The segment interval must be greater than MIN_SEGMENT_INTERVAL
        this.segmentInterval = Math.max(retentionPeriod / (numSegments - 1), MIN_SEGMENT_INTERVAL);

        this.keySerde = keySerde;
        this.valueSerde = valueSerde;

        this.retainDuplicates = retainDuplicates;

        this.getter = new StoreChangeLogger.ValueGetter<Bytes, byte[]>() {
            public byte[] get(Bytes key) {
                return getInternal(key.get());
            }
        };

        // Create a date formatter. Formatted timestamps are used as segment name suffixes
        this.formatter = new SimpleDateFormat("yyyyMMddHHmm");
        this.formatter.setTimeZone(new SimpleTimeZone(0, "GMT"));
    }

    public RocksDBWindowStore<K, V> enableLogging() {
        loggingEnabled = true;
        return this;
    }


    @Override
    public String name() {
        return name;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context, StateStore root) {
        this.context = context;

        // construct the serde
        this.serdes = new StateSerdes<>(name,
                keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
                valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);

        openExistingSegments();

        this.changeLogger = this.loggingEnabled ? new StoreChangeLogger(name, context, WindowStoreUtils.INNER_SERDES) : null;

        // register and possibly restore the state from the logs
        context.register(root, loggingEnabled, new StateRestoreCallback() {
            @Override
            public void restore(byte[] key, byte[] value) {
                // if the value is null, it means that this record has already been
                // deleted while it was captured in the changelog, hence we do not need to put any more.
                if (value != null)
                    putInternal(key, value);
            }
        });

        flush();
        open = true;
    }

    private void openExistingSegments() {
        try {
            File dir = new File(context.stateDir(), name);

            if (dir.exists()) {
                String[] list = dir.list();
                if (list != null) {
                    long[] segmentIds = new long[list.length];
                    for (int i = 0; i < list.length; i++)
                        segmentIds[i] = segmentIdFromSegmentName(list[i]);

                    // open segments in the id order
                    Arrays.sort(segmentIds);
                    for (long segmentId : segmentIds) {
                        if (segmentId >= 0) {
                            currentSegmentId = segmentId;
                            getOrCreateSegment(segmentId);
                        }
                    }
                }
            } else {
                dir.mkdir();
            }
        } catch (Exception ex) {
            // ignore
        }
    }

    @Override
    public boolean persistent() {
        return true;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public void flush() {
        for (KeyValueStore<Bytes, byte[]> segment : segments.values()) {
            if (segment != null) {
                segment.flush();
            }
        }

        if (loggingEnabled)
            changeLogger.logChange(this.getter);
    }

    @Override
    public void close() {
        open = false;
        flush();
        for (KeyValueStore segment : segments.values()) {
            if (segment != null)
                segment.close();
        }
    }

    @Override
    public void put(K key, V value) {
        byte[] rawKey = putAndReturnInternalKey(key, value, context.timestamp());

        if (rawKey != null && loggingEnabled) {
            changeLogger.add(Bytes.wrap(rawKey));
            changeLogger.maybeLogChange(this.getter);
        }
    }

    @Override
    public void put(K key, V value, long timestamp) {
        byte[] rawKey = putAndReturnInternalKey(key, value, timestamp);

        if (rawKey != null && loggingEnabled) {
            changeLogger.add(Bytes.wrap(rawKey));
            changeLogger.maybeLogChange(this.getter);
        }
    }

    private byte[] putAndReturnInternalKey(K key, V value, long timestamp) {
        long segmentId = segmentId(timestamp);

        if (segmentId > currentSegmentId) {
            // A new segment will be created. Clean up old segments first.
            currentSegmentId = segmentId;
            cleanup();
        }

        // If the record is within the retention period, put it in the store.
        KeyValueStore<Bytes, byte[]> segment = getOrCreateSegment(segmentId);
        if (segment != null) {
            if (retainDuplicates)
                seqnum = (seqnum + 1) & 0x7FFFFFFF;
            byte[] binaryKey = WindowStoreUtils.toBinaryKey(key, timestamp, seqnum, serdes);
            segment.put(Bytes.wrap(binaryKey), serdes.rawValue(value));
            return binaryKey;
        } else {
            return null;
        }
    }

    private void putInternal(byte[] binaryKey, byte[] binaryValue) {
        final long timestamp = WindowStoreUtils.timestampFromBinaryKey(binaryKey);
        long segmentId = segmentId(timestamp);

        if (segmentId > currentSegmentId) {
            // A new segment will be created. Clean up old segments first.
            currentSegmentId = segmentId;
            cleanup();
        }

        // If the record is within the retention period, put it in the store.
        Segment segment = getOrCreateSegment(segmentId);
        if (segment != null) {
            segment.writeToStore(Bytes.wrap(binaryKey), binaryValue);
        }
    }

    private byte[] getInternal(byte[] binaryKey) {
        long segmentId = segmentId(WindowStoreUtils.timestampFromBinaryKey(binaryKey));

        KeyValueStore<Bytes, byte[]> segment = getSegment(segmentId);
        if (segment != null) {
            return segment.get(Bytes.wrap(binaryKey));
        } else {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public WindowStoreIterator<V> fetch(K key, long timeFrom, long timeTo) {
        if (!isOpen()) {
            throw new InvalidStateStoreException("Store " + this.name + " is currently not isOpen");
        }
        long segFrom = segmentId(timeFrom);
        long segTo = segmentId(Math.max(0L, timeTo));

        byte[] binaryFrom = WindowStoreUtils.toBinaryKey(key, timeFrom, 0, serdes);
        byte[] binaryTo = WindowStoreUtils.toBinaryKey(key, timeTo, Integer.MAX_VALUE, serdes);

        final List<Segment> segments = new ArrayList<>();
        for (long segmentId = segFrom; segmentId <= segTo; segmentId++) {
            Segment segment = getSegment(segmentId);
            if (segment != null && segment.isOpen()) {
                try {
                    segments.add(segment);
                } catch (InvalidStateStoreException ise) {
                    // segment may have been closed by streams thread;
                }
            }
        }

        if (!segments.isEmpty()) {
            return new RocksDBWindowStoreIterator<>(serdes, Bytes.wrap(binaryFrom), Bytes.wrap(binaryTo), segments.iterator());
        } else {
            return new RocksDBWindowStoreIterator<>(serdes);
        }
    }

    private Segment getSegment(long segmentId) {
        final Segment segment = segments.get(segmentId % numSegments);
        if (!isSegment(segment, segmentId)) {
            return null;
        }
        return segment;
    }

    private boolean isSegment(final Segment store, long segmentId) {
        return store != null && store.id == segmentId;
    }

    private Segment getOrCreateSegment(long segmentId) {
        if (segmentId <= currentSegmentId && segmentId > currentSegmentId - numSegments) {
            final long key = segmentId % numSegments;
            final Segment segment = segments.get(key);
            if (!isSegment(segment, segmentId)) {
                cleanup();
            }
            if (!segments.containsKey(key)) {
                Segment newSegment = new Segment(segmentName(segmentId), name, segmentId);
                newSegment.openDB(context);
                segments.put(key, newSegment);
            }
            return segments.get(key);

        } else {
            return null;
        }
    }

    private void cleanup() {
        for (Map.Entry<Long, Segment> segmentEntry : segments.entrySet()) {
            final Segment segment = segmentEntry.getValue();
            if (segment != null && segment.id <= currentSegmentId - numSegments) {
                segments.remove(segmentEntry.getKey());
                segment.close();
                segment.destroy();
            }
        }
    }

    private long segmentId(long timestamp) {
        return timestamp / segmentInterval;
    }

    // this method is defined public since it is used for unit tests
    public String segmentName(long segmentId) {
        return name + "-" + formatter.format(new Date(segmentId * segmentInterval));
    }

    public long segmentIdFromSegmentName(String segmentName) {
        try {
            Date date = formatter.parse(segmentName.substring(name.length() + 1));
            return date.getTime() / segmentInterval;
        } catch (Exception ex) {
            return -1L;
        }
    }

    // this method is defined public since it is used for unit tests
    public Set<Long> segmentIds() {
        HashSet<Long> segmentIds = new HashSet<>();

        for (Segment segment : segments.values()) {
            if (segment != null) {
                segmentIds.add(segment.id);
            }
        }

        return segmentIds;
    }

}
