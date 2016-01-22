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

import org.apache.kafka.streams.kstream.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.Entry;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.Serdes;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.WindowStoreUtil;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SimpleTimeZone;

public class RocksDBWindowStore<K, V> implements WindowStore<K, V> {

    public static final long MIN_SEGMENT_INTERVAL = 60 * 1000; // one minute

    private static final long USE_CURRENT_TIMESTAMP = -1L;

    private static class RocksDBWindowStoreIterator<V> implements WindowStoreIterator<V> {
        private final Serdes<?, V> serdes;
        private final KeyValueIterator<byte[], byte[]>[] iterators;
        private int index = 0;

        RocksDBWindowStoreIterator(Serdes<?, V> serdes) {
            this(serdes, WindowStoreUtil.NO_ITERATORS);
        }

        RocksDBWindowStoreIterator(Serdes<?, V> serdes, KeyValueIterator<byte[], byte[]>[] iterators) {
            this.serdes = serdes;
            this.iterators = iterators;
        }

        @Override
        public boolean hasNext() {
            while (index < iterators.length) {
                if (iterators[index].hasNext())
                    return true;

                index++;
            }
            return false;
        }

        @Override
        public KeyValue<Long, V> next() {
            if (index >= iterators.length)
                throw new NoSuchElementException();

            Entry<byte[], byte[]> entry = iterators[index].next();

            return new KeyValue<>(WindowStoreUtil.timestampFromBinaryKey(entry.key()),
                                  serdes.valueFrom(entry.value()));
        }

        @Override
        public void remove() {
            if (index < iterators.length)
                iterators[index].remove();
        }

        @Override
        public void close() {
            for (KeyValueIterator<byte[], byte[]> iterator : iterators) {
                iterator.close();
            }
        }
    }

    private final String name;
    private final Serdes<K, V> serdes;
    private final boolean retainDuplicates;

    private final SimpleDateFormat formatter;
    private final RollingRocksDBStore rollingDB;

    private ProcessorContext context;
    private int seqnum = 0;

    public RocksDBWindowStore(String name, long retentionPeriod, int numSegments, boolean retainDuplicates, Serdes<K, V> serdes) {
        this.name = name;
        this.serdes = serdes;
        this.retainDuplicates = retainDuplicates;
        this.rollingDB = new RollingRocksDBStore(name, retentionPeriod, numSegments);

        // Create a date formatter. Formatted timestamps are used as segment name suffixes
        this.formatter = new SimpleDateFormat("yyyyMMddHHmm");
        this.formatter.setTimeZone(new SimpleTimeZone(0, "GMT"));
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        rollingDB.init(context);
    }

    @Override
    public boolean persistent() {
        return true;
    }

    @Override
    public void flush() {
        rollingDB.flush();
    }

    @Override
    public void close() {
        rollingDB.flush();
    }

    @Override
    public void put(K key, V value) {
        put(key, value, USE_CURRENT_TIMESTAMP);
    }

    @Override
    public void put(K key, V value, long t) {
        // determine timestamp to use
        long timestamp = t == USE_CURRENT_TIMESTAMP ? context.timestamp() : t;

        // serialize the key and value
        if (retainDuplicates)
            seqnum = (seqnum + 1) & 0x7FFFFFFF;

        byte[] binaryKey = WindowStoreUtil.toBinaryKey(key, timestamp, seqnum, serdes);
        byte[] binaryValue = serdes.rawValue(value);

        rollingDB.putAndReturnKey(binaryKey, binaryValue, timestamp);

        if (loggingEnabled) {
            changeLogger.add(binKey);
            changeLogger.maybeLogChange(this.getter);
        }

    }

    @SuppressWarnings("unchecked")
    @Override
    public WindowStoreIterator<V> fetch(K key, long timeFrom, long timeTo) {
        byte[] binaryFrom = WindowStoreUtil.toBinaryKey(key, timeFrom, 0, serdes);
        byte[] binaryTo = WindowStoreUtil.toBinaryKey(key, timeTo + 1L, 0, serdes);

        ArrayList<KeyValueIterator<byte[], byte[]>> iters = rollingDB.iterators(timeFrom, timeTo, binaryFrom, binaryTo);

        if (iters.size() > 0) {
            return new RocksDBWindowStoreIterator<>(serdes, iters.toArray(new KeyValueIterator[iters.size()]));
        } else {
            return new RocksDBWindowStoreIterator<>(serdes);
        }
    }

    // this method is public since it is used by test
    public String directorySuffix(long segmentId) {
        return rollingDB.directorySuffix(segmentId);
    }

    // this method is public since it is used by test
    public Set<Long> segmentIds() {
        return rollingDB.segmentIds();
    }

}
