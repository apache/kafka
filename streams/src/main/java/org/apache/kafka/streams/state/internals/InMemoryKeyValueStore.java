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
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
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
                            put(Bytes.wrap(record.key()), record.value());
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
        return position;
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
    public synchronized byte[] get(final Bytes key) {
        return map.get(key);
    }

    @Override
    public synchronized void put(final Bytes key, final byte[] value) {
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
        for (final KeyValue<Bytes, byte[]> entry : entries) {
            putInternal(entry.key, entry.value);
        }
    }

    @Override
    public synchronized <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefixScan(final P prefix, final PS prefixKeySerializer) {

        final Bytes from = Bytes.wrap(prefixKeySerializer.serialize(null, prefix));
        final Bytes to = Bytes.increment(from);

        return new InMemoryKeyValueIterator(map.subMap(from, true, to, false).keySet(), true);
    }

    @Override
    public synchronized byte[] delete(final Bytes key) {
        return map.remove(key);
    }

    @Override
    public synchronized KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
        return range(from, to, true);
    }

    @Override
    public synchronized KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from, final Bytes to) {
        return range(from, to, false);
    }

    private KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to, final boolean forward) {
        if (from == null && to == null) {
            return getKeyValueIterator(map.keySet(), forward);
        } else if (from == null) {
            return getKeyValueIterator(map.headMap(to, true).keySet(), forward);
        } else if (to == null) {
            return getKeyValueIterator(map.tailMap(from, true).keySet(), forward);
        } else if (from.compareTo(to) > 0) {
            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. " +
                    "This may be due to range arguments set in the wrong order, " +
                    "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                    "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        } else {
            return getKeyValueIterator(map.subMap(from, true, to, true).keySet(), forward);
        }
    }

    private KeyValueIterator<Bytes, byte[]> getKeyValueIterator(final Set<Bytes> rangeSet, final boolean forward) {
        return new InMemoryKeyValueIterator(rangeSet, forward);
    }

    @Override
    public synchronized KeyValueIterator<Bytes, byte[]> all() {
        return range(null, null);
    }

    @Override
    public synchronized KeyValueIterator<Bytes, byte[]> reverseAll() {
        return new InMemoryKeyValueIterator(map.keySet(), false);
    }

    @Override
    public long approximateNumEntries() {
        return map.size();
    }

    @Override
    public void flush() {
        // do-nothing since it is in-memory
    }

    @Override
    public void close() {
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
