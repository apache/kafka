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
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.streams.StreamsConfig.InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED;

/**
 * An in-memory LRU cache store based on HashSet and HashMap.
 */
public class MemoryLRUCache implements KeyValueStore<Bytes, byte[]> {
    private final Position position = Position.emptyPosition();

    public interface EldestEntryRemovalListener {
        void apply(Bytes key, byte[] value);
    }

    private final String name;
    protected final Map<Bytes, byte[]> map;

    private boolean restoring = false; // TODO: this is a sub-optimal solution to avoid logging during restoration.
                                       // in the future we should augment the StateRestoreCallback with onComplete etc to better resolve this.
    private volatile boolean open = true;
    protected StateStoreContext context;

    private EldestEntryRemovalListener listener;

    MemoryLRUCache(final String name, final int maxCacheSize) {
        this.name = name;

        // leave room for one extra entry to handle adding an entry before the oldest can be removed
        this.map = new LinkedHashMap<Bytes, byte[]>(maxCacheSize + 1, 1.01f, true) {
            private static final long serialVersionUID = 1L;

            @Override
            protected boolean removeEldestEntry(final Map.Entry<Bytes, byte[]> eldest) {
                final boolean evict = super.size() > maxCacheSize;
                if (evict && !restoring && listener != null) {
                    listener.apply(eldest.getKey(), eldest.getValue());
                }
                return evict;
            }
        };
    }

    void setWhenEldestRemoved(final EldestEntryRemovalListener listener) {
        this.listener = listener;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public void init(final StateStoreContext stateStoreContext, final StateStore root) {
        final boolean consistencyEnabled = StreamsConfig.InternalConfig.getBoolean(
            stateStoreContext.appConfigs(),
            IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED,
            false
        );
        // register the store
        stateStoreContext.register(
            root,
            (RecordBatchingStateRestoreCallback) records -> {
                restoring = true;
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
                restoring = false;
            }
        );
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
    public synchronized byte[] get(final Bytes key) {
        Objects.requireNonNull(key);

        return this.map.get(key);
    }

    @Override
    public synchronized void put(final Bytes key, final byte[] value) {
        Objects.requireNonNull(key);
        synchronized (position) {
            if (value == null) {
                delete(key);
            } else {
                this.map.put(key, value);
            }
            StoreQueryUtils.updatePosition(position, context);
        }
    }

    @Override
    public synchronized byte[] putIfAbsent(final Bytes key, final byte[] value) {
        Objects.requireNonNull(key);
        final byte[] originalValue = get(key);
        if (originalValue == null) {
            put(key, value);
        }
        return originalValue;
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        for (final KeyValue<Bytes, byte[]> entry : entries) {
            put(entry.key, entry.value);
        }
    }

    @Override
    public synchronized byte[] delete(final Bytes key) {
        Objects.requireNonNull(key);
        synchronized (position) {
            StoreQueryUtils.updatePosition(position, context);
            return this.map.remove(key);
        }
    }

    /**
     * @throws UnsupportedOperationException at every invocation
     */
    @Override
    public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
        throw new UnsupportedOperationException("MemoryLRUCache does not support range() function.");
    }

    /**
     * @throws UnsupportedOperationException at every invocation
     */
    @Override
    public KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from, final Bytes to) {
        throw new UnsupportedOperationException("MemoryLRUCache does not support reverseRange() function.");
    }

    /**
     * @throws UnsupportedOperationException at every invocation
     */
    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        throw new UnsupportedOperationException("MemoryLRUCache does not support all() function.");
    }

    /**
     * @throws UnsupportedOperationException at every invocation
     */
    @Override
    public KeyValueIterator<Bytes, byte[]> reverseAll() {
        throw new UnsupportedOperationException("MemoryLRUCache does not support reverseAll() function.");
    }

    /**
     * @throws UnsupportedOperationException at every invocation
     */
    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefixScan(final P prefix,
                                                                                    final PS prefixKeySerializer) {
        throw new UnsupportedOperationException("MemoryLRUCache does not support prefixScan() function.");
    }

    @Override
    public long approximateNumEntries() {
        return this.map.size();
    }

    @Override
    public void flush() {
        // do-nothing since it is in-memory
    }

    @Override
    public void close() {
        open = false;
    }

    public int size() {
        return this.map.size();
    }
}
