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
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.ChangelogRecordDeserializationHelper;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.streams.StreamsConfig.InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED;

/**
 * An in-memory LRU cache store based on HashSet and HashMap.
 */
public class MemoryLRUCache implements KeyValueStore<Bytes, byte[]> {

    protected StateStoreContext context;
    private Position position = Position.emptyPosition();

    private static final Logger log = LoggerFactory.getLogger(MemoryLRUCache.class);

    public interface EldestEntryRemovalListener {
        void apply(Bytes key, byte[] value);
    }

    private final String name;
    protected final Map<Bytes, byte[]> map;

    private boolean restoring = false; // TODO: this is a sub-optimal solution to avoid logging during restoration.
                                       // in the future we should augment the StateRestoreCallback with onComplete etc to better resolve this.
    private volatile boolean open = true;

    private EldestEntryRemovalListener listener;

    MemoryLRUCache(final String name, final int maxCacheSize) {
        this.name = name;

        // leave room for one extra entry to handle adding an entry before the oldest can be removed
        this.map = new LinkedHashMap<Bytes, byte[]>(maxCacheSize + 1, 1.01f, true) {
            private static final long serialVersionUID = 1L;

            @Override
            protected boolean removeEldestEntry(final Map.Entry<Bytes, byte[]> eldest) {
                log.error("SOPHIE: evicting eldest entry=" + eldest);
                System.out.println("SOPHIE: evicting eldest entry=" + eldest);
                final boolean evict = super.size() > maxCacheSize;
                if (evict && !restoring && listener != null) {
                    listener.apply(eldest.getKey(), eldest.getValue());
                }
                return evict;
            }
        };
    }

    void setWhenEldestRemoved(final EldestEntryRemovalListener listener) {
        log.error("SOPHIE: setting 'whenEldestRemoved' to {}", listener);
        System.out.println("SOPHIE: setting 'whenEldestRemoved' to " + listener);
        this.listener = listener;
    }

    @Override
    public String name() {
        log.error("SOPHIE: calling name={}", name);
        System.out.println("SOPHIE: calling name=" + name);
        return this.name;
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        log.error("SOPHIE: initializing the store with old processor context");
        System.out.println("SOPHIE: initializing the store with old processor context");

        // register the store
        context.register(root, (key, value) -> {
            restoring = true;
            put(Bytes.wrap(key), value);
            restoring = false;
        });
    }

    @Override
    public void init(final StateStoreContext context, final StateStore root) {
        log.error("SOPHIE: initializing the store with new statestore context");
        System.out.println("SOPHIE: initializing the store with new statestore context");
        final boolean consistencyEnabled = StreamsConfig.InternalConfig.getBoolean(
            context.appConfigs(),
            IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED,
            false
        );
        // register the store
        context.register(
            root,
            (RecordBatchingStateRestoreCallback) records -> {
                restoring = true;
                for (final ConsumerRecord<byte[], byte[]> record : records) {
                    put(Bytes.wrap(record.key()), record.value());
                    ChangelogRecordDeserializationHelper.applyChecksAndUpdatePosition(
                        record,
                        consistencyEnabled,
                        position
                    );
                }
                restoring = false;
            }
        );
        this.context = context;
    }

    @Override
    public boolean persistent() {
        log.error("SOPHIE: calling persistent()");
        System.out.println("SOPHIE: calling persistent()");
        return false;
    }

    @Override
    public boolean isOpen() {
        log.error("SOPHIE: calling isOpen()");
        System.out.println("SOPHIE: calling isOpen()");
        return open;
    }

    @Override
    public Position getPosition() {
        log.error("SOPHIE: calling getPosition()");
        System.out.println("SOPHIE: calling getPosition()");
        return position;
    }

    @Override
    public synchronized byte[] get(final Bytes key) {
        log.error("SOPHIE: calling get on key={}", key);
        System.out.println("SOPHIE: calling get on key=" + key);
        Objects.requireNonNull(key);

        return this.map.get(key);
    }

    @Override
    public synchronized void put(final Bytes key, final byte[] value) {
        final boolean nullVal = value == null;
        log.error("SOPHIE: calling put on key={}, value bytes length={}", key, nullVal ? null : value.length);
        System.out.println("SOPHIE: calling put on key=" + key + ", value bytes null?=" + nullVal);


        Objects.requireNonNull(key);
        if (value == null) {
            log.error("SOPHIE: going to delete key={}, value={}", key, value);
            System.out.println("SOPHIE: going to delete key=" + key + ", value bytes null?=" + nullVal);
            delete(key);
        } else {
            log.error("SOPHIE: going to put key={}, value={}", key, value);
            System.out.println("SOPHIE: going to put key=" + key + ", value bytes null?=" + nullVal);

            this.map.put(key, value);
        }
        StoreQueryUtils.updatePosition(position, context);
        log.error("SOPHIE: successfully put key={}, value={}", key, value);
    }

    @Override
    public synchronized byte[] putIfAbsent(final Bytes key, final byte[] value) {
        final boolean nullVal = value == null;
        log.error("SOPHIE: calling putIfAbsent on key={}, value bytes length={}", key, nullVal ? null : value.length);
        System.out.println("SOPHIE: calling putIfAbsent on key=" + key + ", value bytes null?=" + nullVal);

        Objects.requireNonNull(key);
        final byte[] originalValue = get(key);
        if (originalValue == null) {
            put(key, value);
        }
        return originalValue;
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        log.error("SOPHIE: calling putAll with entries={}", entries);
        System.out.println("SOPHIE: calling putAll with entries=" + entries);
        for (final KeyValue<Bytes, byte[]> entry : entries) {
            put(entry.key, entry.value);
        }
    }

    @Override
    public synchronized byte[] delete(final Bytes key) {
        log.error("SOPHIE: attempting to delete key={}", key);
       System.out.println("SOPHIE: attempting to delete key={}" + key);

        Objects.requireNonNull(key);
        StoreQueryUtils.updatePosition(position, context);
        final byte[] ret = this.map.remove(key);
        log.error("SOPHIE: success @ delete key={} with return value={}", key, ret);
        System.out.println("SOPHIE: success @ delete key=" + key + "with return value=" + ret);

        return ret;
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
        log.error("SOPHIE: approximateNumEntries=" + map.size());
        System.out.println("SOPHIE: approximateNumEntries=" + map.size());
        return this.map.size();
    }

    @Override
    public void flush() {
        // do-nothing since it is in-memory
        log.error("SOPHIE: flushing for some reason, size=" + map.size());
        System.out.println("SOPHIE: flushing for some reason, size=" + map.size());
    }

    @Override
    public void close() {
        log.error("SOPHIE: calling close on store, current contents={}", map);
        System.out.println("SOPHIE: calling close on store, current contents=" + map);
        open = false;
    }

    public int size() {
        log.error("SOPHIE: size=" + map.size());
        System.out.println("SOPHIE: size=" + map.size());
        return this.map.size();
    }
}
