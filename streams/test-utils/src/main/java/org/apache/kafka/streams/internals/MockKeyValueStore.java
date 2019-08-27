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
package org.apache.kafka.streams.internals;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStore;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MockKeyValueStore<K, V> implements KeyValueStore {
    // keep a global counter of flushes and a local reference to which store had which
    // flush, so we can reason about the order in which stores get flushed.
    private static final AtomicInteger GLOBAL_FLUSH_COUNTER = new AtomicInteger(0);
    private final AtomicInteger instanceLastFlushCount = new AtomicInteger(-1);
    private final String name;
    private final boolean persistent;

    public boolean initialized = false;
    public boolean flushed = false;
    public boolean closed = true;

    public final List<KeyValue> capturedPutCalls = new LinkedList<>();
    public final List<KeyValue> capturedGetCalls = new LinkedList<>();
    public final List<KeyValue> capturedDeleteCalls = new LinkedList<>();

    public final KeyValueStore innerKeyValueStore;

    public MockKeyValueStore(final String name,
                             final boolean persistent) {
        this.name = name;
        this.persistent = persistent;
        innerKeyValueStore = new InMemoryKeyValueStore(name);
    }

    public MockKeyValueStore(final String name,
                             final KeyValueStore innerKeyValueStore,
                             final boolean persistent) {
        this.name = name;
        this.innerKeyValueStore = innerKeyValueStore;
        this.persistent = persistent;

    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        context.register(root, stateRestoreCallback);
        initialized = true;
        closed = false;
    }

    @Override
    public void flush() {
        instanceLastFlushCount.set(GLOBAL_FLUSH_COUNTER.getAndIncrement());
        innerKeyValueStore.flush();
        flushed = true;
    }

    public int getLastFlushCount() {
        return instanceLastFlushCount.get();
    }

    @Override
    public void close() {
        innerKeyValueStore.close();
        closed = true;
    }

    @Override
    public boolean persistent() {
        return persistent;
    }

    @Override
    public boolean isOpen() {
        return !closed;
    }

    public final StateRestoreCallback stateRestoreCallback = new StateRestoreCallback() {
        private final Deserializer<Integer> deserializer = new IntegerDeserializer();

        @Override
        public void restore(final byte[] key,
                            final byte[] value) {
            innerKeyValueStore.put(deserializer.deserialize("", key), value);
        }
    };

    @Override
    public void put(final Object key, final Object value) {
        capturedPutCalls.add(new KeyValue(key, value));
        innerKeyValueStore.put(key, value);
    }

    @Override
    public Object putIfAbsent(final Object key, final Object value) {
        final byte[] originalValue = (byte[])get(key);
        if (originalValue == null) {
            put(key, value);
            capturedPutCalls.add(new KeyValue(key, value));
        }
        return originalValue;
    }

    @Override
    public Object delete(final Object key) {
        Object value = innerKeyValueStore.delete(key);
        capturedDeleteCalls.add(new KeyValue(key, value));
        return value;
    }

    @Override
    public void putAll(final List entries) {
        for (final KeyValue<Bytes, byte[]> entry : (List<KeyValue<Bytes, byte[]>>) entries) {
            put(entry.key, entry.value);
            capturedPutCalls.add(entry);
        }
    }

    @Override
    public Object get(final Object key) {
        byte[] value = (byte[])innerKeyValueStore.get(key);
        capturedGetCalls.add(new KeyValue(key, value));
        return value;
    }

    @Override
    public KeyValueIterator range(final Object from, final Object to) {
        return innerKeyValueStore.range(from, to);
    }

    @Override
    public KeyValueIterator all() {
        return innerKeyValueStore.all();
    }

    @Override
    public long approximateNumEntries() {
        return innerKeyValueStore.approximateNumEntries();
    }
}
