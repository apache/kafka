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
package org.apache.kafka.test;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MockKeyValueStore implements KeyValueStore<Object, Object> {
    // keep a global counter of flushes and a local reference to which store had which
    // flush, so we can reason about the order in which stores get flushed.
    private static final AtomicInteger GLOBAL_FLUSH_COUNTER = new AtomicInteger(0);
    private final AtomicInteger instanceLastFlushCount = new AtomicInteger(-1);
    private final String name;
    private final boolean persistent;

    public boolean initialized = false;
    public boolean flushed = false;
    public boolean closed = true;
    public final ArrayList<Integer> keys = new ArrayList<>();
    public final ArrayList<byte[]> values = new ArrayList<>();

    public MockKeyValueStore(final String name,
                             final boolean persistent) {
        this.name = name;
        this.persistent = persistent;
    }

    @Override
    public String name() {
        return name;
    }

    @Deprecated
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
        flushed = true;
    }

    public int getLastFlushCount() {
        return instanceLastFlushCount.get();
    }

    @Override
    public void close() {
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

    @Override
    public Position getPosition() {
        throw new UnsupportedOperationException("Position handling not implemented");
    }

    public final StateRestoreCallback stateRestoreCallback = new StateRestoreCallback() {
        private final Deserializer<Integer> deserializer = new IntegerDeserializer();

        @Override
        public void restore(final byte[] key,
                            final byte[] value) {
            keys.add(deserializer.deserialize("", key));
            values.add(value);
        }
    };

    @Override
    public void put(final Object key, final Object value) {}

    @Override
    public Object putIfAbsent(final Object key, final Object value) {
        return null;
    }

    @Override
    public Object delete(final Object key) {
        return null;
    }

    @Override
    public void putAll(final List<KeyValue<Object, Object>> entries) {}

    @Override
    public Object get(final Object key) {
        return null;
    }

    @Override
    public KeyValueIterator<Object, Object> range(final Object from, final Object to) {
        return null;
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<Object, Object> prefixScan(P prefix, PS prefixKeySerializer) {
        return null;
    }

    @Override
    public KeyValueIterator<Object, Object> all() {
        return null;
    }

    @Override
    public long approximateNumEntries() {
        return 0;
    }
}
