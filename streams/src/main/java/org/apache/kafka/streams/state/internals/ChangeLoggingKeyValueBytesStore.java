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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.List;

public class ChangeLoggingKeyValueBytesStore
    extends WrappedStateStore.AbstractStateStore<KeyValueStore<Bytes, byte[]>>
    implements KeyValueStore<Bytes, byte[]>, WrappedStateStore<KeyValueStore<Bytes, byte[]>> {

    final KeyValueStore<Bytes, byte[]> inner;
    StoreChangeLogger<Bytes, byte[]> changeLogger;

    ChangeLoggingKeyValueBytesStore(final KeyValueStore<Bytes, byte[]> inner) {
        super(inner);
        this.inner = inner;
    }

    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        inner.init(context, root);
        final String topic = ProcessorStateManager.storeChangelogTopic(context.applicationId(), inner.name());
        this.changeLogger = new StoreChangeLogger<>(inner.name(), context, new StateSerdes<>(topic, Serdes.Bytes(), Serdes.ByteArray()));

        // if the inner store is an LRU cache, add the eviction listener to log removed record
        if (inner instanceof MemoryLRUCache) {
            ((MemoryLRUCache<Bytes, byte[]>) inner).setWhenEldestRemoved((key, value) -> {
                // pass null to indicate removal
                logChange(key, null);
            });
        }
    }

    void logChange(final Bytes key, final byte[] value) {
        changeLogger.logChange(key, value);
    }

    @Override
    public long approximateNumEntries() {
        return inner.approximateNumEntries();
    }

    @Override
    public void put(final Bytes key,
                    final byte[] value) {
        inner.put(key, value);
        logChange(key, value);
    }

    @Override
    public byte[] putIfAbsent(final Bytes key,
                              final byte[] value) {
        final byte[] previous = get(key);
        if (previous == null) {
            put(key, value);
        }
        return previous;
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        inner.putAll(entries);
        for (final KeyValue<Bytes, byte[]> entry : entries) {
            logChange(entry.key, entry.value);
        }
    }

    @Override
    public byte[] delete(final Bytes key) {
        final byte[] oldValue = inner.delete(key);
        logChange(key, null);
        return oldValue;
    }

    @Override
    public byte[] get(final Bytes key) {
        return inner.get(key);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(final Bytes from,
                                                 final Bytes to) {
        return inner.range(from, to);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        return inner.all();
    }
}
