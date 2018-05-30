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
package org.apache.kafka.streams.internal;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;

public class KeyValueStoreFacade<K, V> extends StoreFacade implements KeyValueStore<K, V> {
    private final KeyValueStore<K, V> inner;
    private final InternalProcessorContext context;

    public KeyValueStoreFacade(final KeyValueStore<K, V> inner,
                               final InternalProcessorContext context) {
        super(inner);
        this.inner = inner;
        this.context = context;
    }

    @Override
    public void put(final K key,
                    final V value) {
        setContext();
        inner.put(key, value);
        context.setRecordContext(null);
    }

    @Override
    public V putIfAbsent(final K key,
                         final V value) {
        setContext();
        final V oldValue = inner.putIfAbsent(key, value);
        context.setRecordContext(null);
        return oldValue;
    }

    @Override
    public void putAll(final List<KeyValue<K, V>> entries) {
        setContext();
        inner.putAll(entries);
        context.setRecordContext(null);
    }

    @Override
    public V delete(final K key) {
        setContext();
        final V value = inner.delete(key);
        context.setRecordContext(null);
        return value;
    }

    private void setContext() {
        context.setRecordContext(new ProcessorRecordContext(0L, -1L, -1, null, new RecordHeaders()));
    }

    @Override
    public V get(final K key) {
        return inner.get(key);
    }

    @Override
    public KeyValueIterator<K, V> range(final K from,
                                        final K to) {
        return inner.range(from, to);
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return inner.all();
    }

    @Override
    public long approximateNumEntries() {
        return inner.approximateNumEntries();
    }
}
