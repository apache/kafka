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

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;

/**
 * Metered {@link KeyValueStore} wrapper is used for recording operation metrics, and hence its
 * inner KeyValueStore implementation do not need to provide its own metrics collecting functionality.
 *
 * @param <K>
 * @param <V>
 */
public class MeteredKeyValueStore<K, V> extends WrappedStateStore.AbstractStateStore implements KeyValueStore<K, V> {

    protected final Time time;
    private final InnerMeteredKeyValueStore<K, K, V, V> innerMetered;

    // always wrap the store with the metered store
    public MeteredKeyValueStore(final KeyValueStore<K, V> inner,
                                final String metricScope,
                                final Time time) {
        super(inner);
        this.time = time != null ? time : Time.SYSTEM;
        this.innerMetered = new InnerMeteredKeyValueStore<>(inner, metricScope, new InnerMeteredKeyValueStore.TypeConverter<K, K, V, V>() {
            @Override
            public K innerKey(final K key) {
                return key;
            }

            @Override
            public V innerValue(final V value) {
                return value;
            }

            @Override
            public List<KeyValue<K, V>> innerEntries(final List<KeyValue<K, V>> from) {
                return from;
            }

            @Override
            public V outerValue(final V value) {
                return value;
            }

            @Override
            public KeyValue<K, V> outerKeyValue(final KeyValue<K, V> from) {
                return from;
            }

            @Override
            public K outerKey(final K key) {
                return key;
            }
        }, time);
    }

    @Override
    public void init(ProcessorContext context, StateStore root) {
        innerMetered.init(context, root);
    }

    @Override
    public long approximateNumEntries() {
        return innerMetered.approximateNumEntries();
    }

    @Override
    public V get(final K key) {
        return innerMetered.get(key);
    }

    @Override
    public void put(final K key, final V value) {
        innerMetered.put(key, value);
    }

    @Override
    public V putIfAbsent(final K key, final V value) {
        return innerMetered.putIfAbsent(key, value);
    }

    @Override
    public void putAll(final List<KeyValue<K, V>> entries) {
        innerMetered.putAll(entries);
    }

    @Override
    public V delete(final K key) {
        return innerMetered.delete(key);
    }

    @Override
    public KeyValueIterator<K, V> range(K from, K to) {
        return innerMetered.range(from, to);
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return innerMetered.all();
    }

    @Override
    public void flush() {
        innerMetered.flush();
    }

}
