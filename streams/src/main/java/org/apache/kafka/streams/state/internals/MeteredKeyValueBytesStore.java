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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.ArrayList;
import java.util.List;

/**
 * A Metered {@link KeyValueStore} wrapper that is used for recording operation metrics, and hence its
 * inner KeyValueStore implementation do not need to provide its own metrics collecting functionality.
 * The inner {@link KeyValueStore} of this class is of type &lt;Bytes,byte[]&gt;, hence we use {@link Serde}s
 * to convert from &lt;K,V&gt; to &lt;Bytes,byte[]&gt;
 * @param <K>
 * @param <V>
 */
public class MeteredKeyValueBytesStore<K, V> extends WrappedStateStore.AbstractStateStore implements KeyValueStore<K, V> {

    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private StateSerdes<K, V> serdes;
    private final InnerMeteredKeyValueStore<K, Bytes, V, byte[]> innerMetered;

    // always wrap the store with the metered store
    public MeteredKeyValueBytesStore(final KeyValueStore<Bytes, byte[]> inner,
                                     final String metricScope,
                                     final Time time,
                                     final Serde<K> keySerde,
                                     final Serde<V> valueSerde) {
        super(inner);
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        innerMetered = new InnerMeteredKeyValueStore<>(inner, metricScope, new InnerMeteredKeyValueStore.TypeConverter<K, Bytes, V, byte[]>() {
            @Override
            public Bytes innerKey(final K key) {
                return Bytes.wrap(serdes.rawKey(key));
            }

            @Override
            public byte[] innerValue(final V value) {
                if (value == null) {
                    return null;
                }
                return serdes.rawValue(value);
            }

            @Override
            public List<KeyValue<Bytes, byte[]>> innerEntries(final List<KeyValue<K, V>> from) {
                final List<KeyValue<Bytes, byte[]>> byteEntries = new ArrayList<>();
                for (KeyValue<K, V> entry : from) {
                    byteEntries.add(KeyValue.pair(innerKey(entry.key), serdes.rawValue(entry.value)));

                }
                return byteEntries;
            }

            @Override
            public V outerValue(final byte[] value) {
                return serdes.valueFrom(value);
            }

            @Override
            public KeyValue<K, V> outerKeyValue(final KeyValue<Bytes, byte[]> from) {
                return KeyValue.pair(serdes.keyFrom(from.key.get()), serdes.valueFrom(from.value));
            }

            @Override
            public K outerKey(final Bytes key) {
                return serdes.keyFrom(key.get());
            }
        }, time);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context, StateStore root) {
        this.serdes = new StateSerdes<>(ProcessorStateManager.storeChangelogTopic(context.applicationId(), name()),
                                        keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
                                        valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);
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
