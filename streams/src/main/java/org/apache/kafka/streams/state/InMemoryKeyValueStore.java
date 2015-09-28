/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.state;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * An in-memory key-value store based on a TreeMap
 *
 * @param <K> The key type
 * @param <V> The value type
 */
public class InMemoryKeyValueStore<K, V> extends MeteredKeyValueStore<K, V> {

    /**
     * Create an in-memory key value store that records changes to a Kafka topic and the system time provider.
     * 
     * @param name the name of the store, used in the name of the topic to which entries are recorded
     * @param context the processing context
     * @param keyClass the class for the keys, which must be one of the types for which Kafka has built-in serializers and
     *            deserializers (e.g., {@code String.class}, {@code Integer.class}, {@code Long.class}, or
     *            {@code byte[].class})
     * @param valueClass the class for the values, which must be one of the types for which Kafka has built-in serializers and
     *            deserializers (e.g., {@code String.class}, {@code Integer.class}, {@code Long.class}, or
     *            {@code byte[].class})
     * @return the key-value store
     * @throws IllegalArgumentException if the {@code keyClass} or {@code valueClass} are not one of {@code String.class},
     *             {@code Integer.class}, {@code Long.class}, or
     *             {@code byte[].class}
     */
    public static <K, V> InMemoryKeyValueStore<K, V> create(String name, ProcessorContext context, Class<K> keyClass, Class<V> valueClass) {
        return create(name, context, keyClass, valueClass, new SystemTime());
    }

    /**
     * Create an in-memory key value store that records changes to a Kafka topic and the given time provider.
     * 
     * @param name the name of the store, used in the name of the topic to which entries are recorded
     * @param context the processing context
     * @param keyClass the class for the keys, which must be one of the types for which Kafka has built-in serializers and
     *            deserializers (e.g., {@code String.class}, {@code Integer.class}, {@code Long.class}, or
     *            {@code byte[].class})
     * @param valueClass the class for the values, which must be one of the types for which Kafka has built-in serializers and
     *            deserializers (e.g., {@code String.class}, {@code Integer.class}, {@code Long.class}, or
     *            {@code byte[].class})
     * @param time the time provider; may be null if the system time should be used
     * @return the key-value store
     * @throws IllegalArgumentException if the {@code keyClass} or {@code valueClass} are not one of {@code String.class},
     *             {@code Integer.class}, {@code Long.class}, or
     *             {@code byte[].class}
     */
    public static <K, V> InMemoryKeyValueStore<K, V> create(String name, ProcessorContext context, Class<K> keyClass, Class<V> valueClass,
                                                            Time time) {
        return new InMemoryKeyValueStore<>(name, context, Serdes.withBuiltinTypes(name, keyClass, valueClass), time);
    }

    /**
     * Create an in-memory key value store that records changes to a Kafka topic, the {@link ProcessorContext}'s default
     * serializers and deserializers, and the system time provider.
     * <p>
     * <strong>NOTE:</strong> the default serializers and deserializers in the context <em>must</em> match the key and value types
     * used as parameters for this key value store. This is not checked in this method, and any mismatch will result in
     * class cast exceptions during usage.
     * 
     * @param name the name of the store, used in the name of the topic to which entries are recorded
     * @param context the processing context
     * @return the key-value store
     */
    public static <K, V> InMemoryKeyValueStore<K, V> create(String name, ProcessorContext context) {
        return create(name, context, new SystemTime());
    }

    /**
     * Create an in-memory key value store that records changes to a Kafka topic, the {@link ProcessorContext}'s default
     * serializers and deserializers, and the given time provider.
     * <p>
     * <strong>NOTE:</strong> the default serializers and deserializers in the context <em>must</em> match the key and value types
     * used as parameters for this key value store.
     * 
     * @param name the name of the store, used in the name of the topic to which entries are recorded
     * @param context the processing context
     * @param time the time provider; may be null if the system time should be used
     * @return the key-value store
     */
    public static <K, V> InMemoryKeyValueStore<K, V> create(String name, ProcessorContext context, Time time) {
        Serdes<K, V> serdes = new Serdes<>(name, context);
        return new InMemoryKeyValueStore<>(name, context, serdes, time);
    }

    /**
     * Create an in-memory key value store that records changes to a Kafka topic, the provided serializers and deserializers, and
     * the system time provider.
     * 
     * @param name the name of the store, used in the name of the topic to which entries are recorded
     * @param context the processing context
     * @param keySerializer the serializer for keys; may not be null
     * @param keyDeserializer the deserializer for keys; may not be null
     * @param valueSerializer the serializer for values; may not be null
     * @param valueDeserializer the deserializer for values; may not be null
     * @return the key-value store
     */
    public static <K, V> InMemoryKeyValueStore<K, V> create(String name, ProcessorContext context,
                                                            Serializer<K> keySerializer, Deserializer<K> keyDeserializer,
                                                            Serializer<V> valueSerializer, Deserializer<V> valueDeserializer) {
        return create(name, context, keySerializer, keyDeserializer, valueSerializer, valueDeserializer, new SystemTime());
    }

    /**
     * Create an in-memory key value store that records changes to a Kafka topic, the provided serializers and deserializers, and
     * the given time provider.
     * 
     * @param name the name of the store, used in the name of the topic to which entries are recorded
     * @param context the processing context
     * @param keySerializer the serializer for keys; may not be null
     * @param keyDeserializer the deserializer for keys; may not be null
     * @param valueSerializer the serializer for values; may not be null
     * @param valueDeserializer the deserializer for values; may not be null
     * @param time the time provider; may be null if the system time should be used
     * @return the key-value store
     */
    public static <K, V> InMemoryKeyValueStore<K, V> create(String name, ProcessorContext context,
                                                            Serializer<K> keySerializer, Deserializer<K> keyDeserializer,
                                                            Serializer<V> valueSerializer, Deserializer<V> valueDeserializer,
                                                            Time time) {
        Serdes<K, V> serdes = new Serdes<>(name, keySerializer, keyDeserializer, valueSerializer, valueDeserializer);
        return new InMemoryKeyValueStore<>(name, context, serdes, time);
    }

    protected InMemoryKeyValueStore(String name, ProcessorContext context, Serdes<K, V> serdes, Time time) {
        super(name, new MemoryStore<K, V>(name), context, serdes, "in-memory-state", time != null ? time : new SystemTime());
    }

    private static class MemoryStore<K, V> implements KeyValueStore<K, V> {

        private final String name;
        private final NavigableMap<K, V> map;

        public MemoryStore(String name) {
            super();
            this.name = name;
            this.map = new TreeMap<>();
        }

        @Override
        public String name() {
            return this.name;
        }

        @Override
        public boolean persistent() {
            return false;
        }

        @Override
        public V get(K key) {
            return this.map.get(key);
        }

        @Override
        public void put(K key, V value) {
            this.map.put(key, value);
        }

        @Override
        public void putAll(List<Entry<K, V>> entries) {
            for (Entry<K, V> entry : entries)
                put(entry.key(), entry.value());
        }

        @Override
        public V delete(K key) {
            return this.map.remove(key);
        }

        @Override
        public KeyValueIterator<K, V> range(K from, K to) {
            return new MemoryStoreIterator<K, V>(this.map.subMap(from, true, to, false).entrySet().iterator());
        }

        @Override
        public KeyValueIterator<K, V> all() {
            return new MemoryStoreIterator<K, V>(this.map.entrySet().iterator());
        }

        @Override
        public void flush() {
            // do-nothing since it is in-memory
        }

        @Override
        public void close() {
            // do-nothing
        }

        private static class MemoryStoreIterator<K, V> implements KeyValueIterator<K, V> {
            private final Iterator<Map.Entry<K, V>> iter;

            public MemoryStoreIterator(Iterator<Map.Entry<K, V>> iter) {
                this.iter = iter;
            }

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public Entry<K, V> next() {
                Map.Entry<K, V> entry = iter.next();
                return new Entry<>(entry.getKey(), entry.getValue());
            }

            @Override
            public void remove() {
                iter.remove();
            }

            @Override
            public void close() {
            }

        }
    }
}