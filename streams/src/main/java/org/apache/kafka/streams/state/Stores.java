/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStoreSupplier;
import org.apache.kafka.streams.state.internals.InMemoryLRUCacheStoreSupplier;
import org.apache.kafka.streams.state.internals.RocksDBKeyValueStoreSupplier;

/**
 * Factory for creating key-value stores.
 */
public class Stores {

    /**
     * Begin to create a new {@link org.apache.kafka.streams.processor.StateStoreSupplier} instance.
     *
     * @param name the name of the store
     * @return the factory that can be used to specify other options or configurations for the store; never null
     */
    public static StoreFactory create(final String name) {
        return new StoreFactory() {
            @Override
            public <K> ValueFactory<K> withKeys(final Serializer<K> keySerializer, final Deserializer<K> keyDeserializer) {
                return new ValueFactory<K>() {
                    @Override
                    public <V> KeyValueFactory<K, V> withValues(final Serializer<V> valueSerializer,
                                                                final Deserializer<V> valueDeserializer) {
                        final Serdes<K, V> serdes =
                                new Serdes<>(name, keySerializer, keyDeserializer, valueSerializer, valueDeserializer);
                        return new KeyValueFactory<K, V>() {
                            @Override
                            public InMemoryKeyValueFactory<K, V> inMemory() {
                                return new InMemoryKeyValueFactory<K, V>() {
                                    private int capacity = Integer.MAX_VALUE;

                                    @Override
                                    public InMemoryKeyValueFactory<K, V> maxEntries(int capacity) {
                                        if (capacity < 1) throw new IllegalArgumentException("The capacity must be positive");
                                        this.capacity = capacity;
                                        return this;
                                    }

                                    @Override
                                    public StateStoreSupplier build() {
                                        if (capacity < Integer.MAX_VALUE) {
                                            return new InMemoryLRUCacheStoreSupplier<>(name, capacity, serdes, null);
                                        }
                                        return new InMemoryKeyValueStoreSupplier<>(name, serdes, null);
                                    }
                                };
                            }

                            @Override
                            public LocalDatabaseKeyValueFactory<K, V> localDatabase() {
                                return new LocalDatabaseKeyValueFactory<K, V>() {
                                    @Override
                                    public StateStoreSupplier build() {
                                        return new RocksDBKeyValueStoreSupplier<>(name, serdes, null);
                                    }
                                };
                            }
                        };
                    }
                };
            }
        };
    }

    public static abstract class StoreFactory {
        /**
         * Begin to create a {@link KeyValueStore} by specifying the keys will be {@link String}s.
         *
         * @return the interface used to specify the type of values; never null
         */
        public ValueFactory<String> withStringKeys() {
            return withKeys(new StringSerializer(), new StringDeserializer());
        }

        /**
         * Begin to create a {@link KeyValueStore} by specifying the keys will be {@link Integer}s.
         *
         * @return the interface used to specify the type of values; never null
         */
        public ValueFactory<Integer> withIntegerKeys() {
            return withKeys(new IntegerSerializer(), new IntegerDeserializer());
        }

        /**
         * Begin to create a {@link KeyValueStore} by specifying the keys will be {@link Long}s.
         *
         * @return the interface used to specify the type of values; never null
         */
        public ValueFactory<Long> withLongKeys() {
            return withKeys(new LongSerializer(), new LongDeserializer());
        }

        /**
         * Begin to create a {@link KeyValueStore} by specifying the keys will be byte arrays.
         *
         * @return the interface used to specify the type of values; never null
         */
        public ValueFactory<byte[]> withByteArrayKeys() {
            return withKeys(new ByteArraySerializer(), new ByteArrayDeserializer());
        }

        /**
         * Begin to create a {@link KeyValueStore} by specifying the keys will be either {@link String}, {@link Integer},
         * {@link Long}, or {@code byte[]}.
         *
         * @param keyClass the class for the keys, which must be one of the types for which Kafka has built-in serializers and
         *            deserializers (e.g., {@code String.class}, {@code Integer.class}, {@code Long.class}, or
         *            {@code byte[].class})
         * @return the interface used to specify the type of values; never null
         */
        public <K> ValueFactory<K> withKeys(Class<K> keyClass) {
            return withKeys(Serdes.serializer(keyClass), Serdes.deserializer(keyClass));
        }

        /**
         * Begin to create a {@link KeyValueStore} by specifying the serializer and deserializer for the keys.
         *
         * @param keySerializer the serializer for keys; may not be null
         * @param keyDeserializer the deserializer for keys; may not be null
         * @return the interface used to specify the type of values; never null
         */
        public abstract <K> ValueFactory<K> withKeys(Serializer<K> keySerializer, Deserializer<K> keyDeserializer);
    }

    /**
     * The factory for creating off-heap key-value stores.
     *
     * @param <K> the type of keys
     */
    public static abstract class ValueFactory<K> {
        /**
         * Use {@link String} values.
         *
         * @return the interface used to specify the remaining key-value store options; never null
         */
        public KeyValueFactory<K, String> withStringValues() {
            return withValues(new StringSerializer(), new StringDeserializer());
        }

        /**
         * Use {@link Integer} values.
         *
         * @return the interface used to specify the remaining key-value store options; never null
         */
        public KeyValueFactory<K, Integer> withIntegerValues() {
            return withValues(new IntegerSerializer(), new IntegerDeserializer());
        }

        /**
         * Use {@link Long} values.
         *
         * @return the interface used to specify the remaining key-value store options; never null
         */
        public KeyValueFactory<K, Long> withLongValues() {
            return withValues(new LongSerializer(), new LongDeserializer());
        }

        /**
         * Use byte arrays for values.
         *
         * @return the interface used to specify the remaining key-value store options; never null
         */
        public KeyValueFactory<K, byte[]> withByteArrayValues() {
            return withValues(new ByteArraySerializer(), new ByteArrayDeserializer());
        }

        /**
         * Use values of the specified type, which must be either {@link String}, {@link Integer}, {@link Long}, or {@code byte[]}
         * .
         *
         * @param valueClass the class for the values, which must be one of the types for which Kafka has built-in serializers and
         *            deserializers (e.g., {@code String.class}, {@code Integer.class}, {@code Long.class}, or
         *            {@code byte[].class})
         * @return the interface used to specify the remaining key-value store options; never null
         */
        public <V> KeyValueFactory<K, V> withValues(Class<V> valueClass) {
            return withValues(Serdes.serializer(valueClass), Serdes.deserializer(valueClass));
        }

        /**
         * Use the specified serializer and deserializer for the values.
         *
         * @param valueSerializer the serializer for value; may not be null
         * @param valueDeserializer the deserializer for values; may not be null
         * @return the interface used to specify the remaining key-value store options; never null
         */
        public abstract <V> KeyValueFactory<K, V> withValues(Serializer<V> valueSerializer, Deserializer<V> valueDeserializer);
    }

    /**
     * The interface used to specify the different kinds of key-value stores.
     *
     * @param <K> the type of keys
     * @param <V> the type of values
     */
    public static interface KeyValueFactory<K, V> {
        /**
         * Keep all key-value entries in-memory, although for durability all entries are recorded in a Kafka topic that can be
         * read to restore the entries if they are lost.
         *
         * @return the factory to create in-memory key-value stores; never null
         */
        InMemoryKeyValueFactory<K, V> inMemory();

        /**
         * Keep all key-value entries off-heap in a local database, although for durability all entries are recorded in a Kafka
         * topic that can be read to restore the entries if they are lost.
         *
         * @return the factory to create in-memory key-value stores; never null
         */
        LocalDatabaseKeyValueFactory<K, V> localDatabase();
    }

    /**
     * The interface used to create in-memory key-value stores.
     *
     * @param <K> the type of keys
     * @param <V> the type of values
     */
    public static interface InMemoryKeyValueFactory<K, V> {
        /**
         * Limits the in-memory key-value store to hold a maximum number of entries. The default is {@link Integer#MAX_VALUE}, which is
         * equivalent to not placing a limit on the number of entries.
         *
         * @param capacity the maximum capacity of the in-memory cache; should be one less than a power of 2
         * @return this factory
         * @throws IllegalArgumentException if the capacity is not positive
         */
        InMemoryKeyValueFactory<K, V> maxEntries(int capacity);

        /**
         * Return the instance of StateStoreSupplier of new key-value store.
         * @return the state store supplier; never null
         */
        StateStoreSupplier build();
    }

    /**
     * The interface used to create off-heap key-value stores that use a local database.
     *
     * @param <K> the type of keys
     * @param <V> the type of values
     */
    public static interface LocalDatabaseKeyValueFactory<K, V> {
        /**
         * Return the instance of StateStoreSupplier of new key-value store.
         * @return the key-value store; never null
         */
        StateStoreSupplier build();
    }
}
