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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStoreSupplier;
import org.apache.kafka.streams.state.internals.InMemoryLRUCacheStoreSupplier;
import org.apache.kafka.streams.state.internals.RocksDBKeyValueStoreSupplier;
import org.apache.kafka.streams.state.internals.RocksDBWindowStoreSupplier;

import java.nio.ByteBuffer;

/**
 * Factory for creating state stores in Kafka Streams.
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
            public <K> ValueFactory<K> withKeys(final Serde<K> keySerde) {
                return new ValueFactory<K>() {
                    @Override
                    public <V> KeyValueFactory<K, V> withValues(final Serde<V> valueSerde) {
                        return new KeyValueFactory<K, V>() {
                            @Override
                            public InMemoryKeyValueFactory<K, V> inMemory() {
                                return new InMemoryKeyValueFactory<K, V>() {
                                    private int capacity = Integer.MAX_VALUE;

                                    /**
                                     * @param capacity the maximum capacity of the in-memory cache; should be one less than a power of 2
                                     * @throws IllegalArgumentException if the capacity of the store is zero or negative
                                     */
                                    @Override
                                    public InMemoryKeyValueFactory<K, V> maxEntries(int capacity) {
                                        if (capacity < 1) throw new IllegalArgumentException("The capacity must be positive");
                                        this.capacity = capacity;
                                        return this;
                                    }

                                    @Override
                                    public StateStoreSupplier build() {
                                        if (capacity < Integer.MAX_VALUE) {
                                            return new InMemoryLRUCacheStoreSupplier<>(name, capacity, keySerde, valueSerde);
                                        }
                                        return new InMemoryKeyValueStoreSupplier<>(name, keySerde, valueSerde);
                                    }
                                };
                            }

                            @Override
                            public PersistentKeyValueFactory<K, V> persistent() {
                                return new PersistentKeyValueFactory<K, V>() {
                                    private int numSegments = 0;
                                    private long retentionPeriod = 0L;
                                    private boolean retainDuplicates = false;

                                    @Override
                                    public PersistentKeyValueFactory<K, V> windowed(long retentionPeriod, int numSegments, boolean retainDuplicates) {
                                        this.numSegments = numSegments;
                                        this.retentionPeriod = retentionPeriod;
                                        this.retainDuplicates = retainDuplicates;

                                        return this;
                                    }

                                    @Override
                                    public StateStoreSupplier build() {
                                        if (numSegments > 0) {
                                            return new RocksDBWindowStoreSupplier<>(name, retentionPeriod, numSegments, retainDuplicates, keySerde, valueSerde);
                                        }

                                        return new RocksDBKeyValueStoreSupplier<>(name, keySerde, valueSerde);
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
            return withKeys(Serdes.String());
        }

        /**
         * Begin to create a {@link KeyValueStore} by specifying the keys will be {@link Integer}s.
         *
         * @return the interface used to specify the type of values; never null
         */
        public ValueFactory<Integer> withIntegerKeys() {
            return withKeys(Serdes.Integer());
        }

        /**
         * Begin to create a {@link KeyValueStore} by specifying the keys will be {@link Long}s.
         *
         * @return the interface used to specify the type of values; never null
         */
        public ValueFactory<Long> withLongKeys() {
            return withKeys(Serdes.Long());
        }

        /**
         * Begin to create a {@link KeyValueStore} by specifying the keys will be {@link Double}s.
         *
         * @return the interface used to specify the type of values; never null
         */
        public ValueFactory<Double> withDoubleKeys() {
            return withKeys(Serdes.Double());
        }

        /**
         * Begin to create a {@link KeyValueStore} by specifying the keys will be {@link ByteBuffer}.
         *
         * @return the interface used to specify the type of values; never null
         */
        public ValueFactory<ByteBuffer> withByteBufferKeys() {
            return withKeys(Serdes.ByteBuffer());
        }

        /**
         * Begin to create a {@link KeyValueStore} by specifying the keys will be byte arrays.
         *
         * @return the interface used to specify the type of values; never null
         */
        public ValueFactory<byte[]> withByteArrayKeys() {
            return withKeys(Serdes.ByteArray());
        }

        /**
         * Begin to create a {@link KeyValueStore} by specifying the keys.
         *
         * @param keyClass the class for the keys, which must be one of the types for which Kafka has built-in serdes
         * @return the interface used to specify the type of values; never null
         */
        public <K> ValueFactory<K> withKeys(Class<K> keyClass) {
            return withKeys(Serdes.serdeFrom(keyClass));
        }

        /**
         * Begin to create a {@link KeyValueStore} by specifying the serializer and deserializer for the keys.
         *
         * @param keySerde  the serialization factory for keys; may be null
         * @return          the interface used to specify the type of values; never null
         */
        public abstract <K> ValueFactory<K> withKeys(Serde<K> keySerde);
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
            return withValues(Serdes.String());
        }

        /**
         * Use {@link Integer} values.
         *
         * @return the interface used to specify the remaining key-value store options; never null
         */
        public KeyValueFactory<K, Integer> withIntegerValues() {
            return withValues(Serdes.Integer());
        }

        /**
         * Use {@link Long} values.
         *
         * @return the interface used to specify the remaining key-value store options; never null
         */
        public KeyValueFactory<K, Long> withLongValues() {
            return withValues(Serdes.Long());
        }

        /**
         * Use {@link Double} values.
         *
         * @return the interface used to specify the remaining key-value store options; never null
         */
        public KeyValueFactory<K, Double> withDoubleValues() {
            return withValues(Serdes.Double());
        }

        /**
         * Use {@link ByteBuffer} for values.
         *
         * @return the interface used to specify the remaining key-value store options; never null
         */
        public KeyValueFactory<K, ByteBuffer> withByteBufferValues() {
            return withValues(Serdes.ByteBuffer());
        }

        /**
         * Use byte arrays for values.
         *
         * @return the interface used to specify the remaining key-value store options; never null
         */
        public KeyValueFactory<K, byte[]> withByteArrayValues() {
            return withValues(Serdes.ByteArray());
        }

        /**
         * Use values of the specified type.
         *
         * @param valueClass the class for the values, which must be one of the types for which Kafka has built-in serdes
         * @return the interface used to specify the remaining key-value store options; never null
         */
        public <V> KeyValueFactory<K, V> withValues(Class<V> valueClass) {
            return withValues(Serdes.serdeFrom(valueClass));
        }

        /**
         * Use the specified serializer and deserializer for the values.
         *
         * @param valueSerde    the serialization factory for values; may be null
         * @return              the interface used to specify the remaining key-value store options; never null
         */
        public abstract <V> KeyValueFactory<K, V> withValues(Serde<V> valueSerde);
    }

    /**
     * The interface used to specify the different kinds of key-value stores.
     *
     * @param <K> the type of keys
     * @param <V> the type of values
     */
    public interface KeyValueFactory<K, V> {
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
        PersistentKeyValueFactory<K, V> persistent();
    }

    /**
     * The interface used to create in-memory key-value stores.
     *
     * @param <K> the type of keys
     * @param <V> the type of values
     */
    public interface InMemoryKeyValueFactory<K, V> {
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
    public interface PersistentKeyValueFactory<K, V> {

        /**
         * Set the persistent store as a windowed key-value store
         *
         * @param retentionPeriod the maximum period of time in milli-second to keep each window in this store
         * @param numSegments the maximum number of segments for rolling the windowed store
         * @param retainDuplicates whether or not to retain duplicate data within the window
         */
        PersistentKeyValueFactory<K, V> windowed(long retentionPeriod, int numSegments, boolean retainDuplicates);

        /**
         * Return the instance of StateStoreSupplier of new key-value store.
         * @return the key-value store; never null
         */
        StateStoreSupplier build();
    }
}
