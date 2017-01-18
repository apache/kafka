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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStoreSupplier;
import org.apache.kafka.streams.state.internals.InMemoryLRUCacheStoreSupplier;
import org.apache.kafka.streams.state.internals.RocksDBKeyValueStoreSupplier;
import org.apache.kafka.streams.state.internals.RocksDBSessionStoreSupplier;
import org.apache.kafka.streams.state.internals.RocksDBWindowStoreSupplier;
import org.apache.kafka.streams.state.internals.WindowStoreSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Factory for creating state stores in Kafka Streams.
 */
public class Stores {

    private static final Logger log = LoggerFactory.getLogger(Stores.class);

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
                                return new InMemoryKeyValueFactoryImpl<>(name, keySerde, valueSerde);
                            }
                            @Override
                            public PersistentKeyValueFactory<K, V> persistent() {
                                return new RocksDBKeyValueFactory<>(name, keySerde, valueSerde);
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
         * @return the factory to create persistent key-value stores; never null
         */
        PersistentKeyValueFactory<K, V> persistent();
    }

    private interface InMemoryWrapperFactory<K, V> {
        /**
         * Indicates that a changelog should be created for the store. The changelog will be created
         * with the provided cleanupPolicy and configs.
         *
         * Note: Any unrecognized configs will be ignored.
         * @param config    any configs that should be applied to the changelog
         * @return  the factory to create an in-memory store
         */
        InMemoryWrapperFactory<K, V> enableLogging(final Map<String, String> config);

        /**
         * Indicates that a changelog should not be created for the key-value store
         * @return the factory to create an in-memory store
         */
        InMemoryWrapperFactory<K, V> disableLogging();
    }

    private static abstract class AbstractInMemoryFactory<K, V> implements InMemoryWrapperFactory<K, V> {
        protected final Map<String, String> logConfig = new HashMap<>();

        protected boolean logged = true;

        @Override
        public InMemoryWrapperFactory<K, V> enableLogging(final Map<String, String> config) {
            logged = true;
            logConfig.putAll(config);
            return this;
        }

        @Override
        public InMemoryWrapperFactory<K, V> disableLogging() {
            logged = false;
            logConfig.clear();
            return this;
        }
    }

    /**
     * The interface used to create in-memory key-value stores.
     *
     * @param <K> the type of keys
     * @param <V> the type of values
     */
    public interface InMemoryKeyValueFactory<K, V> extends InMemoryWrapperFactory<K, V> {
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

    private static class InMemoryKeyValueFactoryImpl<K, V> extends AbstractInMemoryFactory<K, V> implements InMemoryKeyValueFactory<K, V> {
        private final String storeName;
        private final Serde<K> keySerde;
        private final Serde<V> valueSerde;

        private int capacity = Integer.MAX_VALUE;

        InMemoryKeyValueFactoryImpl(final String name, final Serde<K> keySerde, final Serde<V> valueSerde) {
            this.storeName = name;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
        }

        @Override
        public InMemoryKeyValueFactory<K, V> maxEntries(int capacity) {
            if (capacity < 1) throw new IllegalArgumentException("The capacity must be positive");
            this.capacity = capacity;
            return this;
        }

        @Override
        public StateStoreSupplier build() {
            log.trace("Creating InMemory Store name={} capacity={} logged={}", storeName, capacity, logged);
            if (capacity < Integer.MAX_VALUE) {
                return new InMemoryLRUCacheStoreSupplier<>(storeName, capacity, keySerde, valueSerde, logged, logConfig);
            }
            return new InMemoryKeyValueStoreSupplier<>(storeName, keySerde, valueSerde, logged, logConfig);
        }
    }

    private interface PersistentWrapperFactory<K, V> {
        /**
         * Indicates that a changelog should be created for the store. The changelog will be created
         * with the provided cleanupPolicy and configs.
         *
         * Note: Any unrecognized configs will be ignored.
         * @param config    any configs that should be applied to the changelog
         * @return  the factory to create a persistent store
         */
        PersistentWrapperFactory<K, V> enableLogging(final Map<String, String> config);

        /**
         * Indicates that a changelog should not be created for the key-value store
         * @return the factory to create a persistent store
         */
        PersistentWrapperFactory<K, V> disableLogging();

        /**
         * Caching should be enabled on the created store.
         * @return the factory to create a store
         */
        PersistentWrapperFactory<K, V> enableCaching();

        /**
         * Caching should be disabled on the created store.
         * @return the factory to create a store
         */
        PersistentWrapperFactory<K, V> disableCaching();
    }

    private static abstract class AbstractPersistentFactory<K, V> implements PersistentWrapperFactory<K, V> {
        protected final Map<String, String> logConfig = new HashMap<>();

        protected boolean logged = true;
        protected boolean cached = false;

        @Override
        public PersistentWrapperFactory<K, V> enableLogging(final Map<String, String> config) {
            logged = true;
            logConfig.putAll(config);
            return this;
        }

        @Override
        public PersistentWrapperFactory<K, V> disableLogging() {
            logged = false;
            logConfig.clear();
            return this;
        }

        @Override
        public PersistentWrapperFactory<K, V> enableCaching() {
            cached = true;
            return this;
        }

        @Override
        public PersistentWrapperFactory<K, V> disableCaching() {
            cached = false;
            return this;
        }
    }

    /**
     * The interface used to create off-heap key-value stores that use a local database.
     *
     * @param <K> the type of keys
     * @param <V> the type of values
     */
    public interface PersistentKeyValueFactory<K, V> extends PersistentWrapperFactory<K, V> {
        /**
         * Set the persistent store as a {@link WindowStore} for use with {@link org.apache.kafka.streams.kstream.TimeWindows}
         * @param windowSize size of the windows
         * @param retentionPeriod the maximum period of time in milli-second to keep each window in this store
         * @param numSegments the maximum number of segments for rolling the windowed store
         * @param retainDuplicates whether or not to retain duplicate data within the window
         */
        PersistentTimeWindowedFactory<K, V> timeWindowed(final long windowSize, final long retentionPeriod, final int numSegments, final boolean retainDuplicates);

        /**
         * Set the persistent store as a {@link SessionStore} for use with {@link org.apache.kafka.streams.kstream.SessionWindows}
         * @param retentionPeriod the maximum period of time in milliseconds to keep each window in this store
         */
        PersistentSessionWindowedFactory<K, V> sessionWindowed(final long retentionPeriod);

        @Override
        PersistentKeyValueFactory<K, V> enableLogging(final Map<String, String> config);

        @Override
        PersistentKeyValueFactory<K, V> disableLogging();

        @Override
        PersistentKeyValueFactory<K, V> enableCaching();

        @Override
        PersistentKeyValueFactory<K, V> disableCaching();

        /**
         * Return the instance of StateStoreSupplier of new key-value store.
         * @return the key-value store; never null
         */
        StateStoreSupplier build();
    }

    private static class RocksDBKeyValueFactory<K, V> extends AbstractPersistentFactory<K, V> implements PersistentKeyValueFactory<K, V> {
        private final String storeName;
        private final Serde<K> keySerde;
        private final Serde<V> valueSerde;

        RocksDBKeyValueFactory(final String name, final Serde<K> keySerde, final Serde<V> valueSerde) {
            this.storeName = name;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
        }

        @Override
        public PersistentKeyValueFactory<K, V> enableLogging(final Map<String, String> config) {
            return (PersistentKeyValueFactory<K, V>) super.enableLogging(config);
        }

        @Override
        public PersistentKeyValueFactory<K, V> disableLogging() {
            return (PersistentKeyValueFactory<K, V>) super.disableLogging();
        }

        @Override
        public PersistentKeyValueFactory<K, V> enableCaching() {
            return (PersistentKeyValueFactory<K, V>) super.enableCaching();
        }

        @Override
        public PersistentKeyValueFactory<K, V> disableCaching() {
            return (PersistentKeyValueFactory<K, V>) super.disableCaching();
        }

        @Override
        public PersistentTimeWindowedFactory<K, V> timeWindowed(final long windowSize, final long retentionPeriod, final int numSegments, final boolean retainDuplicates) {
            return new RocksDBTimeWindowedFactory<>(storeName, keySerde, valueSerde, windowSize, retentionPeriod, numSegments, retainDuplicates);
        }

        @Override
        public PersistentSessionWindowedFactory<K, V> sessionWindowed(final long retentionPeriod) {
            return new RocksDBSessionWindowedFactory<>(storeName, keySerde, valueSerde, retentionPeriod);
        }

        @Override
        public StateStoreSupplier build() {
            log.trace("Creating RocksDb KeyValueStore name={} logged={} cached={}", storeName, logged, cached);

            return new RocksDBKeyValueStoreSupplier<>(storeName, keySerde, valueSerde, logged, logConfig, cached);
        }
    }

    /**
     * The interface used to create off-heap time-windowed stores that use a local database.
     *
     * @param <K> the type of keys
     * @param <V> the type of values
     */
    public interface PersistentTimeWindowedFactory<K, V> extends PersistentWrapperFactory<K, V> {
        @Override
        PersistentTimeWindowedFactory<K, V> enableLogging(final Map<String, String> config);

        @Override
        PersistentTimeWindowedFactory<K, V> disableLogging();

        @Override
        PersistentTimeWindowedFactory<K, V> enableCaching();

        @Override
        PersistentTimeWindowedFactory<K, V> disableCaching();

        /**
         * Return the instance of StateStoreSupplier of new key-value store.
         * @return the key-value store; never null
         */
        WindowStoreSupplier<WindowStore> build();
    }

    private static class RocksDBTimeWindowedFactory<K, V> extends AbstractPersistentFactory<K, V> implements PersistentTimeWindowedFactory<K, V> {
        private final String storeName;
        private final Serde<K> keySerde;
        private final Serde<V> valueSerde;
        private final long windowSize;
        private final int numSegments;
        private final long retentionPeriod;
        private final boolean retainDuplicates;

        RocksDBTimeWindowedFactory(final String name, final Serde<K> keySerde, final Serde<V> valueSerde,
                                   final long windowSize, final long retentionPeriod, final int numSegments, final boolean retainDuplicates) {
            this.storeName = name;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
            this.windowSize = windowSize;
            this.numSegments = numSegments;
            this.retentionPeriod = retentionPeriod;
            this.retainDuplicates = retainDuplicates;
        }

        @Override
        public PersistentTimeWindowedFactory<K, V> enableLogging(final Map<String, String> config) {
            return (PersistentTimeWindowedFactory<K, V>) super.enableLogging(config);
        }

        @Override
        public PersistentTimeWindowedFactory<K, V> disableLogging() {
            return (PersistentTimeWindowedFactory<K, V>) super.disableLogging();
        }

        @Override
        public PersistentTimeWindowedFactory<K, V> enableCaching() {
            return (PersistentTimeWindowedFactory<K, V>) super.enableCaching();
        }

        @Override
        public PersistentTimeWindowedFactory<K, V> disableCaching() {
            return (PersistentTimeWindowedFactory<K, V>) super.disableCaching();
        }

        @Override
        public WindowStoreSupplier<WindowStore> build() {
            log.trace("Creating RocksDb TimeWindowedStore name={} numSegments={} logged={}", storeName, numSegments, logged);

            return new RocksDBWindowStoreSupplier<>(storeName, retentionPeriod, numSegments, retainDuplicates, keySerde, valueSerde, windowSize, logged, logConfig, cached);
        }
    }

    /**
     * The interface used to create off-heap session-windowed stores that use a local database.
     *
     * @param <K> the type of keys
     * @param <V> the type of values
     */
    public interface PersistentSessionWindowedFactory<K, V> extends PersistentWrapperFactory<K, V> {
        @Override
        PersistentSessionWindowedFactory<K, V> enableLogging(final Map<String, String> config);

        @Override
        PersistentSessionWindowedFactory<K, V> disableLogging();

        @Override
        PersistentSessionWindowedFactory<K, V> enableCaching();

        @Override
        PersistentSessionWindowedFactory<K, V> disableCaching();

        /**
         * Return the instance of StateStoreSupplier of new key-value store.
         * @return the key-value store; never null
         */
        WindowStoreSupplier<SessionStore> build();
    }

    private static class RocksDBSessionWindowedFactory<K, V> extends AbstractPersistentFactory<K, V> implements PersistentSessionWindowedFactory<K, V> {
        private final String storeName;
        private final Serde<K> keySerde;
        private final Serde<V> valueSerde;
        private final long retentionPeriod;

        RocksDBSessionWindowedFactory(final String name, final Serde<K> keySerde, final Serde<V> valueSerde, final long retentionPeriod) {
            this.storeName = name;
            this.keySerde = keySerde;
            this.valueSerde = valueSerde;
            this.retentionPeriod = retentionPeriod;
        }

        @Override
        public PersistentSessionWindowedFactory<K, V> enableLogging(final Map<String, String> config) {
            return (PersistentSessionWindowedFactory<K, V>) super.enableLogging(config);
        }

        @Override
        public PersistentSessionWindowedFactory<K, V> disableLogging() {
            return (PersistentSessionWindowedFactory<K, V>) super.disableLogging();
        }

        @Override
        public PersistentSessionWindowedFactory<K, V> enableCaching() {
            return (PersistentSessionWindowedFactory<K, V>) super.enableCaching();
        }

        @Override
        public PersistentSessionWindowedFactory<K, V> disableCaching() {
            return (PersistentSessionWindowedFactory<K, V>) super.disableCaching();
        }

        @Override
        public WindowStoreSupplier<SessionStore> build() {
            log.trace("Creating RocksDb SessionWindowedStore name={} logged={} cached={}", storeName, logged, cached);

            return new RocksDBSessionStoreSupplier<>(storeName, retentionPeriod, keySerde, valueSerde, logged, logConfig, cached);
        }
    }
}

