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
package org.apache.kafka.streams.state;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.streams.processor.internals.RecordCollectorImpl;
import org.apache.kafka.streams.processor.internals.StreamsProducer;
import org.apache.kafka.streams.state.internals.MeteredKeyValueStore;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockClientSupplier;
import org.apache.kafka.test.MockRocksDbConfigSetter;
import org.apache.kafka.test.MockTimestampExtractor;
import org.apache.kafka.test.TestUtils;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A component that provides a {@link #context() ProcessingContext} that can be supplied to a {@link KeyValueStore} so that
 * all entries written to the Kafka topic by the store during {@link KeyValueStore#flush()} are captured for testing purposes.
 * This class simplifies testing of various {@link KeyValueStore} instances, especially those that use
 * {@link MeteredKeyValueStore} to monitor and write its entries to the Kafka topic.
 *
 * <h2>Basic usage</h2>
 * This component can be used to help test a {@link KeyValueStore}'s ability to read and write entries.
 *
 * <pre>
 * // Create the test driver ...
 * KeyValueStoreTestDriver&lt;Integer, String> driver = KeyValueStoreTestDriver.create();
 * KeyValueStore&lt;Integer, String> store = Stores.create("my-store", driver.context())
 *                                              .withIntegerKeys().withStringKeys()
 *                                              .inMemory().build();
 *
 * // Verify that the store reads and writes correctly ...
 * store.put(0, "zero");
 * store.put(1, "one");
 * store.put(2, "two");
 * store.put(4, "four");
 * store.put(5, "five");
 * assertEquals(5, driver.sizeOf(store));
 * assertEquals("zero", store.get(0));
 * assertEquals("one", store.get(1));
 * assertEquals("two", store.get(2));
 * assertEquals("four", store.get(4));
 * assertEquals("five", store.get(5));
 * assertNull(store.get(3));
 * store.delete(5);
 *
 * // Flush the store and verify all current entries were properly flushed ...
 * store.flush();
 * assertEquals("zero", driver.flushedEntryStored(0));
 * assertEquals("one", driver.flushedEntryStored(1));
 * assertEquals("two", driver.flushedEntryStored(2));
 * assertEquals("four", driver.flushedEntryStored(4));
 * assertNull(driver.flushedEntryStored(5));
 *
 * assertEquals(false, driver.flushedEntryRemoved(0));
 * assertEquals(false, driver.flushedEntryRemoved(1));
 * assertEquals(false, driver.flushedEntryRemoved(2));
 * assertEquals(false, driver.flushedEntryRemoved(4));
 * assertEquals(true, driver.flushedEntryRemoved(5));
 * </pre>
 *
 *
 * <h2>Restoring a store</h2>
 * This component can be used to test whether a {@link KeyValueStore} implementation properly
 * {@link ProcessorContext#register(StateStore, StateRestoreCallback) registers itself} with the {@link ProcessorContext}, so that
 * the persisted contents of a store are properly restored from the flushed entries when the store instance is started.
 * <p>
 * To do this, create an instance of this driver component, {@link #addEntryToRestoreLog(Object, Object) add entries} that will be
 * passed to the store upon creation (simulating the entries that were previously flushed to the topic), and then create the store
 * using this driver's {@link #context() ProcessorContext}:
 *
 * <pre>
 * // Create the test driver ...
 * KeyValueStoreTestDriver&lt;Integer, String> driver = KeyValueStoreTestDriver.create(Integer.class, String.class);
 *
 * // Add any entries that will be restored to any store that uses the driver's context ...
 * driver.addRestoreEntry(0, "zero");
 * driver.addRestoreEntry(1, "one");
 * driver.addRestoreEntry(2, "two");
 * driver.addRestoreEntry(4, "four");
 *
 * // Create the store, which should register with the context and automatically
 * // receive the restore entries ...
 * KeyValueStore&lt;Integer, String> store = Stores.create("my-store", driver.context())
 *                                              .withIntegerKeys().withStringKeys()
 *                                              .inMemory().build();
 *
 * // Verify that the store's contents were properly restored ...
 * assertEquals(0, driver.checkForRestoredEntries(store));
 *
 * // and there are no other entries ...
 * assertEquals(4, driver.sizeOf(store));
 * </pre>
 *
 * @param <K> the type of keys placed in the store
 * @param <V> the type of values placed in the store
 */
public class KeyValueStoreTestDriver<K, V> {

    private final Properties props;

    /**
     * Create a driver object that will have a {@link #context()} that records messages
     * {@link ProcessorContext#forward(Object, Object) forwarded} by the store and that provides default serializers and
     * deserializers for the given built-in key and value types (e.g., {@code String.class}, {@code Integer.class},
     * {@code Long.class}, and {@code byte[].class}). This can be used when store is created to rely upon the
     * ProcessorContext's default key and value serializers and deserializers.
     *
     * @param keyClass   the class for the keys; must be one of {@code String.class}, {@code Integer.class},
     *                   {@code Long.class}, or {@code byte[].class}
     * @param valueClass the class for the values; must be one of {@code String.class}, {@code Integer.class},
     *                   {@code Long.class}, or {@code byte[].class}
     * @return the test driver; never null
     */
    public static <K, V> KeyValueStoreTestDriver<K, V> create(final Class<K> keyClass, final Class<V> valueClass) {
        final StateSerdes<K, V> serdes = StateSerdes.withBuiltinTypes("unexpected", keyClass, valueClass);
        return new KeyValueStoreTestDriver<>(serdes);
    }

    /**
     * Create a driver object that will have a {@link #context()} that records messages
     * {@link ProcessorContext#forward(Object, Object) forwarded} by the store and that provides the specified serializers and
     * deserializers. This can be used when store is created to rely upon the ProcessorContext's default key and value serializers
     * and deserializers.
     *
     * @param keySerializer     the key serializer for the {@link ProcessorContext}; may not be null
     * @param keyDeserializer   the key deserializer for the {@link ProcessorContext}; may not be null
     * @param valueSerializer   the value serializer for the {@link ProcessorContext}; may not be null
     * @param valueDeserializer the value deserializer for the {@link ProcessorContext}; may not be null
     * @return the test driver; never null
     */
    public static <K, V> KeyValueStoreTestDriver<K, V> create(final Serializer<K> keySerializer,
                                                              final Deserializer<K> keyDeserializer,
                                                              final Serializer<V> valueSerializer,
                                                              final Deserializer<V> valueDeserializer) {
        final StateSerdes<K, V> serdes = new StateSerdes<>(
            "unexpected",
            Serdes.serdeFrom(keySerializer, keyDeserializer),
            Serdes.serdeFrom(valueSerializer, valueDeserializer));
        return new KeyValueStoreTestDriver<>(serdes);
    }

    private final Map<K, V> flushedEntries = new HashMap<>();
    private final Set<K> flushedRemovals = new HashSet<>();
    private final List<KeyValue<byte[], byte[]>> restorableEntries = new LinkedList<>();

    private final InternalMockProcessorContext context;
    private final StateSerdes<K, V> stateSerdes;

    @SuppressWarnings("unchecked")
    private KeyValueStoreTestDriver(final StateSerdes<K, V> serdes) {
        props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "application-id");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockTimestampExtractor.class);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, serdes.keySerde().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, serdes.valueSerde().getClass());
        props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, MockRocksDbConfigSetter.class);
        props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG");

        final ProcessorTopology topology = mock(ProcessorTopology.class);
        when(topology.sinkTopics()).thenReturn(Collections.emptySet());

        final LogContext logContext = new LogContext("KeyValueStoreTestDriver ");
        final RecordCollector recordCollector = new RecordCollectorImpl(
            logContext,
            new TaskId(0, 0),
            new StreamsProducer(
                new StreamsConfig(props),
                "threadId",
                new MockClientSupplier(),
                null,
                null,
                logContext,
                Time.SYSTEM),
            new DefaultProductionExceptionHandler(),
            new MockStreamsMetrics(new Metrics()),
            topology
        ) {
            @Override
            public <K1, V1> void send(final String topic,
                                      final K1 key,
                                      final V1 value,
                                      final Headers headers,
                                      final Integer partition,
                                      final Long timestamp,
                                      final Serializer<K1> keySerializer,
                                      final Serializer<V1> valueSerializer,
                                      final String processorNodeId,
                                      final InternalProcessorContext<Void, Void> context) {
                // for byte arrays we need to wrap it for comparison

                final byte[] keyBytes = keySerializer.serialize(topic, headers, key);
                final byte[] valueBytes = valueSerializer.serialize(topic, headers, value);

                final K keyTest = serdes.keyFrom(keyBytes);
                final V valueTest = serdes.valueFrom(valueBytes);

                recordFlushed(keyTest, valueTest);
            }

            @Override
            public <K1, V1> void send(final String topic,
                                      final K1 key,
                                      final V1 value,
                                      final Headers headers,
                                      final Long timestamp,
                                      final Serializer<K1> keySerializer,
                                      final Serializer<V1> valueSerializer,
                                      final String processorNodeId,
                                      final InternalProcessorContext<Void, Void> context,
                                      final StreamPartitioner<? super K1, ? super V1> partitioner) {
                throw new UnsupportedOperationException();
            }
        };

        final File stateDir = TestUtils.tempDirectory();
        //noinspection ResultOfMethodCallIgnored
        stateDir.mkdirs();
        stateSerdes = serdes;

        context = new InternalMockProcessorContext(stateDir, serdes.keySerde(), serdes.valueSerde(), recordCollector, null) {
            final ThreadCache cache = new ThreadCache(new LogContext("testCache "), 1024 * 1024L, metrics());

            @Override
            public ThreadCache cache() {
                return cache;
            }

            @Override
            public Map<String, Object> appConfigs() {
                return new StreamsConfig(props).originals();
            }

            @Override
            public Map<String, Object> appConfigsWithPrefix(final String prefix) {
                return new StreamsConfig(props).originalsWithPrefix(prefix);
            }
        };
    }

    private void recordFlushed(final K key, final V value) {
        if (value == null) {
            // This is a removal ...
            flushedRemovals.add(key);
            flushedEntries.remove(key);
        } else {
            // This is a normal add
            flushedEntries.put(key, value);
            flushedRemovals.remove(key);
        }
    }

    /**
     * Get the entries that are restored to a KeyValueStore when it is constructed with this driver's {@link #context()
     * ProcessorContext}.
     *
     * @return the restore entries; never null but possibly a null iterator
     */
    public Iterable<KeyValue<byte[], byte[]>> restoredEntries() {
        return restorableEntries;
    }

    /**
     * This method adds an entry to the "restore log" for the {@link KeyValueStore}, and is used <em>only</em> when testing the
     * restore functionality of a {@link KeyValueStore} implementation.
     * <p>
     * To create such a test, create the test driver, call this method one or more times, and then create the
     * {@link KeyValueStore}. Your tests can then check whether the store contains the entries from the log.
     *
     * <pre>
     * // Set up the driver and pre-populate the log ...
     * KeyValueStoreTestDriver&lt;Integer, String> driver = KeyValueStoreTestDriver.create();
     * driver.addRestoreEntry(1,"value1");
     * driver.addRestoreEntry(2,"value2");
     * driver.addRestoreEntry(3,"value3");
     *
     * // Create the store using the driver's context ...
     * ProcessorContext context = driver.context();
     * KeyValueStore&lt;Integer, String> store = ...
     *
     * // Verify that the store's contents were properly restored from the log ...
     * assertEquals(0, driver.checkForRestoredEntries(store));
     *
     * // and there are no other entries ...
     * assertEquals(3, driver.sizeOf(store));
     * </pre>
     *
     * @param key   the key for the entry
     * @param value the value for the entry
     * @see #checkForRestoredEntries(KeyValueStore)
     */
    public void addEntryToRestoreLog(final K key, final V value) {
        restorableEntries.add(new KeyValue<>(stateSerdes.rawKey(key), stateSerdes.rawValue(value)));
    }

    /**
     * Get the context that should be supplied to a {@link KeyValueStore}'s constructor. This context records any messages
     * written by the store to the Kafka topic, making them available via the {@link #flushedEntryStored(Object)} and
     * {@link #flushedEntryRemoved(Object)} methods.
     * <p>
     * If the {@link KeyValueStore}'s are to be restored upon its startup, be sure to {@link #addEntryToRestoreLog(Object, Object)
     * add the restore entries} before creating the store with the {@link ProcessorContext} returned by this method.
     *
     * @return the processing context; never null
     * @see #addEntryToRestoreLog(Object, Object)
     */
    public StateStoreContext context() {
        return context;
    }

    /**
     * Utility method that will count the number of {@link #addEntryToRestoreLog(Object, Object) restore entries} missing from the
     * supplied store.
     *
     * @param store the store that is to have all of the {@link #restoredEntries() restore entries}
     * @return the number of restore entries missing from the store, or 0 if all restore entries were found
     * @see #addEntryToRestoreLog(Object, Object)
     */
    public int checkForRestoredEntries(final KeyValueStore<K, V> store) {
        int missing = 0;
        for (final KeyValue<byte[], byte[]> kv : restorableEntries) {
            if (kv != null) {
                final V value = store.get(stateSerdes.keyFrom(kv.key));
                if (!Objects.equals(value, stateSerdes.valueFrom(kv.value))) {
                    ++missing;
                }
            }
        }
        return missing;
    }

    /**
     * Utility method to compute the number of entries within the store.
     *
     * @param store the key value store using this {@link #context()}.
     * @return the number of entries
     */
    public int sizeOf(final KeyValueStore<K, V> store) {
        int size = 0;
        try (final KeyValueIterator<K, V> iterator = store.all()) {
            while (iterator.hasNext()) {
                iterator.next();
                ++size;
            }
        }
        return size;
    }

    /**
     * Retrieve the value that the store {@link KeyValueStore#flush() flushed} with the given key.
     *
     * @param key the key
     * @return the value that was flushed with the key, or {@code null} if no such key was flushed or if the entry with this
     * key was removed upon flush
     */
    public V flushedEntryStored(final K key) {
        return flushedEntries.get(key);
    }

    /**
     * Determine whether the store {@link KeyValueStore#flush() flushed} the removal of the given key.
     *
     * @param key the key
     * @return {@code true} if the entry with the given key was removed when flushed, or {@code false} if the entry was not
     * removed when last flushed
     */
    public boolean flushedEntryRemoved(final K key) {
        return flushedRemovals.contains(key);
    }

    /**
     * Return number of removed entry
     */
    public int numFlushedEntryStored() {
        return flushedEntries.size();
    }

    /**
     * Return number of removed entry
     */
    public int numFlushedEntryRemoved() {
        return flushedRemovals.size();
    }

    /**
     * Remove all {@link #flushedEntryStored(Object) flushed entries}, {@link #flushedEntryRemoved(Object) flushed removals},
     */
    public void clear() {
        restorableEntries.clear();
        flushedEntries.clear();
        flushedRemovals.clear();
    }
}
