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

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.RestoreFunc;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.RecordCollector;
import org.apache.kafka.test.MockProcessorContext;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A component that provides a {@link #context() ProcessingContext} that can be supplied to a {@link KeyValueStore} so that
 * all entries written to the Kafka topic by the store during {@link KeyValueStore#flush()} are captured for testing purposes.
 * This class simplifies testing of various {@link KeyValueStore} instances, especially those that use
 * {@link MeteredKeyValueStore} to monitor and write its entries to the Kafka topic.
 * <p>
 * <h2>Basic usage</h2>
 * This component can be used to help test a {@link KeyValueStore}'s ability to read and write entries.
 * 
 * <pre>
 * &#47;&#47; Create the test driver ...
 * KeyValueStoreTestDriver&lt;Integer, String> driver = KeyValueStoreTestDriver.create();
 * InMemoryKeyValueStore&lt;Integer, String> store = InMemoryKeyValueStore.create("my-store", driver.context(),
 *                                                                             Integer.class, String.class);
 * 
 * &#47;&#47; Verify that the store reads and writes correctly ...
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
 * &#47;&#47; Flush the store and verify all current entries were properly flushed ...
 * store.flush();
 * assertEquals("zero", driver.flushedEntryStored(0));
 * assertEquals("one", driver.flushedEntryStored(1));
 * assertEquals("two", driver.flushedEntryStored(2));
 * assertEquals("four", driver.flushedEntryStored(4));
 * assertEquals(null, driver.flushedEntryStored(5));
 * 
 * assertEquals(false, driver.flushedEntryRemoved(0));
 * assertEquals(false, driver.flushedEntryRemoved(1));
 * assertEquals(false, driver.flushedEntryRemoved(2));
 * assertEquals(false, driver.flushedEntryRemoved(4));
 * assertEquals(true, driver.flushedEntryRemoved(5));
 * </pre>
 * 
 * <p>
 * <h2>Restoring a store</h2>
 * This component can be used to test whether a {@link KeyValueStore} implementation properly
 * {@link ProcessorContext#register(StateStore, RestoreFunc) registers itself} with the {@link ProcessorContext}. To do this,
 * create an instance of this driver component, {@link #addRestoreEntry(Object, Object) add entries} that will be passed to the
 * store upon creation, and then create the store using this driver's {@link #context() ProcessorContext}:
 * 
 * <pre>
 * &#47;&#47; Create the test driver ...
 * KeyValueStoreTestDriver&lt;Integer, String> driver = KeyValueStoreTestDriver.create(Integer.class, String.class);
 * 
 * &#47;&#47; Add any entries that will be restored to any store
 * &#47;&#47; that uses the driver's context ...
 * driver.addRestoreEntry(0, "zero");
 * driver.addRestoreEntry(1, "one");
 * driver.addRestoreEntry(2, "two");
 * driver.addRestoreEntry(4, "four");
 * 
 * &#47;&#47; Create the store, which should register with the context and automatically
 * &#47;&#47; receive the restore entries ...
 * InMemoryKeyValueStore&lt;Integer, String> store = InMemoryKeyValueStore.create("my-store", driver.context(),
 *                                                                             Integer.class, String.class);
 * 
 * &#47;&#47; Verify that the store's contents were properly restored ...
 * assertEquals(0, driver.checkForRestoredEntries(store));
 * 
 * &#47;&#47; and there are no other entries ...
 * assertEquals(4, driver.sizeOf(store));
 * </pre>
 * 
 * @param <K> the type of keys placed in the store
 * @param <V> the type of values placed in the store
 */
public class KeyValueStoreTestDriver<K, V> {

    private static <T> Serializer<T> unusableSerializer() {
        return new Serializer<T>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
            }

            @Override
            public byte[] serialize(String topic, T data) {
                throw new UnsupportedOperationException("This serializer should not be used");
            }

            @Override
            public void close() {
            }
        };
    };

    private static <T> Deserializer<T> unusableDeserializer() {
        return new Deserializer<T>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
            }

            @Override
            public T deserialize(String topic, byte[] data) {
                throw new UnsupportedOperationException("This serializer should not be used");
            }

            @Override
            public void close() {
            }
        };
    };

    /**
     * Create a driver object that will have a {@link #context()} that records messages
     * {@link ProcessorContext#forward(Object, Object) forwarded} by the store and that provides <em>unusable</em> default key and
     * value serializers and deserializers. This can be used when the actual serializers and deserializers are supplied to the
     * store during creation, which should eliminate the need for a store to depend on the ProcessorContext's default key and
     * value serializers and deserializers.
     * 
     * @return the test driver; never null
     */
    public static <K, V> KeyValueStoreTestDriver<K, V> create() {
        Serializer<K> keySerializer = unusableSerializer();
        Deserializer<K> keyDeserializer = unusableDeserializer();
        Serializer<V> valueSerializer = unusableSerializer();
        Deserializer<V> valueDeserializer = unusableDeserializer();
        Serdes<K, V> serdes = new Serdes<K, V>("unexpected", keySerializer, keyDeserializer, valueSerializer, valueDeserializer);
        return new KeyValueStoreTestDriver<K, V>(serdes);
    }

    /**
     * Create a driver object that will have a {@link #context()} that records messages
     * {@link ProcessorContext#forward(Object, Object) forwarded} by the store and that provides default serializers and
     * deserializers for the given built-in key and value types (e.g., {@code String.class}, {@code Integer.class},
     * {@code Long.class}, and {@code byte[].class}). This can be used when store is created to rely upon the
     * ProcessorContext's default key and value serializers and deserializers.
     * 
     * @param keyClass the class for the keys; must be one of {@code String.class}, {@code Integer.class},
     *            {@code Long.class}, or {@code byte[].class}
     * @param valueClass the class for the values; must be one of {@code String.class}, {@code Integer.class},
     *            {@code Long.class}, or {@code byte[].class}
     * @return the test driver; never null
     */
    public static <K, V> KeyValueStoreTestDriver<K, V> create(Class<K> keyClass, Class<V> valueClass) {
        Serdes<K, V> serdes = Serdes.withBuiltinTypes("unexpected", keyClass, valueClass);
        return new KeyValueStoreTestDriver<K, V>(serdes);
    }

    /**
     * Create a driver object that will have a {@link #context()} that records messages
     * {@link ProcessorContext#forward(Object, Object) forwarded} by the store and that provides the specified serializers and
     * deserializers. This can be used when store is created to rely upon the ProcessorContext's default key and value serializers
     * and deserializers.
     * 
     * @param keySerializer the key serializer for the {@link ProcessorContext}; may not be null
     * @param keyDeserializer the key deserializer for the {@link ProcessorContext}; may not be null
     * @param valueSerializer the value serializer for the {@link ProcessorContext}; may not be null
     * @param valueDeserializer the value deserializer for the {@link ProcessorContext}; may not be null
     * @return the test driver; never null
     */
    public static <K, V> KeyValueStoreTestDriver<K, V> create(Serializer<K> keySerializer,
                                                              Deserializer<K> keyDeserializer,
                                                              Serializer<V> valueSerializer,
                                                              Deserializer<V> valueDeserializer) {
        Serdes<K, V> serdes = new Serdes<K, V>("unexpected", keySerializer, keyDeserializer, valueSerializer, valueDeserializer);
        return new KeyValueStoreTestDriver<K, V>(serdes);
    }

    private final Serdes<K, V> serdes;
    private final Map<K, V> flushedEntries = new HashMap<>();
    private final Set<K> flushedRemovals = new HashSet<>();
    private final List<Entry<K, V>> restorableEntries = new LinkedList<>();
    private final MockProcessorContext context;
    private final Map<String, StateStore> storeMap = new HashMap<>();
    private final Metrics metrics = new Metrics();
    private final RecordCollector recordCollector;
    private File stateDir = new File("build/data").getAbsoluteFile();

    protected KeyValueStoreTestDriver(Serdes<K, V> serdes) {
        this.serdes = serdes;
        ByteArraySerializer rawSerializer = new ByteArraySerializer();
        Producer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(true, rawSerializer, rawSerializer);
        this.recordCollector = new RecordCollector(producer) {
            @Override
            public <K1, V1> void send(ProducerRecord<K1, V1> record, Serializer<K1> keySerializer, Serializer<V1> valueSerializer) {
                recordFlushed(record.key(), record.value());
            }
        };
        this.context = new MockProcessorContext(null, serdes.keySerializer(), serdes.keyDeserializer(), serdes.valueSerializer(),
                serdes.valueDeserializer(), recordCollector) {
            @Override
            public int id() {
                return 1;
            }

            @Override
            public <K1, V1> void forward(K1 key, V1 value, int childIndex) {
                forward(key, value);
            }

            @Override
            public void register(StateStore store, RestoreFunc func) {
                storeMap.put(store.name(), store);
                restoreEntries(func);
            }

            @Override
            public StateStore getStateStore(String name) {
                return storeMap.get(name);
            }

            @Override
            public Metrics metrics() {
                return metrics;
            }

            @Override
            public File stateDir() {
                if (stateDir == null) {
                    throw new UnsupportedOperationException("No state directory set");
                }
                stateDir.mkdirs();
                return stateDir;
            }
        };
    }

    /**
     * Set the directory that should be used by the store for local disk storage.
     * 
     * @param dir the directory; may be null if no local storage is allowed
     */
    public void useStateDir(File dir) {
        this.stateDir = dir;
    }

    @SuppressWarnings("unchecked")
    protected <K1, V1> void recordFlushed(K1 key, V1 value) {
        K k = (K) key;
        if (value == null) {
            // This is a removal ...
            flushedRemovals.add(k);
            flushedEntries.remove(k);
        } else {
            // This is a normal add
            flushedEntries.put(k, (V) value);
            flushedRemovals.remove(k);
        }
    }

    private void restoreEntries(RestoreFunc func) {
        for (Entry<K, V> entry : restorableEntries) {
            if (entry != null) {
                byte[] rawKey = serdes.rawKey(entry.key());
                byte[] rawValue = serdes.rawValue(entry.value());
                func.apply(rawKey, rawValue);
            }
        }
    }

    /**
     * This method adds an entry to the "restore log" for the {@link KeyValueStore}. This should be called <em>before</em>
     * creating
     * the {@link KeyValueStore} with the {@link #context() ProcessorContext}; when the {@link KeyValueStore} is created, it will
     * {@link ProcessorContext#register(StateStore, RestoreFunc) register} itself with the {@link #context() ProcessorContext},
     * and this object will then pre-populate the store with all restore entries added via this method.
     * 
     * @param key the key for the entry
     * @param value the value for the entry
     */
    public void addRestoreEntry(K key, V value) {
        restorableEntries.add(new Entry<K, V>(key, value));
    }

    /**
     * Get the context that should be supplied to a {@link KeyValueStore}'s constructor. This context records any messages
     * written by the store to the Kafka topic, making them available via the {@link #flushedEntryStored(Object)} and
     * {@link #flushedEntryRemoved(Object)} methods.
     * <p>
     * If the {@link KeyValueStore}'s are to be restored upon its startup, be sure to {@link #addRestoreEntry(Object, Object)
     * add the restore entries} before creating the store with the {@link ProcessorContext} returned by this method.
     * 
     * @return the processing context; never null
     * @see #addRestoreEntry(Object, Object)
     */
    public ProcessorContext context() {
        return context;
    }

    /**
     * Get the entries that are restored to a KeyValueStore when it is constructed with this driver's {@link #context()
     * ProcessorContext}.
     * 
     * @return the restore entries; never null but possibly a null iterator
     */
    public Iterable<Entry<K, V>> restoredEntries() {
        return restorableEntries;
    }

    /**
     * Utility method that will count the number of {@link #addRestoreEntry(Object, Object) restore entries} missing from the
     * supplied store.
     * 
     * @param store the store that is to have all of the {@link #restoredEntries() restore entries}
     * @return the number of restore entries missing from the store, or 0 if all restore entries were found
     */
    public int checkForRestoredEntries(KeyValueStore<K, V> store) {
        int missing = 0;
        for (Entry<K, V> entry : restorableEntries) {
            if (entry != null) {
                V value = store.get(entry.key());
                if (!Objects.equals(value, entry.value())) {
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
    public int sizeOf(KeyValueStore<K, V> store) {
        int size = 0;
        for (KeyValueIterator<K, V> iterator = store.all(); iterator.hasNext();) {
            iterator.next();
            ++size;
        }
        return size;
    }

    /**
     * Retrieve the value that the store {@link KeyValueStore#flush() flushed} with the given key.
     * 
     * @param key the key
     * @return the value that was flushed with the key, or {@code null} if no such key was flushed or if the entry with this
     *         key was {@link #flushedEntryStored(Object) removed} upon flush
     */
    public V flushedEntryStored(K key) {
        return flushedEntries.get(key);
    }

    /**
     * Determine whether the store {@link KeyValueStore#flush() flushed} the removal of the given key.
     * 
     * @param key the key
     * @return {@code true} if the entry with the given key was removed when flushed, or {@code false} if the entry was not
     *         removed when last flushed
     */
    public boolean flushedEntryRemoved(K key) {
        return flushedRemovals.contains(key);
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