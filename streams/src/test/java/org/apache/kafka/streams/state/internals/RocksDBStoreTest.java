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

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.NoOpRecordCollector;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.Options;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RocksDBStoreTest {
    private final File tempDir = TestUtils.tempDirectory();

    private Serializer<String> stringSerializer = new StringSerializer();
    private Deserializer<String> stringDeserializer = new StringDeserializer();
    private RocksDBStore rocksDBStore;
    private InternalMockProcessorContext context;
    private File dir;

    @Before
    public void setUp() {
        rocksDBStore = new RocksDBStore("test");
        dir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext(dir,
            Serdes.String(),
            Serdes.String(),
            new NoOpRecordCollector(),
            new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics())));
    }

    @After
    public void tearDown() {
        rocksDBStore.close();
    }

    @Test
    public void shouldNotThrowExceptionOnRestoreWhenThereIsPreExistingRocksDbFiles() throws Exception {
        rocksDBStore.init(context, rocksDBStore);

        final String message = "how can a 4 ounce bird carry a 2lb coconut";
        int intKey = 1;
        for (int i = 0; i < 2000000; i++) {
            rocksDBStore.put(new Bytes(stringSerializer.serialize(null, "theKeyIs" + intKey++)),
                             stringSerializer.serialize(null, message));
        }

        final List<KeyValue<byte[], byte[]>> restoreBytes = new ArrayList<>();

        final byte[] restoredKey = "restoredKey".getBytes("UTF-8");
        final byte[] restoredValue = "restoredValue".getBytes("UTF-8");
        restoreBytes.add(KeyValue.pair(restoredKey, restoredValue));

        context.restore("test", restoreBytes);

        assertThat(
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "restoredKey")))),
            equalTo("restoredValue"));
    }

    @Test
    public void verifyRocksDbConfigSetterIsCalled() {
        final Map<String, Object> configs = new HashMap<>();
        configs.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-application");
        configs.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test-server:9092");
        configs.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, MockRocksDbConfigSetter.class);
        MockRocksDbConfigSetter.called = false;
        rocksDBStore.openDB(new InternalMockProcessorContext(tempDir, new StreamsConfig(configs)));

        assertTrue(MockRocksDbConfigSetter.called);
    }

    @Test(expected = ProcessorStateException.class)
    public void shouldThrowProcessorStateExceptionOnOpeningReadOnlyDir() throws IOException {
        final File tmpDir = TestUtils.tempDirectory();
        InternalMockProcessorContext tmpContext = new InternalMockProcessorContext(tmpDir,
            Serdes.String(),
            Serdes.Long(),
            new NoOpRecordCollector(),
            new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics())));
        tmpDir.setReadOnly();

        rocksDBStore.openDB(tmpContext);
    }

    @Test
    public void shouldPutAll() {
        List<KeyValue<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValue<>(
            new Bytes(stringSerializer.serialize(null, "1")),
            stringSerializer.serialize(null, "a")));
        entries.add(new KeyValue<>(
            new Bytes(stringSerializer.serialize(null, "2")),
            stringSerializer.serialize(null, "b")));
        entries.add(new KeyValue<>(
            new Bytes(stringSerializer.serialize(null, "3")),
            stringSerializer.serialize(null, "c")));

        rocksDBStore.init(context, rocksDBStore);
        rocksDBStore.putAll(entries);
        rocksDBStore.flush();

        assertEquals(
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "1")))),
            "a");
        assertEquals(
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "2")))),
            "b");
        assertEquals(
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "3")))),
            "c");
    }

    @Test
    public void shouldTogglePrepareForBulkloadSetting() {
        rocksDBStore.init(context, rocksDBStore);
        RocksDBStore.RocksDBBatchingRestoreCallback restoreListener =
            (RocksDBStore.RocksDBBatchingRestoreCallback) rocksDBStore.batchingStateRestoreCallback;

        restoreListener.onRestoreStart(null, null, 0, 0);
        assertTrue("Should have set bulk loading to true", rocksDBStore.isPrepareForBulkload());

        restoreListener.onRestoreEnd(null, null, 0);
        assertFalse("Should have set bulk loading to false", rocksDBStore.isPrepareForBulkload());
    }

    @Test
    public void shouldTogglePrepareForBulkloadSettingWhenPrexistingSstFiles() throws Exception {
        final List<KeyValue<byte[], byte[]>> entries = getKeyValueEntries();

        rocksDBStore.init(context, rocksDBStore);
        context.restore(rocksDBStore.name(), entries);

        RocksDBStore.RocksDBBatchingRestoreCallback restoreListener =
            (RocksDBStore.RocksDBBatchingRestoreCallback) rocksDBStore.batchingStateRestoreCallback;

        restoreListener.onRestoreStart(null, null, 0, 0);
        assertTrue("Should have not set bulk loading to true", rocksDBStore.isPrepareForBulkload());

        restoreListener.onRestoreEnd(null, null, 0);
        assertFalse("Should have set bulk loading to false", rocksDBStore.isPrepareForBulkload());
    }

    @Test
    public void shouldRestoreAll() throws Exception {
        final List<KeyValue<byte[], byte[]>> entries = getKeyValueEntries();

        rocksDBStore.init(context, rocksDBStore);
        context.restore(rocksDBStore.name(), entries);

        assertEquals(
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "1")))),
            "a");
        assertEquals(
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "2")))),
            "b");
        assertEquals(
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "3")))),
            "c");
    }

    @Test
    public void shouldPutOnlyIfAbsentValue() throws Exception {
        rocksDBStore.init(context, rocksDBStore);
        final Bytes keyBytes = new Bytes(stringSerializer.serialize(null, "one"));
        final byte[] valueBytes = stringSerializer.serialize(null, "A");
        final byte[] valueBytesUpdate = stringSerializer.serialize(null, "B");

        rocksDBStore.putIfAbsent(keyBytes, valueBytes);
        rocksDBStore.putIfAbsent(keyBytes, valueBytesUpdate);

        final String retrievedValue = stringDeserializer.deserialize(null, rocksDBStore.get(keyBytes));
        assertEquals(retrievedValue, "A");
    }

    @Test
    public void shouldHandleDeletesOnRestoreAll() throws Exception {
        final List<KeyValue<byte[], byte[]>> entries = getKeyValueEntries();
        entries.add(new KeyValue<>("1".getBytes("UTF-8"), (byte[]) null));

        rocksDBStore.init(context, rocksDBStore);
        context.restore(rocksDBStore.name(), entries);

        final KeyValueIterator<Bytes, byte[]> iterator = rocksDBStore.all();
        final Set<String> keys = new HashSet<>();

        while (iterator.hasNext()) {
            keys.add(stringDeserializer.deserialize(null, iterator.next().key.get()));
        }

        assertThat(keys, equalTo(Utils.mkSet("2", "3")));
    }

    @Test
    public void shouldHandleDeletesAndPutbackOnRestoreAll() throws Exception {
        final List<KeyValue<byte[], byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValue<>("1".getBytes("UTF-8"), "a".getBytes("UTF-8")));
        entries.add(new KeyValue<>("2".getBytes("UTF-8"), "b".getBytes("UTF-8")));
        // this will be deleted
        entries.add(new KeyValue<>("1".getBytes("UTF-8"), (byte[]) null));
        entries.add(new KeyValue<>("3".getBytes("UTF-8"), "c".getBytes("UTF-8")));
        // this will restore key "1" as WriteBatch applies updates in order
        entries.add(new KeyValue<>("1".getBytes("UTF-8"), "restored".getBytes("UTF-8")));

        rocksDBStore.init(context, rocksDBStore);
        context.restore(rocksDBStore.name(), entries);

        final KeyValueIterator<Bytes, byte[]> iterator = rocksDBStore.all();
        final Set<String> keys = new HashSet<>();

        while (iterator.hasNext()) {
            keys.add(stringDeserializer.deserialize(null, iterator.next().key.get()));
        }

        assertThat(keys, equalTo(Utils.mkSet("1", "2", "3")));

        assertEquals(
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "1")))),
            "restored");
        assertEquals(
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "2")))),
            "b");
        assertEquals(
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "3")))),
            "c");
    }

    @Test
    public void shouldRestoreThenDeleteOnRestoreAll() throws Exception {
        final List<KeyValue<byte[], byte[]>> entries = getKeyValueEntries();

        rocksDBStore.init(context, rocksDBStore);

        context.restore(rocksDBStore.name(), entries);

        assertEquals(
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "1")))),
            "a");
        assertEquals(
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "2")))),
            "b");
        assertEquals(
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "3")))),
            "c");

        entries.clear();

        entries.add(new KeyValue<>("2".getBytes("UTF-8"), "b".getBytes("UTF-8")));
        entries.add(new KeyValue<>("3".getBytes("UTF-8"), "c".getBytes("UTF-8")));
        entries.add(new KeyValue<>("1".getBytes("UTF-8"), (byte[]) null));

        context.restore(rocksDBStore.name(), entries);

        final KeyValueIterator<Bytes, byte[]> iterator = rocksDBStore.all();
        final Set<String> keys = new HashSet<>();

        while (iterator.hasNext()) {
            keys.add(stringDeserializer.deserialize(null, iterator.next().key.get()));
        }

        assertThat(keys, equalTo(Utils.mkSet("2", "3")));
    }



    @Test
    public void shouldThrowNullPointerExceptionOnNullPut() {
        rocksDBStore.init(context, rocksDBStore);
        try {
            rocksDBStore.put(null, stringSerializer.serialize(null, "someVal"));
            fail("Should have thrown NullPointerException on null put()");
        } catch (NullPointerException e) { }
    }

    @Test
    public void shouldThrowNullPointerExceptionOnNullPutAll() {
        rocksDBStore.init(context, rocksDBStore);
        try {
            rocksDBStore.put(null, stringSerializer.serialize(null, "someVal"));
            fail("Should have thrown NullPointerException on null put()");
        } catch (NullPointerException e) { }
    }

    @Test
    public void shouldThrowNullPointerExceptionOnNullGet() {
        rocksDBStore.init(context, rocksDBStore);
        try {
            rocksDBStore.get(null);
            fail("Should have thrown NullPointerException on null get()");
        } catch (NullPointerException e) { }
    }

    @Test
    public void shouldThrowNullPointerExceptionOnDelete() {
        rocksDBStore.init(context, rocksDBStore);
        try {
            rocksDBStore.delete(null);
            fail("Should have thrown NullPointerException on deleting null key");
        } catch (NullPointerException e) { }
    }

    @Test
    public void shouldThrowNullPointerExceptionOnRange() {
        rocksDBStore.init(context, rocksDBStore);
        try {
            rocksDBStore.range(null, new Bytes(stringSerializer.serialize(null, "2")));
            fail("Should have thrown NullPointerException on deleting null key");
        } catch (NullPointerException e) { }
    }

    @Test(expected = ProcessorStateException.class)
    public void shouldThrowProcessorStateExceptionOnPutDeletedDir() throws IOException {
        rocksDBStore.init(context, rocksDBStore);
        Utils.delete(dir);
        rocksDBStore.put(
            new Bytes(stringSerializer.serialize(null, "anyKey")),
            stringSerializer.serialize(null, "anyValue"));
        rocksDBStore.flush();
    }

    public static class MockRocksDbConfigSetter implements RocksDBConfigSetter {
        static boolean called;

        @Override
        public void setConfig(final String storeName, final Options options, final Map<String, Object> configs) {
            called = true;
        }
    }

    private List<KeyValue<byte[], byte[]>> getKeyValueEntries() throws UnsupportedEncodingException {
        final List<KeyValue<byte[], byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValue<>("1".getBytes("UTF-8"), "a".getBytes("UTF-8")));
        entries.add(new KeyValue<>("2".getBytes("UTF-8"), "b".getBytes("UTF-8")));
        entries.add(new KeyValue<>("3".getBytes("UTF-8"), "c".getBytes("UTF-8")));
        return entries;
    }
}
