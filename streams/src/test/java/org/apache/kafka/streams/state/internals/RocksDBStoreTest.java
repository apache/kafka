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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.apache.kafka.test.MockProcessorContext;
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

    private RocksDBStore<String, String> subject;
    private MockProcessorContext context;
    private File dir;

    @Before
    public void setUp() {
        subject = new RocksDBStore<>("test", Serdes.String(), Serdes.String());
        dir = TestUtils.tempDirectory();
        context = new MockProcessorContext(dir,
            Serdes.String(),
            Serdes.String(),
            new NoOpRecordCollector(),
            new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics())));
    }

    @After
    public void tearDown() {
        subject.close();
    }

    @Test
    public void shouldNotThrowExceptionOnRestoreWhenThereIsPreExistingRocksDbFiles() throws Exception {
        subject.init(context, subject);

        final String message = "how can a 4 ounce bird carry a 2lb coconut";
        int intKey = 1;
        for (int i = 0; i < 2000000; i++) {
            subject.put("theKeyIs" + intKey++, message);
        }

        final List<KeyValue<byte[], byte[]>> restoreBytes = new ArrayList<>();

        final byte[] restoredKey = "restoredKey".getBytes("UTF-8");
        final byte[] restoredValue = "restoredValue".getBytes("UTF-8");
        restoreBytes.add(KeyValue.pair(restoredKey, restoredValue));

        context.restore("test", restoreBytes);

        assertThat(subject.get("restoredKey"), equalTo("restoredValue"));
    }

    @Test
    public void verifyRocksDbConfigSetterIsCalled() {
        final Map<String, Object> configs = new HashMap<>();
        configs.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-application");
        configs.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test-server:9092");
        configs.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, MockRocksDbConfigSetter.class);
        MockRocksDbConfigSetter.called = false;
        subject.openDB(new MockProcessorContext(tempDir, new StreamsConfig(configs)));

        assertTrue(MockRocksDbConfigSetter.called);
    }

    @Test(expected = ProcessorStateException.class)
    public void shouldThrowProcessorStateExceptionOnOpeningReadOnlyDir() throws IOException {
        final File tmpDir = TestUtils.tempDirectory();
        MockProcessorContext tmpContext = new MockProcessorContext(tmpDir,
            Serdes.String(),
            Serdes.Long(),
            new NoOpRecordCollector(),
            new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics())));
        tmpDir.setReadOnly();

        subject.openDB(tmpContext);
    }

    @Test
    public void shouldPutAll() {
        List<KeyValue<String, String>> entries = new ArrayList<>();
        entries.add(new KeyValue<>("1", "a"));
        entries.add(new KeyValue<>("2", "b"));
        entries.add(new KeyValue<>("3", "c"));

        subject.init(context, subject);
        subject.putAll(entries);
        subject.flush();

        assertEquals(subject.get("1"), "a");
        assertEquals(subject.get("2"), "b");
        assertEquals(subject.get("3"), "c");
    }

    @Test
    public void shouldTogglePrepareForBulkloadSetting() {
        subject.init(context, subject);
        RocksDBStore.RocksDBBatchingRestoreCallback restoreListener =
            (RocksDBStore.RocksDBBatchingRestoreCallback) subject.batchingStateRestoreCallback;

        restoreListener.onRestoreStart(null, null, 0, 0);
        assertTrue("Should have set bulk loading to true", subject.isPrepareForBulkload());

        restoreListener.onRestoreEnd(null, null, 0);
        assertFalse("Should have set bulk loading to false", subject.isPrepareForBulkload());
    }

    @Test
    public void shouldTogglePrepareForBulkloadSettingWhenPrexistingSstFiles() throws Exception {
        final List<KeyValue<byte[], byte[]>> entries = getKeyValueEntries();

        subject.init(context, subject);
        context.restore(subject.name(), entries);

        RocksDBStore.RocksDBBatchingRestoreCallback restoreListener =
            (RocksDBStore.RocksDBBatchingRestoreCallback) subject.batchingStateRestoreCallback;

        restoreListener.onRestoreStart(null, null, 0, 0);
        assertTrue("Should have not set bulk loading to true", subject.isPrepareForBulkload());

        restoreListener.onRestoreEnd(null, null, 0);
        assertFalse("Should have set bulk loading to false", subject.isPrepareForBulkload());
    }

    @Test
    public void shouldRestoreAll() throws Exception {
        final List<KeyValue<byte[], byte[]>> entries = getKeyValueEntries();

        subject.init(context, subject);
        context.restore(subject.name(), entries);

        assertEquals(subject.get("1"), "a");
        assertEquals(subject.get("2"), "b");
        assertEquals(subject.get("3"), "c");
    }


    @Test
    public void shouldHandleDeletesOnRestoreAll() throws Exception {
        final List<KeyValue<byte[], byte[]>> entries = getKeyValueEntries();
        entries.add(new KeyValue<>("1".getBytes("UTF-8"), (byte[]) null));

        subject.init(context, subject);
        context.restore(subject.name(), entries);

        final KeyValueIterator<String, String> iterator = subject.all();
        final Set<String> keys = new HashSet<>();

        while (iterator.hasNext()) {
            keys.add(iterator.next().key);
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

        subject.init(context, subject);
        context.restore(subject.name(), entries);

        final KeyValueIterator<String, String> iterator = subject.all();
        final Set<String> keys = new HashSet<>();

        while (iterator.hasNext()) {
            keys.add(iterator.next().key);
        }

        assertThat(keys, equalTo(Utils.mkSet("1", "2", "3")));

        assertEquals(subject.get("1"), "restored");
        assertEquals(subject.get("2"), "b");
        assertEquals(subject.get("3"), "c");
    }

    @Test
    public void shouldRestoreThenDeleteOnRestoreAll() throws Exception {
        final List<KeyValue<byte[], byte[]>> entries = getKeyValueEntries();

        subject.init(context, subject);

        context.restore(subject.name(), entries);

        assertEquals(subject.get("1"), "a");
        assertEquals(subject.get("2"), "b");
        assertEquals(subject.get("3"), "c");

        entries.clear();

        entries.add(new KeyValue<>("2".getBytes("UTF-8"), "b".getBytes("UTF-8")));
        entries.add(new KeyValue<>("3".getBytes("UTF-8"), "c".getBytes("UTF-8")));
        entries.add(new KeyValue<>("1".getBytes("UTF-8"), (byte[]) null));

        context.restore(subject.name(), entries);

        final KeyValueIterator<String, String> iterator = subject.all();
        final Set<String> keys = new HashSet<>();

        while (iterator.hasNext()) {
            keys.add(iterator.next().key);
        }

        assertThat(keys, equalTo(Utils.mkSet("2", "3")));
    }



    @Test
    public void shouldThrowNullPointerExceptionOnNullPut() {
        subject.init(context, subject);
        try {
            subject.put(null, "someVal");
            fail("Should have thrown NullPointerException on null put()");
        } catch (NullPointerException e) { }
    }

    @Test
    public void shouldThrowNullPointerExceptionOnNullPutAll() {
        subject.init(context, subject);
        try {
            subject.put(null, "someVal");
            fail("Should have thrown NullPointerException on null put()");
        } catch (NullPointerException e) { }
    }

    @Test
    public void shouldThrowNullPointerExceptionOnNullGet() {
        subject.init(context, subject);
        try {
            subject.get(null);
            fail("Should have thrown NullPointerException on null get()");
        } catch (NullPointerException e) { }
    }

    @Test
    public void shouldThrowNullPointerExceptionOnDelete() {
        subject.init(context, subject);
        try {
            subject.delete(null);
            fail("Should have thrown NullPointerException on deleting null key");
        } catch (NullPointerException e) { }
    }

    @Test
    public void shouldThrowNullPointerExceptionOnRange() {
        subject.init(context, subject);
        try {
            subject.range(null, "2");
            fail("Should have thrown NullPointerException on deleting null key");
        } catch (NullPointerException e) { }
    }

    @Test(expected = ProcessorStateException.class)
    public void shouldThrowProcessorStateExceptionOnPutDeletedDir() throws IOException {
        subject.init(context, subject);
        Utils.delete(dir);
        subject.put("anyKey", "anyValue");
        subject.flush();
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
