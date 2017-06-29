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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.AbstractProcessorContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.NoOpRecordCollector;
import org.apache.kafka.test.TestUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.io.IOException;
import org.rocksdb.Options;

public class RocksDBStoreTest {
    private final File tempDir = TestUtils.tempDirectory();

    private RocksDBStore<String, String> subject;
    private MockProcessorContext context;
    private File dir;

    @Before
    public void setUp() throws Exception {
        subject = new RocksDBStore<>("test", Serdes.String(), Serdes.String());
        dir = TestUtils.tempDirectory();
        context = new MockProcessorContext(dir,
            Serdes.String(),
            Serdes.String(),
            new NoOpRecordCollector(),
            new ThreadCache("testCache", 0, new MockStreamsMetrics(new Metrics())));
    }

    @After
    public void tearDown() throws Exception {
        subject.close();
    }

    @Test
    public void verifyRocksDbConfigSetterIsCalled() throws Exception {
        final Map<String, Object> configs = new HashMap<>();
        configs.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-application");
        configs.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "test-server:9092");
        configs.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, MockRocksDbConfigSetter.class);
        MockRocksDbConfigSetter.called = false;
        subject.openDB(new ConfigurableProcessorContext(new StreamsConfig(configs), tempDir));

        assertTrue(MockRocksDbConfigSetter.called);
    }

    @Test(expected = ProcessorStateException.class)
    public void shouldThrowProcessorStateExceptionOnOpeningReadOnlyDir() throws IOException {
        final File tmpDir = TestUtils.tempDirectory();
        MockProcessorContext tmpContext = new MockProcessorContext(tmpDir,
            Serdes.String(),
            Serdes.Long(),
            new NoOpRecordCollector(),
            new ThreadCache("testCache", 0, new MockStreamsMetrics(new Metrics())));
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


    private static class ConfigurableProcessorContext extends AbstractProcessorContext {
        private final File stateDir;

        ConfigurableProcessorContext(StreamsConfig config, File stateDir) {
            super(null, null, config, null, null, null);
            this.stateDir = stateDir;
        }

        @Override
        public File stateDir() {
            return stateDir;
        }

        @Override
        public StateStore getStateStore(String name) {
            return null;
        }

        @Override
        public Cancellable schedule(long interval, PunctuationType type, Punctuator callback) {
            return null;
        }

        @Override
        public void schedule(long interval) { }

        @Override
        public <K, V> void forward(K key, V value) { }

        @Override
        public <K, V> void forward(K key, V value, int childIndex) { }

        @Override
        public <K, V> void forward(K key, V value, String childName) { }

        @Override
        public void commit() { }
    }
}
