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

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRocksDbConfigSetter;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.Filter;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.PlainTableConfig;
import org.rocksdb.Statistics;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.isNull;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.notNull;
import static org.easymock.EasyMock.reset;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.easymock.PowerMock.replay;
import static org.powermock.api.easymock.PowerMock.verify;

public class RocksDBStoreTest {
    private static boolean enableBloomFilters = false;
    final static String DB_NAME = "db-name";
    final static String METRICS_SCOPE = "metrics-scope";

    private File dir;
    private final Time time = new MockTime();
    private final Serializer<String> stringSerializer = new StringSerializer();
    private final Deserializer<String> stringDeserializer = new StringDeserializer();

    private final RocksDBMetricsRecorder metricsRecorder = mock(RocksDBMetricsRecorder.class);

    InternalMockProcessorContext context;
    RocksDBStore rocksDBStore;

    @Before
    public void setUp() {
        final Properties props = StreamsTestUtils.getStreamsConfig();
        props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, MockRocksDbConfigSetter.class);
        dir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext(
            dir,
            Serdes.String(),
            Serdes.String(),
            new StreamsConfig(props)
        );
        rocksDBStore = getRocksDBStore();
    }

    @After
    public void tearDown() {
        rocksDBStore.close();
    }

    RocksDBStore getRocksDBStore() {
        return new RocksDBStore(DB_NAME, METRICS_SCOPE);
    }

    private RocksDBStore getRocksDBStoreWithRocksDBMetricsRecorder() {
        return new RocksDBStore(DB_NAME, METRICS_SCOPE, metricsRecorder);
    }

    private InternalMockProcessorContext getProcessorContext(final Properties streamsProps) {
        return new InternalMockProcessorContext(
            TestUtils.tempDirectory(),
            new StreamsConfig(streamsProps)
        );
    }

    private InternalMockProcessorContext getProcessorContext(
        final RecordingLevel recordingLevel,
        final Class<? extends RocksDBConfigSetter> rocksDBConfigSetterClass) {

        final Properties streamsProps = StreamsTestUtils.getStreamsConfig();
        streamsProps.setProperty(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, recordingLevel.name());
        streamsProps.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, rocksDBConfigSetterClass);
        return getProcessorContext(streamsProps);
    }

    private InternalMockProcessorContext getProcessorContext(final RecordingLevel recordingLevel) {
        final Properties streamsProps = StreamsTestUtils.getStreamsConfig();
        streamsProps.setProperty(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, recordingLevel.name());
        return getProcessorContext(streamsProps);
    }

    @Test
    public void shouldAddValueProvidersWithoutStatisticsToInjectedMetricsRecorderWhenRecordingLevelInfo() {
        rocksDBStore = getRocksDBStoreWithRocksDBMetricsRecorder();
        context = getProcessorContext(RecordingLevel.INFO);
        reset(metricsRecorder);
        metricsRecorder.addValueProviders(eq(DB_NAME), notNull(), notNull(), isNull());
        replay(metricsRecorder);

        rocksDBStore.openDB(context.appConfigs(), context.stateDir());

        verify(metricsRecorder);
        reset(metricsRecorder);
    }

    @Test
    public void shouldAddValueProvidersWithStatisticsToInjectedMetricsRecorderWhenRecordingLevelDebug() {
        rocksDBStore = getRocksDBStoreWithRocksDBMetricsRecorder();
        context = getProcessorContext(RecordingLevel.DEBUG);
        reset(metricsRecorder);
        metricsRecorder.addValueProviders(eq(DB_NAME), notNull(), notNull(), notNull());
        replay(metricsRecorder);

        rocksDBStore.openDB(context.appConfigs(), context.stateDir());

        verify(metricsRecorder);
        reset(metricsRecorder);
    }

    @Test
    public void shouldRemoveValueProvidersFromInjectedMetricsRecorderOnClose() {
        rocksDBStore = getRocksDBStoreWithRocksDBMetricsRecorder();
        try {
            context = getProcessorContext(RecordingLevel.DEBUG);
            rocksDBStore.openDB(context.appConfigs(), context.stateDir());
            reset(metricsRecorder);
            metricsRecorder.removeValueProviders(DB_NAME);
            replay(metricsRecorder);
        } finally {
            rocksDBStore.close();
        }

        verify(metricsRecorder);
    }

    public static class RocksDBConfigSetterWithUserProvidedStatistics implements RocksDBConfigSetter {
        public RocksDBConfigSetterWithUserProvidedStatistics(){}

        public void setConfig(final String storeName, final Options options, final Map<String, Object> configs) {
            options.setStatistics(new Statistics());
        }

        public void close(final String storeName, final Options options) {
            options.statistics().close();
        }
    }

    @Test
    public void shouldNotSetStatisticsInValueProvidersWhenUserProvidesStatistics() {
        rocksDBStore = getRocksDBStoreWithRocksDBMetricsRecorder();
        context = getProcessorContext(RecordingLevel.DEBUG, RocksDBConfigSetterWithUserProvidedStatistics.class);
        metricsRecorder.addValueProviders(eq(DB_NAME), notNull(), notNull(), isNull());
        replay(metricsRecorder);

        rocksDBStore.openDB(context.appConfigs(), context.stateDir());
        verify(metricsRecorder);
        reset(metricsRecorder);
    }

    public static class RocksDBConfigSetterWithUserProvidedNewBlockBasedTableFormatConfig implements RocksDBConfigSetter {
        public RocksDBConfigSetterWithUserProvidedNewBlockBasedTableFormatConfig(){}

        public void setConfig(final String storeName, final Options options, final Map<String, Object> configs) {
            options.setTableFormatConfig(new BlockBasedTableConfig());
        }

        public void close(final String storeName, final Options options) {
            options.statistics().close();
        }
    }

    @Test
    public void shouldThrowWhenUserProvidesNewBlockBasedTableFormatConfig() {
        rocksDBStore = getRocksDBStoreWithRocksDBMetricsRecorder();
        context = getProcessorContext(
            RecordingLevel.DEBUG,
            RocksDBConfigSetterWithUserProvidedNewBlockBasedTableFormatConfig.class
        );
        assertThrows(
            "The used block-based table format configuration does not expose the " +
                "block cache. Use the BlockBasedTableConfig instance provided by Options#tableFormatConfig() to configure " +
                "the block-based table format of RocksDB. Do not provide a new instance of BlockBasedTableConfig to " +
                "the RocksDB options.",
            ProcessorStateException.class,
            () -> rocksDBStore.openDB(context.appConfigs(), context.stateDir())
        );
    }

    public static class RocksDBConfigSetterWithUserProvidedNewPlainTableFormatConfig implements RocksDBConfigSetter {
        public RocksDBConfigSetterWithUserProvidedNewPlainTableFormatConfig(){}

        public void setConfig(final String storeName, final Options options, final Map<String, Object> configs) {
            options.setTableFormatConfig(new PlainTableConfig());
        }

        public void close(final String storeName, final Options options) {
            options.statistics().close();
        }
    }

    @Test
    public void shouldNotSetCacheInValueProvidersWhenUserProvidesPlainTableFormatConfig() {
        rocksDBStore = getRocksDBStoreWithRocksDBMetricsRecorder();
        context = getProcessorContext(
            RecordingLevel.DEBUG,
            RocksDBConfigSetterWithUserProvidedNewPlainTableFormatConfig.class
        );
        metricsRecorder.addValueProviders(eq(DB_NAME), notNull(), isNull(), notNull());
        replay(metricsRecorder);

        rocksDBStore.openDB(context.appConfigs(), context.stateDir());
        verify(metricsRecorder);
        reset(metricsRecorder);
    }

    @Test
    public void shouldNotThrowExceptionOnRestoreWhenThereIsPreExistingRocksDbFiles() {
        rocksDBStore.init((StateStoreContext) context, rocksDBStore);
        rocksDBStore.put(new Bytes("existingKey".getBytes(UTF_8)), "existingValue".getBytes(UTF_8));
        rocksDBStore.flush();

        final List<KeyValue<byte[], byte[]>> restoreBytes = new ArrayList<>();

        final byte[] restoredKey = "restoredKey".getBytes(UTF_8);
        final byte[] restoredValue = "restoredValue".getBytes(UTF_8);
        restoreBytes.add(KeyValue.pair(restoredKey, restoredValue));

        context.restore(DB_NAME, restoreBytes);

        assertThat(
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "restoredKey")))),
            equalTo("restoredValue"));
    }

    @Test
    public void shouldCallRocksDbConfigSetter() {
        MockRocksDbConfigSetter.called = false;

        final Properties props = StreamsTestUtils.getStreamsConfig();
        props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, MockRocksDbConfigSetter.class);
        final Object param = new Object();
        props.put("abc.def", param);
        final InternalMockProcessorContext context = new InternalMockProcessorContext(
            dir,
            Serdes.String(),
            Serdes.String(),
            new StreamsConfig(props)
        );

        rocksDBStore.init((StateStoreContext) context, rocksDBStore);

        assertTrue(MockRocksDbConfigSetter.called);
        assertThat(MockRocksDbConfigSetter.configMap.get("abc.def"), equalTo(param));
    }

    @Test
    public void shouldThrowProcessorStateExceptionOnOpeningReadOnlyDir() {
        final File tmpDir = TestUtils.tempDirectory();
        final InternalMockProcessorContext tmpContext = new InternalMockProcessorContext(tmpDir, new StreamsConfig(StreamsTestUtils.getStreamsConfig()));

        assertTrue(tmpDir.setReadOnly());

        assertThrows(ProcessorStateException.class, () -> rocksDBStore.openDB(tmpContext.appConfigs(), tmpContext.stateDir()));
    }

    @Test
    public void shouldPutAll() {
        final List<KeyValue<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValue<>(
            new Bytes(stringSerializer.serialize(null, "1")),
            stringSerializer.serialize(null, "a")));
        entries.add(new KeyValue<>(
            new Bytes(stringSerializer.serialize(null, "2")),
            stringSerializer.serialize(null, "b")));
        entries.add(new KeyValue<>(
            new Bytes(stringSerializer.serialize(null, "3")),
            stringSerializer.serialize(null, "c")));

        rocksDBStore.init((StateStoreContext) context, rocksDBStore);
        rocksDBStore.putAll(entries);
        rocksDBStore.flush();

        assertEquals(
            "a",
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "1")))));
        assertEquals(
            "b",
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "2")))));
        assertEquals(
            "c",
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "3")))));
    }

    @Test
    public void shouldRestoreAll() {
        final List<KeyValue<byte[], byte[]>> entries = getKeyValueEntries();

        rocksDBStore.init((StateStoreContext) context, rocksDBStore);
        context.restore(rocksDBStore.name(), entries);

        assertEquals(
            "a",
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "1")))));
        assertEquals(
            "b",
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "2")))));
        assertEquals(
            "c",
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "3")))));
    }

    @Test
    public void shouldPutOnlyIfAbsentValue() {
        rocksDBStore.init((StateStoreContext) context, rocksDBStore);
        final Bytes keyBytes = new Bytes(stringSerializer.serialize(null, "one"));
        final byte[] valueBytes = stringSerializer.serialize(null, "A");
        final byte[] valueBytesUpdate = stringSerializer.serialize(null, "B");

        rocksDBStore.putIfAbsent(keyBytes, valueBytes);
        rocksDBStore.putIfAbsent(keyBytes, valueBytesUpdate);

        final String retrievedValue = stringDeserializer.deserialize(null, rocksDBStore.get(keyBytes));
        assertEquals("A", retrievedValue);
    }

    @Test
    public void shouldHandleDeletesOnRestoreAll() {
        final List<KeyValue<byte[], byte[]>> entries = getKeyValueEntries();
        entries.add(new KeyValue<>("1".getBytes(UTF_8), null));

        rocksDBStore.init((StateStoreContext) context, rocksDBStore);
        context.restore(rocksDBStore.name(), entries);

        final KeyValueIterator<Bytes, byte[]> iterator = rocksDBStore.all();
        final Set<String> keys = new HashSet<>();

        while (iterator.hasNext()) {
            keys.add(stringDeserializer.deserialize(null, iterator.next().key.get()));
        }

        assertThat(keys, equalTo(Utils.mkSet("2", "3")));
    }

    @Test
    public void shouldHandleDeletesAndPutBackOnRestoreAll() {
        final List<KeyValue<byte[], byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValue<>("1".getBytes(UTF_8), "a".getBytes(UTF_8)));
        entries.add(new KeyValue<>("2".getBytes(UTF_8), "b".getBytes(UTF_8)));
        // this will be deleted
        entries.add(new KeyValue<>("1".getBytes(UTF_8), null));
        entries.add(new KeyValue<>("3".getBytes(UTF_8), "c".getBytes(UTF_8)));
        // this will restore key "1" as WriteBatch applies updates in order
        entries.add(new KeyValue<>("1".getBytes(UTF_8), "restored".getBytes(UTF_8)));

        rocksDBStore.init((StateStoreContext) context, rocksDBStore);
        context.restore(rocksDBStore.name(), entries);

        final KeyValueIterator<Bytes, byte[]> iterator = rocksDBStore.all();
        final Set<String> keys = new HashSet<>();

        while (iterator.hasNext()) {
            keys.add(stringDeserializer.deserialize(null, iterator.next().key.get()));
        }

        assertThat(keys, equalTo(Utils.mkSet("1", "2", "3")));

        assertEquals(
            "restored",
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "1")))));
        assertEquals(
            "b",
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "2")))));
        assertEquals(
            "c",
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "3")))));
    }

    @Test
    public void shouldRestoreThenDeleteOnRestoreAll() {
        final List<KeyValue<byte[], byte[]>> entries = getKeyValueEntries();

        rocksDBStore.init((StateStoreContext) context, rocksDBStore);

        context.restore(rocksDBStore.name(), entries);

        assertEquals(
            "a",
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "1")))));
        assertEquals(
            "b",
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "2")))));
        assertEquals(
            "c",
            stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "3")))));

        entries.clear();

        entries.add(new KeyValue<>("2".getBytes(UTF_8), "b".getBytes(UTF_8)));
        entries.add(new KeyValue<>("3".getBytes(UTF_8), "c".getBytes(UTF_8)));
        entries.add(new KeyValue<>("1".getBytes(UTF_8), null));

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
        rocksDBStore.init((StateStoreContext) context, rocksDBStore);
        assertThrows(
            NullPointerException.class,
            () -> rocksDBStore.put(null, stringSerializer.serialize(null, "someVal")));
    }

    @Test
    public void shouldThrowNullPointerExceptionOnNullPutAll() {
        rocksDBStore.init((StateStoreContext) context, rocksDBStore);
        assertThrows(
            NullPointerException.class,
            () -> rocksDBStore.put(null, stringSerializer.serialize(null, "someVal")));
    }

    @Test
    public void shouldThrowNullPointerExceptionOnNullGet() {
        rocksDBStore.init((StateStoreContext) context, rocksDBStore);
        assertThrows(
            NullPointerException.class,
            () -> rocksDBStore.get(null));
    }

    @Test
    public void shouldThrowNullPointerExceptionOnDelete() {
        rocksDBStore.init((StateStoreContext) context, rocksDBStore);
        assertThrows(
            NullPointerException.class,
            () -> rocksDBStore.delete(null));
    }

    @Test
    public void shouldThrowNullPointerExceptionOnRange() {
        rocksDBStore.init((StateStoreContext) context, rocksDBStore);
        assertThrows(
            NullPointerException.class,
            () -> rocksDBStore.range(null, new Bytes(stringSerializer.serialize(null, "2"))));
    }

    @Test
    public void shouldThrowProcessorStateExceptionOnPutDeletedDir() throws IOException {
        rocksDBStore.init((StateStoreContext) context, rocksDBStore);
        Utils.delete(dir);
        rocksDBStore.put(
            new Bytes(stringSerializer.serialize(null, "anyKey")),
            stringSerializer.serialize(null, "anyValue"));
        assertThrows(ProcessorStateException.class, () -> rocksDBStore.flush());
    }

    @Test
    public void shouldHandleToggleOfEnablingBloomFilters() {
        final Properties props = StreamsTestUtils.getStreamsConfig();
        props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, TestingBloomFilterRocksDBConfigSetter.class);
        dir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext(dir,
            Serdes.String(),
            Serdes.String(),
            new StreamsConfig(props));

        enableBloomFilters = false;
        rocksDBStore.init((StateStoreContext) context, rocksDBStore);

        final List<String> expectedValues = new ArrayList<>();
        expectedValues.add("a");
        expectedValues.add("b");
        expectedValues.add("c");

        final List<KeyValue<byte[], byte[]>> keyValues = getKeyValueEntries();
        for (final KeyValue<byte[], byte[]> keyValue : keyValues) {
            rocksDBStore.put(new Bytes(keyValue.key), keyValue.value);
        }

        int expectedIndex = 0;
        for (final KeyValue<byte[], byte[]> keyValue : keyValues) {
            final byte[] valBytes = rocksDBStore.get(new Bytes(keyValue.key));
            assertThat(new String(valBytes, UTF_8), is(expectedValues.get(expectedIndex++)));
        }
        assertFalse(TestingBloomFilterRocksDBConfigSetter.bloomFiltersSet);

        rocksDBStore.close();
        expectedIndex = 0;

        // reopen with Bloom Filters enabled
        // should open fine without errors
        enableBloomFilters = true;
        rocksDBStore.init((StateStoreContext) context, rocksDBStore);

        for (final KeyValue<byte[], byte[]> keyValue : keyValues) {
            final byte[] valBytes = rocksDBStore.get(new Bytes(keyValue.key));
            assertThat(new String(valBytes, UTF_8), is(expectedValues.get(expectedIndex++)));
        }

        assertTrue(TestingBloomFilterRocksDBConfigSetter.bloomFiltersSet);
    }

    @Test
    public void shouldVerifyThatMetricsRecordedFromStatisticsGetMeasurementsFromRocksDB() {
        final TaskId taskId = new TaskId(0, 0);

        final Metrics metrics = new Metrics(new MetricConfig().recordLevel(RecordingLevel.DEBUG));
        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, "test-application", StreamsConfig.METRICS_LATEST, time);

        context = EasyMock.niceMock(InternalMockProcessorContext.class);
        EasyMock.expect(context.metrics()).andStubReturn(streamsMetrics);
        EasyMock.expect(context.taskId()).andStubReturn(taskId);
        EasyMock.expect(context.appConfigs())
            .andStubReturn(new StreamsConfig(StreamsTestUtils.getStreamsConfig()).originals());
        EasyMock.expect(context.stateDir()).andStubReturn(dir);
        EasyMock.replay(context);

        rocksDBStore.init((StateStoreContext) context, rocksDBStore);
        final byte[] key = "hello".getBytes();
        final byte[] value = "world".getBytes();
        rocksDBStore.put(Bytes.wrap(key), value);

        streamsMetrics.rocksDBMetricsRecordingTrigger().run();

        final Metric bytesWrittenTotal = metrics.metric(new MetricName(
            "bytes-written-total",
            StreamsMetricsImpl.STATE_STORE_LEVEL_GROUP,
            "description is not verified",
            streamsMetrics.storeLevelTagMap(taskId.toString(), METRICS_SCOPE, DB_NAME)
        ));
        assertThat((double) bytesWrittenTotal.metricValue(), greaterThan(0d));
    }

    @Test
    public void shouldVerifyThatMetricsRecordedFromPropertiesGetMeasurementsFromRocksDB() {
        final TaskId taskId = new TaskId(0, 0);

        final Metrics metrics = new Metrics(new MetricConfig().recordLevel(RecordingLevel.INFO));
        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, "test-application", StreamsConfig.METRICS_LATEST, time);

        context = EasyMock.niceMock(InternalMockProcessorContext.class);
        EasyMock.expect(context.metrics()).andStubReturn(streamsMetrics);
        EasyMock.expect(context.taskId()).andStubReturn(taskId);
        EasyMock.expect(context.appConfigs())
                .andStubReturn(new StreamsConfig(StreamsTestUtils.getStreamsConfig()).originals());
        EasyMock.expect(context.stateDir()).andStubReturn(dir);
        EasyMock.replay(context);

        rocksDBStore.init((StateStoreContext) context, rocksDBStore);
        final byte[] key = "hello".getBytes();
        final byte[] value = "world".getBytes();
        rocksDBStore.put(Bytes.wrap(key), value);

        final Metric numberOfEntriesActiveMemTable = metrics.metric(new MetricName(
            "num-entries-active-mem-table",
            StreamsMetricsImpl.STATE_STORE_LEVEL_GROUP,
            "description is not verified",
            streamsMetrics.storeLevelTagMap(taskId.toString(), METRICS_SCOPE, DB_NAME)
        ));
        assertThat(numberOfEntriesActiveMemTable, notNullValue());
        assertThat((BigInteger) numberOfEntriesActiveMemTable.metricValue(), greaterThan(BigInteger.valueOf(0)));
    }

    @Test
    public void shouldVerifyThatPropertyBasedMetricsUseValidPropertyName() {
        final TaskId taskId = new TaskId(0, 0);

        final Metrics metrics = new Metrics(new MetricConfig().recordLevel(RecordingLevel.INFO));
        final StreamsMetricsImpl streamsMetrics =
            new StreamsMetricsImpl(metrics, "test-application", StreamsConfig.METRICS_LATEST, time);

        final Properties props = StreamsTestUtils.getStreamsConfig();
        context = EasyMock.niceMock(InternalMockProcessorContext.class);
        EasyMock.expect(context.metrics()).andStubReturn(streamsMetrics);
        EasyMock.expect(context.taskId()).andStubReturn(taskId);
        EasyMock.expect(context.appConfigs()).andStubReturn(new StreamsConfig(props).originals());
        EasyMock.expect(context.stateDir()).andStubReturn(dir);
        EasyMock.replay(context);

        rocksDBStore.init((StateStoreContext) context, rocksDBStore);

        final List<String> propertyNames = Arrays.asList(
            "num-entries-active-mem-table",
            "num-deletes-active-mem-table",
            "num-entries-imm-mem-tables",
            "num-deletes-imm-mem-tables",
            "num-immutable-mem-table",
            "cur-size-active-mem-table",
            "cur-size-all-mem-tables",
            "size-all-mem-tables",
            "mem-table-flush-pending",
            "num-running-flushes",
            "compaction-pending",
            "num-running-compactions",
            "estimate-pending-compaction-bytes",
            "total-sst-files-size",
            "live-sst-files-size",
            "num-live-versions",
            "block-cache-capacity",
            "block-cache-usage",
            "block-cache-pinned-usage",
            "estimate-num-keys",
            "estimate-table-readers-mem",
            "background-errors"
        );
        for (final String propertyname : propertyNames) {
            final Metric metric = metrics.metric(new MetricName(
                propertyname,
                StreamsMetricsImpl.STATE_STORE_LEVEL_GROUP,
                "description is not verified",
                streamsMetrics.storeLevelTagMap(taskId.toString(), METRICS_SCOPE, DB_NAME)
            ));
            assertThat("Metric " + propertyname + " not found!", metric, notNullValue());
            metric.metricValue();
        }
    }

    public static class TestingBloomFilterRocksDBConfigSetter implements RocksDBConfigSetter {

        static boolean bloomFiltersSet;
        static Filter filter;
        static Cache cache;

        @Override
        public void setConfig(final String storeName, final Options options, final Map<String, Object> configs) {
            final BlockBasedTableConfig tableConfig = (BlockBasedTableConfig) options.tableFormatConfig();
            cache = new LRUCache(50 * 1024 * 1024L);
            tableConfig.setBlockCache(cache);
            tableConfig.setBlockSize(4096L);
            if (enableBloomFilters) {
                filter = new BloomFilter();
                tableConfig.setFilter(filter);
                options.optimizeFiltersForHits();
                bloomFiltersSet = true;
            } else {
                options.setOptimizeFiltersForHits(false);
                bloomFiltersSet = false;
            }

            options.setTableFormatConfig(tableConfig);
        }

        @Override
        public void close(final String storeName, final Options options) {
            if (filter != null) {
                filter.close();
            }
            cache.close();
        }
    }

    private List<KeyValue<byte[], byte[]>> getKeyValueEntries() {
        final List<KeyValue<byte[], byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValue<>("1".getBytes(UTF_8), "a".getBytes(UTF_8)));
        entries.add(new KeyValue<>("2".getBytes(UTF_8), "b".getBytes(UTF_8)));
        entries.add(new KeyValue<>("3".getBytes(UTF_8), "c".getBytes(UTF_8)));
        return entries;
    }

}
