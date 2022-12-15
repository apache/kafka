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

import java.lang.reflect.Field;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
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
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.ChangelogRecordDeserializationHelper;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRocksDbConfigSetter;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class RocksDBStoreTest extends AbstractKeyValueStoreTest {
    private static boolean enableBloomFilters = false;
    final static String DB_NAME = "db-name";
    final static String METRICS_SCOPE = "metrics-scope";

    private File dir;
    private final Time time = new MockTime();
    private final Serializer<String> stringSerializer = new StringSerializer();
    private final Deserializer<String> stringDeserializer = new StringDeserializer();

    @Mock
    private RocksDBMetricsRecorder metricsRecorder;

    InternalMockProcessorContext context;
    RocksDBStore rocksDBStore;

    @Before
    public void setUp() {
        final Properties props = StreamsTestUtils.getStreamsConfig();
        props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, MockRocksDbConfigSetter.class);
        dir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext<>(
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

    @Override
    protected <K, V> KeyValueStore<K, V> createKeyValueStore(final StateStoreContext context) {
        final StoreBuilder<KeyValueStore<K, V>> storeBuilder = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore("my-store"),
            (Serde<K>) context.keySerde(),
            (Serde<V>) context.valueSerde());

        final KeyValueStore<K, V> store = storeBuilder.build();
        store.init(context, store);
        return store;
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

        rocksDBStore.openDB(context.appConfigs(), context.stateDir());

        verify(metricsRecorder).addValueProviders(eq(DB_NAME), notNull(), notNull(), isNull());
    }

    @Test
    public void shouldAddValueProvidersWithStatisticsToInjectedMetricsRecorderWhenRecordingLevelDebug() {
        rocksDBStore = getRocksDBStoreWithRocksDBMetricsRecorder();
        context = getProcessorContext(RecordingLevel.DEBUG);

        rocksDBStore.openDB(context.appConfigs(), context.stateDir());

        verify(metricsRecorder).addValueProviders(eq(DB_NAME), notNull(), notNull(), notNull());
    }

    @Test
    public void shouldRemoveValueProvidersFromInjectedMetricsRecorderOnClose() {
        rocksDBStore = getRocksDBStoreWithRocksDBMetricsRecorder();
        try {
            context = getProcessorContext(RecordingLevel.DEBUG);
            rocksDBStore.openDB(context.appConfigs(), context.stateDir());
        } finally {
            rocksDBStore.close();
        }

        verify(metricsRecorder).removeValueProviders(DB_NAME);
    }

    public static class RocksDBConfigSetterWithUserProvidedStatistics implements RocksDBConfigSetter {
        public RocksDBConfigSetterWithUserProvidedStatistics(){}

        public void setConfig(final String storeName, final Options options, final Map<String, Object> configs) {
            lastStatistics = new Statistics();
            options.setStatistics(lastStatistics);
        }

        public void close(final String storeName, final Options options) {
            // We want to be in charge of closing our statistics ourselves.
            assertTrue(lastStatistics.isOwningHandle());
            lastStatistics.close();
            lastStatistics = null;
        }

        protected static Statistics lastStatistics = null;
    }

    @Test
    public void shouldNotSetStatisticsInValueProvidersWhenUserProvidesStatistics() {
        rocksDBStore = getRocksDBStoreWithRocksDBMetricsRecorder();
        context = getProcessorContext(RecordingLevel.DEBUG, RocksDBConfigSetterWithUserProvidedStatistics.class);

        rocksDBStore.openDB(context.appConfigs(), context.stateDir());

        verify(metricsRecorder).addValueProviders(eq(DB_NAME), notNull(), notNull(), isNull());
    }


    @Test
    public void shouldCloseStatisticsWhenUserProvidesStatistics() throws Exception {
        rocksDBStore = getRocksDBStoreWithRocksDBMetricsRecorder();
        context = getProcessorContext(RecordingLevel.DEBUG, RocksDBConfigSetterWithUserProvidedStatistics.class);

        rocksDBStore.openDB(context.appConfigs(), context.stateDir());
        final Statistics userStatistics = RocksDBConfigSetterWithUserProvidedStatistics.lastStatistics;
        final Statistics statisticsHandle = getStatistics(rocksDBStore);
        rocksDBStore.close();

        // Both statistics handles must be closed now.
        assertFalse(userStatistics.isOwningHandle());
        assertFalse(statisticsHandle.isOwningHandle());
        assertNull(getStatistics(rocksDBStore));
        assertNull(RocksDBConfigSetterWithUserProvidedStatistics.lastStatistics);
    }

    @Test
    public void shouldSetStatisticsInValueProvidersWhenUserProvidesNoStatistics() throws Exception {
        rocksDBStore = getRocksDBStoreWithRocksDBMetricsRecorder();
        context = getProcessorContext(RecordingLevel.DEBUG);

        rocksDBStore.openDB(context.appConfigs(), context.stateDir());

        verify(metricsRecorder).addValueProviders(eq(DB_NAME), notNull(), notNull(), eq(getStatistics(rocksDBStore)));
    }

    @Test
    public void shouldCloseStatisticsWhenUserProvidesNoStatistics() throws Exception {
        rocksDBStore = getRocksDBStoreWithRocksDBMetricsRecorder();
        context = getProcessorContext(RecordingLevel.DEBUG);

        rocksDBStore.openDB(context.appConfigs(), context.stateDir());
        final Statistics statisticsHandle = getStatistics(rocksDBStore);
        rocksDBStore.close();

        // Statistics handles must be closed now.
        assertFalse(statisticsHandle.isOwningHandle());
        assertNull(getStatistics(rocksDBStore));
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

        rocksDBStore.openDB(context.appConfigs(), context.stateDir());

        verify(metricsRecorder).addValueProviders(eq(DB_NAME), notNull(), isNull(), notNull());
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
    public void shouldMatchPositionAfterPut() {
        rocksDBStore.init((StateStoreContext) context, rocksDBStore);

        context.setRecordContext(new ProcessorRecordContext(0, 1, 0, "", new RecordHeaders()));
        rocksDBStore.put(new Bytes(stringSerializer.serialize(null, "one")), stringSerializer.serialize(null, "A"));
        context.setRecordContext(new ProcessorRecordContext(0, 2, 0, "", new RecordHeaders()));
        rocksDBStore.put(new Bytes(stringSerializer.serialize(null, "two")), stringSerializer.serialize(null, "B"));
        context.setRecordContext(new ProcessorRecordContext(0, 3, 0, "", new RecordHeaders()));
        rocksDBStore.put(new Bytes(stringSerializer.serialize(null, "three")), stringSerializer.serialize(null, "C"));

        final Position expected = Position.fromMap(mkMap(mkEntry("", mkMap(mkEntry(0, 3L)))));
        final Position actual = rocksDBStore.getPosition();
        assertEquals(expected, actual);
    }

    @Test
    public void shouldReturnKeysWithGivenPrefix() {
        final List<KeyValue<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValue<>(
            new Bytes(stringSerializer.serialize(null, "k1")),
            stringSerializer.serialize(null, "a")));
        entries.add(new KeyValue<>(
            new Bytes(stringSerializer.serialize(null, "prefix_3")),
            stringSerializer.serialize(null, "b")));
        entries.add(new KeyValue<>(
            new Bytes(stringSerializer.serialize(null, "k2")),
            stringSerializer.serialize(null, "c")));
        entries.add(new KeyValue<>(
            new Bytes(stringSerializer.serialize(null, "prefix_2")),
            stringSerializer.serialize(null, "d")));
        entries.add(new KeyValue<>(
            new Bytes(stringSerializer.serialize(null, "k3")),
            stringSerializer.serialize(null, "e")));
        entries.add(new KeyValue<>(
            new Bytes(stringSerializer.serialize(null, "prefix_1")),
            stringSerializer.serialize(null, "f")));

        rocksDBStore.init((StateStoreContext) context, rocksDBStore);
        rocksDBStore.putAll(entries);
        rocksDBStore.flush();

        try (final KeyValueIterator<Bytes, byte[]> keysWithPrefix = rocksDBStore.prefixScan("prefix", stringSerializer)) {
            final List<String> valuesWithPrefix = new ArrayList<>();
            int numberOfKeysReturned = 0;

            while (keysWithPrefix.hasNext()) {
                final KeyValue<Bytes, byte[]> next = keysWithPrefix.next();
                valuesWithPrefix.add(new String(next.value));
                numberOfKeysReturned++;
            }
            assertThat(numberOfKeysReturned, is(3));
            assertThat(valuesWithPrefix.get(0), is("f"));
            assertThat(valuesWithPrefix.get(1), is("d"));
            assertThat(valuesWithPrefix.get(2), is("b"));
        }
    }

    @Test
    public void shouldReturnKeysWithGivenPrefixExcludingNextKeyLargestKey() {
        final List<KeyValue<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValue<>(
            new Bytes(stringSerializer.serialize(null, "abc")),
            stringSerializer.serialize(null, "f")));

        entries.add(new KeyValue<>(
            new Bytes(stringSerializer.serialize(null, "abcd")),
            stringSerializer.serialize(null, "f")));

        entries.add(new KeyValue<>(
            new Bytes(stringSerializer.serialize(null, "abce")),
            stringSerializer.serialize(null, "f")));

        rocksDBStore.init((StateStoreContext) context, rocksDBStore);
        rocksDBStore.putAll(entries);
        rocksDBStore.flush();

        try (final KeyValueIterator<Bytes, byte[]> keysWithPrefixAsabcd = rocksDBStore.prefixScan("abcd", stringSerializer)) {
            int numberOfKeysReturned = 0;

            while (keysWithPrefixAsabcd.hasNext()) {
                keysWithPrefixAsabcd.next().key.get();
                numberOfKeysReturned++;
            }

            assertThat(numberOfKeysReturned, is(1));
        }
    }

    @Test
    public void shouldReturnUUIDsWithStringPrefix() {
        final List<KeyValue<Bytes, byte[]>> entries = new ArrayList<>();
        final Serializer<UUID> uuidSerializer = Serdes.UUID().serializer();
        final UUID uuid1 = UUID.randomUUID();
        final UUID uuid2 = UUID.randomUUID();
        final String prefix = uuid1.toString().substring(0, 4);
        final int numMatches = uuid2.toString().substring(0, 4).equals(prefix) ? 2 : 1;

        entries.add(new KeyValue<>(
            new Bytes(uuidSerializer.serialize(null, uuid1)),
            stringSerializer.serialize(null, "a")));
        entries.add(new KeyValue<>(
            new Bytes(uuidSerializer.serialize(null, uuid2)),
            stringSerializer.serialize(null, "b")));

        rocksDBStore.init((StateStoreContext) context, rocksDBStore);
        rocksDBStore.putAll(entries);
        rocksDBStore.flush();

        try (final KeyValueIterator<Bytes, byte[]> keysWithPrefix = rocksDBStore.prefixScan(prefix, stringSerializer)) {
            final List<String> valuesWithPrefix = new ArrayList<>();
            int numberOfKeysReturned = 0;

            while (keysWithPrefix.hasNext()) {
                final KeyValue<Bytes, byte[]> next = keysWithPrefix.next();
                valuesWithPrefix.add(new String(next.value));
                numberOfKeysReturned++;
            }

            assertThat(numberOfKeysReturned, is(numMatches));
            if (numMatches == 2) {
                assertThat(valuesWithPrefix.get(0), either(is("a")).or(is("b")));
            } else {
                assertThat(valuesWithPrefix.get(0), is("a"));
            }
        }
    }

    @Test
    public void shouldReturnNoKeys() {
        final List<KeyValue<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValue<>(
            new Bytes(stringSerializer.serialize(null, "a")),
            stringSerializer.serialize(null, "a")));
        entries.add(new KeyValue<>(
            new Bytes(stringSerializer.serialize(null, "b")),
            stringSerializer.serialize(null, "c")));
        entries.add(new KeyValue<>(
            new Bytes(stringSerializer.serialize(null, "c")),
            stringSerializer.serialize(null, "e")));
        rocksDBStore.init((StateStoreContext) context, rocksDBStore);
        rocksDBStore.putAll(entries);
        rocksDBStore.flush();

        try (final KeyValueIterator<Bytes, byte[]> keysWithPrefix = rocksDBStore.prefixScan("d", stringSerializer)) {
            int numberOfKeysReturned = 0;

            while (keysWithPrefix.hasNext()) {
                keysWithPrefix.next();
                numberOfKeysReturned++;
            }
            assertThat(numberOfKeysReturned, is(0));
        }
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

        try (final KeyValueIterator<Bytes, byte[]> iterator = rocksDBStore.all()) {
            final Set<String> keys = new HashSet<>();

            while (iterator.hasNext()) {
                keys.add(stringDeserializer.deserialize(null, iterator.next().key.get()));
            }

            assertThat(keys, equalTo(Utils.mkSet("2", "3")));
        }
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

        try (final KeyValueIterator<Bytes, byte[]> iterator = rocksDBStore.all()) {
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

        try (final KeyValueIterator<Bytes, byte[]> iterator = rocksDBStore.all()) {
            final Set<String> keys = new HashSet<>();

            while (iterator.hasNext()) {
                keys.add(stringDeserializer.deserialize(null, iterator.next().key.get()));
            }

            assertThat(keys, equalTo(Utils.mkSet("2", "3")));
        }
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
    public void shouldReturnValueOnRange() {
        rocksDBStore.init((StateStoreContext) context, rocksDBStore);

        final KeyValue<String, String> kv0 = new KeyValue<>("0", "zero");
        final KeyValue<String, String> kv1 = new KeyValue<>("1", "one");
        final KeyValue<String, String> kv2 = new KeyValue<>("2", "two");

        rocksDBStore.put(new Bytes(kv0.key.getBytes(UTF_8)), kv0.value.getBytes(UTF_8));
        rocksDBStore.put(new Bytes(kv1.key.getBytes(UTF_8)), kv1.value.getBytes(UTF_8));
        rocksDBStore.put(new Bytes(kv2.key.getBytes(UTF_8)), kv2.value.getBytes(UTF_8));

        final LinkedList<KeyValue<String, String>> expectedContents = new LinkedList<>();
        expectedContents.add(kv0);
        expectedContents.add(kv1);

        try (final KeyValueIterator<Bytes, byte[]> iterator = rocksDBStore.range(null, new Bytes(stringSerializer.serialize(null, "1")))) {
            assertEquals(expectedContents, getDeserializedList(iterator));
        }
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

        context = mock(InternalMockProcessorContext.class);
        when(context.metrics()).thenReturn(streamsMetrics);
        when(context.taskId()).thenReturn(taskId);
        when(context.appConfigs())
            .thenReturn(new StreamsConfig(StreamsTestUtils.getStreamsConfig()).originals());
        when(context.stateDir()).thenReturn(dir);
        final MonotonicProcessorRecordContext processorRecordContext = new MonotonicProcessorRecordContext("test", 0);
        when(context.recordMetadata()).thenReturn(Optional.of(processorRecordContext));

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

        context = mock(InternalMockProcessorContext.class);
        when(context.metrics()).thenReturn(streamsMetrics);
        when(context.taskId()).thenReturn(taskId);
        when(context.appConfigs())
                .thenReturn(new StreamsConfig(StreamsTestUtils.getStreamsConfig()).originals());
        when(context.stateDir()).thenReturn(dir);
        final MonotonicProcessorRecordContext processorRecordContext = new MonotonicProcessorRecordContext("test", 0);
        when(context.recordMetadata()).thenReturn(Optional.of(processorRecordContext));

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
        context = mock(InternalMockProcessorContext.class);
        when(context.metrics()).thenReturn(streamsMetrics);
        when(context.taskId()).thenReturn(taskId);
        when(context.appConfigs()).thenReturn(new StreamsConfig(props).originals());
        when(context.stateDir()).thenReturn(dir);

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

    @Test
    public void shouldPerformRangeQueriesWithCachingDisabled() {
        context.setTime(1L);
        store.put(1, "hi");
        store.put(2, "goodbye");
        try (final KeyValueIterator<Integer, String> range = store.range(1, 2)) {
            assertEquals("hi", range.next().value);
            assertEquals("goodbye", range.next().value);
            assertFalse(range.hasNext());
        }
    }

    @Test
    public void shouldPerformAllQueriesWithCachingDisabled() {
        context.setTime(1L);
        store.put(1, "hi");
        store.put(2, "goodbye");
        try (final KeyValueIterator<Integer, String> range = store.all()) {
            assertEquals("hi", range.next().value);
            assertEquals("goodbye", range.next().value);
            assertFalse(range.hasNext());
        }
    }

    @Test
    public void shouldCloseOpenRangeIteratorsWhenStoreClosedAndThrowInvalidStateStoreOnHasNextAndNext() {
        context.setTime(1L);
        store.put(1, "hi");
        store.put(2, "goodbye");
        try (final KeyValueIterator<Integer, String> iteratorOne = store.range(1, 5);
             final KeyValueIterator<Integer, String> iteratorTwo = store.range(1, 4)) {

            assertTrue(iteratorOne.hasNext());
            assertTrue(iteratorTwo.hasNext());

            store.close();

            Assertions.assertThrows(InvalidStateStoreException.class, () -> iteratorOne.hasNext());
            Assertions.assertThrows(InvalidStateStoreException.class, () -> iteratorOne.next());
            Assertions.assertThrows(InvalidStateStoreException.class, () -> iteratorTwo.hasNext());
            Assertions.assertThrows(InvalidStateStoreException.class, () -> iteratorTwo.next());
        }
    }

    @Test
    public void shouldRestoreRecordsAndConsistencyVectorSingleTopic() {
        final List<ConsumerRecord<byte[], byte[]>> entries = getChangelogRecords();
        final Properties props = StreamsTestUtils.getStreamsConfig();
        props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, MockRocksDbConfigSetter.class);
        props.put(InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED, true);
        dir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext<>(
                dir,
                Serdes.String(),
                Serdes.String(),
                new StreamsConfig(props)
        );
        rocksDBStore.init((StateStoreContext) context, rocksDBStore);
        context.restoreWithHeaders(rocksDBStore.name(), entries);

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

        assertThat(rocksDBStore.getPosition(), Matchers.notNullValue());
        assertThat(rocksDBStore.getPosition().getPartitionPositions(""), Matchers.notNullValue());
        assertThat(rocksDBStore.getPosition().getPartitionPositions(""), hasEntry(0, 3L));
    }

    @Test
    public void shouldRestoreRecordsAndConsistencyVectorMultipleTopics() {
        final List<ConsumerRecord<byte[], byte[]>> entries = getChangelogRecordsMultipleTopics();
        final Properties props = StreamsTestUtils.getStreamsConfig();
        props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, MockRocksDbConfigSetter.class);
        props.put(InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED, true);
        dir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext<>(
                dir,
                Serdes.String(),
                Serdes.String(),
                new StreamsConfig(props)
        );
        rocksDBStore.init((StateStoreContext) context, rocksDBStore);
        context.restoreWithHeaders(rocksDBStore.name(), entries);

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

        assertThat(rocksDBStore.getPosition(), Matchers.notNullValue());
        assertThat(rocksDBStore.getPosition().getPartitionPositions("A"), Matchers.notNullValue());
        assertThat(rocksDBStore.getPosition().getPartitionPositions("A"), hasEntry(0, 3L));
        assertThat(rocksDBStore.getPosition().getPartitionPositions("B"), Matchers.notNullValue());
        assertThat(rocksDBStore.getPosition().getPartitionPositions("B"), hasEntry(0, 2L));
    }

    @Test
    public void shouldHandleTombstoneRecords() {
        final List<ConsumerRecord<byte[], byte[]>> entries = getChangelogRecordsWithTombstones();
        final Properties props = StreamsTestUtils.getStreamsConfig();
        props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, MockRocksDbConfigSetter.class);
        props.put(InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED, true);
        dir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext<>(
                dir,
                Serdes.String(),
                Serdes.String(),
                new StreamsConfig(props)
        );
        rocksDBStore.init((StateStoreContext) context, rocksDBStore);
        context.restoreWithHeaders(rocksDBStore.name(), entries);

        assertNull(stringDeserializer.deserialize(
                null,
                rocksDBStore.get(new Bytes(stringSerializer.serialize(null, "1")))));

        assertThat(rocksDBStore.getPosition(), Matchers.notNullValue());
        assertThat(rocksDBStore.getPosition().getPartitionPositions("A"), hasEntry(0, 2L));
    }

    @Test
    public void shouldNotThrowWhenRestoringOnMissingHeaders() {
        final List<KeyValue<byte[], byte[]>> entries = getChangelogRecordsWithoutHeaders();
        final Properties props = StreamsTestUtils.getStreamsConfig();
        props.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, MockRocksDbConfigSetter.class);
        props.put(InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED, true);
        dir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext<>(
                dir,
                Serdes.String(),
                Serdes.String(),
                new StreamsConfig(props)
        );
        rocksDBStore.init((StateStoreContext) context, rocksDBStore);
        context.restore(rocksDBStore.name(), entries);
        assertThat(rocksDBStore.getPosition(), is(Position.emptyPosition()));
    }

    private List<ConsumerRecord<byte[], byte[]>> getChangelogRecords() {
        final List<ConsumerRecord<byte[], byte[]>> entries = new ArrayList<>();
        entries.add(createChangelogRecord("1".getBytes(UTF_8), "a".getBytes(UTF_8), "", 0, 1));
        entries.add(createChangelogRecord("2".getBytes(UTF_8), "b".getBytes(UTF_8), "", 0, 2));
        entries.add(createChangelogRecord("3".getBytes(UTF_8), "c".getBytes(UTF_8), "", 0, 3));
        return entries;
    }

    private List<ConsumerRecord<byte[], byte[]>> getChangelogRecordsMultipleTopics() {
        final List<ConsumerRecord<byte[], byte[]>> entries = new ArrayList<>();
        entries.add(createChangelogRecord("1".getBytes(UTF_8), "a".getBytes(UTF_8), "A", 0, 1));
        entries.add(createChangelogRecord("2".getBytes(UTF_8), "b".getBytes(UTF_8), "B", 0, 2));
        entries.add(createChangelogRecord("3".getBytes(UTF_8), "c".getBytes(UTF_8), "A", 0, 3));
        return entries;
    }

    private List<ConsumerRecord<byte[], byte[]>> getChangelogRecordsWithTombstones() {
        final List<ConsumerRecord<byte[], byte[]>> entries = new ArrayList<>();
        entries.add(createChangelogRecord("1".getBytes(UTF_8), "a".getBytes(UTF_8), "A", 0, 1));
        entries.add(createChangelogRecord("1".getBytes(UTF_8), null, "A", 0, 2));
        return entries;
    }

    private List<KeyValue<byte[], byte[]>> getChangelogRecordsWithoutHeaders() {
        final List<KeyValue<byte[], byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValue<>("1".getBytes(UTF_8), "a".getBytes(UTF_8)));
        entries.add(new KeyValue<>("2".getBytes(UTF_8), "b".getBytes(UTF_8)));
        entries.add(new KeyValue<>("3".getBytes(UTF_8), "c".getBytes(UTF_8)));
        return entries;
    }

    private ConsumerRecord<byte[], byte[]> createChangelogRecord(
            final byte[] key, final byte[] value, final String topic, final int partition, final long offset) {
        final Headers headers = new RecordHeaders();
        Position position = Position.emptyPosition();
        position = position.withComponent(topic, partition, offset);
        headers.add(ChangelogRecordDeserializationHelper.CHANGELOG_VERSION_HEADER_RECORD_CONSISTENCY);
        headers.add(new RecordHeader(
                ChangelogRecordDeserializationHelper.CHANGELOG_POSITION_HEADER_KEY,
                PositionSerde.serialize(position).array()));
        return new ConsumerRecord<>("", 0, 0L,  RecordBatch.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE, -1, -1,
                key, value, headers, Optional.empty());
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
                tableConfig.setFilterPolicy(filter);
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

    private List<KeyValue<String, String>> getDeserializedList(final KeyValueIterator<Bytes, byte[]> iter) {
        final List<KeyValue<Bytes, byte[]>> bytes = Utils.toList(iter);
        final List<KeyValue<String, String>> result = bytes.stream().map(kv -> new KeyValue<String, String>(kv.key.toString(), stringDeserializer.deserialize(null, kv.value))).collect(Collectors.toList());
        return result;
    }

    private Statistics getStatistics(final RocksDBStore rocksDBStore) throws Exception {
        final Field statisticsField = rocksDBStore.getClass().getDeclaredField("statistics");
        statisticsField.setAccessible(true);
        final Statistics statistics = (Statistics) statisticsField.get(rocksDBStore);
        statisticsField.setAccessible(false);
        return statistics;
    }
}
