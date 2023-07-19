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
package org.apache.kafka.streams.integration;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@Category({IntegrationTest.class})
@RunWith(Parameterized.class)
@SuppressWarnings("deprecation")
public class RocksDBMetricsIntegrationTest {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(600);
    private static final int NUM_BROKERS = 3;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    @BeforeClass
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }

    private static final String STREAM_INPUT_ONE = "STREAM_INPUT_ONE";
    private static final String STREAM_OUTPUT_ONE = "STREAM_OUTPUT_ONE";
    private static final String STREAM_INPUT_TWO = "STREAM_INPUT_TWO";
    private static final String STREAM_OUTPUT_TWO = "STREAM_OUTPUT_TWO";
    private static final String MY_STORE_PERSISTENT_KEY_VALUE = "myStorePersistentKeyValue";
    private static final Duration WINDOW_SIZE = Duration.ofMillis(50);
    private static final long TIMEOUT = 60000;

    // RocksDB metrics
    private static final String METRICS_GROUP = "stream-state-metrics";
    private static final String BYTES_WRITTEN_RATE = "bytes-written-rate";
    private static final String BYTES_WRITTEN_TOTAL = "bytes-written-total";
    private static final String BYTES_READ_RATE = "bytes-read-rate";
    private static final String BYTES_READ_TOTAL = "bytes-read-total";
    private static final String MEMTABLE_BYTES_FLUSHED_RATE = "memtable-bytes-flushed-rate";
    private static final String MEMTABLE_BYTES_FLUSHED_TOTAL = "memtable-bytes-flushed-total";
    private static final String MEMTABLE_HIT_RATIO = "memtable-hit-ratio";
    private static final String WRITE_STALL_DURATION_AVG = "write-stall-duration-avg";
    private static final String WRITE_STALL_DURATION_TOTAL = "write-stall-duration-total";
    private static final String BLOCK_CACHE_DATA_HIT_RATIO = "block-cache-data-hit-ratio";
    private static final String BLOCK_CACHE_INDEX_HIT_RATIO = "block-cache-index-hit-ratio";
    private static final String BLOCK_CACHE_FILTER_HIT_RATIO = "block-cache-filter-hit-ratio";
    private static final String BYTES_READ_DURING_COMPACTION_RATE = "bytes-read-compaction-rate";
    private static final String BYTES_WRITTEN_DURING_COMPACTION_RATE = "bytes-written-compaction-rate";
    private static final String NUMBER_OF_OPEN_FILES = "number-open-files";
    private static final String NUMBER_OF_FILE_ERRORS = "number-file-errors-total";
    private static final String NUMBER_OF_ENTRIES_ACTIVE_MEMTABLE = "num-entries-active-mem-table";
    private static final String NUMBER_OF_DELETES_ACTIVE_MEMTABLE = "num-deletes-active-mem-table";
    private static final String NUMBER_OF_ENTRIES_IMMUTABLE_MEMTABLES = "num-entries-imm-mem-tables";
    private static final String NUMBER_OF_DELETES_IMMUTABLE_MEMTABLES = "num-deletes-imm-mem-tables";
    private static final String NUMBER_OF_IMMUTABLE_MEMTABLES = "num-immutable-mem-table";
    private static final String CURRENT_SIZE_OF_ACTIVE_MEMTABLE = "cur-size-active-mem-table";
    private static final String CURRENT_SIZE_OF_ALL_MEMTABLES = "cur-size-all-mem-tables";
    private static final String SIZE_OF_ALL_MEMTABLES = "size-all-mem-tables";
    private static final String MEMTABLE_FLUSH_PENDING = "mem-table-flush-pending";
    private static final String NUMBER_OF_RUNNING_FLUSHES = "num-running-flushes";
    private static final String COMPACTION_PENDING = "compaction-pending";
    private static final String NUMBER_OF_RUNNING_COMPACTIONS = "num-running-compactions";
    private static final String ESTIMATED_BYTES_OF_PENDING_COMPACTION = "estimate-pending-compaction-bytes";
    private static final String TOTAL_SST_FILES_SIZE = "total-sst-files-size";
    private static final String LIVE_SST_FILES_SIZE = "live-sst-files-size";
    private static final String NUMBER_OF_LIVE_VERSIONS = "num-live-versions";
    private static final String CAPACITY_OF_BLOCK_CACHE = "block-cache-capacity";
    private static final String USAGE_OF_BLOCK_CACHE = "block-cache-usage";
    private static final String PINNED_USAGE_OF_BLOCK_CACHE = "block-cache-pinned-usage";
    private static final String ESTIMATED_NUMBER_OF_KEYS = "estimate-num-keys";
    private static final String ESTIMATED_MEMORY_OF_TABLE_READERS = "estimate-table-readers-mem";
    private static final String NUMBER_OF_BACKGROUND_ERRORS = "background-errors";

    @SuppressWarnings("deprecation")
    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {StreamsConfig.AT_LEAST_ONCE},
            {StreamsConfig.EXACTLY_ONCE},
            {StreamsConfig.EXACTLY_ONCE_V2}
        });
    }

    @Parameter
    public String processingGuarantee;

    @Rule
    public TestName testName = new TestName();

    @Before
    public void before() throws Exception {
        CLUSTER.createTopic(STREAM_INPUT_ONE, 1, 3);
        CLUSTER.createTopic(STREAM_INPUT_TWO, 1, 3);
    }

    @After
    public void after() throws Exception {
        CLUSTER.deleteTopicsAndWait(STREAM_INPUT_ONE, STREAM_INPUT_TWO, STREAM_OUTPUT_ONE, STREAM_OUTPUT_TWO);
    }

    @FunctionalInterface
    private interface MetricsVerifier {
        void verify(final KafkaStreams kafkaStreams, final String metricScope) throws Exception;
    }

    @Test
    public void shouldExposeRocksDBMetricsBeforeAndAfterFailureWithEmptyStateDir() throws Exception {
        final Properties streamsConfiguration = streamsConfig();
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
        final StreamsBuilder builder = builderForStateStores();

        cleanUpStateRunVerifyAndClose(
            builder,
            streamsConfiguration,
            this::verifyThatRocksDBMetricsAreExposed
        );

        // simulated failure

        cleanUpStateRunVerifyAndClose(
            builder,
            streamsConfiguration,
            this::verifyThatRocksDBMetricsAreExposed
        );
    }

    private Properties streamsConfig() {
        final Properties streamsConfiguration = new Properties();
        final String safeTestName = safeUniqueTestName(getClass(), testName);
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-application-" + safeTestName);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.DEBUG.name);
        streamsConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, processingGuarantee);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        return streamsConfiguration;
    }

    private StreamsBuilder builderForStateStores() {
        final StreamsBuilder builder = new StreamsBuilder();
        // create two state stores, one non-segmented and one segmented
        builder.table(
            STREAM_INPUT_ONE,
            Materialized.as(Stores.persistentKeyValueStore(MY_STORE_PERSISTENT_KEY_VALUE)).withCachingEnabled()
        ).toStream().to(STREAM_OUTPUT_ONE);
        builder.stream(STREAM_INPUT_TWO, Consumed.with(Serdes.Integer(), Serdes.String()))
            .groupByKey()
            .windowedBy(TimeWindows.of(WINDOW_SIZE).grace(Duration.ZERO))
            .aggregate(() -> 0L,
                (aggKey, newValue, aggValue) -> aggValue,
                Materialized.<Integer, Long, WindowStore<Bytes, byte[]>>as("time-windowed-aggregated-stream-store")
                    .withValueSerde(Serdes.Long())
                    .withRetention(WINDOW_SIZE))
            .toStream()
            .map((key, value) -> KeyValue.pair(value, value))
            .to(STREAM_OUTPUT_TWO, Produced.with(Serdes.Long(), Serdes.Long()));
        return builder;
    }

    private void cleanUpStateRunVerifyAndClose(final StreamsBuilder builder,
                                               final Properties streamsConfiguration,
                                               final MetricsVerifier metricsVerifier) throws Exception {
        final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.cleanUp();
        produceRecords();

        startApplicationAndWaitUntilRunning(kafkaStreams);

        metricsVerifier.verify(kafkaStreams, "rocksdb-state-id");
        metricsVerifier.verify(kafkaStreams, "rocksdb-window-state-id");
        kafkaStreams.close();
    }

    private void produceRecords() {
        final MockTime mockTime = new MockTime(WINDOW_SIZE.toMillis());
        final Properties prop = TestUtils.producerConfig(
            CLUSTER.bootstrapServers(),
            IntegerSerializer.class,
            StringSerializer.class,
            new Properties()
        );
        // non-segmented store do not need records with different timestamps
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            STREAM_INPUT_ONE,
            Utils.mkSet(new KeyValue<>(1, "A"), new KeyValue<>(1, "B"), new KeyValue<>(1, "C")),
            prop,
            mockTime.milliseconds()
        );
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            STREAM_INPUT_TWO,
            Collections.singleton(new KeyValue<>(1, "A")),
            prop,
            mockTime.milliseconds()
        );
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            STREAM_INPUT_TWO,
            Collections.singleton(new KeyValue<>(1, "B")),
            prop,
            mockTime.milliseconds()
        );
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            STREAM_INPUT_TWO,
            Collections.singleton(new KeyValue<>(1, "C")),
            prop,
            mockTime.milliseconds()
        );
    }

    private void verifyThatRocksDBMetricsAreExposed(final KafkaStreams kafkaStreams,
                                                    final String metricsScope) {
        final List<Metric> listMetricStore = getRocksDBMetrics(kafkaStreams, metricsScope);
        checkMetricByName(listMetricStore, BYTES_WRITTEN_RATE, 1);
        checkMetricByName(listMetricStore, BYTES_WRITTEN_TOTAL, 1);
        checkMetricByName(listMetricStore, BYTES_READ_RATE, 1);
        checkMetricByName(listMetricStore, BYTES_READ_TOTAL, 1);
        checkMetricByName(listMetricStore, MEMTABLE_BYTES_FLUSHED_RATE, 1);
        checkMetricByName(listMetricStore, MEMTABLE_BYTES_FLUSHED_TOTAL, 1);
        checkMetricByName(listMetricStore, MEMTABLE_HIT_RATIO, 1);
        checkMetricByName(listMetricStore, WRITE_STALL_DURATION_AVG, 1);
        checkMetricByName(listMetricStore, WRITE_STALL_DURATION_TOTAL, 1);
        checkMetricByName(listMetricStore, BLOCK_CACHE_DATA_HIT_RATIO, 1);
        checkMetricByName(listMetricStore, BLOCK_CACHE_INDEX_HIT_RATIO, 1);
        checkMetricByName(listMetricStore, BLOCK_CACHE_FILTER_HIT_RATIO, 1);
        checkMetricByName(listMetricStore, BYTES_READ_DURING_COMPACTION_RATE, 1);
        checkMetricByName(listMetricStore, BYTES_WRITTEN_DURING_COMPACTION_RATE, 1);
        checkMetricByName(listMetricStore, NUMBER_OF_OPEN_FILES, 1);
        checkMetricByName(listMetricStore, NUMBER_OF_FILE_ERRORS, 1);
        checkMetricByName(listMetricStore, NUMBER_OF_ENTRIES_ACTIVE_MEMTABLE, 1);
        checkMetricByName(listMetricStore, NUMBER_OF_DELETES_ACTIVE_MEMTABLE, 1);
        checkMetricByName(listMetricStore, NUMBER_OF_ENTRIES_IMMUTABLE_MEMTABLES, 1);
        checkMetricByName(listMetricStore, NUMBER_OF_DELETES_IMMUTABLE_MEMTABLES, 1);
        checkMetricByName(listMetricStore, NUMBER_OF_IMMUTABLE_MEMTABLES, 1);
        checkMetricByName(listMetricStore, CURRENT_SIZE_OF_ACTIVE_MEMTABLE, 1);
        checkMetricByName(listMetricStore, CURRENT_SIZE_OF_ALL_MEMTABLES, 1);
        checkMetricByName(listMetricStore, SIZE_OF_ALL_MEMTABLES, 1);
        checkMetricByName(listMetricStore, MEMTABLE_FLUSH_PENDING, 1);
        checkMetricByName(listMetricStore, NUMBER_OF_RUNNING_FLUSHES, 1);
        checkMetricByName(listMetricStore, COMPACTION_PENDING, 1);
        checkMetricByName(listMetricStore, NUMBER_OF_RUNNING_COMPACTIONS, 1);
        checkMetricByName(listMetricStore, ESTIMATED_BYTES_OF_PENDING_COMPACTION, 1);
        checkMetricByName(listMetricStore, TOTAL_SST_FILES_SIZE, 1);
        checkMetricByName(listMetricStore, LIVE_SST_FILES_SIZE, 1);
        checkMetricByName(listMetricStore, NUMBER_OF_LIVE_VERSIONS, 1);
        checkMetricByName(listMetricStore, CAPACITY_OF_BLOCK_CACHE, 1);
        checkMetricByName(listMetricStore, USAGE_OF_BLOCK_CACHE, 1);
        checkMetricByName(listMetricStore, PINNED_USAGE_OF_BLOCK_CACHE, 1);
        checkMetricByName(listMetricStore, ESTIMATED_NUMBER_OF_KEYS, 1);
        checkMetricByName(listMetricStore, ESTIMATED_MEMORY_OF_TABLE_READERS, 1);
        checkMetricByName(listMetricStore, NUMBER_OF_BACKGROUND_ERRORS, 1);
    }

    private void checkMetricByName(final List<Metric> listMetric,
                                   final String metricName,
                                   final int numMetric) {
        final List<Metric> metrics = listMetric.stream()
            .filter(m -> m.metricName().name().equals(metricName))
            .collect(Collectors.toList());
        assertThat(
            "Size of metrics of type:'" + metricName + "' must be equal to " + numMetric + " but it's equal to " + metrics.size(),
            metrics.size(),
            is(numMetric)
        );
        for (final Metric metric : metrics) {
            assertThat("Metric:'" + metric.metricName() + "' must be not null", metric.metricValue(), is(notNullValue()));
        }
    }

    private List<Metric> getRocksDBMetrics(final KafkaStreams kafkaStreams,
                                           final String metricsScope) {
        return new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
            .filter(m -> m.metricName().group().equals(METRICS_GROUP) && m.metricName().tags().containsKey(metricsScope))
            .collect(Collectors.toList());
    }
}