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
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
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
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

@Category({IntegrationTest.class})
@RunWith(Parameterized.class)
public class RocksDBMetricsIntegrationTest {

    private static final int NUM_BROKERS = 3;

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    private static final String STREAM_INPUT = "STREAM_INPUT";
    private static final String STREAM_OUTPUT = "STREAM_OUTPUT";
    private static final String MY_STORE_PERSISTENT_KEY_VALUE = "myStorePersistentKeyValue";
    private static final Duration WINDOW_SIZE = Duration.ofMillis(50);

    // RocksDB metrics
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

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {StreamsConfig.EXACTLY_ONCE},
            {StreamsConfig.AT_LEAST_ONCE}
        });
    }

    @Parameter
    public String processingGuarantee;

    @Before
    public void before() throws Exception {
        CLUSTER.createTopic(STREAM_INPUT, 1, 3);
    }

    @After
    public void after() throws Exception {
        CLUSTER.deleteTopicsAndWait(STREAM_INPUT, STREAM_OUTPUT);
    }

    @Test
    public void shouldExposeRocksDBMetricsForNonSegmentedStateStoreBeforeAndAfterFailureWithEmptyStateDir() throws Exception {
        final Properties streamsConfiguration = streamsConfig();
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
        final StreamsBuilder builder = builderForNonSegmentedStateStore();

        cleanUpStateRunAndVerify(
            builder,
            streamsConfiguration,
            IntegerDeserializer.class,
            StringDeserializer.class,
            "rocksdb-state-id"
        );

        cleanUpStateRunAndVerify(
            builder,
            streamsConfiguration,
            IntegerDeserializer.class,
            StringDeserializer.class,
            "rocksdb-state-id"
        );
    }

    @Test
    public void shouldExposeRocksDBMetricsForSegmentedStateStoreBeforeAndAfterFailureWithEmptyStateDir() throws Exception {
        final Properties streamsConfiguration = streamsConfig();
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
        final StreamsBuilder builder = builderForSegmentedStateStore();

        cleanUpStateRunAndVerify(
            builder,
            streamsConfiguration,
            LongDeserializer.class,
            LongDeserializer.class,
            "rocksdb-window-state-id"
        );

        cleanUpStateRunAndVerify(
            builder,
            streamsConfiguration,
            LongDeserializer.class,
            LongDeserializer.class,
            "rocksdb-window-state-id"
        );
    }

    private Properties streamsConfig() {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-application");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.DEBUG.name);
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);
        streamsConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, processingGuarantee);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        return streamsConfiguration;
    }

    private StreamsBuilder builderForNonSegmentedStateStore() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.table(
            STREAM_INPUT,
            Materialized.as(Stores.persistentKeyValueStore(MY_STORE_PERSISTENT_KEY_VALUE)).withCachingEnabled()
        ).toStream().to(STREAM_OUTPUT);
        return builder;
    }

    private StreamsBuilder builderForSegmentedStateStore() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(STREAM_INPUT, Consumed.with(Serdes.Integer(), Serdes.String()))
            .groupByKey()
            .windowedBy(TimeWindows.of(WINDOW_SIZE).grace(Duration.ZERO))
            .aggregate(() -> 0L,
                (aggKey, newValue, aggValue) -> aggValue,
                Materialized.<Integer, Long, WindowStore<Bytes, byte[]>>as("time-windowed-aggregated-stream-store")
                    .withValueSerde(Serdes.Long())
                    .withRetention(WINDOW_SIZE))
            .toStream()
            .map((key, value) -> KeyValue.pair(value, value))
            .to(STREAM_OUTPUT, Produced.with(Serdes.Long(), Serdes.Long()));
        return builder;
    }

    private void cleanUpStateRunAndVerify(final StreamsBuilder builder,
                                          final Properties streamsConfiguration,
                                          final Class outputKeyDeserializer,
                                          final Class outputValueDeserializer,
                                          final String metricsScope) throws Exception {
        final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.cleanUp();
        produceRecords();

        StreamsTestUtils.startKafkaStreamsAndWaitForRunningState(kafkaStreams, 60000);

        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            TestUtils.consumerConfig(
                CLUSTER.bootstrapServers(),
                "consumerApp",
                outputKeyDeserializer,
                outputValueDeserializer,
                new Properties()
            ),
            STREAM_OUTPUT,
            1
        );
        verifyRocksDBMetrics(kafkaStreams, metricsScope);
        kafkaStreams.close();
    }

    private void produceRecords() throws Exception {
        final MockTime mockTime = new MockTime(WINDOW_SIZE.toMillis());
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            STREAM_INPUT,
            Collections.singletonList(new KeyValue<>(1, "A")),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                StringSerializer.class,
                new Properties()
            ),
            mockTime.milliseconds()
        );
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            STREAM_INPUT,
            Collections.singletonList(new KeyValue<>(1, "B")),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                StringSerializer.class,
                new Properties()
            ),
            mockTime.milliseconds()
        );
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            STREAM_INPUT,
            Collections.singletonList(new KeyValue<>(1, "C")),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                StringSerializer.class,
                new Properties()
            ),
            mockTime.milliseconds()
        );
    }

    private void verifyRocksDBMetrics(final KafkaStreams kafkaStreams, final String metricsScope) {
        final List<Metric> listMetricStore = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
            .filter(m -> m.metricName().group().equals("stream-state-metrics") && m.metricName().tags().containsKey(metricsScope))
            .collect(Collectors.toList());
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
    }

    private void checkMetricByName(final List<Metric> listMetric, final String metricName, final int numMetric) {
        final List<Metric> metrics = listMetric.stream()
            .filter(m -> m.metricName().name().equals(metricName))
            .collect(Collectors.toList());
        Assert.assertEquals("Size of metrics of type:'" + metricName + "' must be equal to " + numMetric + " but it's equal to " + metrics.size(), numMetric, metrics.size());
        for (final Metric m : metrics) {
            Assert.assertNotNull("Metric:'" + m.metricName() + "' must be not null", m.metricValue());
        }
    }
}
