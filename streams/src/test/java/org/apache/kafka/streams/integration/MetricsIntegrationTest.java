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
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@SuppressWarnings("unchecked")
@Category({IntegrationTest.class})
public class MetricsIntegrationTest {

    private static final int NUM_BROKERS = 1;

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final long timeout = 60000;

    private final static String APPLICATION_ID_VALUE = "stream-metrics-test";

    // Metric group
    private static final String STREAM_CLIENT_NODE_METRICS = "stream-metrics";
    private static final String STREAM_THREAD_NODE_METRICS_0100_TO_24 = "stream-metrics";
    private static final String STREAM_THREAD_NODE_METRICS = "stream-thread-metrics";
    private static final String STREAM_TASK_NODE_METRICS = "stream-task-metrics";
    private static final String STREAM_PROCESSOR_NODE_METRICS = "stream-processor-node-metrics";
    private static final String STREAM_CACHE_NODE_METRICS = "stream-record-cache-metrics";

    private static final String IN_MEMORY_KVSTORE_TAG_KEY = "in-memory-state-id";
    private static final String IN_MEMORY_LRUCACHE_TAG_KEY = "in-memory-lru-state-id";
    private static final String ROCKSDB_KVSTORE_TAG_KEY = "rocksdb-state-id";
    private static final String STATE_STORE_LEVEL_GROUP_IN_MEMORY_KVSTORE_0100_TO_24 = "stream-in-memory-state-metrics";
    private static final String STATE_STORE_LEVEL_GROUP_IN_MEMORY_LRUCACHE_0100_TO_24 = "stream-in-memory-lru-state-metrics";
    private static final String STATE_STORE_LEVEL_GROUP_ROCKSDB_KVSTORE_0100_TO_24 = "stream-rocksdb-state-metrics";
    private static final String STATE_STORE_LEVEL_GROUP = "stream-state-metrics";
    private static final String STATE_STORE_LEVEL_GROUP_ROCKSDB_WINDOW_STORE_0100_TO_24 = "stream-rocksdb-window-state-metrics";
    private static final String STATE_STORE_LEVEL_GROUP_ROCKSDB_SESSION_STORE_0100_TO_24 = "stream-rocksdb-session-state-metrics";
    private static final String BUFFER_LEVEL_GROUP_0100_TO_24 = "stream-buffer-metrics";

    // Metrics name
    private static final String VERSION = "version";
    private static final String COMMIT_ID = "commit-id";
    private static final String APPLICATION_ID = "application-id";
    private static final String TOPOLOGY_DESCRIPTION = "topology-description";
    private static final String STATE = "state";
    private static final String PUT_LATENCY_AVG = "put-latency-avg";
    private static final String PUT_LATENCY_MAX = "put-latency-max";
    private static final String PUT_IF_ABSENT_LATENCY_AVG = "put-if-absent-latency-avg";
    private static final String PUT_IF_ABSENT_LATENCY_MAX = "put-if-absent-latency-max";
    private static final String GET_LATENCY_AVG = "get-latency-avg";
    private static final String GET_LATENCY_MAX = "get-latency-max";
    private static final String DELETE_LATENCY_AVG = "delete-latency-avg";
    private static final String DELETE_LATENCY_MAX = "delete-latency-max";
    private static final String REMOVE_LATENCY_AVG = "remove-latency-avg";
    private static final String REMOVE_LATENCY_MAX = "remove-latency-max";
    private static final String PUT_ALL_LATENCY_AVG = "put-all-latency-avg";
    private static final String PUT_ALL_LATENCY_MAX = "put-all-latency-max";
    private static final String ALL_LATENCY_AVG = "all-latency-avg";
    private static final String ALL_LATENCY_MAX = "all-latency-max";
    private static final String RANGE_LATENCY_AVG = "range-latency-avg";
    private static final String RANGE_LATENCY_MAX = "range-latency-max";
    private static final String FLUSH_LATENCY_AVG = "flush-latency-avg";
    private static final String FLUSH_LATENCY_MAX = "flush-latency-max";
    private static final String RESTORE_LATENCY_AVG = "restore-latency-avg";
    private static final String RESTORE_LATENCY_MAX = "restore-latency-max";
    private static final String PUT_RATE = "put-rate";
    private static final String PUT_TOTAL = "put-total";
    private static final String PUT_IF_ABSENT_RATE = "put-if-absent-rate";
    private static final String PUT_IF_ABSENT_TOTAL = "put-if-absent-total";
    private static final String GET_RATE = "get-rate";
    private static final String GET_TOTAL = "get-total";
    private static final String FETCH_RATE = "fetch-rate";
    private static final String FETCH_TOTAL = "fetch-total";
    private static final String FETCH_LATENCY_AVG = "fetch-latency-avg";
    private static final String FETCH_LATENCY_MAX = "fetch-latency-max";
    private static final String DELETE_RATE = "delete-rate";
    private static final String DELETE_TOTAL = "delete-total";
    private static final String REMOVE_RATE = "remove-rate";
    private static final String REMOVE_TOTAL = "remove-total";
    private static final String PUT_ALL_RATE = "put-all-rate";
    private static final String PUT_ALL_TOTAL = "put-all-total";
    private static final String ALL_RATE = "all-rate";
    private static final String ALL_TOTAL = "all-total";
    private static final String RANGE_RATE = "range-rate";
    private static final String RANGE_TOTAL = "range-total";
    private static final String FLUSH_RATE = "flush-rate";
    private static final String FLUSH_TOTAL = "flush-total";
    private static final String RESTORE_RATE = "restore-rate";
    private static final String RESTORE_TOTAL = "restore-total";
    private static final String PROCESS_LATENCY_AVG = "process-latency-avg";
    private static final String PROCESS_LATENCY_MAX = "process-latency-max";
    private static final String PUNCTUATE_LATENCY_AVG = "punctuate-latency-avg";
    private static final String PUNCTUATE_LATENCY_MAX = "punctuate-latency-max";
    private static final String CREATE_LATENCY_AVG = "create-latency-avg";
    private static final String CREATE_LATENCY_MAX = "create-latency-max";
    private static final String DESTROY_LATENCY_AVG = "destroy-latency-avg";
    private static final String DESTROY_LATENCY_MAX = "destroy-latency-max";
    private static final String PROCESS_RATE = "process-rate";
    private static final String PROCESS_TOTAL = "process-total";
    private static final String PUNCTUATE_RATE = "punctuate-rate";
    private static final String PUNCTUATE_TOTAL = "punctuate-total";
    private static final String CREATE_RATE = "create-rate";
    private static final String CREATE_TOTAL = "create-total";
    private static final String DESTROY_RATE = "destroy-rate";
    private static final String DESTROY_TOTAL = "destroy-total";
    private static final String FORWARD_TOTAL = "forward-total";
    private static final String STREAM_STRING = "stream";
    private static final String COMMIT_LATENCY_AVG = "commit-latency-avg";
    private static final String COMMIT_LATENCY_MAX = "commit-latency-max";
    private static final String POLL_LATENCY_AVG = "poll-latency-avg";
    private static final String POLL_LATENCY_MAX = "poll-latency-max";
    private static final String COMMIT_RATE = "commit-rate";
    private static final String COMMIT_TOTAL = "commit-total";
    private static final String ENFORCED_PROCESSING_RATE = "enforced-processing-rate";
    private static final String ENFORCED_PROCESSING_TOTAL = "enforced-processing-total";
    private static final String POLL_RATE = "poll-rate";
    private static final String POLL_TOTAL = "poll-total";
    private static final String TASK_CREATED_RATE = "task-created-rate";
    private static final String TASK_CREATED_TOTAL = "task-created-total";
    private static final String TASK_CLOSED_RATE = "task-closed-rate";
    private static final String TASK_CLOSED_TOTAL = "task-closed-total";
    private static final String SKIPPED_RECORDS_RATE = "skipped-records-rate";
    private static final String SKIPPED_RECORDS_TOTAL = "skipped-records-total";
    private static final String RECORD_LATENESS_AVG = "record-lateness-avg";
    private static final String RECORD_LATENESS_MAX = "record-lateness-max";
    private static final String HIT_RATIO_AVG_BEFORE_24 = "hitRatio-avg";
    private static final String HIT_RATIO_MIN_BEFORE_24 = "hitRatio-min";
    private static final String HIT_RATIO_MAX_BEFORE_24 = "hitRatio-max";
    private static final String HIT_RATIO_AVG = "hit-ratio-avg";
    private static final String HIT_RATIO_MIN = "hit-ratio-min";
    private static final String HIT_RATIO_MAX = "hit-ratio-max";
    private static final String SUPPRESSION_BUFFER_SIZE_CURRENT = "suppression-buffer-size-current";
    private static final String SUPPRESSION_BUFFER_SIZE_AVG = "suppression-buffer-size-avg";
    private static final String SUPPRESSION_BUFFER_SIZE_MAX = "suppression-buffer-size-max";
    private static final String SUPPRESSION_BUFFER_COUNT_CURRENT = "suppression-buffer-count-current";
    private static final String SUPPRESSION_BUFFER_COUNT_AVG = "suppression-buffer-count-avg";
    private static final String SUPPRESSION_BUFFER_COUNT_MAX = "suppression-buffer-count-max";
    private static final String EXPIRED_WINDOW_RECORD_DROP_RATE = "expired-window-record-drop-rate";
    private static final String EXPIRED_WINDOW_RECORD_DROP_TOTAL = "expired-window-record-drop-total";

    // RocksDB metrics
    private static final String BYTES_WRITTEN_RATE = "bytes-written-rate";
    private static final String BYTES_WRITTEN_TOTAL = "bytes-written-total";
    private static final String BYTES_READ_RATE = "bytes-read-rate";
    private static final String BYTES_READ_TOTAL = "bytes-read-total";
    private static final String MEMTABLE_BYTES_FLUSHED_RATE = "memtable-bytes-flushed-rate";
    private static final String MEMTABLE_BYTES_FLUSHED_TOTAL = "memtable-bytes-flushed-total";
    private static final String MEMTABLE_HIT_RATIO = "memtable-hit-ratio";
    private static final String MEMTABLE_FLUSH_TIME_AVG = "memtable-flush-time-avg";
    private static final String MEMTABLE_FLUSH_TIME_MIN = "memtable-flush-time-min";
    private static final String MEMTABLE_FLUSH_TIME_MAX = "memtable-flush-time-max";
    private static final String WRITE_STALL_DURATION_AVG = "write-stall-duration-avg";
    private static final String WRITE_STALL_DURATION_TOTAL = "write-stall-duration-total";
    private static final String BLOCK_CACHE_DATA_HIT_RATIO = "block-cache-data-hit-ratio";
    private static final String BLOCK_CACHE_INDEX_HIT_RATIO = "block-cache-index-hit-ratio";
    private static final String BLOCK_CACHE_FILTER_HIT_RATIO = "block-cache-filter-hit-ratio";
    private static final String BYTES_READ_DURING_COMPACTION_RATE = "bytes-read-compaction-rate";
    private static final String BYTES_WRITTEN_DURING_COMPACTION_RATE = "bytes-written-compaction-rate";
    private static final String COMPACTION_TIME_AVG = "compaction-time-avg";
    private static final String COMPACTION_TIME_MIN = "compaction-time-min";
    private static final String COMPACTION_TIME_MAX = "compaction-time-max";
    private static final String NUMBER_OF_OPEN_FILES = "number-open-files";
    private static final String NUMBER_OF_FILE_ERRORS = "number-file-errors-total";

    // stores name
    private static final String TIME_WINDOWED_AGGREGATED_STREAM_STORE = "time-windowed-aggregated-stream-store";
    private static final String SESSION_AGGREGATED_STREAM_STORE = "session-aggregated-stream-store";
    private static final String MY_STORE_IN_MEMORY = "myStoreInMemory";
    private static final String MY_STORE_PERSISTENT_KEY_VALUE = "myStorePersistentKeyValue";
    private static final String MY_STORE_LRU_MAP = "myStoreLruMap";

    // topic names
    private static final String STREAM_INPUT = "STREAM_INPUT";
    private static final String STREAM_OUTPUT_1 = "STREAM_OUTPUT_1";
    private static final String STREAM_OUTPUT_2 = "STREAM_OUTPUT_2";
    private static final String STREAM_OUTPUT_3 = "STREAM_OUTPUT_3";
    private static final String STREAM_OUTPUT_4 = "STREAM_OUTPUT_4";

    private StreamsBuilder builder;
    private Properties streamsConfiguration;
    private KafkaStreams kafkaStreams;

    @Before
    public void before() throws InterruptedException {
        builder = new StreamsBuilder();
        CLUSTER.createTopics(STREAM_INPUT, STREAM_OUTPUT_1, STREAM_OUTPUT_2, STREAM_OUTPUT_3, STREAM_OUTPUT_4);
        streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID_VALUE);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.DEBUG.name);
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
    }

    @After
    public void after() throws InterruptedException {
        CLUSTER.deleteTopics(STREAM_INPUT, STREAM_OUTPUT_1, STREAM_OUTPUT_2, STREAM_OUTPUT_3, STREAM_OUTPUT_4);
    }

    private void startApplication() throws InterruptedException {
        final Topology topology = builder.build();
        kafkaStreams = new KafkaStreams(topology, streamsConfiguration);

        verifyStateMetric(State.CREATED);
        verifyTopologyDescriptionMetric(topology.describe().toString());
        verifyApplicationIdMetric(APPLICATION_ID_VALUE);

        kafkaStreams.start();
        TestUtils.waitForCondition(
            () -> kafkaStreams.state() == State.RUNNING,
            timeout,
            () -> "Kafka Streams application did not reach state RUNNING in " + timeout + " ms");
    }

    private void produceRecordsForTwoSegments(final Duration segmentInterval) throws Exception {
        final MockTime mockTime = new MockTime(Math.max(segmentInterval.toMillis(), 60_000L));
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            STREAM_INPUT,
            Collections.singletonList(new KeyValue<>(1, "A")),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                StringSerializer.class,
                new Properties()),
            mockTime.milliseconds()
        );
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            STREAM_INPUT,
            Collections.singletonList(new KeyValue<>(1, "B")),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                StringSerializer.class,
                new Properties()),
            mockTime.milliseconds()
        );
    }

    private void produceRecordsForClosingWindow(final Duration windowSize) throws Exception {
        final MockTime mockTime = new MockTime(windowSize.toMillis() + 1);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            STREAM_INPUT,
            Collections.singletonList(new KeyValue<>(1, "A")),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                StringSerializer.class,
                new Properties()),
            mockTime.milliseconds()
        );
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            STREAM_INPUT,
            Collections.singletonList(new KeyValue<>(1, "B")),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                StringSerializer.class,
                new Properties()),
            mockTime.milliseconds()
        );
    }

    private void waitUntilAllRecordsAreConsumed(final int numberOfExpectedRecords) throws Exception {
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            TestUtils.consumerConfig(
                CLUSTER.bootstrapServers(),
                "consumerApp",
                LongDeserializer.class,
                LongDeserializer.class,
                new Properties()
            ),
            STREAM_OUTPUT_1,
            numberOfExpectedRecords
        );
    }

    private void closeApplication() throws Exception {
        kafkaStreams.close();
        kafkaStreams.cleanUp();
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
        final long timeout = 60000;
        TestUtils.waitForCondition(
            () -> kafkaStreams.state() == State.NOT_RUNNING,
            timeout,
            () -> "Kafka Streams application did not reach state NOT_RUNNING in " + timeout + " ms");
    }

    @Test
    public void shouldAddMetricsOnAllLevelsWithBuiltInMetricsLatestVersion() throws Exception {
        shouldAddMetricsOnAllLevels(StreamsConfig.METRICS_LATEST);
    }

    @Test
    public void shouldAddMetricsOnAllLevelsWithBuiltInMetricsVersion0100To24() throws Exception {
        shouldAddMetricsOnAllLevels(StreamsConfig.METRICS_0100_TO_24);
    }

    private void shouldAddMetricsOnAllLevels(final String builtInMetricsVersion) throws Exception {
        streamsConfiguration.put(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, builtInMetricsVersion);

        builder.stream(STREAM_INPUT, Consumed.with(Serdes.Integer(), Serdes.String()))
            .to(STREAM_OUTPUT_1, Produced.with(Serdes.Integer(), Serdes.String()));
        builder.table(STREAM_OUTPUT_1,
                      Materialized.as(Stores.inMemoryKeyValueStore(MY_STORE_IN_MEMORY)).withCachingEnabled())
            .toStream()
            .to(STREAM_OUTPUT_2);
        builder.table(STREAM_OUTPUT_2,
                      Materialized.as(Stores.persistentKeyValueStore(MY_STORE_PERSISTENT_KEY_VALUE)).withCachingEnabled())
            .toStream()
            .to(STREAM_OUTPUT_3);
        builder.table(STREAM_OUTPUT_3,
                      Materialized.as(Stores.lruMap(MY_STORE_LRU_MAP, 10000)).withCachingEnabled())
            .toStream()
            .to(STREAM_OUTPUT_4);
        startApplication();

        verifyStateMetric(State.RUNNING);
        checkClientLevelMetrics();
        checkThreadLevelMetrics(builtInMetricsVersion);
        checkTaskLevelMetrics(builtInMetricsVersion);
        checkProcessorLevelMetrics();
        checkKeyValueStoreMetrics(
            STATE_STORE_LEVEL_GROUP_IN_MEMORY_KVSTORE_0100_TO_24,
            IN_MEMORY_KVSTORE_TAG_KEY,
            builtInMetricsVersion
        );
        checkKeyValueStoreMetrics(
            STATE_STORE_LEVEL_GROUP_ROCKSDB_KVSTORE_0100_TO_24,
            ROCKSDB_KVSTORE_TAG_KEY,
            builtInMetricsVersion
        );
        checkKeyValueStoreMetrics(
            STATE_STORE_LEVEL_GROUP_IN_MEMORY_LRUCACHE_0100_TO_24,
            IN_MEMORY_LRUCACHE_TAG_KEY,
            builtInMetricsVersion
        );
        checkRocksDBMetricsByTag(
            "rocksdb-state-id",
            RecordingLevel.valueOf(streamsConfiguration.getProperty(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG))
        );
        checkCacheMetrics(builtInMetricsVersion);

        closeApplication();

        checkMetricsDeregistration();
    }

    @Test
    public void shouldAddMetricsForWindowStoreAndSuppressionBufferWithBuiltInMetricsLatestVersion() throws Exception {
        shouldAddMetricsForWindowStoreAndSuppressionBuffer(StreamsConfig.METRICS_LATEST);
    }

    @Test
    public void shouldAddMetricsForWindowStoreAndSuppressionBufferWithBuiltInMetricsVersion0100To24() throws Exception {
        shouldAddMetricsForWindowStoreAndSuppressionBuffer(StreamsConfig.METRICS_0100_TO_24);
    }

    private void shouldAddMetricsForWindowStoreAndSuppressionBuffer(final String builtInMetricsVersion) throws Exception {
        streamsConfiguration.put(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, builtInMetricsVersion);

        final Duration windowSize = Duration.ofMillis(50);
        builder.stream(STREAM_INPUT, Consumed.with(Serdes.Integer(), Serdes.String()))
            .groupByKey()
            .windowedBy(TimeWindows.of(windowSize).grace(Duration.ZERO))
            .aggregate(() -> 0L,
                (aggKey, newValue, aggValue) -> aggValue,
                Materialized.<Integer, Long, WindowStore<Bytes, byte[]>>as(TIME_WINDOWED_AGGREGATED_STREAM_STORE)
                    .withValueSerde(Serdes.Long())
                    .withRetention(windowSize))
            .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
            .toStream()
            .map((key, value) -> KeyValue.pair(value, value))
            .to(STREAM_OUTPUT_1, Produced.with(Serdes.Long(), Serdes.Long()));

        produceRecordsForClosingWindow(windowSize);
        startApplication();

        verifyStateMetric(State.RUNNING);

        waitUntilAllRecordsAreConsumed(1);

        checkWindowStoreAndSuppressionBufferMetrics(builtInMetricsVersion);
        checkRocksDBMetricsByTag(
            "rocksdb-window-state-id",
            RecordingLevel.valueOf(streamsConfiguration.getProperty(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG))
        );

        closeApplication();

        checkMetricsDeregistration();
    }

    @Test
    public void shouldAddMetricsForSessionStoreWithBuiltInMetricsLatestVersion() throws Exception {
        shouldAddMetricsForSessionStore(StreamsConfig.METRICS_LATEST);
    }

    @Test
    public void shouldAddMetricsForSessionStoreWithBuiltInMetricsVersion0100To24() throws Exception {
        shouldAddMetricsForSessionStore(StreamsConfig.METRICS_0100_TO_24);
    }

    private void shouldAddMetricsForSessionStore(final String builtInMetricsVersion) throws Exception {
        streamsConfiguration.put(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, builtInMetricsVersion);

        final Duration inactivityGap = Duration.ofMillis(50);
        builder.stream(STREAM_INPUT, Consumed.with(Serdes.Integer(), Serdes.String()))
            .groupByKey()
            .windowedBy(SessionWindows.with(inactivityGap).grace(Duration.ZERO))
            .aggregate(() -> 0L,
                (aggKey, newValue, aggValue) -> aggValue,
                (aggKey, leftAggValue, rightAggValue) -> leftAggValue,
                Materialized.<Integer, Long, SessionStore<Bytes, byte[]>>as(SESSION_AGGREGATED_STREAM_STORE)
                    .withValueSerde(Serdes.Long())
                    .withRetention(inactivityGap))
            .toStream()
            .map((key, value) -> KeyValue.pair(value, value))
            .to(STREAM_OUTPUT_1, Produced.with(Serdes.Long(), Serdes.Long()));

        produceRecordsForTwoSegments(inactivityGap);

        startApplication();

        verifyStateMetric(State.RUNNING);

        waitUntilAllRecordsAreConsumed(2);

        checkSessionStoreMetrics(builtInMetricsVersion);
        checkRocksDBMetricsByTag(
            "rocksdb-session-state-id",
            RecordingLevel.valueOf(streamsConfiguration.getProperty(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG))
        );

        closeApplication();

        checkMetricsDeregistration();
    }

    @Test
    public void shouldNotAddRocksDBMetricsIfRecordingLevelIsInfo() throws Exception {
        builder.table(
            STREAM_INPUT,
            Materialized.as(Stores.persistentKeyValueStore(MY_STORE_PERSISTENT_KEY_VALUE)).withCachingEnabled()
        ).toStream().to(STREAM_OUTPUT_1);
        streamsConfiguration.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.INFO.name);
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.start();
        TestUtils.waitForCondition(
            () -> kafkaStreams.state() == State.RUNNING,
            timeout,
            () -> "Kafka Streams application did not reach state RUNNING in " + timeout + " ms");

        checkRocksDBMetricsByTag(
            ROCKSDB_KVSTORE_TAG_KEY,
            RecordingLevel.valueOf(streamsConfiguration.getProperty(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG))
        );

        closeApplication();
    }

    private void verifyStateMetric(final State state) {
        final List<Metric> metricsList = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
            .filter(m -> m.metricName().name().equals(STATE) &&
                m.metricName().group().equals(STREAM_CLIENT_NODE_METRICS))
            .collect(Collectors.toList());
        assertThat(metricsList.size(), is(1));
        assertThat(metricsList.get(0).metricValue(), is(state));
        assertThat(metricsList.get(0).metricValue().toString(), is(state.toString()));
    }

    private void verifyTopologyDescriptionMetric(final String topologyDescription) {
        final List<Metric> metricsList = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
            .filter(m -> m.metricName().name().equals(TOPOLOGY_DESCRIPTION) &&
                m.metricName().group().equals(STREAM_CLIENT_NODE_METRICS))
            .collect(Collectors.toList());
        assertThat(metricsList.size(), is(1));
        assertThat(metricsList.get(0).metricValue(), is(topologyDescription));
    }

    private void verifyApplicationIdMetric(final String applicationId) {
        final List<Metric> metricsList = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
            .filter(m -> m.metricName().name().equals(APPLICATION_ID) &&
                m.metricName().group().equals(STREAM_CLIENT_NODE_METRICS))
            .collect(Collectors.toList());
        assertThat(metricsList.size(), is(1));
        assertThat(metricsList.get(0).metricValue(), is(applicationId));
    }

    private void checkClientLevelMetrics() {
        final List<Metric> listMetricThread = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
            .filter(m -> m.metricName().group().equals(STREAM_CLIENT_NODE_METRICS))
            .collect(Collectors.toList());
        checkMetricByName(listMetricThread, VERSION, 1);
        checkMetricByName(listMetricThread, COMMIT_ID, 1);
        checkMetricByName(listMetricThread, APPLICATION_ID, 1);
        checkMetricByName(listMetricThread, TOPOLOGY_DESCRIPTION, 1);
        checkMetricByName(listMetricThread, STATE, 1);
    }

    private void checkThreadLevelMetrics(final String builtInMetricsVersion) {
        final List<Metric> listMetricThread = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
            .filter(m -> m.metricName().group().equals(
                StreamsConfig.METRICS_LATEST.equals(builtInMetricsVersion) ? STREAM_THREAD_NODE_METRICS
                    : STREAM_THREAD_NODE_METRICS_0100_TO_24))
            .collect(Collectors.toList());
        checkMetricByName(listMetricThread, COMMIT_LATENCY_AVG, 1);
        checkMetricByName(listMetricThread, COMMIT_LATENCY_MAX, 1);
        checkMetricByName(listMetricThread, POLL_LATENCY_AVG, 1);
        checkMetricByName(listMetricThread, POLL_LATENCY_MAX, 1);
        checkMetricByName(listMetricThread, PROCESS_LATENCY_AVG, 1);
        checkMetricByName(listMetricThread, PROCESS_LATENCY_MAX, 1);
        checkMetricByName(listMetricThread, PUNCTUATE_LATENCY_AVG, 1);
        checkMetricByName(listMetricThread, PUNCTUATE_LATENCY_MAX, 1);
        checkMetricByName(listMetricThread, COMMIT_RATE, 1);
        checkMetricByName(listMetricThread, COMMIT_TOTAL, 1);
        checkMetricByName(listMetricThread, POLL_RATE, 1);
        checkMetricByName(listMetricThread, POLL_TOTAL, 1);
        checkMetricByName(listMetricThread, PROCESS_RATE, 1);
        checkMetricByName(listMetricThread, PROCESS_TOTAL, 1);
        checkMetricByName(listMetricThread, PUNCTUATE_RATE, 1);
        checkMetricByName(listMetricThread, PUNCTUATE_TOTAL, 1);
        checkMetricByName(listMetricThread, TASK_CREATED_RATE, 1);
        checkMetricByName(listMetricThread, TASK_CREATED_TOTAL, 1);
        checkMetricByName(listMetricThread, TASK_CLOSED_RATE, 1);
        checkMetricByName(listMetricThread, TASK_CLOSED_TOTAL, 1);
        checkMetricByName(
            listMetricThread,
            SKIPPED_RECORDS_RATE,
            StreamsConfig.METRICS_LATEST.equals(builtInMetricsVersion) ? 0 : 1
        );
        checkMetricByName(
            listMetricThread,
            SKIPPED_RECORDS_TOTAL,
            StreamsConfig.METRICS_LATEST.equals(builtInMetricsVersion) ? 0 : 1
        );
    }

    private void checkTaskLevelMetrics(final String builtInMetricsVersion) {
        final List<Metric> listMetricTask = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
            .filter(m -> m.metricName().group().equals(STREAM_TASK_NODE_METRICS))
            .collect(Collectors.toList());
        checkMetricByName(
            listMetricTask,
            COMMIT_LATENCY_AVG,
            StreamsConfig.METRICS_LATEST.equals(builtInMetricsVersion) ? 4 : 5
        );
        checkMetricByName(
            listMetricTask,
            COMMIT_LATENCY_MAX,
            StreamsConfig.METRICS_LATEST.equals(builtInMetricsVersion) ? 4 : 5
        );
        checkMetricByName(
            listMetricTask,
            COMMIT_RATE,
            StreamsConfig.METRICS_LATEST.equals(builtInMetricsVersion) ? 4 : 5
        );
        checkMetricByName(
            listMetricTask,
            COMMIT_TOTAL,
            StreamsConfig.METRICS_LATEST.equals(builtInMetricsVersion) ? 4 : 5
        );
        checkMetricByName(listMetricTask, ENFORCED_PROCESSING_RATE, 4);
        checkMetricByName(listMetricTask, ENFORCED_PROCESSING_TOTAL, 4);
        checkMetricByName(listMetricTask, RECORD_LATENESS_AVG, 4);
        checkMetricByName(listMetricTask, RECORD_LATENESS_MAX, 4);
        checkTaskLevelMetricsForBuiltInMetricsVersionLatest(
            listMetricTask,
            StreamsConfig.METRICS_LATEST.equals(builtInMetricsVersion) ? 4 : 0
        );
    }

    private void checkTaskLevelMetricsForBuiltInMetricsVersionLatest(final List<Metric> metrics,
                                                                     final int count) {
        checkMetricByName(metrics, PROCESS_LATENCY_AVG, count);
        checkMetricByName(metrics, PROCESS_LATENCY_MAX, count);
        checkMetricByName(metrics, PUNCTUATE_LATENCY_AVG, count);
        checkMetricByName(metrics, PUNCTUATE_LATENCY_MAX, count);
        checkMetricByName(metrics, PUNCTUATE_RATE, count);
        checkMetricByName(metrics, PUNCTUATE_TOTAL, count);
    }

    private void checkProcessorLevelMetrics() {
        final List<Metric> listMetricProcessor = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
            .filter(m -> m.metricName().group().equals(STREAM_PROCESSOR_NODE_METRICS))
            .collect(Collectors.toList());
        checkMetricByName(listMetricProcessor, PROCESS_LATENCY_AVG, 18);
        checkMetricByName(listMetricProcessor, PROCESS_LATENCY_MAX, 18);
        checkMetricByName(listMetricProcessor, PUNCTUATE_LATENCY_AVG, 18);
        checkMetricByName(listMetricProcessor, PUNCTUATE_LATENCY_MAX, 18);
        checkMetricByName(listMetricProcessor, CREATE_LATENCY_AVG, 18);
        checkMetricByName(listMetricProcessor, CREATE_LATENCY_MAX, 18);
        checkMetricByName(listMetricProcessor, DESTROY_LATENCY_AVG, 18);
        checkMetricByName(listMetricProcessor, DESTROY_LATENCY_MAX, 18);
        checkMetricByName(listMetricProcessor, PROCESS_RATE, 18);
        checkMetricByName(listMetricProcessor, PROCESS_TOTAL, 18);
        checkMetricByName(listMetricProcessor, PUNCTUATE_RATE, 18);
        checkMetricByName(listMetricProcessor, PUNCTUATE_TOTAL, 18);
        checkMetricByName(listMetricProcessor, CREATE_RATE, 18);
        checkMetricByName(listMetricProcessor, CREATE_TOTAL, 18);
        checkMetricByName(listMetricProcessor, DESTROY_RATE, 18);
        checkMetricByName(listMetricProcessor, DESTROY_TOTAL, 18);
        checkMetricByName(listMetricProcessor, FORWARD_TOTAL, 18);
    }

    private void checkRocksDBMetricsByTag(final String tag, final RecordingLevel recordingLevel) {
        final List<Metric> listMetricStore = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
            .filter(m -> m.metricName().group().equals(STATE_STORE_LEVEL_GROUP) && m.metricName().tags().containsKey(tag))
            .collect(Collectors.toList());
        final int expectedNumberOfMetrics = recordingLevel == RecordingLevel.DEBUG ? 1 : 0;
        checkMetricByName(listMetricStore, BYTES_WRITTEN_RATE, expectedNumberOfMetrics);
        checkMetricByName(listMetricStore, BYTES_WRITTEN_TOTAL, expectedNumberOfMetrics);
        checkMetricByName(listMetricStore, BYTES_READ_RATE, expectedNumberOfMetrics);
        checkMetricByName(listMetricStore, BYTES_READ_TOTAL, expectedNumberOfMetrics);
        checkMetricByName(listMetricStore, MEMTABLE_BYTES_FLUSHED_RATE, expectedNumberOfMetrics);
        checkMetricByName(listMetricStore, MEMTABLE_BYTES_FLUSHED_TOTAL, expectedNumberOfMetrics);
        checkMetricByName(listMetricStore, MEMTABLE_HIT_RATIO, expectedNumberOfMetrics);
        checkMetricByName(listMetricStore, WRITE_STALL_DURATION_AVG, expectedNumberOfMetrics);
        checkMetricByName(listMetricStore, WRITE_STALL_DURATION_TOTAL, expectedNumberOfMetrics);
        checkMetricByName(listMetricStore, BLOCK_CACHE_DATA_HIT_RATIO, expectedNumberOfMetrics);
        checkMetricByName(listMetricStore, BLOCK_CACHE_INDEX_HIT_RATIO, expectedNumberOfMetrics);
        checkMetricByName(listMetricStore, BLOCK_CACHE_FILTER_HIT_RATIO, expectedNumberOfMetrics);
        checkMetricByName(listMetricStore, BYTES_READ_DURING_COMPACTION_RATE, expectedNumberOfMetrics);
        checkMetricByName(listMetricStore, BYTES_WRITTEN_DURING_COMPACTION_RATE, expectedNumberOfMetrics);
        checkMetricByName(listMetricStore, NUMBER_OF_OPEN_FILES, expectedNumberOfMetrics);
        checkMetricByName(listMetricStore, NUMBER_OF_FILE_ERRORS, expectedNumberOfMetrics);
    }

    private void checkKeyValueStoreMetrics(final String group0100To24,
                                           final String tagKey,
                                           final String builtInMetricsVersion) {
        final List<Metric> listMetricStore = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
            .filter(m -> m.metricName().tags().containsKey(tagKey) &&
                m.metricName().group().equals(StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion) ? group0100To24 : STATE_STORE_LEVEL_GROUP))
            .collect(Collectors.toList());
        final int expectedNumberOfLatencyMetrics = StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion) ? 2 : 1;
        final int expectedNumberOfRateMetrics = StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion) ? 2 : 1;
        final int expectedNumberOfTotalMetrics = StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion) ? 2 : 0;
        checkMetricByName(listMetricStore, PUT_LATENCY_AVG, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, PUT_LATENCY_MAX, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, PUT_IF_ABSENT_LATENCY_AVG, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, PUT_IF_ABSENT_LATENCY_MAX, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, GET_LATENCY_AVG, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, GET_LATENCY_MAX, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, DELETE_LATENCY_AVG, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, DELETE_LATENCY_MAX, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, REMOVE_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, REMOVE_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, PUT_ALL_LATENCY_AVG, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, PUT_ALL_LATENCY_MAX, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, ALL_LATENCY_AVG, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, ALL_LATENCY_MAX, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, RANGE_LATENCY_AVG, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, RANGE_LATENCY_MAX, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, FLUSH_LATENCY_AVG, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, FLUSH_LATENCY_MAX, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, RESTORE_LATENCY_AVG, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, RESTORE_LATENCY_MAX, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, FETCH_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, FETCH_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, PUT_RATE, expectedNumberOfRateMetrics);
        checkMetricByName(listMetricStore, PUT_TOTAL, expectedNumberOfTotalMetrics);
        checkMetricByName(listMetricStore, PUT_IF_ABSENT_RATE, expectedNumberOfRateMetrics);
        checkMetricByName(listMetricStore, PUT_IF_ABSENT_TOTAL, expectedNumberOfTotalMetrics);
        checkMetricByName(listMetricStore, GET_RATE, expectedNumberOfRateMetrics);
        checkMetricByName(listMetricStore, GET_TOTAL, expectedNumberOfTotalMetrics);
        checkMetricByName(listMetricStore, DELETE_RATE, expectedNumberOfRateMetrics);
        checkMetricByName(listMetricStore, DELETE_TOTAL, expectedNumberOfTotalMetrics);
        checkMetricByName(listMetricStore, REMOVE_RATE, 0);
        checkMetricByName(listMetricStore, REMOVE_TOTAL, 0);
        checkMetricByName(listMetricStore, PUT_ALL_RATE, expectedNumberOfRateMetrics);
        checkMetricByName(listMetricStore, PUT_ALL_TOTAL, expectedNumberOfTotalMetrics);
        checkMetricByName(listMetricStore, ALL_RATE, expectedNumberOfRateMetrics);
        checkMetricByName(listMetricStore, ALL_TOTAL, expectedNumberOfTotalMetrics);
        checkMetricByName(listMetricStore, RANGE_RATE, expectedNumberOfRateMetrics);
        checkMetricByName(listMetricStore, RANGE_TOTAL, expectedNumberOfTotalMetrics);
        checkMetricByName(listMetricStore, FLUSH_RATE, expectedNumberOfRateMetrics);
        checkMetricByName(listMetricStore, FLUSH_TOTAL, expectedNumberOfTotalMetrics);
        checkMetricByName(listMetricStore, RESTORE_RATE, expectedNumberOfRateMetrics);
        checkMetricByName(listMetricStore, RESTORE_TOTAL, expectedNumberOfTotalMetrics);
        checkMetricByName(listMetricStore, FETCH_RATE, 0);
        checkMetricByName(listMetricStore, FETCH_TOTAL, 0);
        checkMetricByName(listMetricStore, SUPPRESSION_BUFFER_COUNT_CURRENT, 0);
        checkMetricByName(listMetricStore, SUPPRESSION_BUFFER_COUNT_AVG, 0);
        checkMetricByName(listMetricStore, SUPPRESSION_BUFFER_COUNT_MAX, 0);
        checkMetricByName(listMetricStore, SUPPRESSION_BUFFER_SIZE_CURRENT, 0);
        checkMetricByName(listMetricStore, SUPPRESSION_BUFFER_SIZE_AVG, 0);
        checkMetricByName(listMetricStore, SUPPRESSION_BUFFER_SIZE_MAX, 0);
    }

    private void checkMetricsDeregistration() {
        final List<Metric> listMetricAfterClosingApp = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
            .filter(m -> m.metricName().group().contains(STREAM_STRING))
            .collect(Collectors.toList());
        assertThat(listMetricAfterClosingApp.size(), is(0));
    }

    private void checkCacheMetrics(final String builtInMetricsVersion) {
        final List<Metric> listMetricCache = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
            .filter(m -> m.metricName().group().equals(STREAM_CACHE_NODE_METRICS))
            .collect(Collectors.toList());
        checkMetricByName(
            listMetricCache,
            builtInMetricsVersion.equals(StreamsConfig.METRICS_LATEST) ? HIT_RATIO_AVG : HIT_RATIO_AVG_BEFORE_24,
            builtInMetricsVersion.equals(StreamsConfig.METRICS_LATEST) ? 3 : 6 /* includes parent sensors */
        );
        checkMetricByName(
            listMetricCache,
            builtInMetricsVersion.equals(StreamsConfig.METRICS_LATEST) ? HIT_RATIO_MIN : HIT_RATIO_MIN_BEFORE_24,
            builtInMetricsVersion.equals(StreamsConfig.METRICS_LATEST) ? 3 : 6 /* includes parent sensors */
        );
        checkMetricByName(
            listMetricCache,
            builtInMetricsVersion.equals(StreamsConfig.METRICS_LATEST) ? HIT_RATIO_MAX : HIT_RATIO_MAX_BEFORE_24,
            builtInMetricsVersion.equals(StreamsConfig.METRICS_LATEST) ? 3 : 6 /* includes parent sensors */
        );
    }

    private void checkWindowStoreAndSuppressionBufferMetrics(final String builtInMetricsVersion) {
        final List<Metric> listMetricStore = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
            .filter(m -> m.metricName().group().equals(STATE_STORE_LEVEL_GROUP_ROCKSDB_WINDOW_STORE_0100_TO_24) ||
                m.metricName().group().equals(BUFFER_LEVEL_GROUP_0100_TO_24) ||
                m.metricName().group().equals("stream-rocksdb-window-metrics") ||
                m.metricName().group().equals(STATE_STORE_LEVEL_GROUP)
            ).collect(Collectors.toList());
        final int expectedNumberOfLatencyMetrics = StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion) ? 2 : 1;
        final int expectedNumberOfRateMetrics = StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion) ? 2 : 1;
        final int expectedNumberOfTotalMetrics = StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion) ? 2 : 0;
        final int expectedNumberOfRemovedMetrics = StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion) ? 1 : 0;
        checkMetricByName(listMetricStore, PUT_LATENCY_AVG, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, PUT_LATENCY_MAX, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, PUT_IF_ABSENT_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, PUT_IF_ABSENT_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, GET_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, GET_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, DELETE_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, DELETE_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, REMOVE_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, REMOVE_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, PUT_ALL_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, PUT_ALL_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, ALL_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, ALL_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, RANGE_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, RANGE_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, FLUSH_LATENCY_AVG, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, FLUSH_LATENCY_MAX, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, RESTORE_LATENCY_AVG, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, RESTORE_LATENCY_MAX, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, FETCH_LATENCY_AVG, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, FETCH_LATENCY_MAX, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, PUT_RATE, expectedNumberOfRateMetrics);
        checkMetricByName(listMetricStore, PUT_TOTAL, expectedNumberOfTotalMetrics);
        checkMetricByName(listMetricStore, PUT_IF_ABSENT_RATE, 0);
        checkMetricByName(listMetricStore, PUT_IF_ABSENT_TOTAL, 0);
        checkMetricByName(listMetricStore, GET_RATE, 0);
        checkMetricByName(listMetricStore, GET_TOTAL, 0);
        checkMetricByName(listMetricStore, DELETE_RATE, 0);
        checkMetricByName(listMetricStore, DELETE_TOTAL, 0);
        checkMetricByName(listMetricStore, REMOVE_RATE, 0);
        checkMetricByName(listMetricStore, REMOVE_TOTAL, 0);
        checkMetricByName(listMetricStore, PUT_ALL_RATE, 0);
        checkMetricByName(listMetricStore, PUT_ALL_TOTAL, 0);
        checkMetricByName(listMetricStore, ALL_RATE, 0);
        checkMetricByName(listMetricStore, ALL_TOTAL, 0);
        checkMetricByName(listMetricStore, RANGE_RATE, 0);
        checkMetricByName(listMetricStore, RANGE_TOTAL, 0);
        checkMetricByName(listMetricStore, FLUSH_RATE, expectedNumberOfRateMetrics);
        checkMetricByName(listMetricStore, FLUSH_TOTAL, expectedNumberOfTotalMetrics);
        checkMetricByName(listMetricStore, RESTORE_RATE, expectedNumberOfRateMetrics);
        checkMetricByName(listMetricStore, RESTORE_TOTAL, expectedNumberOfTotalMetrics);
        checkMetricByName(listMetricStore, FETCH_RATE, expectedNumberOfRateMetrics);
        checkMetricByName(listMetricStore, FETCH_TOTAL, expectedNumberOfTotalMetrics);
        checkMetricByName(listMetricStore, EXPIRED_WINDOW_RECORD_DROP_RATE, expectedNumberOfRemovedMetrics);
        checkMetricByName(listMetricStore, EXPIRED_WINDOW_RECORD_DROP_TOTAL, expectedNumberOfRemovedMetrics);
        checkMetricByName(listMetricStore, SUPPRESSION_BUFFER_COUNT_CURRENT, expectedNumberOfRemovedMetrics);
        checkMetricByName(listMetricStore, SUPPRESSION_BUFFER_COUNT_AVG, 1);
        checkMetricByName(listMetricStore, SUPPRESSION_BUFFER_COUNT_MAX, 1);
        checkMetricByName(listMetricStore, SUPPRESSION_BUFFER_SIZE_CURRENT, expectedNumberOfRemovedMetrics);
        checkMetricByName(listMetricStore, SUPPRESSION_BUFFER_SIZE_AVG, 1);
        checkMetricByName(listMetricStore, SUPPRESSION_BUFFER_SIZE_MAX, 1);
    }

    private void checkSessionStoreMetrics(final String builtInMetricsVersion) {
        final List<Metric> listMetricStore = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
            .filter(m -> m.metricName().group().equals(STATE_STORE_LEVEL_GROUP_ROCKSDB_SESSION_STORE_0100_TO_24) ||
                m.metricName().group().equals(BUFFER_LEVEL_GROUP_0100_TO_24) ||
                m.metricName().group().equals("stream-rocksdb-session-metrics") ||
                m.metricName().group().equals(STATE_STORE_LEVEL_GROUP)
            ).collect(Collectors.toList());
        final int expectedNumberOfLatencyMetrics = StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion) ? 2 : 1;
        final int expectedNumberOfRateMetrics = StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion) ? 2 : 1;
        final int expectedNumberOfTotalMetrics = StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion) ? 2 : 0;
        final int expectedNumberOfRemovedMetrics = StreamsConfig.METRICS_0100_TO_24.equals(builtInMetricsVersion) ? 1 : 0;
        checkMetricByName(listMetricStore, PUT_LATENCY_AVG, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, PUT_LATENCY_MAX, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, PUT_IF_ABSENT_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, PUT_IF_ABSENT_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, GET_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, GET_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, DELETE_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, DELETE_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, REMOVE_LATENCY_AVG, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, REMOVE_LATENCY_MAX, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, PUT_ALL_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, PUT_ALL_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, ALL_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, ALL_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, RANGE_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, RANGE_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, FLUSH_LATENCY_AVG, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, FLUSH_LATENCY_MAX, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, RESTORE_LATENCY_AVG, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, RESTORE_LATENCY_MAX, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, FETCH_LATENCY_AVG, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, FETCH_LATENCY_MAX, expectedNumberOfLatencyMetrics);
        checkMetricByName(listMetricStore, PUT_RATE, expectedNumberOfRateMetrics);
        checkMetricByName(listMetricStore, PUT_TOTAL, expectedNumberOfTotalMetrics);
        checkMetricByName(listMetricStore, PUT_IF_ABSENT_RATE, 0);
        checkMetricByName(listMetricStore, PUT_IF_ABSENT_TOTAL, 0);
        checkMetricByName(listMetricStore, GET_RATE, 0);
        checkMetricByName(listMetricStore, GET_TOTAL, 0);
        checkMetricByName(listMetricStore, DELETE_RATE, 0);
        checkMetricByName(listMetricStore, DELETE_TOTAL, 0);
        checkMetricByName(listMetricStore, REMOVE_RATE, expectedNumberOfRateMetrics);
        checkMetricByName(listMetricStore, REMOVE_TOTAL, expectedNumberOfTotalMetrics);
        checkMetricByName(listMetricStore, PUT_ALL_RATE, 0);
        checkMetricByName(listMetricStore, PUT_ALL_TOTAL, 0);
        checkMetricByName(listMetricStore, ALL_RATE, 0);
        checkMetricByName(listMetricStore, ALL_TOTAL, 0);
        checkMetricByName(listMetricStore, RANGE_RATE, 0);
        checkMetricByName(listMetricStore, RANGE_TOTAL, 0);
        checkMetricByName(listMetricStore, FLUSH_RATE, expectedNumberOfRateMetrics);
        checkMetricByName(listMetricStore, FLUSH_TOTAL, expectedNumberOfTotalMetrics);
        checkMetricByName(listMetricStore, RESTORE_RATE, expectedNumberOfRateMetrics);
        checkMetricByName(listMetricStore, RESTORE_TOTAL, expectedNumberOfTotalMetrics);
        checkMetricByName(listMetricStore, FETCH_RATE, expectedNumberOfRateMetrics);
        checkMetricByName(listMetricStore, FETCH_TOTAL, expectedNumberOfTotalMetrics);
        checkMetricByName(listMetricStore, EXPIRED_WINDOW_RECORD_DROP_RATE, expectedNumberOfRemovedMetrics);
        checkMetricByName(listMetricStore, EXPIRED_WINDOW_RECORD_DROP_TOTAL, expectedNumberOfRemovedMetrics);
        checkMetricByName(listMetricStore, SUPPRESSION_BUFFER_COUNT_CURRENT, 0);
        checkMetricByName(listMetricStore, SUPPRESSION_BUFFER_COUNT_AVG, 0);
        checkMetricByName(listMetricStore, SUPPRESSION_BUFFER_COUNT_MAX, 0);
        checkMetricByName(listMetricStore, SUPPRESSION_BUFFER_SIZE_CURRENT, 0);
        checkMetricByName(listMetricStore, SUPPRESSION_BUFFER_SIZE_AVG, 0);
        checkMetricByName(listMetricStore, SUPPRESSION_BUFFER_SIZE_MAX, 0);
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