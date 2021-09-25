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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Category({IntegrationTest.class})
@SuppressWarnings("deprecation")
public class MetricsIntegrationTest {

    private static final int NUM_BROKERS = 1;
    private static final int NUM_THREADS = 2;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    @BeforeClass
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }

    private final long timeout = 60000;

    // Metric group
    private static final String STREAM_CLIENT_NODE_METRICS = "stream-metrics";
    private static final String STREAM_THREAD_NODE_METRICS = "stream-thread-metrics";
    private static final String STREAM_TASK_NODE_METRICS = "stream-task-metrics";
    private static final String STREAM_PROCESSOR_NODE_METRICS = "stream-processor-node-metrics";
    private static final String STREAM_CACHE_NODE_METRICS = "stream-record-cache-metrics";

    private static final String IN_MEMORY_KVSTORE_TAG_KEY = "in-memory-state-id";
    private static final String IN_MEMORY_LRUCACHE_TAG_KEY = "in-memory-lru-state-id";
    private static final String ROCKSDB_KVSTORE_TAG_KEY = "rocksdb-state-id";
    private static final String STATE_STORE_LEVEL_GROUP = "stream-state-metrics";

    // Metrics name
    private static final String VERSION = "version";
    private static final String COMMIT_ID = "commit-id";
    private static final String APPLICATION_ID = "application-id";
    private static final String TOPOLOGY_DESCRIPTION = "topology-description";
    private static final String STATE = "state";
    private static final String ALIVE_STREAM_THREADS = "alive-stream-threads";
    private static final String FAILED_STREAM_THREADS = "failed-stream-threads";
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
    private static final String PROCESS_RATIO = "process-ratio";
    private static final String PROCESS_RECORDS_AVG = "process-records-avg";
    private static final String PROCESS_RECORDS_MAX = "process-records-max";
    private static final String PUNCTUATE_RATE = "punctuate-rate";
    private static final String PUNCTUATE_TOTAL = "punctuate-total";
    private static final String PUNCTUATE_RATIO = "punctuate-ratio";
    private static final String CREATE_RATE = "create-rate";
    private static final String CREATE_TOTAL = "create-total";
    private static final String DESTROY_RATE = "destroy-rate";
    private static final String DESTROY_TOTAL = "destroy-total";
    private static final String FORWARD_TOTAL = "forward-total";
    private static final String FORWARD_RATE = "forward-rate";
    private static final String STREAM_STRING = "stream";
    private static final String COMMIT_LATENCY_AVG = "commit-latency-avg";
    private static final String COMMIT_LATENCY_MAX = "commit-latency-max";
    private static final String POLL_LATENCY_AVG = "poll-latency-avg";
    private static final String POLL_LATENCY_MAX = "poll-latency-max";
    private static final String COMMIT_RATE = "commit-rate";
    private static final String COMMIT_TOTAL = "commit-total";
    private static final String COMMIT_RATIO = "commit-ratio";
    private static final String ENFORCED_PROCESSING_RATE = "enforced-processing-rate";
    private static final String ENFORCED_PROCESSING_TOTAL = "enforced-processing-total";
    private static final String POLL_RATE = "poll-rate";
    private static final String POLL_TOTAL = "poll-total";
    private static final String POLL_RATIO = "poll-ratio";
    private static final String POLL_RECORDS_AVG = "poll-records-avg";
    private static final String POLL_RECORDS_MAX = "poll-records-max";
    private static final String TASK_CREATED_RATE = "task-created-rate";
    private static final String TASK_CREATED_TOTAL = "task-created-total";
    private static final String TASK_CLOSED_RATE = "task-closed-rate";
    private static final String TASK_CLOSED_TOTAL = "task-closed-total";
    private static final String BLOCKED_TIME_TOTAL = "blocked-time-ns-total";
    private static final String THREAD_START_TIME = "thread-start-time";
    private static final String ACTIVE_PROCESS_RATIO = "active-process-ratio";
    private static final String ACTIVE_BUFFER_COUNT = "active-buffer-count";
    private static final String SKIPPED_RECORDS_RATE = "skipped-records-rate";
    private static final String SKIPPED_RECORDS_TOTAL = "skipped-records-total";
    private static final String RECORD_LATENESS_AVG = "record-lateness-avg";
    private static final String RECORD_LATENESS_MAX = "record-lateness-max";
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
    private static final String RECORD_E2E_LATENCY_AVG = "record-e2e-latency-avg";
    private static final String RECORD_E2E_LATENCY_MIN = "record-e2e-latency-min";
    private static final String RECORD_E2E_LATENCY_MAX = "record-e2e-latency-max";

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

    private String appId;

    @Rule
    public TestName testName = new TestName();

    @Before
    public void before() throws InterruptedException {
        builder = new StreamsBuilder();
        CLUSTER.createTopics(STREAM_INPUT, STREAM_OUTPUT_1, STREAM_OUTPUT_2, STREAM_OUTPUT_3, STREAM_OUTPUT_4);

        final String safeTestName = safeUniqueTestName(getClass(), testName);
        appId = "app-" + safeTestName;

        streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.DEBUG.name);
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, NUM_THREADS);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
    }

    @After
    public void after() throws InterruptedException {
        CLUSTER.deleteTopics(STREAM_INPUT, STREAM_OUTPUT_1, STREAM_OUTPUT_2, STREAM_OUTPUT_3, STREAM_OUTPUT_4);
    }

    private void startApplication() throws InterruptedException {
        final Topology topology = builder.build();
        kafkaStreams = new KafkaStreams(topology, streamsConfiguration);

        verifyAliveStreamThreadsMetric();
        verifyStateMetric(State.CREATED);
        verifyTopologyDescriptionMetric(topology.describe().toString());
        verifyApplicationIdMetric();

        kafkaStreams.start();
        TestUtils.waitForCondition(
            () -> kafkaStreams.state() == State.RUNNING,
            timeout,
            () -> "Kafka Streams application did not reach state RUNNING in " + timeout + " ms");

        verifyAliveStreamThreadsMetric();
        verifyStateMetric(State.RUNNING);
    }

    private void produceRecordsForTwoSegments(final Duration segmentInterval) {
        final MockTime mockTime = new MockTime(Math.max(segmentInterval.toMillis(), 60_000L));
        final Properties props = TestUtils.producerConfig(
            CLUSTER.bootstrapServers(),
            IntegerSerializer.class,
            StringSerializer.class,
            new Properties());
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            STREAM_INPUT,
            Collections.singletonList(new KeyValue<>(1, "A")),
            props,
            mockTime.milliseconds()
        );
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            STREAM_INPUT,
            Collections.singletonList(new KeyValue<>(1, "B")),
            props,
            mockTime.milliseconds()
        );
    }

    private void produceRecordsForClosingWindow(final Duration windowSize) {
        final MockTime mockTime = new MockTime(windowSize.toMillis() + 1);
        final Properties props = TestUtils.producerConfig(
            CLUSTER.bootstrapServers(),
            IntegerSerializer.class,
            StringSerializer.class,
            new Properties());
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            STREAM_INPUT,
            Collections.singletonList(new KeyValue<>(1, "A")),
            props,
            mockTime.milliseconds()
        );
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            STREAM_INPUT,
            Collections.singletonList(new KeyValue<>(1, "B")),
            props,
            mockTime.milliseconds()
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
    public void shouldAddMetricsOnAllLevels() throws Exception {
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
        checkThreadLevelMetrics();
        checkTaskLevelMetrics();
        checkProcessorNodeLevelMetrics();
        checkKeyValueStoreMetrics(IN_MEMORY_KVSTORE_TAG_KEY);
        checkKeyValueStoreMetrics(ROCKSDB_KVSTORE_TAG_KEY);
        checkKeyValueStoreMetrics(IN_MEMORY_LRUCACHE_TAG_KEY);
        checkCacheMetrics();

        closeApplication();

        checkMetricsDeregistration();
    }

    @Test
    public void shouldAddMetricsForWindowStoreAndSuppressionBuffer() throws Exception {
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

        checkWindowStoreAndSuppressionBufferMetrics();

        closeApplication();

        checkMetricsDeregistration();
    }

    @Test
    public void shouldAddMetricsForSessionStore() throws Exception {
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

        checkSessionStoreMetrics();

        closeApplication();

        checkMetricsDeregistration();
    }

    private void verifyAliveStreamThreadsMetric() {
        final List<Metric> metricsList = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
            .filter(m -> m.metricName().name().equals(ALIVE_STREAM_THREADS) &&
                m.metricName().group().equals(STREAM_CLIENT_NODE_METRICS))
            .collect(Collectors.toList());
        assertThat(metricsList.size(), is(1));
        assertThat(metricsList.get(0).metricValue(), is(NUM_THREADS));
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

    private void verifyApplicationIdMetric() {
        final List<Metric> metricsList = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
            .filter(m -> m.metricName().name().equals(APPLICATION_ID) &&
                m.metricName().group().equals(STREAM_CLIENT_NODE_METRICS))
            .collect(Collectors.toList());
        assertThat(metricsList.size(), is(1));
        assertThat(metricsList.get(0).metricValue(), is(appId));
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
        checkMetricByName(listMetricThread, ALIVE_STREAM_THREADS, 1);
        checkMetricByName(listMetricThread, FAILED_STREAM_THREADS, 1);
    }

    private void checkThreadLevelMetrics() {
        final List<Metric> listMetricThread = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
            .filter(m -> m.metricName().group().equals(STREAM_THREAD_NODE_METRICS))
            .collect(Collectors.toList());
        checkMetricByName(listMetricThread, COMMIT_LATENCY_AVG, NUM_THREADS);
        checkMetricByName(listMetricThread, COMMIT_LATENCY_MAX, NUM_THREADS);
        checkMetricByName(listMetricThread, POLL_LATENCY_AVG, NUM_THREADS);
        checkMetricByName(listMetricThread, POLL_LATENCY_MAX, NUM_THREADS);
        checkMetricByName(listMetricThread, PROCESS_LATENCY_AVG, NUM_THREADS);
        checkMetricByName(listMetricThread, PROCESS_LATENCY_MAX, NUM_THREADS);
        checkMetricByName(listMetricThread, PUNCTUATE_LATENCY_AVG, NUM_THREADS);
        checkMetricByName(listMetricThread, PUNCTUATE_LATENCY_MAX, NUM_THREADS);
        checkMetricByName(listMetricThread, COMMIT_RATE, NUM_THREADS);
        checkMetricByName(listMetricThread, COMMIT_TOTAL, NUM_THREADS);
        checkMetricByName(listMetricThread, COMMIT_RATIO, NUM_THREADS);
        checkMetricByName(listMetricThread, POLL_RATE, NUM_THREADS);
        checkMetricByName(listMetricThread, POLL_TOTAL, NUM_THREADS);
        checkMetricByName(listMetricThread, POLL_RATIO, NUM_THREADS);
        checkMetricByName(listMetricThread, POLL_RECORDS_AVG, NUM_THREADS);
        checkMetricByName(listMetricThread, POLL_RECORDS_MAX, NUM_THREADS);
        checkMetricByName(listMetricThread, PROCESS_RATE, NUM_THREADS);
        checkMetricByName(listMetricThread, PROCESS_TOTAL, NUM_THREADS);
        checkMetricByName(listMetricThread, PROCESS_RATIO, NUM_THREADS);
        checkMetricByName(listMetricThread, PROCESS_RECORDS_AVG, NUM_THREADS);
        checkMetricByName(listMetricThread, PROCESS_RECORDS_MAX, NUM_THREADS);
        checkMetricByName(listMetricThread, PUNCTUATE_RATE, NUM_THREADS);
        checkMetricByName(listMetricThread, PUNCTUATE_TOTAL, NUM_THREADS);
        checkMetricByName(listMetricThread, PUNCTUATE_RATIO, NUM_THREADS);
        checkMetricByName(listMetricThread, TASK_CREATED_RATE, NUM_THREADS);
        checkMetricByName(listMetricThread, TASK_CREATED_TOTAL, NUM_THREADS);
        checkMetricByName(listMetricThread, TASK_CLOSED_RATE, NUM_THREADS);
        checkMetricByName(listMetricThread, TASK_CLOSED_TOTAL, NUM_THREADS);
        checkMetricByName(listMetricThread, BLOCKED_TIME_TOTAL, NUM_THREADS);
        checkMetricByName(listMetricThread, THREAD_START_TIME, NUM_THREADS);
    }

    private void checkTaskLevelMetrics() {
        final List<Metric> listMetricTask = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
            .filter(m -> m.metricName().group().equals(STREAM_TASK_NODE_METRICS))
            .collect(Collectors.toList());
        checkMetricByName(listMetricTask, ENFORCED_PROCESSING_RATE, 4);
        checkMetricByName(listMetricTask, ENFORCED_PROCESSING_TOTAL, 4);
        checkMetricByName(listMetricTask, RECORD_LATENESS_AVG, 4);
        checkMetricByName(listMetricTask, RECORD_LATENESS_MAX, 4);
        checkMetricByName(listMetricTask, ACTIVE_PROCESS_RATIO, 4);
        checkMetricByName(listMetricTask, ACTIVE_BUFFER_COUNT, 4);
        checkMetricByName(listMetricTask, PROCESS_LATENCY_AVG, 4);
        checkMetricByName(listMetricTask, PROCESS_LATENCY_MAX, 4);
        checkMetricByName(listMetricTask, PUNCTUATE_LATENCY_AVG, 4);
        checkMetricByName(listMetricTask, PUNCTUATE_LATENCY_MAX, 4);
        checkMetricByName(listMetricTask, PUNCTUATE_RATE, 4);
        checkMetricByName(listMetricTask, PUNCTUATE_TOTAL, 4);
        checkMetricByName(listMetricTask, PROCESS_RATE, 4);
        checkMetricByName(listMetricTask, PROCESS_TOTAL, 4);
    }

    private void checkProcessorNodeLevelMetrics() {
        final List<Metric> listMetricProcessor = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
            .filter(m -> m.metricName().group().equals(STREAM_PROCESSOR_NODE_METRICS))
            .collect(Collectors.toList());
        final int numberOfSourceNodes = 4;
        final int numberOfTerminalNodes = 4;
        checkMetricByName(listMetricProcessor, PROCESS_RATE, 4);
        checkMetricByName(listMetricProcessor, PROCESS_TOTAL, 4);
        checkMetricByName(listMetricProcessor, RECORD_E2E_LATENCY_AVG, numberOfSourceNodes + numberOfTerminalNodes);
        checkMetricByName(listMetricProcessor, RECORD_E2E_LATENCY_MIN, numberOfSourceNodes + numberOfTerminalNodes);
        checkMetricByName(listMetricProcessor, RECORD_E2E_LATENCY_MAX, numberOfSourceNodes + numberOfTerminalNodes);
    }

    private void checkKeyValueStoreMetrics(final String tagKey) {
        final List<Metric> listMetricStore = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
            .filter(m -> m.metricName().tags().containsKey(tagKey) && m.metricName().group().equals(STATE_STORE_LEVEL_GROUP))
            .collect(Collectors.toList());

        final int expectedNumberOfLatencyMetrics = 1;
        final int expectedNumberOfRateMetrics = 1;
        final int expectedNumberOfTotalMetrics = 0;
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
        checkMetricByName(listMetricStore, RECORD_E2E_LATENCY_AVG, 1);
        checkMetricByName(listMetricStore, RECORD_E2E_LATENCY_MIN, 1);
        checkMetricByName(listMetricStore, RECORD_E2E_LATENCY_MAX, 1);
    }

    private void checkMetricsDeregistration() {
        final List<Metric> listMetricAfterClosingApp = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
            .filter(m -> m.metricName().group().contains(STREAM_STRING))
            .collect(Collectors.toList());
        assertThat(listMetricAfterClosingApp.size(), is(0));
    }

    private void checkCacheMetrics() {
        final List<Metric> listMetricCache = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
            .filter(m -> m.metricName().group().equals(STREAM_CACHE_NODE_METRICS))
            .collect(Collectors.toList());
        checkMetricByName(listMetricCache, HIT_RATIO_AVG, 3);
        checkMetricByName(listMetricCache, HIT_RATIO_MIN, 3);
        checkMetricByName(listMetricCache, HIT_RATIO_MAX, 3);
    }

    private void checkWindowStoreAndSuppressionBufferMetrics() {
        final List<Metric> listMetricStore = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
            .filter(m -> m.metricName().group().equals(STATE_STORE_LEVEL_GROUP))
            .collect(Collectors.toList());
        checkMetricByName(listMetricStore, PUT_LATENCY_AVG, 1);
        checkMetricByName(listMetricStore, PUT_LATENCY_MAX, 1);
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
        checkMetricByName(listMetricStore, FLUSH_LATENCY_AVG, 1);
        checkMetricByName(listMetricStore, FLUSH_LATENCY_MAX, 1);
        checkMetricByName(listMetricStore, RESTORE_LATENCY_AVG, 1);
        checkMetricByName(listMetricStore, RESTORE_LATENCY_MAX, 1);
        checkMetricByName(listMetricStore, FETCH_LATENCY_AVG, 1);
        checkMetricByName(listMetricStore, FETCH_LATENCY_MAX, 1);
        checkMetricByName(listMetricStore, PUT_RATE, 1);
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
        checkMetricByName(listMetricStore, FLUSH_RATE, 1);
        checkMetricByName(listMetricStore, RESTORE_RATE, 1);
        checkMetricByName(listMetricStore, FETCH_RATE, 1);
        checkMetricByName(listMetricStore, SUPPRESSION_BUFFER_COUNT_AVG, 1);
        checkMetricByName(listMetricStore, SUPPRESSION_BUFFER_COUNT_MAX, 1);
        checkMetricByName(listMetricStore, SUPPRESSION_BUFFER_SIZE_AVG, 1);
        checkMetricByName(listMetricStore, SUPPRESSION_BUFFER_SIZE_MAX, 1);
        checkMetricByName(listMetricStore, RECORD_E2E_LATENCY_AVG, 1);
        checkMetricByName(listMetricStore, RECORD_E2E_LATENCY_MIN, 1);
        checkMetricByName(listMetricStore, RECORD_E2E_LATENCY_MAX, 1);
    }

    private void checkSessionStoreMetrics() {
        final List<Metric> listMetricStore = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
            .filter(m -> m.metricName().group().equals(STATE_STORE_LEVEL_GROUP))
            .collect(Collectors.toList());
        checkMetricByName(listMetricStore, PUT_LATENCY_AVG, 1);
        checkMetricByName(listMetricStore, PUT_LATENCY_MAX, 1);
        checkMetricByName(listMetricStore, PUT_IF_ABSENT_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, PUT_IF_ABSENT_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, GET_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, GET_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, DELETE_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, DELETE_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, REMOVE_LATENCY_AVG, 1);
        checkMetricByName(listMetricStore, REMOVE_LATENCY_MAX, 1);
        checkMetricByName(listMetricStore, PUT_ALL_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, PUT_ALL_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, ALL_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, ALL_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, RANGE_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, RANGE_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, FLUSH_LATENCY_AVG, 1);
        checkMetricByName(listMetricStore, FLUSH_LATENCY_MAX, 1);
        checkMetricByName(listMetricStore, RESTORE_LATENCY_AVG, 1);
        checkMetricByName(listMetricStore, RESTORE_LATENCY_MAX, 1);
        checkMetricByName(listMetricStore, FETCH_LATENCY_AVG, 1);
        checkMetricByName(listMetricStore, FETCH_LATENCY_MAX, 1);
        checkMetricByName(listMetricStore, PUT_RATE, 1);
        checkMetricByName(listMetricStore, PUT_IF_ABSENT_RATE, 0);
        checkMetricByName(listMetricStore, PUT_IF_ABSENT_TOTAL, 0);
        checkMetricByName(listMetricStore, GET_RATE, 0);
        checkMetricByName(listMetricStore, GET_TOTAL, 0);
        checkMetricByName(listMetricStore, DELETE_RATE, 0);
        checkMetricByName(listMetricStore, DELETE_TOTAL, 0);
        checkMetricByName(listMetricStore, REMOVE_RATE, 1);
        checkMetricByName(listMetricStore, PUT_ALL_RATE, 0);
        checkMetricByName(listMetricStore, PUT_ALL_TOTAL, 0);
        checkMetricByName(listMetricStore, ALL_RATE, 0);
        checkMetricByName(listMetricStore, ALL_TOTAL, 0);
        checkMetricByName(listMetricStore, RANGE_RATE, 0);
        checkMetricByName(listMetricStore, RANGE_TOTAL, 0);
        checkMetricByName(listMetricStore, FLUSH_RATE, 1);
        checkMetricByName(listMetricStore, RESTORE_RATE, 1);
        checkMetricByName(listMetricStore, FETCH_RATE, 1);
        checkMetricByName(listMetricStore, SUPPRESSION_BUFFER_COUNT_CURRENT, 0);
        checkMetricByName(listMetricStore, SUPPRESSION_BUFFER_COUNT_AVG, 0);
        checkMetricByName(listMetricStore, SUPPRESSION_BUFFER_COUNT_MAX, 0);
        checkMetricByName(listMetricStore, SUPPRESSION_BUFFER_SIZE_CURRENT, 0);
        checkMetricByName(listMetricStore, SUPPRESSION_BUFFER_SIZE_AVG, 0);
        checkMetricByName(listMetricStore, SUPPRESSION_BUFFER_SIZE_MAX, 0);
        checkMetricByName(listMetricStore, RECORD_E2E_LATENCY_AVG, 1);
        checkMetricByName(listMetricStore, RECORD_E2E_LATENCY_MIN, 1);
        checkMetricByName(listMetricStore, RECORD_E2E_LATENCY_MAX, 1);
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