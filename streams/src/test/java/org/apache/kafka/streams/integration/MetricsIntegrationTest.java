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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.processor.internals.ProcessorNode.NodeMetrics.DROPPED_LATE_RECORDS;
import static org.apache.kafka.streams.processor.internals.ProcessorNode.NodeMetrics.STREAM_PROCESSOR_NODE_METRICS;
import static org.apache.kafka.streams.processor.internals.ProcessorNode.NodeMetrics.SUPPRESSION_EMIT_RECORDS;
import static org.apache.kafka.streams.processor.internals.StreamTask.TaskMetrics.ENFORCED_PROCESSING;
import static org.apache.kafka.streams.processor.internals.StreamTask.TaskMetrics.RECORD_LATENESS;
import static org.apache.kafka.streams.processor.internals.StreamTask.TaskMetrics.SKIPPED_RECORDS;
import static org.apache.kafka.streams.processor.internals.StreamTask.TaskMetrics.STREAM_TASK_METRICS;
import static org.apache.kafka.streams.processor.internals.StreamThread.StreamThreadMetrics.COMMIT;
import static org.apache.kafka.streams.processor.internals.StreamThread.StreamThreadMetrics.COMMIT_LATENCY;
import static org.apache.kafka.streams.processor.internals.StreamThread.StreamThreadMetrics.POLL;
import static org.apache.kafka.streams.processor.internals.StreamThread.StreamThreadMetrics.POLL_LATENCY;
import static org.apache.kafka.streams.processor.internals.StreamThread.StreamThreadMetrics.PROCESS;
import static org.apache.kafka.streams.processor.internals.StreamThread.StreamThreadMetrics.PROCESS_LATENCY;
import static org.apache.kafka.streams.processor.internals.StreamThread.StreamThreadMetrics.PUNCTUATE;
import static org.apache.kafka.streams.processor.internals.StreamThread.StreamThreadMetrics.PUNCTUATE_LATENCY;
import static org.apache.kafka.streams.processor.internals.StreamThread.StreamThreadMetrics.STREAM_THREAD_METRICS;
import static org.apache.kafka.streams.processor.internals.StreamThread.StreamThreadMetrics.TASK_CLOSED;
import static org.apache.kafka.streams.processor.internals.StreamThread.StreamThreadMetrics.TASK_CREATED;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.AVG_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.LATENCY_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.MAX_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.MIN_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.RATE_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.TOTAL_SUFFIX;
import static org.apache.kafka.streams.state.internals.NamedCache.NamedCacheMetrics.HIT_RATIO;
import static org.apache.kafka.streams.state.internals.NamedCache.NamedCacheMetrics.STREAM_RECORD_CACHE_METRICS;
import static org.apache.kafka.streams.state.internals.StoreMetrics.ALL;
import static org.apache.kafka.streams.state.internals.StoreMetrics.DELETE;
import static org.apache.kafka.streams.state.internals.StoreMetrics.FLUSH;
import static org.apache.kafka.streams.state.internals.StoreMetrics.GET;
import static org.apache.kafka.streams.state.internals.StoreMetrics.IN_MEMORY_LRU;
import static org.apache.kafka.streams.state.internals.StoreMetrics.IN_MEMORY_STATE;
import static org.apache.kafka.streams.state.internals.StoreMetrics.PUT;
import static org.apache.kafka.streams.state.internals.StoreMetrics.PUT_ALL;
import static org.apache.kafka.streams.state.internals.StoreMetrics.PUT_IF_ABSENT;
import static org.apache.kafka.streams.state.internals.StoreMetrics.RANGE;
import static org.apache.kafka.streams.state.internals.StoreMetrics.RESTORE;
import static org.apache.kafka.streams.state.internals.StoreMetrics.ROCKSDB_STATE;
import static org.apache.kafka.streams.state.internals.StoreMetrics.SESSION_ROCKSDB_STATE;
import static org.apache.kafka.streams.state.internals.StoreMetrics.WINDOW_ROCKSDB_STATE;

@SuppressWarnings("unchecked")
@Category({IntegrationTest.class})
public class MetricsIntegrationTest {

    private static final int NUM_BROKERS = 1;

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER =
            new EmbeddedKafkaCluster(NUM_BROKERS);

    private static final String STREAM_STRING = "stream";

    // Metric group
    private static final String STREAM_STORE_IN_MEMORY_STATE_METRICS = StreamsMetricsImpl.groupNameFromScope(IN_MEMORY_STATE);
    private static final String STREAM_STORE_IN_MEMORY_LRU_STATE_METRICS = StreamsMetricsImpl.groupNameFromScope(IN_MEMORY_LRU);
    private static final String STREAM_STORE_ROCKSDB_STATE_METRICS = StreamsMetricsImpl.groupNameFromScope(ROCKSDB_STATE);
    private static final String STREAM_STORE_WINDOW_ROCKSDB_STATE_METRICS = StreamsMetricsImpl.groupNameFromScope(WINDOW_ROCKSDB_STATE);
    private static final String STREAM_STORE_SESSION_ROCKSDB_STATE_METRICS = StreamsMetricsImpl.groupNameFromScope(SESSION_ROCKSDB_STATE);

    // Metrics name
    private static final String PUT_LATENCY_AVG = PUT + LATENCY_SUFFIX + AVG_SUFFIX;
    private static final String PUT_LATENCY_MAX = PUT + LATENCY_SUFFIX + MAX_SUFFIX;
    private static final String PUT_RATE = PUT + RATE_SUFFIX;
    private static final String PUT_TOTAL = PUT + TOTAL_SUFFIX;
    private static final String PUT_IF_ABSENT_LATENCY_AVG = PUT_IF_ABSENT + LATENCY_SUFFIX + AVG_SUFFIX;
    private static final String PUT_IF_ABSENT_LATENCY_MAX = PUT_IF_ABSENT + LATENCY_SUFFIX + MAX_SUFFIX;
    private static final String PUT_IF_ABSENT_RATE = PUT_IF_ABSENT + RATE_SUFFIX;
    private static final String PUT_IF_ABSENT_TOTAL = PUT_IF_ABSENT + TOTAL_SUFFIX;
    private static final String GET_LATENCY_AVG = GET + LATENCY_SUFFIX + AVG_SUFFIX;
    private static final String GET_LATENCY_MAX = GET + LATENCY_SUFFIX + MAX_SUFFIX;
    private static final String GET_RATE = GET + RATE_SUFFIX;
    private static final String GET_TOTAL = GET + TOTAL_SUFFIX;
    private static final String DELETE_LATENCY_AVG = DELETE + LATENCY_SUFFIX + AVG_SUFFIX;
    private static final String DELETE_LATENCY_MAX = DELETE + LATENCY_SUFFIX + MAX_SUFFIX;
    private static final String DELETE_RATE = DELETE + RATE_SUFFIX;
    private static final String DELETE_TOTAL = DELETE + TOTAL_SUFFIX;
    private static final String PUT_ALL_LATENCY_AVG = PUT_ALL + LATENCY_SUFFIX + AVG_SUFFIX;
    private static final String PUT_ALL_LATENCY_MAX = PUT_ALL + LATENCY_SUFFIX + MAX_SUFFIX;
    private static final String PUT_ALL_RATE = PUT_ALL + RATE_SUFFIX;
    private static final String PUT_ALL_TOTAL = PUT_ALL + TOTAL_SUFFIX;
    private static final String ALL_LATENCY_AVG = ALL + LATENCY_SUFFIX + AVG_SUFFIX;
    private static final String ALL_LATENCY_MAX = ALL + LATENCY_SUFFIX + MAX_SUFFIX;
    private static final String ALL_RATE = ALL + RATE_SUFFIX;
    private static final String ALL_TOTAL = ALL + TOTAL_SUFFIX;
    private static final String RANGE_LATENCY_AVG = RANGE + LATENCY_SUFFIX + AVG_SUFFIX;
    private static final String RANGE_LATENCY_MAX = RANGE + LATENCY_SUFFIX + MAX_SUFFIX;
    private static final String RANGE_RATE = RANGE + RATE_SUFFIX;
    private static final String RANGE_TOTAL = RANGE + TOTAL_SUFFIX;
    private static final String FLUSH_LATENCY_AVG = FLUSH + LATENCY_SUFFIX + AVG_SUFFIX;
    private static final String FLUSH_LATENCY_MAX = FLUSH + LATENCY_SUFFIX + MAX_SUFFIX;
    private static final String FLUSH_RATE = FLUSH + RATE_SUFFIX;
    private static final String FLUSH_TOTAL = FLUSH + TOTAL_SUFFIX;
    private static final String RESTORE_LATENCY_AVG = RESTORE + LATENCY_SUFFIX + AVG_SUFFIX;
    private static final String RESTORE_LATENCY_MAX = RESTORE + LATENCY_SUFFIX + MAX_SUFFIX;
    private static final String RESTORE_RATE = RESTORE + RATE_SUFFIX;
    private static final String RESTORE_TOTAL = RESTORE + TOTAL_SUFFIX;
    private static final String PROCESS_LATENCY_AVG = PROCESS_LATENCY + AVG_SUFFIX;
    private static final String PROCESS_LATENCY_MAX = PROCESS_LATENCY + MAX_SUFFIX;
    private static final String PROCESS_RATE = PROCESS + RATE_SUFFIX;
    private static final String PROCESS_TOTAL = PROCESS + TOTAL_SUFFIX;
    private static final String PUNCTUATE_LATENCY_AVG = PUNCTUATE_LATENCY + AVG_SUFFIX;
    private static final String PUNCTUATE_LATENCY_MAX = PUNCTUATE_LATENCY + MAX_SUFFIX;
    private static final String PUNCTUATE_RATE = PUNCTUATE + RATE_SUFFIX;
    private static final String PUNCTUATE_TOTAL = PUNCTUATE + TOTAL_SUFFIX;
    private static final String COMMIT_LATENCY_AVG = COMMIT_LATENCY + AVG_SUFFIX;
    private static final String COMMIT_LATENCY_MAX = COMMIT_LATENCY + MAX_SUFFIX;
    private static final String COMMIT_RATE = COMMIT + RATE_SUFFIX;
    private static final String COMMIT_TOTAL = COMMIT + TOTAL_SUFFIX;
    private static final String POLL_LATENCY_AVG = POLL_LATENCY + AVG_SUFFIX;
    private static final String POLL_LATENCY_MAX = POLL_LATENCY + MAX_SUFFIX;
    private static final String POLL_RATE = POLL + RATE_SUFFIX;
    private static final String POLL_TOTAL = POLL + TOTAL_SUFFIX;
    private static final String TASK_CREATED_RATE = TASK_CREATED + RATE_SUFFIX;
    private static final String TASK_CREATED_TOTAL = TASK_CREATED + TOTAL_SUFFIX;
    private static final String TASK_CLOSED_RATE = TASK_CLOSED + RATE_SUFFIX;
    private static final String TASK_CLOSED_TOTAL = TASK_CLOSED + TOTAL_SUFFIX;
    private static final String SKIPPED_RECORDS_RATE = SKIPPED_RECORDS + RATE_SUFFIX;
    private static final String SKIPPED_RECORDS_TOTAL = SKIPPED_RECORDS + TOTAL_SUFFIX;
    private static final String DROPPED_LATE_RECORDS_RATE = DROPPED_LATE_RECORDS + RATE_SUFFIX;
    private static final String DROPPED_LATE_RECORDS_TOTAL = DROPPED_LATE_RECORDS + TOTAL_SUFFIX;
    private static final String SUPPRESSION_EMIT_RECORDS_RATE = SUPPRESSION_EMIT_RECORDS + RATE_SUFFIX;
    private static final String SUPPRESSION_EMIT_RECORDS_TOTAL = SUPPRESSION_EMIT_RECORDS + TOTAL_SUFFIX;
    private static final String ENFORCED_PROCESSING_RATE = ENFORCED_PROCESSING + RATE_SUFFIX;
    private static final String ENFORCED_PROCESSING_TOTAL = ENFORCED_PROCESSING + TOTAL_SUFFIX;
    private static final String RECORD_LATENESS_AVG = RECORD_LATENESS + AVG_SUFFIX;
    private static final String RECORD_LATENESS_MAX = RECORD_LATENESS + MAX_SUFFIX;
    private static final String HIT_RATIO_AVG = HIT_RATIO + AVG_SUFFIX;
    private static final String HIT_RATIO_MIN = HIT_RATIO + MIN_SUFFIX;
    private static final String HIT_RATIO_MAX = HIT_RATIO + MAX_SUFFIX;

    // stores name
    private static final String TIME_WINDOWED_AGGREGATED_STREAM_STORE = "time-windowed-aggregated-stream-store";
    private static final String SESSION_AGGREGATED_STREAM_STORE = "session-aggregated-stream-store";
    private static final String MY_STORE_IN_MEMORY = "myStoreInMemory";
    private static final String MY_STORE_PERSISTENT_KEY_VALUE = "myStorePersistentKeyValue";
    private static final String MY_STORE_LRU_MAP = "myStoreLruMap";

    private StreamsBuilder builder;
    private Properties streamsConfiguration;
    private KafkaStreams kafkaStreams;

    // topic names
    private static final String STREAM_INPUT = "STREAM_INPUT";
    private static final String STREAM_OUTPUT_1 = "STREAM_OUTPUT_1";
    private static final String STREAM_OUTPUT_2 = "STREAM_OUTPUT_2";
    private static final String STREAM_OUTPUT_3 = "STREAM_OUTPUT_3";
    private static final String STREAM_OUTPUT_4 = "STREAM_OUTPUT_4";

    private final String appId = "stream-metrics-test";

    @Before
    public void before() throws InterruptedException {
        builder = new StreamsBuilder();
        CLUSTER.createTopics(STREAM_INPUT, STREAM_OUTPUT_1, STREAM_OUTPUT_2, STREAM_OUTPUT_3, STREAM_OUTPUT_4);
        streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.DEBUG.name);
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);
    }

    @After
    public void after() throws InterruptedException {
        CLUSTER.deleteTopics(STREAM_INPUT, STREAM_OUTPUT_1, STREAM_OUTPUT_2, STREAM_OUTPUT_3, STREAM_OUTPUT_4);
    }

    private void startApplication() {
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.start();
    }

    private void closeApplication() throws Exception {
        kafkaStreams.close();
        kafkaStreams.cleanUp();
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    private void checkMetricDeregistration() throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            final List<Metric> listMetricAfterClosingApp = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream().filter(m -> m.metricName().group().contains(STREAM_STRING)).collect(Collectors.toList());
            return listMetricAfterClosingApp.size() == 0;
        }, 10000, "de-registration of metrics");
    }

    @Test
    public void testStreamMetric() throws Exception {
        final StringBuilder errorMessage = new StringBuilder();
        builder.stream(STREAM_INPUT, Consumed.with(Serdes.Integer(), Serdes.String()))
            .to(STREAM_OUTPUT_1, Produced.with(Serdes.Integer(), Serdes.String()));
        builder.table(STREAM_OUTPUT_1, Materialized.as(Stores.inMemoryKeyValueStore(MY_STORE_IN_MEMORY)).withCachingEnabled())
                .toStream()
                .to(STREAM_OUTPUT_2);
        builder.table(STREAM_OUTPUT_2, Materialized.as(Stores.persistentKeyValueStore(MY_STORE_PERSISTENT_KEY_VALUE)).withCachingEnabled())
                .toStream()
                .to(STREAM_OUTPUT_3);
        final KGroupedStream<Integer, Integer> groupedStream = builder.stream(STREAM_OUTPUT_3, Consumed.with(Serdes.Integer(), Serdes.Integer()))
            .groupByKey();
        groupedStream.aggregate(() -> 0L, (aggKey, newValue, aggValue) -> aggValue,
                Materialized.<Integer, Long>as(Stores.lruMap(MY_STORE_LRU_MAP, 10000))
                    .withValueSerde(Serdes.Long()))
            .suppress(Suppressed.untilTimeLimit(Duration.ofMillis(10000L), Suppressed.BufferConfig.unbounded()))
            .toStream()
            .to(STREAM_OUTPUT_4);

        startApplication();

        // metric level : Thread
        TestUtils.waitForCondition(() -> testThreadMetric(errorMessage), 10000, () -> "testThreadMetric -> " + errorMessage.toString());

        // metric level : Task
        TestUtils.waitForCondition(() -> testTaskMetric(errorMessage), 10000, () -> "testTaskMetric -> " + errorMessage.toString());

        // metric level : Processor
        TestUtils.waitForCondition(() -> testProcessorMetric(errorMessage), 10000, () -> "testProcessorMetric -> " + errorMessage.toString());

        // metric level : Store (in-memory-state, in-memory-lru-state, rocksdb-state)
        TestUtils.waitForCondition(() -> testStoreMetricKeyValueByType(STREAM_STORE_IN_MEMORY_STATE_METRICS, errorMessage), 10000, () -> "testStoreMetricKeyValueByType:" + STREAM_STORE_IN_MEMORY_STATE_METRICS + " -> " + errorMessage.toString());
        TestUtils.waitForCondition(() -> testStoreMetricKeyValueByType(STREAM_STORE_IN_MEMORY_LRU_STATE_METRICS, errorMessage), 10000, () -> "testStoreMetricKeyValueByType:" + STREAM_STORE_IN_MEMORY_LRU_STATE_METRICS + " -> " + errorMessage.toString());
        TestUtils.waitForCondition(() -> testStoreMetricKeyValueByType(STREAM_STORE_ROCKSDB_STATE_METRICS, errorMessage), 10000, () -> "testStoreMetricKeyValueByType:" + STREAM_STORE_ROCKSDB_STATE_METRICS + " -> " + errorMessage.toString());

        //metric level : Cache
        TestUtils.waitForCondition(() -> testCacheMetric(errorMessage), 10000, () -> "testCacheMetric -> " + errorMessage.toString());

        closeApplication();

        // check all metrics de-registered
        checkMetricDeregistration();
    }

    @Test
    public void testStreamMetricOfWindowStore() throws Exception {
        final StringBuilder errorMessage = new StringBuilder();
        final KGroupedStream<Integer, String> groupedStream = builder.stream(STREAM_INPUT, Consumed.with(Serdes.Integer(), Serdes.String()))
            .groupByKey();
        groupedStream.windowedBy(TimeWindows.of(Duration.ofMillis(50)))
                .aggregate(() -> 0L, (aggKey, newValue, aggValue) -> aggValue,
                        Materialized.<Integer, Long, WindowStore<Bytes, byte[]>>as(TIME_WINDOWED_AGGREGATED_STREAM_STORE)
                                .withValueSerde(Serdes.Long()));

        startApplication();

        // metric level : Store (window)
        TestUtils.waitForCondition(() -> testStoreMetricWindow(errorMessage), 10000, () -> "testStoreMetricWindow -> " + errorMessage.toString());

        // metric level : Processor
        TestUtils.waitForCondition(() -> testProcessorDroppedLateRecordsMetric(errorMessage), 10000, () -> "testProcessorDroppedLateRecordsMetric -> " + errorMessage.toString());

        closeApplication();

        // check all metrics de-registered
        checkMetricDeregistration();
    }

    @Test
    public void testStreamMetricOfSessionStore() throws Exception {
        final StringBuilder errorMessage = new StringBuilder();
        final KGroupedStream<Integer, String> groupedStream = builder.stream(STREAM_INPUT, Consumed.with(Serdes.Integer(), Serdes.String()))
            .groupByKey();
        groupedStream.windowedBy(SessionWindows.with(Duration.ofMillis(50)))
                .aggregate(() -> 0L, (aggKey, newValue, aggValue) -> aggValue, (aggKey, leftAggValue, rightAggValue) -> leftAggValue,
                        Materialized.<Integer, Long, SessionStore<Bytes, byte[]>>as(SESSION_AGGREGATED_STREAM_STORE)
                                .withValueSerde(Serdes.Long()));

        startApplication();

        // metric level : Store (session)
        TestUtils.waitForCondition(() -> testStoreMetricSession(errorMessage), 10000, () -> "testStoreMetricSession -> " + errorMessage.toString());

        // metric level : Processor
        TestUtils.waitForCondition(() -> testProcessorDroppedLateRecordsMetric(errorMessage), 10000, () -> "testProcessorDroppedLateRecordsMetric -> " + errorMessage.toString());

        closeApplication();

        // check all metrics de-registered
        checkMetricDeregistration();
    }

    private boolean testThreadMetric(final StringBuilder errorMessage) {
        errorMessage.setLength(0);
        try {
            final List<Metric> listMetricThread = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream().filter(m -> m.metricName().group().equals(STREAM_THREAD_METRICS)).collect(Collectors.toList());
            testMetricByName(listMetricThread, COMMIT_LATENCY_AVG, 1);
            testMetricByName(listMetricThread, COMMIT_LATENCY_MAX, 1);
            testMetricByName(listMetricThread, POLL_LATENCY_AVG, 1);
            testMetricByName(listMetricThread, POLL_LATENCY_MAX, 1);
            testMetricByName(listMetricThread, PROCESS_LATENCY_AVG, 1);
            testMetricByName(listMetricThread, PROCESS_LATENCY_MAX, 1);
            testMetricByName(listMetricThread, PUNCTUATE_LATENCY_AVG, 1);
            testMetricByName(listMetricThread, PUNCTUATE_LATENCY_MAX, 1);
            testMetricByName(listMetricThread, COMMIT_RATE, 1);
            testMetricByName(listMetricThread, COMMIT_TOTAL, 1);
            testMetricByName(listMetricThread, POLL_RATE, 1);
            testMetricByName(listMetricThread, POLL_TOTAL, 1);
            testMetricByName(listMetricThread, PROCESS_RATE, 1);
            testMetricByName(listMetricThread, PROCESS_TOTAL, 1);
            testMetricByName(listMetricThread, PUNCTUATE_RATE, 1);
            testMetricByName(listMetricThread, PUNCTUATE_TOTAL, 1);
            testMetricByName(listMetricThread, TASK_CREATED_RATE, 1);
            testMetricByName(listMetricThread, TASK_CREATED_TOTAL, 1);
            testMetricByName(listMetricThread, TASK_CLOSED_RATE, 1);
            testMetricByName(listMetricThread, TASK_CLOSED_TOTAL, 1);
            testMetricByName(listMetricThread, SKIPPED_RECORDS_RATE, 0);
            testMetricByName(listMetricThread, SKIPPED_RECORDS_TOTAL, 0);
            return true;
        } catch (final Throwable e) {
            errorMessage.append(e.getMessage());
            return false;
        }
    }

    private boolean testTaskMetric(final StringBuilder errorMessage) {
        errorMessage.setLength(0);
        try {
            final List<Metric> listMetricTask = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream().filter(m -> m.metricName().group().equals(STREAM_TASK_METRICS)).collect(Collectors.toList());
            testMetricByName(listMetricTask, COMMIT_LATENCY_AVG, 4);
            testMetricByName(listMetricTask, COMMIT_LATENCY_MAX, 4);
            testMetricByName(listMetricTask, COMMIT_RATE, 4);
            testMetricByName(listMetricTask, COMMIT_TOTAL, 4);
            testMetricByName(listMetricTask, PROCESS_LATENCY_AVG, 4);
            testMetricByName(listMetricTask, PROCESS_LATENCY_MAX, 4);
            testMetricByName(listMetricTask, PROCESS_RATE, 4);
            testMetricByName(listMetricTask, PROCESS_TOTAL, 4);
            testMetricByName(listMetricTask, PUNCTUATE_LATENCY_AVG, 4);
            testMetricByName(listMetricTask, PUNCTUATE_LATENCY_MAX, 4);
            testMetricByName(listMetricTask, PUNCTUATE_RATE, 4);
            testMetricByName(listMetricTask, PUNCTUATE_TOTAL, 4);
            testMetricByName(listMetricTask, RECORD_LATENESS_AVG, 4);
            testMetricByName(listMetricTask, RECORD_LATENESS_MAX, 4);
            testMetricByName(listMetricTask, SKIPPED_RECORDS_RATE, 4);
            testMetricByName(listMetricTask, SKIPPED_RECORDS_TOTAL, 4);
            testMetricByName(listMetricTask, ENFORCED_PROCESSING_RATE, 4);
            testMetricByName(listMetricTask, ENFORCED_PROCESSING_TOTAL, 4);
            return true;
        } catch (final Throwable e) {
            errorMessage.append(e.getMessage());
            return false;
        }
    }

    private boolean testProcessorMetric(final StringBuilder errorMessage) {
        errorMessage.setLength(0);
        try {
            final List<Metric> listMetricProcessor = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream().filter(m -> m.metricName().group().equals(STREAM_PROCESSOR_NODE_METRICS)).collect(Collectors.toList());
            testMetricByName(listMetricProcessor, PROCESS_LATENCY_AVG, 0);
            testMetricByName(listMetricProcessor, PROCESS_LATENCY_MAX, 0);
            testMetricByName(listMetricProcessor, PUNCTUATE_LATENCY_AVG, 0);
            testMetricByName(listMetricProcessor, PUNCTUATE_LATENCY_MAX, 0);
            testMetricByName(listMetricProcessor, PROCESS_RATE, 15);
            testMetricByName(listMetricProcessor, PROCESS_TOTAL, 15);
            testMetricByName(listMetricProcessor, PUNCTUATE_RATE, 0);
            testMetricByName(listMetricProcessor, PUNCTUATE_TOTAL, 0);
            testMetricByName(listMetricProcessor, SKIPPED_RECORDS_RATE, 3);
            testMetricByName(listMetricProcessor, SKIPPED_RECORDS_TOTAL, 3);
            testMetricByName(listMetricProcessor, DROPPED_LATE_RECORDS_RATE, 0);
            testMetricByName(listMetricProcessor, DROPPED_LATE_RECORDS_TOTAL, 0);
            testMetricByName(listMetricProcessor, SUPPRESSION_EMIT_RECORDS_RATE, 1);
            testMetricByName(listMetricProcessor, SUPPRESSION_EMIT_RECORDS_TOTAL, 1);

            return true;
        } catch (final Throwable e) {
            errorMessage.append(e.getMessage());
            return false;
        }
    }

    private boolean testProcessorDroppedLateRecordsMetric(final StringBuilder errorMessage) {
        errorMessage.setLength(0);
        try {
            final List<Metric> listMetricProcessor = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream().filter(m -> m.metricName().group().equals(STREAM_PROCESSOR_NODE_METRICS)).collect(Collectors.toList());
            testMetricByName(listMetricProcessor, DROPPED_LATE_RECORDS_RATE, 1);
            testMetricByName(listMetricProcessor, DROPPED_LATE_RECORDS_TOTAL, 1);

            return true;
        } catch (final Throwable e) {
            errorMessage.append(e.getMessage());
            return false;
        }
    }

    private boolean testStoreMetricWindow(final StringBuilder errorMessage) {
        errorMessage.setLength(0);
        try {
            final List<Metric> listMetricStore = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
                    .filter(m -> m.metricName().group().equals(STREAM_STORE_WINDOW_ROCKSDB_STATE_METRICS))
                    .collect(Collectors.toList());
            testMetricByName(listMetricStore, PUT_LATENCY_AVG, 1);
            testMetricByName(listMetricStore, PUT_LATENCY_MAX, 1);
            testMetricByName(listMetricStore, PUT_IF_ABSENT_LATENCY_AVG, 0);
            testMetricByName(listMetricStore, PUT_IF_ABSENT_LATENCY_MAX, 0);
            testMetricByName(listMetricStore, GET_LATENCY_AVG, 1);
            testMetricByName(listMetricStore, GET_LATENCY_MAX, 1);
            testMetricByName(listMetricStore, DELETE_LATENCY_AVG, 0);
            testMetricByName(listMetricStore, DELETE_LATENCY_MAX, 0);
            testMetricByName(listMetricStore, PUT_ALL_LATENCY_AVG, 0);
            testMetricByName(listMetricStore, PUT_ALL_LATENCY_MAX, 0);
            testMetricByName(listMetricStore, ALL_LATENCY_AVG, 0);
            testMetricByName(listMetricStore, ALL_LATENCY_MAX, 0);
            testMetricByName(listMetricStore, RANGE_LATENCY_AVG, 1);
            testMetricByName(listMetricStore, RANGE_LATENCY_MAX, 1);
            testMetricByName(listMetricStore, FLUSH_LATENCY_AVG, 1);
            testMetricByName(listMetricStore, FLUSH_LATENCY_MAX, 1);
            testMetricByName(listMetricStore, RESTORE_LATENCY_AVG, 1);
            testMetricByName(listMetricStore, RESTORE_LATENCY_MAX, 1);
            testMetricByName(listMetricStore, PUT_RATE, 1);
            testMetricByName(listMetricStore, PUT_TOTAL, 1);
            testMetricByName(listMetricStore, PUT_IF_ABSENT_RATE, 0);
            testMetricByName(listMetricStore, PUT_IF_ABSENT_TOTAL, 0);
            testMetricByName(listMetricStore, GET_RATE, 1);
            testMetricByName(listMetricStore, GET_TOTAL, 1);
            testMetricByName(listMetricStore, DELETE_RATE, 0);
            testMetricByName(listMetricStore, DELETE_TOTAL, 0);
            testMetricByName(listMetricStore, PUT_ALL_RATE, 0);
            testMetricByName(listMetricStore, PUT_ALL_TOTAL, 0);
            testMetricByName(listMetricStore, ALL_RATE, 0);
            testMetricByName(listMetricStore, ALL_TOTAL, 0);
            testMetricByName(listMetricStore, RANGE_RATE, 1);
            testMetricByName(listMetricStore, RANGE_TOTAL, 1);
            testMetricByName(listMetricStore, FLUSH_RATE, 1);
            testMetricByName(listMetricStore, FLUSH_TOTAL, 1);
            testMetricByName(listMetricStore, RESTORE_RATE, 1);
            testMetricByName(listMetricStore, RESTORE_TOTAL, 1);
            return true;
        } catch (final Throwable e) {
            errorMessage.append(e.getMessage());
            return false;
        }
    }

    private boolean testStoreMetricSession(final StringBuilder errorMessage) {
        errorMessage.setLength(0);
        try {
            final List<Metric> listMetricStore = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
                    .filter(m -> m.metricName().group().equals(STREAM_STORE_SESSION_ROCKSDB_STATE_METRICS))
                    .collect(Collectors.toList());
            testMetricByName(listMetricStore, PUT_LATENCY_AVG, 1);
            testMetricByName(listMetricStore, PUT_LATENCY_MAX, 1);
            testMetricByName(listMetricStore, PUT_IF_ABSENT_LATENCY_AVG, 0);
            testMetricByName(listMetricStore, PUT_IF_ABSENT_LATENCY_MAX, 0);
            testMetricByName(listMetricStore, GET_LATENCY_AVG, 1);
            testMetricByName(listMetricStore, GET_LATENCY_MAX, 1);
            testMetricByName(listMetricStore, DELETE_LATENCY_AVG, 1);
            testMetricByName(listMetricStore, DELETE_LATENCY_MAX, 1);
            testMetricByName(listMetricStore, PUT_ALL_LATENCY_AVG, 0);
            testMetricByName(listMetricStore, PUT_ALL_LATENCY_MAX, 0);
            testMetricByName(listMetricStore, ALL_LATENCY_AVG, 0);
            testMetricByName(listMetricStore, ALL_LATENCY_MAX, 0);
            testMetricByName(listMetricStore, RANGE_LATENCY_AVG, 1);
            testMetricByName(listMetricStore, RANGE_LATENCY_MAX, 1);
            testMetricByName(listMetricStore, FLUSH_LATENCY_AVG, 1);
            testMetricByName(listMetricStore, FLUSH_LATENCY_MAX, 1);
            testMetricByName(listMetricStore, RESTORE_LATENCY_AVG, 1);
            testMetricByName(listMetricStore, RESTORE_LATENCY_MAX, 1);
            testMetricByName(listMetricStore, PUT_RATE, 1);
            testMetricByName(listMetricStore, PUT_TOTAL, 1);
            testMetricByName(listMetricStore, PUT_IF_ABSENT_RATE, 0);
            testMetricByName(listMetricStore, PUT_IF_ABSENT_TOTAL, 0);
            testMetricByName(listMetricStore, GET_RATE, 1);
            testMetricByName(listMetricStore, GET_TOTAL, 1);
            testMetricByName(listMetricStore, DELETE_RATE, 1);
            testMetricByName(listMetricStore, DELETE_TOTAL, 1);
            testMetricByName(listMetricStore, PUT_ALL_RATE, 0);
            testMetricByName(listMetricStore, PUT_ALL_TOTAL, 0);
            testMetricByName(listMetricStore, ALL_RATE, 0);
            testMetricByName(listMetricStore, ALL_TOTAL, 0);
            testMetricByName(listMetricStore, RANGE_RATE, 1);
            testMetricByName(listMetricStore, RANGE_TOTAL, 1);
            testMetricByName(listMetricStore, FLUSH_RATE, 1);
            testMetricByName(listMetricStore, FLUSH_TOTAL, 1);
            testMetricByName(listMetricStore, RESTORE_RATE, 1);
            testMetricByName(listMetricStore, RESTORE_TOTAL, 1);
            return true;
        } catch (final Throwable e) {
            errorMessage.append(e.getMessage());
            return false;
        }
    }

    private boolean testStoreMetricKeyValueByType(final String storeType, final StringBuilder errorMessage) {
        errorMessage.setLength(0);
        try {
            final List<Metric> listMetricStore = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
                    .filter(m -> m.metricName().group().equals(storeType))
                    .collect(Collectors.toList());
            testMetricByName(listMetricStore, PUT_LATENCY_AVG, 1);
            testMetricByName(listMetricStore, PUT_LATENCY_MAX, 1);
            testMetricByName(listMetricStore, PUT_IF_ABSENT_LATENCY_AVG, 1);
            testMetricByName(listMetricStore, PUT_IF_ABSENT_LATENCY_MAX, 1);
            testMetricByName(listMetricStore, GET_LATENCY_AVG, 1);
            testMetricByName(listMetricStore, GET_LATENCY_MAX, 1);
            testMetricByName(listMetricStore, DELETE_LATENCY_AVG, 1);
            testMetricByName(listMetricStore, DELETE_LATENCY_MAX, 1);
            testMetricByName(listMetricStore, PUT_ALL_LATENCY_AVG, 1);
            testMetricByName(listMetricStore, PUT_ALL_LATENCY_MAX, 1);
            testMetricByName(listMetricStore, ALL_LATENCY_AVG, 1);
            testMetricByName(listMetricStore, ALL_LATENCY_MAX, 1);
            testMetricByName(listMetricStore, RANGE_LATENCY_AVG, 1);
            testMetricByName(listMetricStore, RANGE_LATENCY_MAX, 1);
            testMetricByName(listMetricStore, FLUSH_LATENCY_AVG, 1);
            testMetricByName(listMetricStore, FLUSH_LATENCY_MAX, 1);
            testMetricByName(listMetricStore, RESTORE_LATENCY_AVG, 1);
            testMetricByName(listMetricStore, RESTORE_LATENCY_MAX, 1);
            testMetricByName(listMetricStore, PUT_RATE, 1);
            testMetricByName(listMetricStore, PUT_TOTAL, 1);
            testMetricByName(listMetricStore, PUT_IF_ABSENT_RATE, 1);
            testMetricByName(listMetricStore, PUT_IF_ABSENT_TOTAL, 1);
            testMetricByName(listMetricStore, GET_RATE, 1);
            testMetricByName(listMetricStore, DELETE_RATE, 1);
            testMetricByName(listMetricStore, DELETE_TOTAL, 1);
            testMetricByName(listMetricStore, PUT_ALL_RATE, 1);
            testMetricByName(listMetricStore, PUT_ALL_TOTAL, 1);
            testMetricByName(listMetricStore, ALL_RATE, 1);
            testMetricByName(listMetricStore, ALL_TOTAL, 1);
            testMetricByName(listMetricStore, RANGE_RATE, 1);
            testMetricByName(listMetricStore, RANGE_TOTAL, 1);
            testMetricByName(listMetricStore, FLUSH_RATE, 1);
            testMetricByName(listMetricStore, FLUSH_TOTAL, 1);
            testMetricByName(listMetricStore, RESTORE_RATE, 1);
            testMetricByName(listMetricStore, RESTORE_TOTAL, 1);
            return true;
        } catch (final Throwable e) {
            errorMessage.append(e.getMessage());
            return false;
        }
    }

    private boolean testCacheMetric(final StringBuilder errorMessage) {
        errorMessage.setLength(0);
        try {
            final List<Metric> listMetricCache = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream().filter(m -> m.metricName().group().equals(STREAM_RECORD_CACHE_METRICS)).collect(Collectors.toList());
            testMetricByName(listMetricCache, HIT_RATIO_AVG, 3);
            testMetricByName(listMetricCache, HIT_RATIO_MIN, 3);
            testMetricByName(listMetricCache, HIT_RATIO_MAX, 3);
            return true;
        } catch (final Throwable e) {
            errorMessage.append(e.getMessage());
            return false;
        }
    }

    private void testMetricByName(final List<Metric> listMetric, final String metricName, final int numMetric) {
        final List<Metric> metrics = listMetric.stream().filter(m -> m.metricName().name().equals(metricName)).collect(Collectors.toList());
        Assert.assertEquals("Size of metrics of type:'" + metricName + "' must be equal to:" + numMetric + " but it's equal to " + metrics.size(), numMetric, metrics.size());
        for (final Metric m : metrics) {
            Assert.assertNotNull("Metric:'" + m.metricName() + "' must be not null", m.metricValue());
        }
    }
}