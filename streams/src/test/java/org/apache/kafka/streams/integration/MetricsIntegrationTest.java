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
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
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

    // Metric group
    private static final String STREAM_THREAD_NODE_METRICS = "stream-metrics";
    private static final String STREAM_TASK_NODE_METRICS = "stream-task-metrics";
    private static final String STREAM_PROCESSOR_NODE_METRICS = "stream-processor-node-metrics";
    private static final String STREAM_CACHE_NODE_METRICS = "stream-record-cache-metrics";
    private static final String STREAM_STORE_IN_MEMORY_STATE_METRICS = "stream-in-memory-state-metrics";
    private static final String STREAM_STORE_IN_MEMORY_LRU_STATE_METRICS = "stream-in-memory-lru-state-metrics";
    private static final String STREAM_STORE_ROCKSDB_STATE_METRICS = "stream-rocksdb-state-metrics";
    private static final String STREAM_STORE_WINDOW_ROCKSDB_STATE_METRICS = "stream-rocksdb-window-state-metrics";
    private static final String STREAM_STORE_SESSION_ROCKSDB_STATE_METRICS = "stream-rocksdb-session-state-metrics";

    // Metrics name
    private static final String PUT_LATENCY_AVG = "put-latency-avg";
    private static final String PUT_LATENCY_MAX = "put-latency-max";
    private static final String PUT_IF_ABSENT_LATENCY_AVG = "put-if-absent-latency-avg";
    private static final String PUT_IF_ABSENT_LATENCY_MAX = "put-if-absent-latency-max";
    private static final String GET_LATENCY_AVG = "get-latency-avg";
    private static final String GET_LATENCY_MAX = "get-latency-max";
    private static final String DELETE_LATENCY_AVG = "delete-latency-avg";
    private static final String DELETE_LATENCY_MAX = "delete-latency-max";
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
    private static final String DELETE_RATE = "delete-rate";
    private static final String DELETE_TOTAL = "delete-total";
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
    private static final String HIT_RATIO_AVG = "hitRatio-avg";
    private static final String HIT_RATIO_MIN = "hitRatio-min";
    private static final String HIT_RATIO_MAX = "hitRatio-max";

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
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-metrics-test");
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
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.start();
        final long timeout = 60000;
        TestUtils.waitForCondition(
            () -> kafkaStreams.state() == State.RUNNING,
            timeout,
            () -> "Kafka Streams application did not reach state RUNNING in " + timeout + " ms");
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

        checkThreadLevelMetrics();
        checkTaskLevelMetrics();
        checkProcessorLevelMetrics();
        checkKeyValueStoreMetricsByType(STREAM_STORE_IN_MEMORY_STATE_METRICS);
        checkKeyValueStoreMetricsByType(STREAM_STORE_IN_MEMORY_LRU_STATE_METRICS);
        checkKeyValueStoreMetricsByType(STREAM_STORE_ROCKSDB_STATE_METRICS);
        checkCacheMetrics();

        closeApplication();

        checkMetricsDeregistration();
    }

    @Test
    public void shouldAddMetricsForWindowStore() throws Exception {
        builder.stream(STREAM_INPUT, Consumed.with(Serdes.Integer(), Serdes.String()))
            .groupByKey()
            .windowedBy(TimeWindows.of(Duration.ofMillis(50)))
            .aggregate(() -> 0L,
                (aggKey, newValue, aggValue) -> aggValue,
                Materialized.<Integer, Long, WindowStore<Bytes, byte[]>>as(TIME_WINDOWED_AGGREGATED_STREAM_STORE)
                    .withValueSerde(Serdes.Long()));
        startApplication();

        checkWindowStoreMetrics();

        closeApplication();

        checkMetricsDeregistration();
    }

    @Test
    public void shouldAddMetricsForSessionStore() throws Exception {
        builder.stream(STREAM_INPUT, Consumed.with(Serdes.Integer(), Serdes.String()))
            .groupByKey()
            .windowedBy(SessionWindows.with(Duration.ofMillis(50)))
            .aggregate(() -> 0L,
                (aggKey, newValue, aggValue) -> aggValue,
                (aggKey, leftAggValue, rightAggValue) -> leftAggValue,
                Materialized.<Integer, Long, SessionStore<Bytes, byte[]>>as(SESSION_AGGREGATED_STREAM_STORE)
                    .withValueSerde(Serdes.Long()));
        startApplication();

        checkSessionStoreMetrics();

        closeApplication();

        checkMetricsDeregistration();
    }

    private void checkThreadLevelMetrics() {
        final List<Metric> listMetricThread = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
            .filter(m -> m.metricName().group().equals(STREAM_THREAD_NODE_METRICS))
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
        checkMetricByName(listMetricThread, SKIPPED_RECORDS_RATE, 1);
        checkMetricByName(listMetricThread, SKIPPED_RECORDS_TOTAL, 1);
    }

    private void checkTaskLevelMetrics() {
        final List<Metric> listMetricTask = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
            .filter(m -> m.metricName().group().equals(STREAM_TASK_NODE_METRICS))
            .collect(Collectors.toList());
        checkMetricByName(listMetricTask, COMMIT_LATENCY_AVG, 5);
        checkMetricByName(listMetricTask, COMMIT_LATENCY_MAX, 5);
        checkMetricByName(listMetricTask, COMMIT_RATE, 5);
        checkMetricByName(listMetricTask, COMMIT_TOTAL, 5);
        checkMetricByName(listMetricTask, RECORD_LATENESS_AVG, 4);
        checkMetricByName(listMetricTask, RECORD_LATENESS_MAX, 4);
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

    private void checkKeyValueStoreMetricsByType(final String storeType) {
        final List<Metric> listMetricStore = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
            .filter(m -> m.metricName().group().equals(storeType))
            .collect(Collectors.toList());
        checkMetricByName(listMetricStore, PUT_LATENCY_AVG, 2);
        checkMetricByName(listMetricStore, PUT_LATENCY_MAX, 2);
        checkMetricByName(listMetricStore, PUT_IF_ABSENT_LATENCY_AVG, 2);
        checkMetricByName(listMetricStore, PUT_IF_ABSENT_LATENCY_MAX, 2);
        checkMetricByName(listMetricStore, GET_LATENCY_AVG, 2);
        checkMetricByName(listMetricStore, GET_LATENCY_MAX, 2);
        checkMetricByName(listMetricStore, DELETE_LATENCY_AVG, 2);
        checkMetricByName(listMetricStore, DELETE_LATENCY_MAX, 2);
        checkMetricByName(listMetricStore, PUT_ALL_LATENCY_AVG, 2);
        checkMetricByName(listMetricStore, PUT_ALL_LATENCY_MAX, 2);
        checkMetricByName(listMetricStore, ALL_LATENCY_AVG, 2);
        checkMetricByName(listMetricStore, ALL_LATENCY_MAX, 2);
        checkMetricByName(listMetricStore, RANGE_LATENCY_AVG, 2);
        checkMetricByName(listMetricStore, RANGE_LATENCY_MAX, 2);
        checkMetricByName(listMetricStore, FLUSH_LATENCY_AVG, 2);
        checkMetricByName(listMetricStore, FLUSH_LATENCY_MAX, 2);
        checkMetricByName(listMetricStore, RESTORE_LATENCY_AVG, 2);
        checkMetricByName(listMetricStore, RESTORE_LATENCY_MAX, 2);
        checkMetricByName(listMetricStore, PUT_RATE, 2);
        checkMetricByName(listMetricStore, PUT_TOTAL, 2);
        checkMetricByName(listMetricStore, PUT_IF_ABSENT_RATE, 2);
        checkMetricByName(listMetricStore, PUT_IF_ABSENT_TOTAL, 2);
        checkMetricByName(listMetricStore, GET_RATE, 2);
        checkMetricByName(listMetricStore, DELETE_RATE, 2);
        checkMetricByName(listMetricStore, DELETE_TOTAL, 2);
        checkMetricByName(listMetricStore, PUT_ALL_RATE, 2);
        checkMetricByName(listMetricStore, PUT_ALL_TOTAL, 2);
        checkMetricByName(listMetricStore, ALL_RATE, 2);
        checkMetricByName(listMetricStore, ALL_TOTAL, 2);
        checkMetricByName(listMetricStore, RANGE_RATE, 2);
        checkMetricByName(listMetricStore, RANGE_TOTAL, 2);
        checkMetricByName(listMetricStore, FLUSH_RATE, 2);
        checkMetricByName(listMetricStore, FLUSH_TOTAL, 2);
        checkMetricByName(listMetricStore, RESTORE_RATE, 2);
        checkMetricByName(listMetricStore, RESTORE_TOTAL, 2);
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
        checkMetricByName(listMetricCache, HIT_RATIO_AVG, 6);
        checkMetricByName(listMetricCache, HIT_RATIO_MIN, 6);
        checkMetricByName(listMetricCache, HIT_RATIO_MAX, 6);
    }

    private void checkWindowStoreMetrics() {
        final List<Metric> listMetricStore = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
            .filter(m -> m.metricName().group().equals(STREAM_STORE_WINDOW_ROCKSDB_STATE_METRICS))
            .collect(Collectors.toList());
        checkMetricByName(listMetricStore, PUT_LATENCY_AVG, 2);
        checkMetricByName(listMetricStore, PUT_LATENCY_MAX, 2);
        checkMetricByName(listMetricStore, PUT_IF_ABSENT_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, PUT_IF_ABSENT_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, GET_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, GET_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, DELETE_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, DELETE_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, PUT_ALL_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, PUT_ALL_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, ALL_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, ALL_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, RANGE_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, RANGE_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, FLUSH_LATENCY_AVG, 2);
        checkMetricByName(listMetricStore, FLUSH_LATENCY_MAX, 2);
        checkMetricByName(listMetricStore, RESTORE_LATENCY_AVG, 2);
        checkMetricByName(listMetricStore, RESTORE_LATENCY_MAX, 2);
        checkMetricByName(listMetricStore, PUT_RATE, 2);
        checkMetricByName(listMetricStore, PUT_TOTAL, 2);
        checkMetricByName(listMetricStore, PUT_IF_ABSENT_RATE, 0);
        checkMetricByName(listMetricStore, PUT_IF_ABSENT_TOTAL, 0);
        checkMetricByName(listMetricStore, GET_RATE, 0);
        checkMetricByName(listMetricStore, DELETE_RATE, 0);
        checkMetricByName(listMetricStore, DELETE_TOTAL, 0);
        checkMetricByName(listMetricStore, PUT_ALL_RATE, 0);
        checkMetricByName(listMetricStore, PUT_ALL_TOTAL, 0);
        checkMetricByName(listMetricStore, ALL_RATE, 0);
        checkMetricByName(listMetricStore, ALL_TOTAL, 0);
        checkMetricByName(listMetricStore, RANGE_RATE, 0);
        checkMetricByName(listMetricStore, RANGE_TOTAL, 0);
        checkMetricByName(listMetricStore, FLUSH_RATE, 2);
        checkMetricByName(listMetricStore, FLUSH_TOTAL, 2);
        checkMetricByName(listMetricStore, RESTORE_RATE, 2);
        checkMetricByName(listMetricStore, RESTORE_TOTAL, 2);
    }

    private void checkSessionStoreMetrics() {
        final List<Metric> listMetricStore = new ArrayList<Metric>(kafkaStreams.metrics().values()).stream()
            .filter(m -> m.metricName().group().equals(STREAM_STORE_SESSION_ROCKSDB_STATE_METRICS))
            .collect(Collectors.toList());
        checkMetricByName(listMetricStore, PUT_LATENCY_AVG, 2);
        checkMetricByName(listMetricStore, PUT_LATENCY_MAX, 2);
        checkMetricByName(listMetricStore, PUT_IF_ABSENT_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, PUT_IF_ABSENT_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, GET_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, GET_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, DELETE_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, DELETE_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, PUT_ALL_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, PUT_ALL_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, ALL_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, ALL_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, RANGE_LATENCY_AVG, 0);
        checkMetricByName(listMetricStore, RANGE_LATENCY_MAX, 0);
        checkMetricByName(listMetricStore, FLUSH_LATENCY_AVG, 2);
        checkMetricByName(listMetricStore, FLUSH_LATENCY_MAX, 2);
        checkMetricByName(listMetricStore, RESTORE_LATENCY_AVG, 2);
        checkMetricByName(listMetricStore, RESTORE_LATENCY_MAX, 2);
        checkMetricByName(listMetricStore, PUT_RATE, 2);
        checkMetricByName(listMetricStore, PUT_TOTAL, 2);
        checkMetricByName(listMetricStore, PUT_IF_ABSENT_RATE, 0);
        checkMetricByName(listMetricStore, PUT_IF_ABSENT_TOTAL, 0);
        checkMetricByName(listMetricStore, GET_RATE, 0);
        checkMetricByName(listMetricStore, DELETE_RATE, 0);
        checkMetricByName(listMetricStore, DELETE_TOTAL, 0);
        checkMetricByName(listMetricStore, PUT_ALL_RATE, 0);
        checkMetricByName(listMetricStore, PUT_ALL_TOTAL, 0);
        checkMetricByName(listMetricStore, ALL_RATE, 0);
        checkMetricByName(listMetricStore, ALL_TOTAL, 0);
        checkMetricByName(listMetricStore, RANGE_RATE, 0);
        checkMetricByName(listMetricStore, RANGE_TOTAL, 0);
        checkMetricByName(listMetricStore, FLUSH_RATE, 2);
        checkMetricByName(listMetricStore, FLUSH_TOTAL, 2);
        checkMetricByName(listMetricStore, RESTORE_RATE, 2);
        checkMetricByName(listMetricStore, RESTORE_TOTAL, 2);
    }

    private void checkMetricByName(final List<Metric> listMetric, final String metricName, final int numMetric) {
        final List<Metric> metrics = listMetric.stream()
            .filter(m -> m.metricName().name().equals(metricName))
            .collect(Collectors.toList());
        Assert.assertEquals("Size of metrics of type:'" + metricName + "' must be equal to:" + numMetric + " but it's equal to " + metrics.size(), numMetric, metrics.size());
        for (final Metric m : metrics) {
            Assert.assertNotNull("Metric:'" + m.metricName() + "' must be not null", m.metricValue());
        }
    }
}