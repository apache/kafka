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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
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

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

@Category({IntegrationTest.class})
public class KTableSourceTopicRestartIntegrationTest {
    private static final int NUM_BROKERS = 3;
    private static final String SOURCE_TOPIC = "source-topic";
    private static final Properties PRODUCER_CONFIG = new Properties();
    private static final Properties STREAMS_CONFIG = new Properties();

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    @BeforeClass
    public static void startCluster() throws IOException {
        CLUSTER.start();
        STREAMS_CONFIG.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        STREAMS_CONFIG.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        STREAMS_CONFIG.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        STREAMS_CONFIG.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5L);
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        STREAMS_CONFIG.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 1000);
        STREAMS_CONFIG.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 300);

        PRODUCER_CONFIG.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        PRODUCER_CONFIG.put(ProducerConfig.ACKS_CONFIG, "all");
        PRODUCER_CONFIG.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        PRODUCER_CONFIG.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }


    private final Time time = CLUSTER.time;
    private final StreamsBuilder streamsBuilder = new StreamsBuilder();
    private final Map<String, String> readKeyValues = new ConcurrentHashMap<>();

    private String sourceTopic;
    private KafkaStreams streams;
    private Map<String, String> expectedInitialResultsMap;
    private Map<String, String> expectedResultsWithDataWrittenDuringRestoreMap;

    @Rule
    public TestName testName = new TestName();

    @Before
    public void before() throws Exception {
        sourceTopic = SOURCE_TOPIC + "-" + testName.getMethodName();
        CLUSTER.createTopic(sourceTopic);

        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, IntegrationTestUtils.safeUniqueTestName(getClass(), testName));

        final KTable<String, String> kTable = streamsBuilder.table(sourceTopic, Materialized.as("store"));
        kTable.toStream().foreach(readKeyValues::put);

        expectedInitialResultsMap = createExpectedResultsMap("a", "b", "c");
        expectedResultsWithDataWrittenDuringRestoreMap = createExpectedResultsMap("a", "b", "c", "d", "f", "g", "h");
    }

    @After
    public void after() throws Exception {
        IntegrationTestUtils.purgeLocalStreamsState(STREAMS_CONFIG);
    }

    @Test
    public void shouldRestoreAndProgressWhenTopicWrittenToDuringRestorationWithEosDisabled() throws Exception {
        try {
            streams = new KafkaStreams(streamsBuilder.build(), STREAMS_CONFIG);
            streams.start();

            produceKeyValues("a", "b", "c");

            assertNumberValuesRead(readKeyValues, expectedInitialResultsMap, "Table did not read all values");

            streams.close();
            streams = new KafkaStreams(streamsBuilder.build(), STREAMS_CONFIG);
            // the state restore listener will append one record to the log
            streams.setGlobalStateRestoreListener(new UpdatingSourceTopicOnRestoreStartStateRestoreListener());
            streams.start();

            produceKeyValues("f", "g", "h");

            assertNumberValuesRead(
                readKeyValues,
                expectedResultsWithDataWrittenDuringRestoreMap,
                "Table did not get all values after restart");
        } finally {
            streams.close(Duration.ofSeconds(5));
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldRestoreAndProgressWhenTopicWrittenToDuringRestorationWithEosAlphaEnabled() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        shouldRestoreAndProgressWhenTopicWrittenToDuringRestorationWithEosEnabled();
    }

    @Test
    public void shouldRestoreAndProgressWhenTopicWrittenToDuringRestorationWithEosV2Enabled() throws Exception {
        STREAMS_CONFIG.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        shouldRestoreAndProgressWhenTopicWrittenToDuringRestorationWithEosEnabled();
    }

    private void shouldRestoreAndProgressWhenTopicWrittenToDuringRestorationWithEosEnabled() throws Exception {
        try {
            streams = new KafkaStreams(streamsBuilder.build(), STREAMS_CONFIG);
            streams.start();

            produceKeyValues("a", "b", "c");

            assertNumberValuesRead(readKeyValues, expectedInitialResultsMap, "Table did not read all values");

            streams.close();
            streams = new KafkaStreams(streamsBuilder.build(), STREAMS_CONFIG);
            // the state restore listener will append one record to the log
            streams.setGlobalStateRestoreListener(new UpdatingSourceTopicOnRestoreStartStateRestoreListener());
            streams.start();

            produceKeyValues("f", "g", "h");

            assertNumberValuesRead(
                readKeyValues,
                expectedResultsWithDataWrittenDuringRestoreMap,
                "Table did not get all values after restart");
        } finally {
            streams.close(Duration.ofSeconds(5));
        }
    }

    @Test
    public void shouldRestoreAndProgressWhenTopicNotWrittenToDuringRestoration() throws Exception {
        try {
            streams = new KafkaStreams(streamsBuilder.build(), STREAMS_CONFIG);
            streams.start();

            produceKeyValues("a", "b", "c");

            assertNumberValuesRead(readKeyValues, expectedInitialResultsMap, "Table did not read all values");

            streams.close();
            streams = new KafkaStreams(streamsBuilder.build(), STREAMS_CONFIG);
            streams.start();

            produceKeyValues("f", "g", "h");

            final Map<String, String> expectedValues = createExpectedResultsMap("a", "b", "c", "f", "g", "h");

            assertNumberValuesRead(readKeyValues, expectedValues, "Table did not get all values after restart");
        } finally {
            streams.close(Duration.ofSeconds(5));
        }
    }

    private void assertNumberValuesRead(final Map<String, String> valueMap,
                                        final Map<String, String> expectedMap,
                                        final String errorMessage) throws InterruptedException {
        TestUtils.waitForCondition(
            () -> valueMap.equals(expectedMap),
            30 * 1000L,
            errorMessage);
    }

    private void produceKeyValues(final String... keys) {
        final List<KeyValue<String, String>> keyValueList = new ArrayList<>();

        for (final String key : keys) {
            keyValueList.add(new KeyValue<>(key, key + "1"));
        }

        IntegrationTestUtils.produceKeyValuesSynchronously(sourceTopic,
                                                           keyValueList,
                                                           PRODUCER_CONFIG,
                                                           time);
    }

    private Map<String, String> createExpectedResultsMap(final String... keys) {
        final Map<String, String> expectedMap = new HashMap<>();
        for (final String key : keys) {
            expectedMap.put(key, key + "1");
        }
        return expectedMap;
    }

    private class UpdatingSourceTopicOnRestoreStartStateRestoreListener implements StateRestoreListener {

        @Override
        public void onRestoreStart(final TopicPartition topicPartition,
                                   final String storeName,
                                   final long startingOffset,
                                   final long endingOffset) {
            produceKeyValues("d");
        }

        @Override
        public void onBatchRestored(final TopicPartition topicPartition,
                                    final String storeName,
                                    final long batchEndOffset,
                                    final long numRestored) {
        }

        @Override
        public void onRestoreEnd(final TopicPartition topicPartition,
                                 final String storeName,
                                 final long totalRestored) {
        }
    }

}
