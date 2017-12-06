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
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Category({IntegrationTest.class})
public class KTableSourceTopicRestartIntegrationTest {


    private static final int NUM_BROKERS = 1;
    private static final String SOURCE_TOPIC = "source-topic";
    private static final String TABLE_SOURCE_TOPIC = "table-source-topic";

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final Time time = CLUSTER.time;
    private KafkaStreams streamsOne;
    private KafkaStreams streamsTwo;
    private static final Properties PRODUCER_CONFIG = new Properties();
    private static final Properties STREAMS_CONFIG = new Properties();
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final Serde<Long> LONG_SERDE = Serdes.Long();


    @BeforeClass
    public static void setUpBeforeAllTests() throws Exception {

        CLUSTER.createTopic(SOURCE_TOPIC, 1, 1);
        CLUSTER.createTopic(TABLE_SOURCE_TOPIC, 1, 1);

        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-restore-from-source");
        STREAMS_CONFIG.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        STREAMS_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        STREAMS_CONFIG.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        STREAMS_CONFIG.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        STREAMS_CONFIG.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);
        STREAMS_CONFIG.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5);
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        STREAMS_CONFIG.put(ConsumerConfig.GROUP_ID_CONFIG, "ktable-group");

        PRODUCER_CONFIG.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        PRODUCER_CONFIG.put(ProducerConfig.ACKS_CONFIG, "all");
        PRODUCER_CONFIG.put(ProducerConfig.RETRIES_CONFIG, 0);
        PRODUCER_CONFIG.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        PRODUCER_CONFIG.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    }


    @After
    public void after() throws IOException {
        IntegrationTestUtils.purgeLocalStreamsState(STREAMS_CONFIG);
    }


    @Test
    public void shouldRestoreOnlyToLastCommit() throws Exception {
        try {
            final StreamsBuilder streamsBuilder = new StreamsBuilder();
            final Map<String, Long> readKeyValues = new HashMap<>();

            streamsBuilder.<String, String>stream(SOURCE_TOPIC)
                .groupByKey()
                .count()
                .toStream()
                .to(TABLE_SOURCE_TOPIC, Produced.with(STRING_SERDE, LONG_SERDE));

            KTable<String, Long> countsTable = streamsBuilder.table(TABLE_SOURCE_TOPIC,
                                                                    Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>with(STRING_SERDE, LONG_SERDE)
                                                                        .withLoggingEnabled(new HashMap<String, String>())
                                                                        .withCachingDisabled());

            countsTable.toStream().foreach(new ForeachAction<String, Long>() {
                @Override
                public void apply(String key, Long value) {
                    readKeyValues.put(key, value);
                }
            });

            streamsOne = new KafkaStreams(streamsBuilder.build(), STREAMS_CONFIG);
            streamsOne.setGlobalStateRestoreListener(new TestingStateRestoreListener("streams-one"));
            streamsOne.start();

            streamsTwo = new KafkaStreams(streamsBuilder.build(), STREAMS_CONFIG);
            streamsTwo.setGlobalStateRestoreListener(new TestingStateRestoreListener("streams-two"));
            streamsTwo.start();

            final List<KeyValue<String, String>> initialKeyValues = Arrays.asList(
                new KeyValue<>("a", "A3"),
                new KeyValue<>("b", "B3"),
                new KeyValue<>("c", "C3"),
                new KeyValue<>("d", "D3"),
                new KeyValue<>("e", "E3"),
                new KeyValue<>("f", "F3")
            );
            IntegrationTestUtils.produceKeyValuesSynchronously(SOURCE_TOPIC, initialKeyValues, PRODUCER_CONFIG, time);

            TestUtils.waitForCondition(new TestCondition() {
                @Override
                public boolean conditionMet() {
                    return readKeyValues.size() == 6;
                }
            }, "Table did not get all values");

            streamsTwo.close();
            streamsTwo.cleanUp();

            final List<KeyValue<String, String>> additionalValuesWritten = Arrays.asList(
                new KeyValue<>("g", "G3"),
                new KeyValue<>("i", "I5"),
                new KeyValue<>("j", "K6"),
                new KeyValue<>("l", "K6"),
                new KeyValue<>("m", "K6"),
                new KeyValue<>("n", "K6")
            );

            IntegrationTestUtils.produceKeyValuesSynchronously(SOURCE_TOPIC, additionalValuesWritten, PRODUCER_CONFIG, time);

            final List<KeyValue<String, Long>> valuesToTableTopics = Arrays.asList(
                new KeyValue<>("FOO", 10L),
                new KeyValue<>("BAR", 20L),
                new KeyValue<>("BAZ", 30L)

            );

            PRODUCER_CONFIG.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
            IntegrationTestUtils.produceKeyValuesSynchronously(TABLE_SOURCE_TOPIC, valuesToTableTopics, PRODUCER_CONFIG, time);
            // sleeps needed for forcing recovery
            Thread.sleep(5000L);

            streamsTwo = new KafkaStreams(streamsBuilder.build(), STREAMS_CONFIG);
            streamsTwo.setGlobalStateRestoreListener(new TestingStateRestoreListener("streams-two-restarted"));
            // expecting this to fail as restoration
            streamsTwo.start();
            // sleep needed to for restoration
            Thread.sleep(5000L);


        } finally {
            streamsOne.close(5, TimeUnit.SECONDS);
            streamsTwo.close(5, TimeUnit.SECONDS);
        }
    }


    private static class TestingStateRestoreListener implements StateRestoreListener {

        private String streamsInstance;

        public TestingStateRestoreListener(String streamsInstance) {
            this.streamsInstance = streamsInstance;
        }

        @Override
        public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {
            System.out.println(
                "[" + streamsInstance + "] recovering for TopicPartition " + topicPartition + " storeName " + storeName + " offset " + startingOffset
                + " ending offset " + endingOffset);
        }

        @Override
        public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {

        }

        @Override
        public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
            System.out.println("[" + streamsInstance + "] Done recovering for TopicPartition " + topicPartition + " storeName " + storeName + " totalRestored "
                               + totalRestored);
        }
    }

}
