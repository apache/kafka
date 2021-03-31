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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(value = Parameterized.class)
@Category({IntegrationTest.class})
public class StreamTableJoinTopologyOptimizationIntegrationTest {
    private static final int NUM_BROKERS = 1;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    @BeforeClass
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }

    private String tableTopic;
    private String inputTopic;
    private String outputTopic;
    private String applicationId;
    private KafkaStreams kafkaStreams;

    private Properties streamsConfiguration;

    @Rule
    public TestName testName = new TestName();

    @Parameterized.Parameter
    public String topologyOptimization;

    @Parameterized.Parameters(name = "Optimization = {0}")
    public static Collection<?> topologyOptimization() {
        return Arrays.asList(new String[][]{
            {StreamsConfig.OPTIMIZE},
            {StreamsConfig.NO_OPTIMIZATION}
        });
    }

    @Before
    public void before() throws InterruptedException {
        streamsConfiguration = new Properties();

        final String safeTestName = safeUniqueTestName(getClass(), testName);

        tableTopic = "table-topic" + safeTestName;
        inputTopic = "stream-topic-" + safeTestName;
        outputTopic = "output-topic-" + safeTestName;
        applicationId = "app-" + safeTestName;

        CLUSTER.createTopic(inputTopic, 4, 1);
        CLUSTER.createTopic(tableTopic, 2, 1);
        CLUSTER.createTopic(outputTopic, 4, 1);

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, topologyOptimization);
    }

    @After
    public void whenShuttingDown() throws IOException {
        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    @Test
    public void shouldDoStreamTableJoinWithDifferentNumberOfPartitions() throws Exception {
        final String storeName = "store";
        final String selectKeyName = "selectKey";

        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        final KStream<Integer, String> stream = streamsBuilder.stream(inputTopic);
        final KTable<Integer, String> table = streamsBuilder.table(tableTopic, Materialized.as(storeName));

        stream
            .selectKey((key, value) -> key, Named.as(selectKeyName))
            .join(table, (value1, value2) -> value2)
            .to(outputTopic);

        kafkaStreams = startStreams(streamsBuilder);

        final long timestamp = System.currentTimeMillis();

        final List<KeyValue<Integer, String>> expectedRecords = Arrays.asList(
            new KeyValue<>(1, "A"),
            new KeyValue<>(2, "B")
        );

        sendEvents(inputTopic, timestamp, expectedRecords);
        sendEvents(outputTopic, timestamp, expectedRecords);

        validateReceivedMessages(
            outputTopic,
            new IntegerDeserializer(),
            new StringDeserializer(),
            expectedRecords
        );

        final Set<String> allTopicsInCluster = CLUSTER.getAllTopicsInCluster();

        final String repartitionTopicName = applicationId + "-" + selectKeyName + "-repartition";
        final String tableChangelogStoreName = applicationId + "-" + storeName + "-changelog";

        assertTrue(topicExists(repartitionTopicName));
        assertEquals(2, getNumberOfPartitionsForTopic(repartitionTopicName));

        if (StreamsConfig.OPTIMIZE.equals(topologyOptimization)) {
            assertFalse(allTopicsInCluster.contains(tableChangelogStoreName));
        } else if (StreamsConfig.NO_OPTIMIZATION.equals(topologyOptimization)) {
            assertTrue(allTopicsInCluster.contains(tableChangelogStoreName));
        }
    }

    private KafkaStreams startStreams(final StreamsBuilder builder) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(streamsConfiguration), streamsConfiguration);

        kafkaStreams.setStateListener((newState, oldState) -> {
            if (KafkaStreams.State.REBALANCING == oldState && KafkaStreams.State.RUNNING == newState) {
                latch.countDown();
            }
        });

        kafkaStreams.start();

        latch.await(IntegrationTestUtils.DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);

        return kafkaStreams;
    }

    private int getNumberOfPartitionsForTopic(final String topic) throws Exception {
        try (final AdminClient adminClient = createAdminClient()) {
            final TopicDescription topicDescription = adminClient.describeTopics(Collections.singleton(topic))
                                                                 .values()
                                                                 .get(topic)
                                                                 .get(IntegrationTestUtils.DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);

            return topicDescription.partitions().size();
        }
    }

    private boolean topicExists(final String topic) {
        return CLUSTER.getAllTopicsInCluster().contains(topic);
    }

    private <K, V> void sendEvents(final String topic,
                                   final long timestamp,
                                   final List<KeyValue<K, V>> events) throws Exception {
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            topic,
            events,
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                StringSerializer.class,
                new Properties()
            ),
            timestamp
        );
    }

    private <K, V> void validateReceivedMessages(final String topic,
                                                 final Deserializer<K> keySerializer,
                                                 final Deserializer<V> valueSerializer,
                                                 final List<KeyValue<K, V>> expectedRecords) throws Exception {

        final String safeTestName = safeUniqueTestName(getClass(), testName);
        final Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-" + safeTestName);
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.setProperty(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            keySerializer.getClass().getName()
        );
        consumerProperties.setProperty(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            valueSerializer.getClass().getName()
        );

        IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(
            consumerProperties,
            topic,
            expectedRecords
        );
    }

    private static AdminClient createAdminClient() {
        final Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());

        return AdminClient.create(properties);
    }
}
