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

import kafka.utils.MockTime;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serde;
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
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.MockStateStoreSupplier;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * End-to-end integration test based on using regex and named topics for creating sources, using
 * an embedded Kafka cluster.
 */
@Category({IntegrationTest.class})
public class RegexSourceIntegrationTest {
    private static final int NUM_BROKERS = 1;
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final MockTime mockTime = CLUSTER.time;

    private static final String TOPIC_1 = "topic-1";
    private static final String TOPIC_2 = "topic-2";
    private static final String TOPIC_A = "topic-A";
    private static final String TOPIC_C = "topic-C";
    private static final String TOPIC_Y = "topic-Y";
    private static final String TOPIC_Z = "topic-Z";
    private static final String FA_TOPIC = "fa";
    private static final String FOO_TOPIC = "foo";
    private static final String PARTITIONED_TOPIC_1 = "partitioned-1";
    private static final String PARTITIONED_TOPIC_2 = "partitioned-2";

    private static final String DEFAULT_OUTPUT_TOPIC = "outputTopic";
    private static final String STRING_SERDE_CLASSNAME = Serdes.String().getClass().getName();
    private Properties streamsConfiguration;
    private static final String STREAM_TASKS_NOT_UPDATED = "Stream tasks not updated";
    private KafkaStreams streams;


    @BeforeClass
    public static void startKafkaCluster() throws InterruptedException {
        CLUSTER.createTopics(
            TOPIC_1,
            TOPIC_2,
            TOPIC_A,
            TOPIC_C,
            TOPIC_Y,
            TOPIC_Z,
            FA_TOPIC,
            FOO_TOPIC,
            DEFAULT_OUTPUT_TOPIC);
        CLUSTER.createTopic(PARTITIONED_TOPIC_1, 2, 1);
        CLUSTER.createTopic(PARTITIONED_TOPIC_2, 2, 1);
    }

    @Before
    public void setUp() {
        final Properties properties = new Properties();
        properties.put(IntegrationTestUtils.INTERNAL_LEAVE_GROUP_ON_CLOSE, true);
        streamsConfiguration = StreamsTestUtils.getStreamsConfig("regex-source-integration-test",
                                                                 CLUSTER.bootstrapServers(),
                                                                 STRING_SERDE_CLASSNAME,
                                                                 STRING_SERDE_CLASSNAME,
                                                                 properties);
    }

    @After
    public void tearDown() throws IOException {
        if (streams != null) {
            streams.close();
        }
        // Remove any state from previous test runs
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    @Test
    public void testRegexMatchesTopicsAWhenCreated() throws Exception {

        final Serde<String> stringSerde = Serdes.String();
        final List<String> expectedFirstAssignment = Arrays.asList("TEST-TOPIC-1");
        final List<String> expectedSecondAssignment = Arrays.asList("TEST-TOPIC-1", "TEST-TOPIC-2");

        final StreamsConfig streamsConfig = new StreamsConfig(streamsConfiguration);

        CLUSTER.createTopic("TEST-TOPIC-1");

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> pattern1Stream = builder.stream(Pattern.compile("TEST-TOPIC-\\d"));

        pattern1Stream.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);
        final List<String> assignedTopics = new ArrayList<>();
        streams = new KafkaStreams(builder.build(), streamsConfig, new DefaultKafkaClientSupplier() {
            @Override
            public Consumer<byte[], byte[]> getConsumer(final Map<String, Object> config) {
                return new KafkaConsumer<byte[], byte[]>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer()) {
                    @Override
                    public void subscribe(final Pattern topics, final ConsumerRebalanceListener listener) {
                        super.subscribe(topics, new TheConsumerRebalanceListener(assignedTopics, listener));
                    }
                };

            }
        });


        streams.start();
        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                return assignedTopics.equals(expectedFirstAssignment);
            }
        }, STREAM_TASKS_NOT_UPDATED);

        CLUSTER.createTopic("TEST-TOPIC-2");

        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                return assignedTopics.equals(expectedSecondAssignment);
            }
        }, STREAM_TASKS_NOT_UPDATED);

    }

    @Test
    public void testRegexMatchesTopicsAWhenDeleted() throws Exception {

        final Serde<String> stringSerde = Serdes.String();
        final List<String> expectedFirstAssignment = Arrays.asList("TEST-TOPIC-A", "TEST-TOPIC-B");
        final List<String> expectedSecondAssignment = Arrays.asList("TEST-TOPIC-B");

        final StreamsConfig streamsConfig = new StreamsConfig(streamsConfiguration);

        CLUSTER.createTopics("TEST-TOPIC-A", "TEST-TOPIC-B");

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> pattern1Stream = builder.stream(Pattern.compile("TEST-TOPIC-[A-Z]"));

        pattern1Stream.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);

        final List<String> assignedTopics = new ArrayList<>();
        streams = new KafkaStreams(builder.build(), streamsConfig, new DefaultKafkaClientSupplier() {
            @Override
            public Consumer<byte[], byte[]> getConsumer(final Map<String, Object> config) {
                return new KafkaConsumer<byte[], byte[]>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer()) {
                    @Override
                    public void subscribe(final Pattern topics, final ConsumerRebalanceListener listener) {
                        super.subscribe(topics, new TheConsumerRebalanceListener(assignedTopics, listener));
                    }
                };

            }
        });


        streams.start();
        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                return assignedTopics.equals(expectedFirstAssignment);
            }
        }, STREAM_TASKS_NOT_UPDATED);

        CLUSTER.deleteTopic("TEST-TOPIC-A");

        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                return assignedTopics.equals(expectedSecondAssignment);
            }
        }, STREAM_TASKS_NOT_UPDATED);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldAddStateStoreToRegexDefinedSource() throws InterruptedException {

        final ProcessorSupplier<String, String> processorSupplier = new MockProcessorSupplier<>();
        final MockStateStoreSupplier stateStoreSupplier = new MockStateStoreSupplier("testStateStore", false);
        final long thirtySecondTimeout = 30 * 1000;

        final TopologyBuilder builder = new TopologyBuilder()
                .addSource("ingest", Pattern.compile("topic-\\d+"))
                .addProcessor("my-processor", processorSupplier, "ingest")
                .addStateStore(stateStoreSupplier, "my-processor");


        streams = new KafkaStreams(builder, streamsConfiguration);
        try {
            streams.start();

            final TestCondition stateStoreNameBoundToSourceTopic = new TestCondition() {
                @Override
                public boolean conditionMet() {
                    final Map<String, List<String>> stateStoreToSourceTopic = builder.stateStoreNameToSourceTopics();
                    final List<String> topicNamesList = stateStoreToSourceTopic.get("testStateStore");
                    return topicNamesList != null && !topicNamesList.isEmpty() && topicNamesList.get(0).equals("topic-1");
                }
            };

            TestUtils.waitForCondition(stateStoreNameBoundToSourceTopic, thirtySecondTimeout, "Did not find topic: [topic-1] connected to state store: [testStateStore]");

        } finally {
            streams.close();
        }
    }


    @Test
    public void testShouldReadFromRegexAndNamedTopics() throws Exception {

        final String topic1TestMessage = "topic-1 test";
        final String topic2TestMessage = "topic-2 test";
        final String topicATestMessage = "topic-A test";
        final String topicCTestMessage = "topic-C test";
        final String topicYTestMessage = "topic-Y test";
        final String topicZTestMessage = "topic-Z test";


        final Serde<String> stringSerde = Serdes.String();

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> pattern1Stream = builder.stream(Pattern.compile("topic-\\d"));
        final KStream<String, String> pattern2Stream = builder.stream(Pattern.compile("topic-[A-D]"));
        final KStream<String, String> namedTopicsStream = builder.stream(Arrays.asList(TOPIC_Y, TOPIC_Z));

        pattern1Stream.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);
        pattern2Stream.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);
        namedTopicsStream.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);

        streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();

        final Properties producerConfig = TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class);

        IntegrationTestUtils.produceValuesSynchronously(TOPIC_1, Arrays.asList(topic1TestMessage), producerConfig, mockTime);
        IntegrationTestUtils.produceValuesSynchronously(TOPIC_2, Arrays.asList(topic2TestMessage), producerConfig, mockTime);
        IntegrationTestUtils.produceValuesSynchronously(TOPIC_A, Arrays.asList(topicATestMessage), producerConfig, mockTime);
        IntegrationTestUtils.produceValuesSynchronously(TOPIC_C, Arrays.asList(topicCTestMessage), producerConfig, mockTime);
        IntegrationTestUtils.produceValuesSynchronously(TOPIC_Y, Arrays.asList(topicYTestMessage), producerConfig, mockTime);
        IntegrationTestUtils.produceValuesSynchronously(TOPIC_Z, Arrays.asList(topicZTestMessage), producerConfig, mockTime);

        final Properties consumerConfig = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer.class, StringDeserializer.class);

        final List<String> expectedReceivedValues = Arrays.asList(topicATestMessage, topic1TestMessage, topic2TestMessage, topicCTestMessage, topicYTestMessage, topicZTestMessage);
        final List<KeyValue<String, String>> receivedKeyValues = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, DEFAULT_OUTPUT_TOPIC, 6);
        final List<String> actualValues = new ArrayList<>(6);

        for (final KeyValue<String, String> receivedKeyValue : receivedKeyValues) {
            actualValues.add(receivedKeyValue.value);
        }

        Collections.sort(actualValues);
        Collections.sort(expectedReceivedValues);
        assertThat(actualValues, equalTo(expectedReceivedValues));
    }

    @Test
    public void testMultipleConsumersCanReadFromPartitionedTopic() throws Exception {

        KafkaStreams partitionedStreamsLeader = null;
        KafkaStreams partitionedStreamsFollower = null;
        try {
            final Serde<String> stringSerde = Serdes.String();
            final StreamsBuilder builderLeader = new StreamsBuilder();
            final StreamsBuilder builderFollower = new StreamsBuilder();
            final List<String> expectedAssignment = Arrays.asList(PARTITIONED_TOPIC_1,  PARTITIONED_TOPIC_2);

            final KStream<String, String> partitionedStreamLeader = builderLeader.stream(Pattern.compile("partitioned-\\d"));
            final KStream<String, String> partitionedStreamFollower = builderFollower.stream(Pattern.compile("partitioned-\\d"));


            partitionedStreamLeader.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);
            partitionedStreamFollower.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);

            final List<String> leaderAssignment = new ArrayList<>();
            final List<String> followerAssignment = new ArrayList<>();
            StreamsConfig config = new StreamsConfig(streamsConfiguration);

            partitionedStreamsLeader  = new KafkaStreams(builderLeader.build(), config, new DefaultKafkaClientSupplier() {
                @Override
                public Consumer<byte[], byte[]> getConsumer(final Map<String, Object> config) {
                    return new KafkaConsumer<byte[], byte[]>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer()) {
                        @Override
                        public void subscribe(final Pattern topics, final ConsumerRebalanceListener listener) {
                            super.subscribe(topics, new TheConsumerRebalanceListener(leaderAssignment, listener));
                        }
                    };

                }
            });
            partitionedStreamsFollower  = new KafkaStreams(builderFollower.build(), config, new DefaultKafkaClientSupplier() {
                @Override
                public Consumer<byte[], byte[]> getConsumer(final Map<String, Object> config) {
                    return new KafkaConsumer<byte[], byte[]>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer()) {
                        @Override
                        public void subscribe(final Pattern topics, final ConsumerRebalanceListener listener) {
                            super.subscribe(topics, new TheConsumerRebalanceListener(followerAssignment, listener));
                        }
                    };

                }
            });


            partitionedStreamsLeader.start();
            partitionedStreamsFollower.start();
            TestUtils.waitForCondition(new TestCondition() {
                @Override
                public boolean conditionMet() {
                    return followerAssignment.equals(expectedAssignment) && leaderAssignment.equals(expectedAssignment);
                }
            }, "topic assignment not completed");
        } finally {
            if (partitionedStreamsLeader != null) {
                partitionedStreamsLeader.close();
            }
            if (partitionedStreamsFollower != null) {
                partitionedStreamsFollower.close();
            }
        }

    }

    // TODO should be updated to expected = TopologyBuilderException after KAFKA-3708
    @Test(expected = AssertionError.class)
    public void testNoMessagesSentExceptionFromOverlappingPatterns() throws Exception {

        final String fooMessage = "fooMessage";
        final String fMessage = "fMessage";


        final Serde<String> stringSerde = Serdes.String();

        final StreamsBuilder builder = new StreamsBuilder();


        // overlapping patterns here, no messages should be sent as TopologyBuilderException
        // will be thrown when the processor topology is built.

        final KStream<String, String> pattern1Stream = builder.stream(Pattern.compile("foo.*"));
        final KStream<String, String> pattern2Stream = builder.stream(Pattern.compile("f.*"));


        pattern1Stream.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);
        pattern2Stream.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);

        streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();

        final Properties producerConfig = TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class);

        IntegrationTestUtils.produceValuesSynchronously(FA_TOPIC, Arrays.asList(fMessage), producerConfig, mockTime);
        IntegrationTestUtils.produceValuesSynchronously(FOO_TOPIC, Arrays.asList(fooMessage), producerConfig, mockTime);

        final Properties consumerConfig = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer.class, StringDeserializer.class);

        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, DEFAULT_OUTPUT_TOPIC, 2, 5000);
        fail("Should not get here");
    }

    private static class TheConsumerRebalanceListener implements ConsumerRebalanceListener {
        private final List<String> assignedTopics;
        private final ConsumerRebalanceListener listener;

        TheConsumerRebalanceListener(final List<String> assignedTopics, final ConsumerRebalanceListener listener) {
            this.assignedTopics = assignedTopics;
            this.listener = listener;
        }

        @Override
        public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
            assignedTopics.clear();
            listener.onPartitionsRevoked(partitions);
        }

        @Override
        public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
            for (final TopicPartition partition : partitions) {
                assignedTopics.add(partition.topic());
            }
            Collections.sort(assignedTopics);
            listener.onPartitionsAssigned(partitions);
        }
    }

}
