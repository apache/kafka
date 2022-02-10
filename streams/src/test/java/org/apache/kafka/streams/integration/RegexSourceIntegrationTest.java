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
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyWrapper;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.MockApiProcessorSupplier;
import org.apache.kafka.test.MockKeyValueStoreBuilder;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

/**
 * End-to-end integration test based on using regex and named topics for creating sources, using
 * an embedded Kafka cluster.
 */
@Category({IntegrationTest.class})
public class RegexSourceIntegrationTest {
    private static final int NUM_BROKERS = 1;
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    @BeforeClass
    public static void startCluster() throws IOException, InterruptedException {
        CLUSTER.start();
        CLUSTER.createTopics(
                TOPIC_1,
                TOPIC_2,
                TOPIC_A,
                TOPIC_C,
                TOPIC_Y,
                TOPIC_Z,
                FA_TOPIC,
                FOO_TOPIC);
        CLUSTER.createTopic(PARTITIONED_TOPIC_1, 2, 1);
        CLUSTER.createTopic(PARTITIONED_TOPIC_2, 2, 1);
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }

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

    private static final String STRING_SERDE_CLASSNAME = Serdes.String().getClass().getName();
    private Properties streamsConfiguration;
    private static final String STREAM_TASKS_NOT_UPDATED = "Stream tasks not updated";
    private KafkaStreams streams;
    private static volatile AtomicInteger topicSuffixGenerator = new AtomicInteger(0);
    private String outputTopic;

    @Before
    public void setUp() throws InterruptedException {
        outputTopic = createTopic(topicSuffixGenerator.incrementAndGet());
        final Properties properties = new Properties();
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        properties.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1000");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, 0L);
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);

        streamsConfiguration = StreamsTestUtils.getStreamsConfig(
            IntegrationTestUtils.safeUniqueTestName(RegexSourceIntegrationTest.class, new TestName()),
            CLUSTER.bootstrapServers(),
            STRING_SERDE_CLASSNAME,
            STRING_SERDE_CLASSNAME,
            properties
        );
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
        try {
            final Serde<String> stringSerde = Serdes.String();

            final List<String> expectedFirstAssignment = Collections.singletonList("TEST-TOPIC-1");
            // we compare lists of subscribed topics and hence requiring the order as well; this is guaranteed
            // with KIP-429 since we would NOT revoke TEST-TOPIC-1 but only add TEST-TOPIC-2 so the list is always
            // in the order of "TEST-TOPIC-1, TEST-TOPIC-2". Note if KIP-429 behavior ever changed it may become a flaky test
            final List<String> expectedSecondAssignment = Arrays.asList("TEST-TOPIC-1", "TEST-TOPIC-2");

            CLUSTER.createTopic("TEST-TOPIC-1");

            final StreamsBuilder builder = new StreamsBuilder();

            final KStream<String, String> pattern1Stream = builder.stream(Pattern.compile("TEST-TOPIC-\\d"));

            pattern1Stream.to(outputTopic, Produced.with(stringSerde, stringSerde));
            final List<String> assignedTopics = new CopyOnWriteArrayList<>();
            streams = new KafkaStreams(builder.build(), streamsConfiguration, new DefaultKafkaClientSupplier() {
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
            TestUtils.waitForCondition(() -> assignedTopics.equals(expectedFirstAssignment), STREAM_TASKS_NOT_UPDATED);

            CLUSTER.createTopic("TEST-TOPIC-2");

            TestUtils.waitForCondition(() -> assignedTopics.equals(expectedSecondAssignment), STREAM_TASKS_NOT_UPDATED);

            streams.close();
        } finally {
            CLUSTER.deleteTopicsAndWait("TEST-TOPIC-1", "TEST-TOPIC-2");
        }
    }

    @Test
    public void testRegexRecordsAreProcessedAfterNewTopicCreatedWithMultipleSubtopologies() throws Exception {
        final String topic1 = "TEST-TOPIC-1";
        final String topic2 = "TEST-TOPIC-2";

        try {
            CLUSTER.createTopic(topic1);

            final StreamsBuilder builder = new StreamsBuilder();
            final KStream<String, String> pattern1Stream = builder.stream(Pattern.compile("TEST-TOPIC-\\d"));
            final KStream<String, String> otherStream = builder.stream(Pattern.compile("not-a-match"));

            pattern1Stream
                .selectKey((k, v) -> k)
                .groupByKey()
                .aggregate(() -> "", (k, v, a) -> v)
                .toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

            final Topology topology = builder.build();
            assertThat(topology.describe().subtopologies().size(), greaterThan(1));
            streams = new KafkaStreams(topology, streamsConfiguration);

            startApplicationAndWaitUntilRunning(Collections.singletonList(streams), Duration.ofSeconds(30));

            CLUSTER.createTopic(topic2);

            final KeyValue<String, String> record1 = new KeyValue<>("1", "1");
            final KeyValue<String, String> record2 = new KeyValue<>("2", "2");
            IntegrationTestUtils.produceKeyValuesSynchronously(
                topic1,
                Collections.singletonList(record1),
                TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class),
                CLUSTER.time
            );
            IntegrationTestUtils.produceKeyValuesSynchronously(
                topic2,
                Collections.singletonList(record2),
                TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class),
                CLUSTER.time
            );
            IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(
                TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer.class, StringDeserializer.class),
                outputTopic,
                Arrays.asList(record1, record2)
            );

            streams.close();
        } finally {
            CLUSTER.deleteTopicsAndWait(topic1, topic2);
        }
    }

    private String createTopic(final int suffix) throws InterruptedException {
        final String outputTopic = "outputTopic_" + suffix;
        CLUSTER.createTopic(outputTopic);
        return outputTopic;
    }

    @Test
    public void testRegexMatchesTopicsAWhenDeleted() throws Exception {
        final Serde<String> stringSerde = Serdes.String();
        final List<String> expectedFirstAssignment = Arrays.asList("TEST-TOPIC-A", "TEST-TOPIC-B");
        final List<String> expectedSecondAssignment = Collections.singletonList("TEST-TOPIC-B");
        final List<String> assignedTopics = new CopyOnWriteArrayList<>();

        try {
            CLUSTER.createTopics("TEST-TOPIC-A", "TEST-TOPIC-B");

            final StreamsBuilder builder = new StreamsBuilder();

            final KStream<String, String> pattern1Stream = builder.stream(Pattern.compile("TEST-TOPIC-[A-Z]"));

            pattern1Stream.to(outputTopic, Produced.with(stringSerde, stringSerde));

            streams = new KafkaStreams(builder.build(), streamsConfiguration, new DefaultKafkaClientSupplier() {
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
            TestUtils.waitForCondition(() -> assignedTopics.equals(expectedFirstAssignment), STREAM_TASKS_NOT_UPDATED);
        } finally {
            CLUSTER.deleteTopic("TEST-TOPIC-A");
        }

        TestUtils.waitForCondition(() -> assignedTopics.equals(expectedSecondAssignment), STREAM_TASKS_NOT_UPDATED);
    }

    @Test
    public void shouldAddStateStoreToRegexDefinedSource() throws Exception {
        final StoreBuilder<KeyValueStore<Object, Object>> storeBuilder = new MockKeyValueStoreBuilder("testStateStore", false);
        final long thirtySecondTimeout = 30 * 1000;

        final TopologyWrapper topology = new TopologyWrapper();
        topology.addSource("ingest", Pattern.compile("topic-\\d+"));
        topology.addProcessor("my-processor", new MockApiProcessorSupplier<>(), "ingest");
        topology.addStateStore(storeBuilder, "my-processor");

        streams = new KafkaStreams(topology, streamsConfiguration);
        streams.start();

        final TestCondition stateStoreNameBoundToSourceTopic = () -> {
            final Map<String, List<String>> stateStoreToSourceTopic = topology.getInternalBuilder().stateStoreNameToFullSourceTopicNames();
            final List<String> topicNamesList = stateStoreToSourceTopic.get("testStateStore");
            return topicNamesList != null && !topicNamesList.isEmpty() && topicNamesList.get(0).equals("topic-1");
        };

        TestUtils.waitForCondition(stateStoreNameBoundToSourceTopic, thirtySecondTimeout, "Did not find topic: [topic-1] connected to state store: [testStateStore]");
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

        pattern1Stream.to(outputTopic, Produced.with(stringSerde, stringSerde));
        pattern2Stream.to(outputTopic, Produced.with(stringSerde, stringSerde));
        namedTopicsStream.to(outputTopic, Produced.with(stringSerde, stringSerde));

        streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();

        final Properties producerConfig = TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class);

        IntegrationTestUtils.produceValuesSynchronously(TOPIC_1, Collections.singleton(topic1TestMessage), producerConfig, mockTime);
        IntegrationTestUtils.produceValuesSynchronously(TOPIC_2, Collections.singleton(topic2TestMessage), producerConfig, mockTime);
        IntegrationTestUtils.produceValuesSynchronously(TOPIC_A, Collections.singleton(topicATestMessage), producerConfig, mockTime);
        IntegrationTestUtils.produceValuesSynchronously(TOPIC_C, Collections.singleton(topicCTestMessage), producerConfig, mockTime);
        IntegrationTestUtils.produceValuesSynchronously(TOPIC_Y, Collections.singleton(topicYTestMessage), producerConfig, mockTime);
        IntegrationTestUtils.produceValuesSynchronously(TOPIC_Z, Collections.singleton(topicZTestMessage), producerConfig, mockTime);

        final Properties consumerConfig = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer.class, StringDeserializer.class);

        final List<String> expectedReceivedValues = Arrays.asList(topicATestMessage, topic1TestMessage, topic2TestMessage, topicCTestMessage, topicYTestMessage, topicZTestMessage);
        final List<KeyValue<String, String>> receivedKeyValues = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, outputTopic, 6);
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
            final List<String> expectedAssignment = Arrays.asList(PARTITIONED_TOPIC_1, PARTITIONED_TOPIC_2);

            final KStream<String, String> partitionedStreamLeader = builderLeader.stream(Pattern.compile("partitioned-\\d"));
            final KStream<String, String> partitionedStreamFollower = builderFollower.stream(Pattern.compile("partitioned-\\d"));


            partitionedStreamLeader.to(outputTopic, Produced.with(stringSerde, stringSerde));
            partitionedStreamFollower.to(outputTopic, Produced.with(stringSerde, stringSerde));

            final List<String> leaderAssignment = new CopyOnWriteArrayList<>();
            final List<String> followerAssignment = new CopyOnWriteArrayList<>();

            partitionedStreamsLeader = new KafkaStreams(builderLeader.build(), streamsConfiguration, new DefaultKafkaClientSupplier() {
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
            partitionedStreamsFollower = new KafkaStreams(builderFollower.build(), streamsConfiguration, new DefaultKafkaClientSupplier() {
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
            TestUtils.waitForCondition(() -> followerAssignment.equals(expectedAssignment) && leaderAssignment.equals(expectedAssignment), "topic assignment not completed");
        } finally {
            if (partitionedStreamsLeader != null) {
                partitionedStreamsLeader.close();
            }
            if (partitionedStreamsFollower != null) {
                partitionedStreamsFollower.close();
            }
        }
    }

    @Test
    public void testNoMessagesSentExceptionFromOverlappingPatterns() throws Exception {
        final String fMessage = "fMessage";
        final String fooMessage = "fooMessage";
        final Serde<String> stringSerde = Serdes.String();
        final StreamsBuilder builder = new StreamsBuilder();

        // overlapping patterns here, no messages should be sent as TopologyException
        // will be thrown when the processor topology is built.
        final KStream<String, String> pattern1Stream = builder.stream(Pattern.compile("foo.*"));
        final KStream<String, String> pattern2Stream = builder.stream(Pattern.compile("f.*"));

        pattern1Stream.to(outputTopic, Produced.with(stringSerde, stringSerde));
        pattern2Stream.to(outputTopic, Produced.with(stringSerde, stringSerde));

        final AtomicBoolean expectError = new AtomicBoolean(false);

        streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.ERROR) {
                expectError.set(true);
            }
        });
        streams.setUncaughtExceptionHandler(e -> {
            expectError.set(true);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
        streams.start();

        final Properties producerConfig = TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class);

        IntegrationTestUtils.produceValuesSynchronously(FA_TOPIC, Collections.singleton(fMessage), producerConfig, mockTime);
        IntegrationTestUtils.produceValuesSynchronously(FOO_TOPIC, Collections.singleton(fooMessage), producerConfig, mockTime);

        final Properties consumerConfig = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer.class, StringDeserializer.class);
        try {
            IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, outputTopic, 2, 5000);
            throw new IllegalStateException("This should not happen: an assertion error should have been thrown before this.");
        } catch (final AssertionError e) {
            // this is fine
        }

        assertThat(expectError.get(), is(true));
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
            for (final TopicPartition partition : partitions) {
                assignedTopics.remove(partition.topic());
            }
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
