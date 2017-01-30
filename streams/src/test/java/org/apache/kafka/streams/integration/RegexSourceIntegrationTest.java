/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */


package org.apache.kafka.streams.integration;

import kafka.utils.MockTime;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.StreamTask;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.processor.internals.StreamsMetadataState;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * End-to-end integration test based on using regex and named topics for creating sources, using
 * an embedded Kafka cluster.
 */
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


    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(TOPIC_1);
        CLUSTER.createTopic(TOPIC_2);
        CLUSTER.createTopic(TOPIC_A);
        CLUSTER.createTopic(TOPIC_C);
        CLUSTER.createTopic(TOPIC_Y);
        CLUSTER.createTopic(TOPIC_Z);
        CLUSTER.createTopic(FA_TOPIC);
        CLUSTER.createTopic(FOO_TOPIC);
        CLUSTER.createTopic(PARTITIONED_TOPIC_1, 2, 1);
        CLUSTER.createTopic(PARTITIONED_TOPIC_2, 2, 1);
        CLUSTER.createTopic(DEFAULT_OUTPUT_TOPIC);

    }

    @Before
    public void setUp() {

        streamsConfiguration = StreamsTestUtils.getStreamsConfig(CLUSTER.bootstrapServers(),
            STRING_SERDE_CLASSNAME,
            STRING_SERDE_CLASSNAME);
    }

    @After
    public void tearDown() throws Exception {
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

        final KStreamBuilder builder = new KStreamBuilder();

        final KStream<String, String> pattern1Stream = builder.stream(Pattern.compile("TEST-TOPIC-\\d"));

        pattern1Stream.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);

        final KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);

        final Field streamThreadsField = streams.getClass().getDeclaredField("threads");
        streamThreadsField.setAccessible(true);
        final StreamThread[] streamThreads = (StreamThread[]) streamThreadsField.get(streams);
        final StreamThread originalThread = streamThreads[0];

        final TestStreamThread testStreamThread = new TestStreamThread(builder, streamsConfig,
            new DefaultKafkaClientSupplier(),
            originalThread.applicationId, originalThread.clientId, originalThread.processId, new Metrics(), Time.SYSTEM);

        final TestCondition oneTopicAdded = new TestCondition() {
            @Override
            public boolean conditionMet() {
                return testStreamThread.assignedTopicPartitions.equals(expectedFirstAssignment);
            }
        };

        streamThreads[0] = testStreamThread;
        streams.start();

        TestUtils.waitForCondition(oneTopicAdded, STREAM_TASKS_NOT_UPDATED);

        CLUSTER.createTopic("TEST-TOPIC-2");

        final TestCondition secondTopicAdded = new TestCondition() {
            @Override
            public boolean conditionMet() {
                return testStreamThread.assignedTopicPartitions.equals(expectedSecondAssignment);
            }
        };

        TestUtils.waitForCondition(secondTopicAdded, STREAM_TASKS_NOT_UPDATED);

        streams.close();
    }

    @Test
    public void testRegexMatchesTopicsAWhenDeleted() throws Exception {

        final Serde<String> stringSerde = Serdes.String();
        final List<String> expectedFirstAssignment = Arrays.asList("TEST-TOPIC-A", "TEST-TOPIC-B");
        final List<String> expectedSecondAssignment = Arrays.asList("TEST-TOPIC-B");

        final StreamsConfig streamsConfig = new StreamsConfig(streamsConfiguration);

        CLUSTER.createTopic("TEST-TOPIC-A");
        CLUSTER.createTopic("TEST-TOPIC-B");

        final KStreamBuilder builder = new KStreamBuilder();

        final KStream<String, String> pattern1Stream = builder.stream(Pattern.compile("TEST-TOPIC-[A-Z]"));

        pattern1Stream.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);

        final KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);

        final Field streamThreadsField = streams.getClass().getDeclaredField("threads");
        streamThreadsField.setAccessible(true);
        final StreamThread[] streamThreads = (StreamThread[]) streamThreadsField.get(streams);
        final StreamThread originalThread = streamThreads[0];

        final TestStreamThread testStreamThread = new TestStreamThread(builder, streamsConfig,
            new DefaultKafkaClientSupplier(),
            originalThread.applicationId, originalThread.clientId, originalThread.processId, new Metrics(), Time.SYSTEM);

        streamThreads[0] = testStreamThread;

        final TestCondition bothTopicsAdded = new TestCondition() {
            @Override
            public boolean conditionMet() {
                return testStreamThread.assignedTopicPartitions.equals(expectedFirstAssignment);
            }
        };
        streams.start();

        TestUtils.waitForCondition(bothTopicsAdded, STREAM_TASKS_NOT_UPDATED);

        CLUSTER.deleteTopic("TEST-TOPIC-A");

        final TestCondition oneTopicRemoved = new TestCondition() {
            @Override
            public boolean conditionMet() {
                return testStreamThread.assignedTopicPartitions.equals(expectedSecondAssignment);
            }
        };

        TestUtils.waitForCondition(oneTopicRemoved, STREAM_TASKS_NOT_UPDATED);

        streams.close();
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

        final KStreamBuilder builder = new KStreamBuilder();

        final KStream<String, String> pattern1Stream = builder.stream(Pattern.compile("topic-\\d"));
        final KStream<String, String> pattern2Stream = builder.stream(Pattern.compile("topic-[A-D]"));
        final KStream<String, String> namedTopicsStream = builder.stream(TOPIC_Y, TOPIC_Z);

        pattern1Stream.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);
        pattern2Stream.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);
        namedTopicsStream.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);

        final KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
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

        streams.close();
        Collections.sort(actualValues);
        Collections.sort(expectedReceivedValues);
        assertThat(actualValues, equalTo(expectedReceivedValues));
    }

    @Test
    public void testMultipleConsumersCanReadFromPartitionedTopic() throws Exception {

        final Serde<String> stringSerde = Serdes.String();
        final KStreamBuilder builderLeader = new KStreamBuilder();
        final KStreamBuilder builderFollower = new KStreamBuilder();
        final List<String> expectedAssignment = Arrays.asList(PARTITIONED_TOPIC_1,  PARTITIONED_TOPIC_2);

        final KStream<String, String> partitionedStreamLeader = builderLeader.stream(Pattern.compile("partitioned-\\d"));
        final KStream<String, String> partitionedStreamFollower = builderFollower.stream(Pattern.compile("partitioned-\\d"));


        partitionedStreamLeader.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);
        partitionedStreamFollower.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);

        final KafkaStreams partitionedStreamsLeader  = new KafkaStreams(builderLeader, streamsConfiguration);
        final KafkaStreams partitionedStreamsFollower  = new KafkaStreams(builderFollower, streamsConfiguration);

        final StreamsConfig streamsConfig = new StreamsConfig(streamsConfiguration);


        final Field leaderStreamThreadsField = partitionedStreamsLeader.getClass().getDeclaredField("threads");
        leaderStreamThreadsField.setAccessible(true);
        final StreamThread[] leaderStreamThreads = (StreamThread[]) leaderStreamThreadsField.get(partitionedStreamsLeader);
        final StreamThread originalLeaderThread = leaderStreamThreads[0];

        final TestStreamThread leaderTestStreamThread = new TestStreamThread(builderLeader, streamsConfig,
                new DefaultKafkaClientSupplier(),
                originalLeaderThread.applicationId, originalLeaderThread.clientId, originalLeaderThread.processId, new Metrics(), Time.SYSTEM);

        leaderStreamThreads[0] = leaderTestStreamThread;

        final TestCondition bothTopicsAddedToLeader = new TestCondition() {
            @Override
            public boolean conditionMet() {
                return leaderTestStreamThread.assignedTopicPartitions.equals(expectedAssignment);
            }
        };



        final Field followerStreamThreadsField = partitionedStreamsFollower.getClass().getDeclaredField("threads");
        followerStreamThreadsField.setAccessible(true);
        final StreamThread[] followerStreamThreads = (StreamThread[]) followerStreamThreadsField.get(partitionedStreamsFollower);
        final StreamThread originalFollowerThread = followerStreamThreads[0];

        final TestStreamThread followerTestStreamThread = new TestStreamThread(builderFollower, streamsConfig,
                new DefaultKafkaClientSupplier(),
                originalFollowerThread.applicationId, originalFollowerThread.clientId, originalFollowerThread.processId, new Metrics(), Time.SYSTEM);

        followerStreamThreads[0] = followerTestStreamThread;


        final TestCondition bothTopicsAddedToFollower = new TestCondition() {
            @Override
            public boolean conditionMet() {
                return followerTestStreamThread.assignedTopicPartitions.equals(expectedAssignment);
            }
        };

        partitionedStreamsLeader.start();
        TestUtils.waitForCondition(bothTopicsAddedToLeader, "Topics never assigned to leader stream");


        partitionedStreamsFollower.start();
        TestUtils.waitForCondition(bothTopicsAddedToFollower, "Topics never assigned to follower stream");

        partitionedStreamsLeader.close();
        partitionedStreamsFollower.close();

    }

    // TODO should be updated to expected = TopologyBuilderException after KAFKA-3708
    @Test(expected = AssertionError.class)
    public void testNoMessagesSentExceptionFromOverlappingPatterns() throws Exception {

        final String fooMessage = "fooMessage";
        final String fMessage = "fMessage";


        final Serde<String> stringSerde = Serdes.String();

        final KStreamBuilder builder = new KStreamBuilder();


        // overlapping patterns here, no messages should be sent as TopologyBuilderException
        // will be thrown when the processor topology is built.

        final KStream<String, String> pattern1Stream = builder.stream(Pattern.compile("foo.*"));
        final KStream<String, String> pattern2Stream = builder.stream(Pattern.compile("f.*"));


        pattern1Stream.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);
        pattern2Stream.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);

        final KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();

        final Properties producerConfig = TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class);

        IntegrationTestUtils.produceValuesSynchronously(FA_TOPIC, Arrays.asList(fMessage), producerConfig, mockTime);
        IntegrationTestUtils.produceValuesSynchronously(FOO_TOPIC, Arrays.asList(fooMessage), producerConfig, mockTime);

        final Properties consumerConfig = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer.class, StringDeserializer.class);

        try {
            IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, DEFAULT_OUTPUT_TOPIC, 2, 5000);
            fail("Should not get here");
        } finally {
            streams.close();
        }

    }

    private class TestStreamThread extends StreamThread {
        public volatile List<String> assignedTopicPartitions = new ArrayList<>();

        public TestStreamThread(final TopologyBuilder builder, final StreamsConfig config, final KafkaClientSupplier clientSupplier, final String applicationId, final String clientId, final UUID processId, final Metrics metrics, final Time time) {
            super(builder, config, clientSupplier, applicationId, clientId, processId, metrics, time, new StreamsMetadataState(builder, StreamsMetadataState.UNKNOWN_HOST),
                  0);
        }

        @Override
        public StreamTask createStreamTask(final TaskId id, final Collection<TopicPartition> partitions) {
            final List<String> topicPartitions = new ArrayList<>();
            for (final TopicPartition partition : partitions) {
                topicPartitions.add(partition.topic());
            }
            Collections.sort(topicPartitions);

            assignedTopicPartitions = topicPartitions;
            return super.createStreamTask(id, partitions);
        }

    }
}
