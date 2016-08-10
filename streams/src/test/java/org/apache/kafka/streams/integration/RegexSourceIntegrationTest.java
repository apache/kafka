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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedSingleNodeKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.StreamTask;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.processor.internals.StreamsMetadataState;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.StreamsTestUtils;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

    private static final String TOPIC_1 = "topic-1";
    private static final String TOPIC_2 = "topic-2";
    private static final String TOPIC_A = "topic-A";
    private static final String TOPIC_C = "topic-C";
    private static final String TOPIC_Y = "topic-Y";
    private static final String TOPIC_Z = "topic-Z";
    private static final String FA_TOPIC = "fa";
    private static final String FOO_TOPIC = "foo";

    private static final int FIRST_UPDATE = 0;
    private static final int SECOND_UPDATE = 1;

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

        StreamsConfig streamsConfig = new StreamsConfig(streamsConfiguration);

        CLUSTER.createTopic("TEST-TOPIC-1");

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> pattern1Stream = builder.stream(Pattern.compile("TEST-TOPIC-\\d"));

        pattern1Stream.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);

        Field streamThreadsField = streams.getClass().getDeclaredField("threads");
        streamThreadsField.setAccessible(true);
        StreamThread[] streamThreads =  (StreamThread[]) streamThreadsField.get(streams);
        StreamThread originalThread = streamThreads[0];

        final TestStreamThread testStreamThread = new TestStreamThread(builder, streamsConfig,
                                           new DefaultKafkaClientSupplier(),
                                           originalThread.applicationId, originalThread.clientId, originalThread.processId, new Metrics(), new SystemTime());

        TestCondition oneTopicAdded  = new TestCondition() {
            @Override
            public boolean conditionMet() {
                List<String> assignedTopics = testStreamThread.assignedTopicPartitions.get(FIRST_UPDATE);
                return assignedTopics != null && assignedTopics.contains("TEST-TOPIC-1") && !assignedTopics.contains("TEST-TOPIC-2");
            }
        };

        streamThreads[0] = testStreamThread;
        streams.start();

        TestUtils.waitForCondition(oneTopicAdded,  STREAM_TASKS_NOT_UPDATED);

        CLUSTER.createTopic("TEST-TOPIC-2");

        TestCondition secondTopicAdded  = new TestCondition() {
            @Override
            public boolean conditionMet() {
                List<String> assignedTopics = testStreamThread.assignedTopicPartitions.get(SECOND_UPDATE);
                return assignedTopics != null && assignedTopics.contains("TEST-TOPIC-1") && assignedTopics.contains("TEST-TOPIC-2");
            }
        };

        TestUtils.waitForCondition(secondTopicAdded,  STREAM_TASKS_NOT_UPDATED);

        streams.close();

        List<String> expectedFirstAssignment = Arrays.asList("TEST-TOPIC-1");
        List<String> expectedSecondAssignment = Arrays.asList("TEST-TOPIC-1", "TEST-TOPIC-2");

        assertThat(testStreamThread.assignedTopicPartitions.get(FIRST_UPDATE), equalTo(expectedFirstAssignment));
        assertThat(testStreamThread.assignedTopicPartitions.get(SECOND_UPDATE), equalTo(expectedSecondAssignment));
    }

    @Test
    public void testRegexMatchesTopicsAWhenDeleted() throws Exception {

        final Serde<String> stringSerde = Serdes.String();

        StreamsConfig streamsConfig = new StreamsConfig(streamsConfiguration);

        CLUSTER.createTopic("TEST-TOPIC-A");
        CLUSTER.createTopic("TEST-TOPIC-B");

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> pattern1Stream = builder.stream(Pattern.compile("TEST-TOPIC-[A-Z]"));

        pattern1Stream.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);

        Field streamThreadsField = streams.getClass().getDeclaredField("threads");
        streamThreadsField.setAccessible(true);
        StreamThread[] streamThreads =  (StreamThread[]) streamThreadsField.get(streams);
        StreamThread originalThread = streamThreads[0];

        final TestStreamThread testStreamThread = new TestStreamThread(builder, streamsConfig,
                new DefaultKafkaClientSupplier(),
                originalThread.applicationId, originalThread.clientId, originalThread.processId, new Metrics(), new SystemTime());

        streamThreads[0] = testStreamThread;

        TestCondition bothTopicsAdded  = new TestCondition() {
            @Override
            public boolean conditionMet() {
                List<String> assignedTopics = testStreamThread.assignedTopicPartitions.get(FIRST_UPDATE);
                return assignedTopics != null && assignedTopics.contains("TEST-TOPIC-A") && assignedTopics.contains("TEST-TOPIC-B");
            }
        };
        streams.start();

        TestUtils.waitForCondition(bothTopicsAdded,  STREAM_TASKS_NOT_UPDATED);

        CLUSTER.deleteTopic("TEST-TOPIC-A");


        TestCondition oneTopicRemoved  = new TestCondition() {
            @Override
            public boolean conditionMet() {
                List<String> assignedTopics = testStreamThread.assignedTopicPartitions.get(SECOND_UPDATE);
                return assignedTopics != null && !assignedTopics.contains("TEST-TOPIC-A") && assignedTopics.contains("TEST-TOPIC-B");
            }
        };

        TestUtils.waitForCondition(oneTopicRemoved,  STREAM_TASKS_NOT_UPDATED);

        streams.close();

        List<String> expectedFirstAssignment = Arrays.asList("TEST-TOPIC-A", "TEST-TOPIC-B");
        List<String> expectedSecondAssignment = Arrays.asList("TEST-TOPIC-B");

        assertThat(testStreamThread.assignedTopicPartitions.get(FIRST_UPDATE), equalTo(expectedFirstAssignment));
        assertThat(testStreamThread.assignedTopicPartitions.get(SECOND_UPDATE), equalTo(expectedSecondAssignment));
    }


    @Test
    public void testShouldReadFromRegexAndNamedTopics() throws Exception {

        String topic1TestMessage = "topic-1 test";
        String topic2TestMessage = "topic-2 test";
        String topicATestMessage = "topic-A test";
        String topicCTestMessage = "topic-C test";
        String topicYTestMessage = "topic-Y test";
        String topicZTestMessage = "topic-Z test";


        final Serde<String> stringSerde = Serdes.String();

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> pattern1Stream = builder.stream(Pattern.compile("topic-\\d"));
        KStream<String, String> pattern2Stream = builder.stream(Pattern.compile("topic-[A-D]"));
        KStream<String, String> namedTopicsStream = builder.stream(TOPIC_Y, TOPIC_Z);

        pattern1Stream.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);
        pattern2Stream.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);
        namedTopicsStream.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();

        Properties producerConfig = TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class);

        IntegrationTestUtils.produceValuesSynchronously(TOPIC_1, Arrays.asList(topic1TestMessage), producerConfig);
        IntegrationTestUtils.produceValuesSynchronously(TOPIC_2, Arrays.asList(topic2TestMessage), producerConfig);
        IntegrationTestUtils.produceValuesSynchronously(TOPIC_A, Arrays.asList(topicATestMessage), producerConfig);
        IntegrationTestUtils.produceValuesSynchronously(TOPIC_C, Arrays.asList(topicCTestMessage), producerConfig);
        IntegrationTestUtils.produceValuesSynchronously(TOPIC_Y, Arrays.asList(topicYTestMessage), producerConfig);
        IntegrationTestUtils.produceValuesSynchronously(TOPIC_Z, Arrays.asList(topicZTestMessage), producerConfig);

        Properties consumerConfig = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer.class, StringDeserializer.class);

        List<String> expectedReceivedValues = Arrays.asList(topicATestMessage, topic1TestMessage, topic2TestMessage, topicCTestMessage, topicYTestMessage, topicZTestMessage);
        List<KeyValue<String, String>> receivedKeyValues = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, DEFAULT_OUTPUT_TOPIC, 6);
        List<String> actualValues = new ArrayList<>(6);

        for (KeyValue<String, String> receivedKeyValue : receivedKeyValues) {
            actualValues.add(receivedKeyValue.value);
        }

        streams.close();
        Collections.sort(actualValues);
        Collections.sort(expectedReceivedValues);
        assertThat(actualValues, equalTo(expectedReceivedValues));
    }

    //TODO should be updated to expected = TopologyBuilderException after KAFKA-3708
    @Test(expected = AssertionError.class)
    public void testNoMessagesSentExceptionFromOverlappingPatterns() throws Exception {

        String fooMessage = "fooMessage";
        String fMessage = "fMessage";


        final Serde<String> stringSerde = Serdes.String();

        KStreamBuilder builder = new KStreamBuilder();


        //  overlapping patterns here, no messages should be sent as TopologyBuilderException
        //  will be thrown when the processor topology is built.

        KStream<String, String> pattern1Stream = builder.stream(Pattern.compile("foo.*"));
        KStream<String, String> pattern2Stream = builder.stream(Pattern.compile("f.*"));


        pattern1Stream.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);
        pattern2Stream.to(stringSerde, stringSerde, DEFAULT_OUTPUT_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();

        Properties producerConfig = TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class);

        IntegrationTestUtils.produceValuesSynchronously(FA_TOPIC, Arrays.asList(fMessage), producerConfig);
        IntegrationTestUtils.produceValuesSynchronously(FOO_TOPIC, Arrays.asList(fooMessage), producerConfig);

        Properties consumerConfig = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer.class, StringDeserializer.class);

        try {
            IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, DEFAULT_OUTPUT_TOPIC, 2, 5000);
            fail("Should not get here");
        } finally {
            streams.close();
        }

    }

    private class TestStreamThread extends StreamThread {

        public Map<Integer, List<String>> assignedTopicPartitions = new HashMap<>();
        private int index =  0;

        public TestStreamThread(TopologyBuilder builder, StreamsConfig config, KafkaClientSupplier clientSupplier, String applicationId, String clientId, UUID processId, Metrics metrics, Time time) {
            super(builder, config, clientSupplier, applicationId, clientId, processId, metrics, time, new StreamsMetadataState(builder));
        }

        @Override
        public StreamTask createStreamTask(TaskId id, Collection<TopicPartition> partitions) {
            List<String> assignedTopics = new ArrayList<>();
            for (TopicPartition partition : partitions) {
                assignedTopics.add(partition.topic());
            }
            Collections.sort(assignedTopics);
            assignedTopicPartitions.put(index++, assignedTopics);
            return super.createStreamTask(id, partitions);
        }

    }

}
