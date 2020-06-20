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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

@Category({IntegrationTest.class})
public class FineGrainedAutoResetIntegrationTest {

    private static final int NUM_BROKERS = 1;
    private static final String DEFAULT_OUTPUT_TOPIC = "outputTopic";
    private static final String OUTPUT_TOPIC_0 = "outputTopic_0";
    private static final String OUTPUT_TOPIC_1 = "outputTopic_1";
    private static final String OUTPUT_TOPIC_2 = "outputTopic_2";

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final MockTime mockTime = CLUSTER.time;

    private static final String TOPIC_1_0 = "topic-1_0";
    private static final String TOPIC_2_0 = "topic-2_0";
    private static final String TOPIC_A_0 = "topic-A_0";
    private static final String TOPIC_C_0 = "topic-C_0";
    private static final String TOPIC_Y_0 = "topic-Y_0";
    private static final String TOPIC_Z_0 = "topic-Z_0";
    private static final String TOPIC_1_1 = "topic-1_1";
    private static final String TOPIC_2_1 = "topic-2_1";
    private static final String TOPIC_A_1 = "topic-A_1";
    private static final String TOPIC_C_1 = "topic-C_1";
    private static final String TOPIC_Y_1 = "topic-Y_1";
    private static final String TOPIC_Z_1 = "topic-Z_1";
    private static final String TOPIC_1_2 = "topic-1_2";
    private static final String TOPIC_2_2 = "topic-2_2";
    private static final String TOPIC_A_2 = "topic-A_2";
    private static final String TOPIC_C_2 = "topic-C_2";
    private static final String TOPIC_Y_2 = "topic-Y_2";
    private static final String TOPIC_Z_2 = "topic-Z_2";
    private static final String NOOP = "noop";
    private final Serde<String> stringSerde = Serdes.String();

    private static final String STRING_SERDE_CLASSNAME = Serdes.String().getClass().getName();
    private Properties streamsConfiguration;

    private final String topic1TestMessage = "topic-1 test";
    private final String topic2TestMessage = "topic-2 test";
    private final String topicATestMessage = "topic-A test";
    private final String topicCTestMessage = "topic-C test";
    private final String topicYTestMessage = "topic-Y test";
    private final String topicZTestMessage = "topic-Z test";


    @BeforeClass
    public static void startKafkaCluster() throws InterruptedException {
        CLUSTER.createTopics(
            TOPIC_1_0,
            TOPIC_2_0,
            TOPIC_A_0,
            TOPIC_C_0,
            TOPIC_Y_0,
            TOPIC_Z_0,
            TOPIC_1_1,
            TOPIC_2_1,
            TOPIC_A_1,
            TOPIC_C_1,
            TOPIC_Y_1,
            TOPIC_Z_1,
            TOPIC_1_2,
            TOPIC_2_2,
            TOPIC_A_2,
            TOPIC_C_2,
            TOPIC_Y_2,
            TOPIC_Z_2,
            NOOP,
            DEFAULT_OUTPUT_TOPIC,
            OUTPUT_TOPIC_0,
            OUTPUT_TOPIC_1,
            OUTPUT_TOPIC_2);
    }

    @Before
    public void setUp() throws IOException {

        final Properties props = new Properties();
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        streamsConfiguration = StreamsTestUtils.getStreamsConfig(
                "testAutoOffsetId",
                CLUSTER.bootstrapServers(),
                STRING_SERDE_CLASSNAME,
                STRING_SERDE_CLASSNAME,
                props);

        // Remove any state from previous test runs
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    @Test
    public void shouldOnlyReadRecordsWhereEarliestSpecifiedWithNoCommittedOffsetsWithGlobalAutoOffsetResetLatest() throws Exception {
        streamsConfiguration.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "latest");

        final List<String> expectedReceivedValues = Arrays.asList(topic1TestMessage, topic2TestMessage);
        shouldOnlyReadForEarliest("_0", TOPIC_1_0, TOPIC_2_0, TOPIC_A_0, TOPIC_C_0, TOPIC_Y_0, TOPIC_Z_0, OUTPUT_TOPIC_0, expectedReceivedValues);
    }

    @Test
    public void shouldOnlyReadRecordsWhereEarliestSpecifiedWithNoCommittedOffsetsWithDefaultGlobalAutoOffsetResetEarliest() throws Exception {
        final List<String> expectedReceivedValues = Arrays.asList(topic1TestMessage, topic2TestMessage, topicYTestMessage, topicZTestMessage);
        shouldOnlyReadForEarliest("_1", TOPIC_1_1, TOPIC_2_1, TOPIC_A_1, TOPIC_C_1, TOPIC_Y_1, TOPIC_Z_1, OUTPUT_TOPIC_1, expectedReceivedValues);
    }

    @Test
    public void shouldOnlyReadRecordsWhereEarliestSpecifiedWithInvalidCommittedOffsets() throws Exception {
        commitInvalidOffsets();

        final List<String> expectedReceivedValues = Arrays.asList(topic1TestMessage, topic2TestMessage, topicYTestMessage, topicZTestMessage);
        shouldOnlyReadForEarliest("_2", TOPIC_1_2, TOPIC_2_2, TOPIC_A_2, TOPIC_C_2, TOPIC_Y_2, TOPIC_Z_2, OUTPUT_TOPIC_2, expectedReceivedValues);
    }

    private void shouldOnlyReadForEarliest(
        final String topicSuffix,
        final String topic1,
        final String topic2,
        final String topicA,
        final String topicC,
        final String topicY,
        final String topicZ,
        final String outputTopic,
        final List<String> expectedReceivedValues) throws Exception {

        final StreamsBuilder builder = new StreamsBuilder();


        final KStream<String, String> pattern1Stream = builder.stream(Pattern.compile("topic-\\d" + topicSuffix), Consumed.with(Topology.AutoOffsetReset.EARLIEST));
        final KStream<String, String> pattern2Stream = builder.stream(Pattern.compile("topic-[A-D]" + topicSuffix), Consumed.with(Topology.AutoOffsetReset.LATEST));
        final KStream<String, String> namedTopicsStream = builder.stream(Arrays.asList(topicY, topicZ));

        pattern1Stream.to(outputTopic, Produced.with(stringSerde, stringSerde));
        pattern2Stream.to(outputTopic, Produced.with(stringSerde, stringSerde));
        namedTopicsStream.to(outputTopic, Produced.with(stringSerde, stringSerde));

        final Properties producerConfig = TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class);

        IntegrationTestUtils.produceValuesSynchronously(topic1, Collections.singletonList(topic1TestMessage), producerConfig, mockTime);
        IntegrationTestUtils.produceValuesSynchronously(topic2, Collections.singletonList(topic2TestMessage), producerConfig, mockTime);
        IntegrationTestUtils.produceValuesSynchronously(topicA, Collections.singletonList(topicATestMessage), producerConfig, mockTime);
        IntegrationTestUtils.produceValuesSynchronously(topicC, Collections.singletonList(topicCTestMessage), producerConfig, mockTime);
        IntegrationTestUtils.produceValuesSynchronously(topicY, Collections.singletonList(topicYTestMessage), producerConfig, mockTime);
        IntegrationTestUtils.produceValuesSynchronously(topicZ, Collections.singletonList(topicZTestMessage), producerConfig, mockTime);

        final Properties consumerConfig = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer.class, StringDeserializer.class);

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();

        final List<KeyValue<String, String>> receivedKeyValues = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, outputTopic, expectedReceivedValues.size());
        final List<String> actualValues = new ArrayList<>(expectedReceivedValues.size());

        for (final KeyValue<String, String> receivedKeyValue : receivedKeyValues) {
            actualValues.add(receivedKeyValue.value);
        }

        streams.close();
        Collections.sort(actualValues);
        Collections.sort(expectedReceivedValues);
        assertThat(actualValues, equalTo(expectedReceivedValues));
    }

    private void commitInvalidOffsets() {
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(TestUtils.consumerConfig(
            CLUSTER.bootstrapServers(),
            "commit_invalid_offset_app", // Having a separate application id to avoid waiting for last test poll interval timeout.
            StringDeserializer.class,
            StringDeserializer.class));

        final Map<TopicPartition, OffsetAndMetadata> invalidOffsets = new HashMap<>();
        invalidOffsets.put(new TopicPartition(TOPIC_1_2, 0), new OffsetAndMetadata(5, null));
        invalidOffsets.put(new TopicPartition(TOPIC_2_2, 0), new OffsetAndMetadata(5, null));
        invalidOffsets.put(new TopicPartition(TOPIC_A_2, 0), new OffsetAndMetadata(5, null));
        invalidOffsets.put(new TopicPartition(TOPIC_C_2, 0), new OffsetAndMetadata(5, null));
        invalidOffsets.put(new TopicPartition(TOPIC_Y_2, 0), new OffsetAndMetadata(5, null));
        invalidOffsets.put(new TopicPartition(TOPIC_Z_2, 0), new OffsetAndMetadata(5, null));

        consumer.commitSync(invalidOffsets);

        consumer.close();
    }

    @Test
    public void shouldThrowExceptionOverlappingPattern() {
        final StreamsBuilder builder = new StreamsBuilder();
        //NOTE this would realistically get caught when building topology, the test is for completeness
        builder.stream(Pattern.compile("topic-[A-D]_1"), Consumed.with(Topology.AutoOffsetReset.EARLIEST));

        try {
            builder.stream(Pattern.compile("topic-[A-D]_1"), Consumed.with(Topology.AutoOffsetReset.LATEST));
            builder.build();
            fail("Should have thrown TopologyException");
        } catch (final TopologyException expected) {
            // do nothing
        }
    }

    @Test
    public void shouldThrowExceptionOverlappingTopic() {
        final StreamsBuilder builder = new StreamsBuilder();
        //NOTE this would realistically get caught when building topology, the test is for completeness
        builder.stream(Pattern.compile("topic-[A-D]_1"), Consumed.with(Topology.AutoOffsetReset.EARLIEST));
        try {
            builder.stream(Arrays.asList(TOPIC_A_1, TOPIC_Z_1), Consumed.with(Topology.AutoOffsetReset.LATEST));
            builder.build();
            fail("Should have thrown TopologyException");
        } catch (final TopologyException expected) {
            // do nothing
        }
    }

    @Test
    public void shouldThrowStreamsExceptionNoResetSpecified() throws InterruptedException {
        final Properties props = new Properties();
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

        final Properties localConfig = StreamsTestUtils.getStreamsConfig(
                "testAutoOffsetWithNone",
                CLUSTER.bootstrapServers(),
                STRING_SERDE_CLASSNAME,
                STRING_SERDE_CLASSNAME,
                props);

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> exceptionStream = builder.stream(NOOP);

        exceptionStream.to(DEFAULT_OUTPUT_TOPIC, Produced.with(stringSerde, stringSerde));

        final KafkaStreams streams = new KafkaStreams(builder.build(), localConfig);

        final TestingUncaughtExceptionHandler uncaughtExceptionHandler = new TestingUncaughtExceptionHandler();

        streams.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        streams.start();
        TestUtils.waitForCondition(() -> uncaughtExceptionHandler.correctExceptionThrown,
                "The expected NoOffsetForPartitionException was never thrown");
        streams.close();
    }


    private static final class TestingUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
        boolean correctExceptionThrown = false;
        @Override
        public void uncaughtException(final Thread t, final Throwable e) {
            assertThat(e.getClass().getSimpleName(), is("StreamsException"));
            assertThat(e.getCause().getClass().getSimpleName(), is("NoOffsetForPartitionException"));
            correctExceptionThrown = true;
        }
    }

}
