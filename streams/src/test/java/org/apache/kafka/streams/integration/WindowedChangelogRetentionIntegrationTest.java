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
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.MockMapper;
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
import java.util.Collections;
import java.util.Properties;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

@SuppressWarnings("deprecation")
@Category({IntegrationTest.class})
public class WindowedChangelogRetentionIntegrationTest {
    private static final int NUM_BROKERS = 1;
    private static final Duration DEFAULT_RETENTION = Duration.ofDays(1);

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    private StreamsBuilder builder;
    private Properties streamsConfiguration;
    private KafkaStreams kafkaStreams;
    private String streamOneInput;
    private String streamTwoInput;
    private String outputTopic;
    private KGroupedStream<String, String> groupedStream;
    private KStream<Integer, String> stream;
    private KStream<Integer, String> stream1;
    private KStream<Integer, String> stream2;

    @Rule
    public TestName testName = new TestName();

    @BeforeClass
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @Before
    public void before() throws InterruptedException {
        builder = new StreamsBuilder();
        createTopics();
        streamsConfiguration = new Properties();
        final String safeTestName = safeUniqueTestName(getClass(), testName);
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + safeTestName);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());

        final KeyValueMapper<Integer, String, String> mapper = MockMapper.selectValueMapper();
        stream = builder.stream(streamOneInput, Consumed.with(Serdes.Integer(), Serdes.String()));
        groupedStream = stream.groupBy(mapper, Grouped.with(Serdes.String(), Serdes.String()));
    }

    @After
    public void whenShuttingDown() throws Exception {
        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    @Test
    public void timeWindowedChangelogShouldHaveRetentionOfWindowSize() throws Exception {
        final Duration windowSize = Duration.ofDays(2);
        final String storeName = "windowed-store";
        final Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class, windowSize.toMillis());
        groupedStream.windowedBy(TimeWindows.of(windowSize))
                .count(Materialized.as(storeName))
                .toStream()
                .to(outputTopic, Produced.with(windowedSerde, Serdes.Long()));

        startStreams();

        verifyChangelogRetentionOfWindowedStore(storeName, windowSize);
    }

    @Test
    public void timeWindowedChangelogShouldHaveDefaultRetentionWithoutGrace() throws Exception {
        final Duration windowSize = Duration.ofMillis(500);
        final String storeName = "windowed-store";
        final Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class, windowSize.toMillis());
        groupedStream.windowedBy(TimeWindows.of(Duration.ofMillis(500)))
                .count(Materialized.as(storeName))
                .toStream()
                .to(outputTopic, Produced.with(windowedSerde, Serdes.Long()));

        startStreams();

        verifyChangelogRetentionOfWindowedStore(storeName, DEFAULT_RETENTION);
    }

    @Test
    public void timeWindowedChangelogShouldHaveDefaultRetentionWithGrace() throws Exception {
        final Duration windowSize = Duration.ofMillis(500);
        final Duration grace = Duration.ofMillis(1000);
        final String storeName = "windowed-store";
        final Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class, windowSize.toMillis());
        groupedStream.windowedBy(TimeWindows.of(windowSize).grace(grace))
                .count(Materialized.as(storeName))
                .toStream()
                .to(outputTopic, Produced.with(windowedSerde, Serdes.Long()));

        startStreams();

        verifyChangelogRetentionOfWindowedStore(storeName, DEFAULT_RETENTION);
    }

    @Test
    public void timeWindowedChangelogShouldHaveRetentionOfWindowSizeAndGrace() throws Exception {
        final Duration windowSize = Duration.ofDays(1);
        final Duration grace = Duration.ofHours(12);
        final String storeName = "windowed-store";
        final Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class, windowSize.toMillis());
        groupedStream.windowedBy(TimeWindows.of(windowSize).grace(grace))
                .count(Materialized.as(storeName))
                .toStream()
                .to(outputTopic, Produced.with(windowedSerde, Serdes.Long()));

        startStreams();

        verifyChangelogRetentionOfWindowedStore(storeName, windowSize.plus(grace));
    }

    @Test
    public void timeWindowedChangelogShouldHaveUserSpecifiedRetention() throws Exception {
        final Duration windowSize = Duration.ofDays(1);
        final Duration grace = Duration.ofHours(12);
        final String storeName = "windowed-store";
        final Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class, windowSize.toMillis());
        groupedStream.windowedBy(TimeWindows.of(windowSize).grace(grace))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(storeName).withRetention(windowSize.multipliedBy(3)))
                .toStream()
                .to(outputTopic, Produced.with(windowedSerde, Serdes.Long()));

        startStreams();

        verifyChangelogRetentionOfWindowedStore(storeName, windowSize.multipliedBy(3));
    }

    @Test
    public void sessionWindowedChangelogShouldHaveRetentionOfGap() throws Exception {
        final Duration gap = Duration.ofDays(2);
        final String storeName = "windowed-store";
        final Serde<Windowed<String>> windowedSerde = WindowedSerdes.sessionWindowedSerdeFrom(String.class);
        groupedStream.windowedBy(SessionWindows.with(gap))
                .count(Materialized.as(storeName))
                .toStream()
                .to(outputTopic, Produced.with(windowedSerde, Serdes.Long()));

        startStreams();

        verifyChangelogRetentionOfWindowedStore(storeName, gap);
    }

    @Test
    public void sessionWindowedChangelogShouldHaveDefaultRetentionWithoutGrace() throws Exception {
        final Duration gap = Duration.ofMillis(500);
        final String storeName = "windowed-store";
        final Serde<Windowed<String>> windowedSerde = WindowedSerdes.sessionWindowedSerdeFrom(String.class);
        groupedStream.windowedBy(SessionWindows.with(gap))
                .count(Materialized.as(storeName))
                .toStream()
                .to(outputTopic, Produced.with(windowedSerde, Serdes.Long()));

        startStreams();

        verifyChangelogRetentionOfWindowedStore(storeName, DEFAULT_RETENTION);
    }

    @Test
    public void sessionWindowedChangelogShouldHaveDefaultRetentionWithGrace() throws Exception {
        final Duration gap = Duration.ofMillis(500);
        final Duration grace = Duration.ofMillis(1000);
        final String storeName = "windowed-store";
        final Serde<Windowed<String>> windowedSerde = WindowedSerdes.sessionWindowedSerdeFrom(String.class);
        groupedStream.windowedBy(SessionWindows.with(gap).grace(grace))
                .count(Materialized.as(storeName))
                .toStream()
                .to(outputTopic, Produced.with(windowedSerde, Serdes.Long()));

        startStreams();

        verifyChangelogRetentionOfWindowedStore(storeName, DEFAULT_RETENTION);
    }

    @Test
    public void sessionWindowedChangelogShouldHaveUserSpecifiedRetention() throws Exception {
        final Duration gap = Duration.ofDays(2);
        final Duration grace = Duration.ofHours(12);
        final String storeName = "windowed-store";
        final Serde<Windowed<String>> windowedSerde = WindowedSerdes.sessionWindowedSerdeFrom(String.class);
        groupedStream.windowedBy(SessionWindows.with(gap).grace(grace))
                .count(Materialized.<String, Long, SessionStore<Bytes, byte[]>>as(storeName).withRetention(gap.multipliedBy(3)))
                .toStream()
                .to(outputTopic, Produced.with(windowedSerde, Serdes.Long()));

        startStreams();

        verifyChangelogRetentionOfWindowedStore(storeName, gap.multipliedBy(3));
    }

    @Test
    public void joinWindowedChangelogShouldHaveRetentionOfDoubleWindowSize() throws Exception {
        final Duration windowSize = Duration.ofDays(2);
        final String joinName = "testjoin";
        final String thisStoreName = joinName + "-this-join-store";
        final String otherStoreName = joinName + "-other-join-store";
        stream1 = builder.stream(streamOneInput, Consumed.with(Serdes.Integer(), Serdes.String()));
        stream2 = builder.stream(streamTwoInput, Consumed.with(Serdes.Integer(), Serdes.String()));
        stream1.join(stream2, (left, right) -> left, JoinWindows.of(windowSize), StreamJoined.as(joinName))
                .to(outputTopic, Produced.with(Serdes.Integer(), Serdes.String()));

        startStreams();

        verifyChangelogRetentionOfWindowedStore(thisStoreName, windowSize.multipliedBy(2));
        verifyChangelogRetentionOfWindowedStore(otherStoreName, windowSize.multipliedBy(2));
    }

    @Test
    public void joinWindowedChangelogShouldHaveDefaultRetentionWithoutGrace() throws Exception {
        final Duration windowSize = Duration.ofHours(6);
        final String joinName = "testjoin";
        final String thisStoreName = joinName + "-this-join-store";
        final String otherStoreName = joinName + "-other-join-store";
        stream1 = builder.stream(streamOneInput, Consumed.with(Serdes.Integer(), Serdes.String()));
        stream2 = builder.stream(streamTwoInput, Consumed.with(Serdes.Integer(), Serdes.String()));
        stream1.join(stream2, (left, right) -> left, JoinWindows.of(windowSize), StreamJoined.as(joinName))
                .to(outputTopic, Produced.with(Serdes.Integer(), Serdes.String()));

        startStreams();

        verifyChangelogRetentionOfWindowedStore(thisStoreName, DEFAULT_RETENTION);
        verifyChangelogRetentionOfWindowedStore(otherStoreName, DEFAULT_RETENTION);
    }

    @Test
    public void joinWindowedChangelogShouldHaveRetentionOfDoubleWindowSizeAndGraceLessThanDefault() throws Exception {
        final Duration windowSize = Duration.ofHours(6);
        final Duration grace = Duration.ofHours(3);
        final String joinName = "testjoin";
        final String thisStoreName = joinName + "-this-join-store";
        final String otherStoreName = joinName + "-other-join-store";
        stream1 = builder.stream(streamOneInput, Consumed.with(Serdes.Integer(), Serdes.String()));
        stream2 = builder.stream(streamTwoInput, Consumed.with(Serdes.Integer(), Serdes.String()));
        stream1.join(stream2, (left, right) -> left, JoinWindows.of(windowSize).grace(grace), StreamJoined.as(joinName))
                .to(outputTopic, Produced.with(Serdes.Integer(), Serdes.String()));

        startStreams();

        verifyChangelogRetentionOfWindowedStore(thisStoreName, windowSize.multipliedBy(2).plus(grace));
        verifyChangelogRetentionOfWindowedStore(otherStoreName, windowSize.multipliedBy(2).plus(grace));
    }

    @Test
    public void joinWindowedChangelogShouldHaveRetentionOfDoubleWindowSizeAndGraceGreaterThanDefault() throws Exception {
        final Duration windowSize = Duration.ofDays(2);
        final Duration grace = Duration.ofHours(12);
        final String joinName = "testjoin";
        final String thisStoreName = joinName + "-this-join-store";
        final String otherStoreName = joinName + "-other-join-store";
        stream1 = builder.stream(streamOneInput, Consumed.with(Serdes.Integer(), Serdes.String()));
        stream2 = builder.stream(streamTwoInput, Consumed.with(Serdes.Integer(), Serdes.String()));
        stream1.join(stream2, (left, right) -> left, JoinWindows.of(windowSize).grace(grace), StreamJoined.as(joinName))
                .to(outputTopic, Produced.with(Serdes.Integer(), Serdes.String()));

        startStreams();

        verifyChangelogRetentionOfWindowedStore(thisStoreName, windowSize.multipliedBy(2).plus(grace));
        verifyChangelogRetentionOfWindowedStore(otherStoreName, windowSize.multipliedBy(2).plus(grace));
    }

    private void startStreams() throws Exception {
        final Topology topology = builder.build();
        System.out.println(topology.describe().toString());
        kafkaStreams = new KafkaStreams(topology, streamsConfiguration);
        kafkaStreams.start();
        IntegrationTestUtils.waitForApplicationState(Collections.singletonList(kafkaStreams), State.RUNNING, Duration.ofSeconds(30));
    }

    private void createTopics() throws InterruptedException {
        final String safeTestName = safeUniqueTestName(getClass(), testName);
        streamOneInput = "stream-one-" + safeTestName;
        streamTwoInput = "stream-two-" + safeTestName;
        outputTopic = "output-" + safeTestName;
        CLUSTER.createTopic(streamOneInput, 3, 1);
        CLUSTER.createTopic(streamTwoInput, 3, 1);
        CLUSTER.createTopics(outputTopic);
    }

    private void verifyChangelogRetentionOfWindowedStore(final String storeName, final Duration retention) {
        final Duration windowStoreChangelogAdditionalRetention = Duration.ofDays(1);
        final Properties logConfig = CLUSTER.getLogConfig(
                streamsConfiguration.getProperty(StreamsConfig.APPLICATION_ID_CONFIG) + "-" + storeName + "-changelog"
        );
        assertThat(
                Long.parseLong(logConfig.getProperty(TopicConfig.RETENTION_MS_CONFIG)),
                is(retention.toMillis() + windowStoreChangelogAdditionalRetention.toMillis())
        );
    }
}
