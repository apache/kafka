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
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
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
    private static final String STREAM_ONE_INPUT = "stream-one";
    private static final String STREAM_TWO_INPUT = "stream-two";
    private static final String OUTPUT_TOPIC = "output";

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    private StreamsBuilder builder;
    private Properties streamsConfiguration;
    private KafkaStreams kafkaStreams;
    private KGroupedStream<String, String> groupedStream;

    @Rule
    public TestName testName = new TestName();

    @BeforeClass
    public static void startCluster() throws IOException, InterruptedException {
        CLUSTER.start();
        createTopics();
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @Before
    public void before() {
        builder = new StreamsBuilder();
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
        final KStream<Integer, String> stream = builder.stream(STREAM_ONE_INPUT, Consumed.with(Serdes.Integer(), Serdes.String()));
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
    public void timeWindowedChangelogShouldHaveRetentionOfWindowSizeIfWindowSizeLargerThanDefaultRetention() throws Exception {
        final Duration windowSize = DEFAULT_RETENTION.plus(Duration.ofHours(1));
        final Duration expectedRetention = windowSize;
        runAndVerifyTimeWindows(TimeWindows.of(windowSize), null, expectedRetention);
    }

    @Test
    public void timeWindowedChangelogShouldHaveDefaultRetentionIfWindowSizeLessThanDefaultRetention() throws Exception {
        final Duration windowSize = DEFAULT_RETENTION.minus(Duration.ofHours(1));
        final Duration expectedRetention = DEFAULT_RETENTION;
        runAndVerifyTimeWindows(TimeWindows.of(windowSize), null, expectedRetention);
    }

    @Test
    public void timeWindowedChangelogShouldHaveRetentionOfWindowSizePlusGraceIfWindowSizePlusGraceLargerThanDefaultRetention() throws Exception {
        final Duration windowSize = DEFAULT_RETENTION.plus(Duration.ofHours(1));
        final Duration grace = Duration.ofHours(12);
        final Duration expectedRetention = windowSize.plus(grace);
        runAndVerifyTimeWindows(TimeWindows.of(windowSize).grace(grace), null, expectedRetention);
    }

    @Test
    public void timeWindowedChangelogShouldHaveRetentionOfWindowSizePlusGraceIfWindowSizePlusGraceLessThanDefaultRetention() throws Exception {
        final Duration grace = Duration.ofMillis(1000);
        final Duration windowSize = DEFAULT_RETENTION.minus(grace).minus(Duration.ofMillis(500));
        final Duration expectedRetention = windowSize.plus(grace);
        runAndVerifyTimeWindows(TimeWindows.of(windowSize).grace(grace), null, expectedRetention);
    }

    @Test
    public void timeWindowedChangelogShouldHaveUserSpecifiedRetentionIfUserSpecifiedRetentionLessThanDefaultRetention() throws Exception {
        final Duration grace = Duration.ofHours(6);
        final Duration windowSize = DEFAULT_RETENTION.minus(grace).minus(Duration.ofHours(1));
        final Duration userSpecifiedRetention = windowSize.plus(grace).plus(Duration.ofHours(1));
        final Duration expectedRetention = userSpecifiedRetention;
        runAndVerifyTimeWindows(TimeWindows.of(windowSize).grace(grace), userSpecifiedRetention, expectedRetention);
    }

    private void runAndVerifyTimeWindows(final Windows<TimeWindow> window,
                                         final Duration userSpecifiedRetention,
                                         final Duration expectedRetention) throws Exception {
        final String storeName = "windowed-store";
        final Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class, window.size());
        groupedStream.windowedBy(window)
            .count(userSpecifiedRetention != null
                ? Materialized.<String, Long, WindowStore<Bytes, byte[]>>as(storeName).withRetention(userSpecifiedRetention)
                : Materialized.as(storeName))
            .toStream()
            .to(OUTPUT_TOPIC, Produced.with(windowedSerde, Serdes.Long()));

        startStreams();

        verifyChangelogRetentionOfWindowedStore(storeName, expectedRetention);
    }

    @Test
    public void sessionWindowedChangelogShouldHaveRetentionOfGapIfGapLargerThanDefaultRetention() throws Exception {
        final Duration gap = DEFAULT_RETENTION.plus(Duration.ofHours(1));
        final Duration expectedRetention = gap;
        runAndVerifySessionWindows(SessionWindows.with(gap), null, expectedRetention);
    }

    @Test
    public void sessionWindowedChangelogShouldHaveDefaultRetentionIfGapLessThanDefaultRetention() throws Exception {
        final Duration gap = DEFAULT_RETENTION.minus(Duration.ofHours(1));
        final Duration expectedRetention = DEFAULT_RETENTION;
        runAndVerifySessionWindows(SessionWindows.with(gap), null, expectedRetention);
    }

    @Test
    public void sessionWindowedChangelogShouldHaveRetentionOfGapPlusGraceIfGapPlusGraceLessThanDefaultRetention() throws Exception {
        final Duration grace = Duration.ofHours(1);
        final Duration gap = DEFAULT_RETENTION.minus(grace).minus(Duration.ofHours(1));
        final Duration expectedRetention = gap.plus(grace);
        runAndVerifySessionWindows(SessionWindows.with(gap).grace(grace), null, expectedRetention);
    }

    @Test
    public void sessionWindowedChangelogShouldHaveRetentionOfGapPlusGraceIfGapPlusGraceGreaterThanDefaultRetention() throws Exception {
        final Duration gap = DEFAULT_RETENTION;
        final Duration grace = Duration.ofHours(1);
        final Duration expectedRetention = gap.plus(grace);
        runAndVerifySessionWindows(SessionWindows.with(gap).grace(grace), null, expectedRetention);
    }

    @Test
    public void sessionWindowedChangelogShouldHaveUserSpecifiedRetentionIfUserSpecifiedRetentionLessThanDefaultRetention() throws Exception {
        final Duration grace = Duration.ofHours(6);
        final Duration gap = DEFAULT_RETENTION.minus(grace).minus(Duration.ofHours(1));
        final Duration userSpecifiedRetention = gap.plus(grace).plus(Duration.ofHours(1));
        final Duration expectedRetention = userSpecifiedRetention;
        runAndVerifySessionWindows(SessionWindows.with(gap).grace(grace), userSpecifiedRetention, expectedRetention);
    }

    private void runAndVerifySessionWindows(final SessionWindows window,
                                            final Duration userSpecifiedRetention,
                                            final Duration expectedRetention) throws Exception {
        final String storeName = "windowed-store";
        final Serde<Windowed<String>> windowedSerde = WindowedSerdes.sessionWindowedSerdeFrom(String.class);
        groupedStream.windowedBy(window)
            .count(userSpecifiedRetention != null
                ? Materialized.<String, Long, SessionStore<Bytes, byte[]>>as(storeName).withRetention(userSpecifiedRetention)
                : Materialized.as(storeName))
            .toStream()
            .to(OUTPUT_TOPIC, Produced.with(windowedSerde, Serdes.Long()));

        startStreams();

        verifyChangelogRetentionOfWindowedStore(storeName, expectedRetention);
    }

    @Test
    public void joinWindowedChangelogShouldHaveRetentionOfDoubleWindowSizeIfWindowSizeLargerThanDefaultRetention() throws Exception {
        final Duration windowSize = DEFAULT_RETENTION.plus(Duration.ofHours(1));
        final Duration expectedRetention = windowSize.multipliedBy(2);
        runAndVerifyJoinWindows(JoinWindows.of(windowSize), expectedRetention);
    }

    @Test
    public void joinWindowedChangelogShouldHaveDefaultRetentionIfDoubleWindowSizeLessThanDefaultRetention() throws Exception {
        final Duration windowSize = DEFAULT_RETENTION.dividedBy(2).minus(Duration.ofHours(1));
        final Duration expectedRetention = DEFAULT_RETENTION;
        runAndVerifyJoinWindows(JoinWindows.of(windowSize), expectedRetention);
    }

    @Test
    public void joinWindowedChangelogShouldHaveRetentionOfDoubleWindowSizePlusGraceIfDoubleWindowSizePlusGraceLessThanDefaultRetention() throws Exception {
        final Duration grace = Duration.ofHours(3);
        final Duration windowSize = DEFAULT_RETENTION.dividedBy(2).minus(grace).minus(Duration.ofHours(1));
        final Duration expectedRetention = windowSize.multipliedBy(2).plus(grace);
        runAndVerifyJoinWindows(JoinWindows.of(windowSize).grace(grace), expectedRetention);
    }

    @Test
    public void joinWindowedChangelogShouldHaveRetentionOfDoubleWindowSizePlusGraceIfDoubleWindowSizePlusGraceGreaterThanDefaultRetention() throws Exception {
        final Duration windowSize = DEFAULT_RETENTION.plus(Duration.ofHours(1));
        final Duration grace = Duration.ofHours(3);
        final Duration expectedRetention = windowSize.multipliedBy(2).plus(grace);
        runAndVerifyJoinWindows(JoinWindows.of(windowSize).grace(grace), expectedRetention);
    }

    @Test
    public void joinWindowedChangelogShouldHaveRetentionOfBeforePlusAfterPlusGraceIfBeforePlusAfterPlusGraceGreaterThanDefaultRetention() throws Exception {
        final Duration before = DEFAULT_RETENTION.plus(Duration.ofHours(1));
        final Duration after = DEFAULT_RETENTION.minus(Duration.ofHours(1));
        final Duration grace = Duration.ofHours(3);
        final Duration expectedRetention = before.plus(after).plus(grace);
        runAndVerifyJoinWindows(JoinWindows.of(before).after(after).grace(grace), expectedRetention);
    }

    @Test
    public void joinWindowedChangelogShouldHaveRetentionOfBeforePlusAfterPlusGraceIfBeforePlusAfterePlusGraceLessThanDefaultRetention() throws Exception {
        final Duration grace = Duration.ofHours(3);
        final Duration before = DEFAULT_RETENTION.dividedBy(2).minus(grace).minus(Duration.ofHours(1));
        final Duration after = DEFAULT_RETENTION.dividedBy(2).minus(grace).minus(Duration.ofHours(4));
        final Duration expectedRetention = before.plus(after).plus(grace);
        runAndVerifyJoinWindows(JoinWindows.of(before).after(after).grace(grace), expectedRetention);
    }

    private void runAndVerifyJoinWindows(final JoinWindows window,
                                         final Duration expectedRetention) throws Exception {
        final String joinName = "testjoin";
        final String thisStoreName = joinName + "-this-join-store";
        final String otherStoreName = joinName + "-other-join-store";
        final KStream<Integer, String> stream1 = builder.stream(STREAM_ONE_INPUT, Consumed.with(Serdes.Integer(), Serdes.String()));
        final KStream<Integer, String> stream2 = builder.stream(STREAM_TWO_INPUT, Consumed.with(Serdes.Integer(), Serdes.String()));
        stream1.join(stream2, (left, right) -> left, window, StreamJoined.as(joinName))
            .to(OUTPUT_TOPIC, Produced.with(Serdes.Integer(), Serdes.String()));

        startStreams();

        verifyChangelogRetentionOfWindowedStore(thisStoreName, expectedRetention);
        verifyChangelogRetentionOfWindowedStore(otherStoreName, expectedRetention);
    }

    private void startStreams() throws Exception {
        final Topology topology = builder.build();
        System.out.println(topology.describe().toString());
        kafkaStreams = new KafkaStreams(topology, streamsConfiguration);
        kafkaStreams.start();
        IntegrationTestUtils.waitForApplicationState(Collections.singletonList(kafkaStreams), State.RUNNING, Duration.ofSeconds(30));
    }

    private static void createTopics() throws InterruptedException {
        CLUSTER.createTopic(STREAM_ONE_INPUT, 3, 1);
        CLUSTER.createTopic(STREAM_TWO_INPUT, 3, 1);
        CLUSTER.createTopics(OUTPUT_TOPIC);
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
