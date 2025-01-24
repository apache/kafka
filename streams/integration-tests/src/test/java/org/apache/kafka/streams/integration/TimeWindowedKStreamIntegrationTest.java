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
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.EmitStrategy.StrategyType;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SessionWindowedDeserializer;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.UnlimitedWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.test.MockAggregator;
import org.apache.kafka.test.MockInitializer;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static java.time.Duration.ofMillis;
import static java.time.Instant.ofEpochMilli;
import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings({"unchecked"})
@Tag("integration")
@Timeout(600)
public class TimeWindowedKStreamIntegrationTest {

    private static final int NUM_BROKERS = 1;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS,
        mkProperties(
            mkMap(mkEntry("log.retention.hours", "-1"), mkEntry("log.retention.bytes", "-1")) // Don't expire records since we manipulate timestamp
        )
    );

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }


    private StreamsBuilder builder;
    private Properties streamsConfiguration;
    private KafkaStreams kafkaStreams;
    private String streamOneInput;
    private String streamTwoInput;
    private String outputTopic;
    private String safeTestName;

    @BeforeEach
    public void before(final TestInfo testInfo) throws InterruptedException {
        builder = new StreamsBuilder();
        safeTestName = safeUniqueTestName(testInfo);
        createTopics();
        streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + safeTestName);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(InternalConfig.EMIT_INTERVAL_MS_KSTREAMS_WINDOWED_AGGREGATION, 0); // Always process
        streamsConfiguration.put(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG, Long.MAX_VALUE); // Don't expire changelog
    }

    @AfterEach
    public void whenShuttingDown() throws IOException {
        if (kafkaStreams != null) {
            kafkaStreams.close(Duration.ofSeconds(60));
            kafkaStreams.cleanUp();
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    @ParameterizedTest
    @CsvSource({"ON_WINDOW_UPDATE, true", "ON_WINDOW_UPDATE, false", "ON_WINDOW_CLOSE, true", "ON_WINDOW_CLOSE, false"})
    public void shouldAggregateWindowedWithNoGrace(final StrategyType type, final boolean withCache) throws Exception {
        produceMessages(
            streamOneInput,
            new KeyValueTimestamp<>("A", "1", 0),
            new KeyValueTimestamp<>("A", "2", 5),
            new KeyValueTimestamp<>("A", "3", 10), // close [0, 10)
            new KeyValueTimestamp<>("B", "4", 6),  // late and skip for [0, 10)
            new KeyValueTimestamp<>("B", "5", 11),
            new KeyValueTimestamp<>("B", "6", 15), // close [5, 15)
            new KeyValueTimestamp<>("C", "7", 25)  // close [10, 20), [15, 25)
        );

        final Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class, 10L);
        builder.stream(streamOneInput, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(ofMillis(10L)).advanceBy(ofMillis(5L)))
            .emitStrategy(StrategyType.forType(type))
            .aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                getMaterialized(withCache)
            )
            .toStream()
            .to(outputTopic, Produced.with(windowedSerde, new StringSerde()));

        startStreams();

        // on window close
        // observedStreamTime : 10, retentionPeriod: 10, actualFrom: 0, timeTo: 0, timeFrom: 0
        // observedStreamTime : 15, retentionPeriod: 10, actualFrom: 5, timeTo: 5, timeFrom: 1
        // observedStreamTime : 25, retentionPeriod: 10, actualFrom: 15, timeTo: 15, timeFrom: 6

        final boolean emitFinal = type == StrategyType.ON_WINDOW_CLOSE;
        final List<KeyValueTimestamp<Windowed<String>, String>> windowedMessages = receiveMessagesWithTimestamp(
                new TimeWindowedDeserializer<>(new StringDeserializer(), 10L),
                new StringDeserializer(),
                10L,
                String.class,
                emitFinal ? 4 : 12);

        final List<KeyValueTimestamp<Windowed<String>, String>> expectResult;
        if (emitFinal) {
            expectResult = asList(
                    new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0L, 10L)), "0+1+2", 5),
                    new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(5L, 15L)), "0+2+3", 10),
                    new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(5L, 15L)), "0+4+5", 11),
                    new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(15L, 25L)), "0+6", 15)
            );
        } else {
            expectResult = asList(
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0L, 10L)), "0+1", 0),
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0L, 10L)), "0+1+2", 5),
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(5L, 15L)), "0+2", 5),
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(5L, 15L)), "0+2+3", 10),
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(10L, 20L)), "0+3", 10),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(5L, 15L)), "0+4", 6),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(5L, 15L)), "0+4+5", 11),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(10L, 20L)), "0+5", 11),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(10L, 20L)), "0+5+6", 15),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(15L, 25L)), "0+6", 15),
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(20L, 30L)), "0+7", 25),
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(25L, 35L)), "0+7", 25)
            );
        }

        assertThat(windowedMessages, is(expectResult));
    }

    @ParameterizedTest
    @CsvSource({"ON_WINDOW_UPDATE, true", "ON_WINDOW_UPDATE, false", "ON_WINDOW_CLOSE, true", "ON_WINDOW_CLOSE, false"})
    public void shouldAggregateWindowedWithGrace(final StrategyType type, final boolean withCache) throws Exception {
        produceMessages(
            streamOneInput,
            new KeyValueTimestamp<>("A", "1", 0),
            new KeyValueTimestamp<>("A", "2", 5),
            new KeyValueTimestamp<>("A", "3", 10),
            new KeyValueTimestamp<>("B", "4", 6),
            new KeyValueTimestamp<>("B", "5", 11),
            new KeyValueTimestamp<>("B", "6", 15), // close [0, 10), output A, B [0, 10)
            new KeyValueTimestamp<>("C", "7", 25)  // close [5, 15), [10, 20)
        );

        final Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class, 10L);
        builder.stream(streamOneInput, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeAndGrace(ofMillis(10L), ofMillis(5)).advanceBy(ofMillis(5L)))
            .emitStrategy(StrategyType.forType(type))
            .aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                getMaterialized(withCache)
            )
            .toStream()
            .to(outputTopic, Produced.with(windowedSerde, new StringSerde()));

        startStreams();

        // on window close
        // observedStreamTime : 15, retentionPeriod: 15, actualFrom: 0, timeTo: 0, timeFrom: 0
        // observedStreamTime : 25, retentionPeriod: 15, actualFrom: 10, timeTo: 10, timeFrom: 1

        final boolean emitFinal = type == StrategyType.ON_WINDOW_CLOSE;
        final List<KeyValueTimestamp<Windowed<String>, String>> windowedMessages = receiveMessagesWithTimestamp(
            new TimeWindowedDeserializer<>(new StringDeserializer(), 10L),
            new StringDeserializer(),
            10L,
            String.class,
            emitFinal ? 4 : 13);

        final List<KeyValueTimestamp<Windowed<String>, String>> expectResult;
        if (emitFinal) {
            expectResult = asList(
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0L, 10L)), "0+1+2", 5),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(0L, 10L)), "0+4", 6),
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(10L, 20L)), "0+3", 10),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(10L, 20L)), "0+5+6", 15)
            );
        } else {
            expectResult = asList(
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0L, 10L)), "0+1", 0),
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0L, 10L)), "0+1+2", 5),
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(5L, 15L)), "0+2", 5),
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(5L, 15L)), "0+2+3", 10),
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(10L, 20L)), "0+3", 10),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(0L, 10L)), "0+4", 6),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(5L, 15L)), "0+4", 6),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(5L, 15L)), "0+4+5", 11),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(10L, 20L)), "0+5", 11),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(10L, 20L)), "0+5+6", 15),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(15L, 25L)), "0+6", 15),
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(20L, 30L)), "0+7", 25),
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(25L, 35L)), "0+7", 25)
            );
        }

        assertThat(windowedMessages, is(expectResult));
    }

    @ParameterizedTest
    @CsvSource({"ON_WINDOW_UPDATE, true", "ON_WINDOW_UPDATE, false", "ON_WINDOW_CLOSE, true", "ON_WINDOW_CLOSE, false"})
    public void shouldRestoreAfterJoinRestart(final StrategyType type, final boolean withCache) throws Exception {
        produceMessages(
            streamOneInput,
            new KeyValueTimestamp<>("A", "L1", 0),
            new KeyValueTimestamp<>("A", "L1", 5),
            new KeyValueTimestamp<>("B", "L2", 11), // close [0, 10)
            new KeyValueTimestamp<>("B", "L2", 15), // close [5, 15)
            new KeyValueTimestamp<>("C", "L3", 25)  // close [15, 25), [10, 20)
        );

        produceMessages(
            streamTwoInput,
            new KeyValueTimestamp<>("A", "R1", 0),
            new KeyValueTimestamp<>("A", "R1", 5),
            new KeyValueTimestamp<>("B", "R2", 11), // close [0, 10)
            new KeyValueTimestamp<>("B", "R2", 15), // close [5, 15)
            new KeyValueTimestamp<>("C", "R3", 25)  // close [15, 25), [10, 20)
        );

        final Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(
            String.class, 10L);
        final KStream<String, String> streamOne = builder.stream(streamOneInput,
            Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> streamTwo = builder.stream(streamTwoInput,
            Consumed.with(Serdes.String(), Serdes.String()));

        final KStream<String, String> joinedStream = streamOne
            .join(streamTwo, (v1, v2) -> v1 + "," + v2,
                JoinWindows.ofTimeDifferenceWithNoGrace(ofMillis(2)));

        joinedStream.groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(ofMillis(10L)).advanceBy(ofMillis(5L)))
            .emitStrategy(StrategyType.forType(type))
            .aggregate(
                MockInitializer.STRING_INIT,
                MockAggregator.TOSTRING_ADDER,
                getMaterialized(withCache)
            )
            .toStream()
            .to(outputTopic, Produced.with(windowedSerde, new StringSerde()));

        startStreams();

        // ON_WINDOW_CLOSE expires all records.
        final boolean emitFinal = type == StrategyType.ON_WINDOW_CLOSE;
        List<KeyValueTimestamp<Windowed<String>, String>> windowedMessages = receiveMessagesWithTimestamp(
            new TimeWindowedDeserializer<>(new StringDeserializer(), 10L),
            new StringDeserializer(),
            10L,
            String.class,
            emitFinal ? 4 : 9);

        List<KeyValueTimestamp<Windowed<String>, String>> expectResult;
        if (emitFinal) {
            expectResult = asList(
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0L, 10L)),
                    "0+L1,R1+L1,R1", 5),
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(5L, 15L)), "0+L1,R1",
                    5),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(5L, 15L)), "0+L2,R2",
                    11),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(15L, 25L)),
                    "0+L2,R2", 15)
            );
        } else {
            expectResult = asList(
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0L, 10L)), "0+L1,R1",
                    0),
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0L, 10L)),
                    "0+L1,R1+L1,R1", 5),
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(5L, 15L)), "0+L1,R1",
                    5),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(5L, 15L)), "0+L2,R2",
                    11),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(10L, 20L)),
                    "0+L2,R2", 11),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(10L, 20L)),
                    "0+L2,R2+L2,R2", 15),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(15L, 25L)),
                    "0+L2,R2", 15),
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(20L, 30L)),
                    "0+L3,R3", 25),
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(25L, 35L)),
                    "0+L3,R3", 25)
            );
        }

        assertThat(windowedMessages, is(expectResult));

        kafkaStreams.close();
        kafkaStreams.cleanUp(); // Purge store to force restoration

        produceMessages(
            streamOneInput,
            new KeyValueTimestamp<>("C", "L3", 35)  // close [20, 30), [25, 35)
        );
        produceMessages(
            streamTwoInput,
            new KeyValueTimestamp<>("C", "R3", 35)  // close [20, 30), [25, 35)
        );

        // Restart
        startStreams();

        if (emitFinal) {
            windowedMessages = receiveMessagesWithTimestamp(
                    new TimeWindowedDeserializer<>(new StringDeserializer(), 10L),
                    new StringDeserializer(),
                    10L,
                    String.class,
                    1);

            // Output just new/unexpired closed window for C
            expectResult = Collections.singletonList(
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(25L, 35L)),
                    "0+L3,R3", 25)
            );
        } else {
            windowedMessages = receiveMessagesWithTimestamp(
                    new TimeWindowedDeserializer<>(new StringDeserializer(), 10L),
                    new StringDeserializer(),
                    10L,
                    String.class,
                    2);

            expectResult = asList(
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(30L, 40L)),
                    "0+L3,R3", 35),
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(35L, 45L)),
                    "0+L3,R3", 35)
            );
        }

        assertThat(windowedMessages, is(expectResult));
    }

    @ParameterizedTest
    @EnumSource(StrategyType.class)
    public void shouldThrowUnlimitedWindows(final StrategyType type) {
        final TimeWindowedKStream<String, String> windowedStream = builder.stream(streamOneInput, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .windowedBy(
            UnlimitedWindows.of().startOn(ofEpochMilli(0))
        );

        final boolean emitFinal = type == StrategyType.ON_WINDOW_CLOSE;
        if (emitFinal) {
            assertThrows(IllegalArgumentException.class, () -> windowedStream.emitStrategy(StrategyType.forType(type)));
        } else {
            windowedStream.emitStrategy(StrategyType.forType(type));
        }
    }


    private void produceMessages(final String topic, final KeyValueTimestamp<String, String>... records) {
        IntegrationTestUtils.produceSynchronously(
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                StringSerializer.class,
                StringSerializer.class),
            false,
            topic,
            Optional.empty(),
            Arrays.asList(records)
        );
    }

    private Materialized getMaterialized(final boolean withCache) {
        if (withCache) {
            return Materialized.with(null, new StringSerde()).withCachingEnabled();
        }
        return Materialized.with(null, new StringSerde()).withCachingDisabled();
    }

    private void createTopics() throws InterruptedException {
        streamOneInput = "stream-one-" + safeTestName;
        streamTwoInput = "stream-two-" + safeTestName;
        outputTopic = "output-" + safeTestName;
        CLUSTER.createTopic(streamOneInput, 1, 1);
        CLUSTER.createTopic(streamTwoInput, 1, 1);
        CLUSTER.createTopic(outputTopic);
    }

    private void startStreams() {
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.start();
    }

    private <K, V> List<KeyValueTimestamp<K, V>> receiveMessagesWithTimestamp(final Deserializer<K> keyDeserializer,
                                                                              final Deserializer<V> valueDeserializer,
                                                                              final long windowSize,
                                                                              final Class innerClass,
                                                                              final int numMessages) throws Exception {
        final Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-" + safeTestName);
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass().getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass().getName());
        consumerProperties.put(StreamsConfig.WINDOW_SIZE_MS_CONFIG, windowSize);
        if (keyDeserializer instanceof TimeWindowedDeserializer || keyDeserializer instanceof SessionWindowedDeserializer) {
            consumerProperties.setProperty(StreamsConfig.WINDOWED_INNER_CLASS_SERDE,
                Serdes.serdeFrom(innerClass).getClass().getName());
        }
        return IntegrationTestUtils.waitUntilMinKeyValueWithTimestampRecordsReceived(
            consumerProperties,
            outputTopic,
            numMessages,
            60 * 1000);
    }
}
