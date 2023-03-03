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

import kafka.tools.ConsoleConsumer;
import kafka.utils.MockTime;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.SessionWindowedDeserializer;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.UnlimitedWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.kstream.internals.UnlimitedWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.test.MockMapper;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofMinutes;
import static java.time.Instant.ofEpochMilli;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings({"unchecked", "deprecation"})
@Timeout(600)
@Tag("integration")
public class KStreamAggregationIntegrationTest {
    private static final int NUM_BROKERS = 1;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }


    private final MockTime mockTime = CLUSTER.time;
    private StreamsBuilder builder;
    private Properties streamsConfiguration;
    private KafkaStreams kafkaStreams;
    private String streamOneInput;
    private String userSessionsStream;
    private String outputTopic;
    private KGroupedStream<String, String> groupedStream;
    private Reducer<String> reducer;
    private Initializer<Integer> initializer;
    private Aggregator<String, String, Integer> aggregator;
    private KStream<Integer, String> stream;

    @BeforeEach
    public void before(final TestInfo testInfo) throws InterruptedException {
        builder = new StreamsBuilder();
        createTopics(testInfo);
        streamsConfiguration = new Properties();
        final String safeTestName = safeUniqueTestName(getClass(), testInfo);
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + safeTestName);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());

        final KeyValueMapper<Integer, String, String> mapper = MockMapper.selectValueMapper();
        stream = builder.stream(streamOneInput, Consumed.with(Serdes.Integer(), Serdes.String()));
        groupedStream = stream.groupBy(mapper, Grouped.with(Serdes.String(), Serdes.String()));

        reducer = (value1, value2) -> value1 + ":" + value2;
        initializer = () -> 0;
        aggregator = (aggKey, value, aggregate) -> aggregate + value.length();
    }

    @AfterEach
    public void whenShuttingDown() throws IOException {
        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    @Test
    public void shouldReduce(final TestInfo testInfo) throws Exception {
        produceMessages(mockTime.milliseconds());
        groupedStream
            .reduce(reducer, Materialized.as("reduce-by-key"))
            .toStream()
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        startStreams();

        produceMessages(mockTime.milliseconds());

        final List<KeyValueTimestamp<String, String>> results = receiveMessages(
            new StringDeserializer(),
            new StringDeserializer(),
            10,
            testInfo);

        results.sort(KStreamAggregationIntegrationTest::compare);

        assertThat(results, is(Arrays.asList(
            new KeyValueTimestamp("A", "A", mockTime.milliseconds()),
            new KeyValueTimestamp("A", "A:A", mockTime.milliseconds()),
            new KeyValueTimestamp("B", "B", mockTime.milliseconds()),
            new KeyValueTimestamp("B", "B:B", mockTime.milliseconds()),
            new KeyValueTimestamp("C", "C", mockTime.milliseconds()),
            new KeyValueTimestamp("C", "C:C", mockTime.milliseconds()),
            new KeyValueTimestamp("D", "D", mockTime.milliseconds()),
            new KeyValueTimestamp("D", "D:D", mockTime.milliseconds()),
            new KeyValueTimestamp("E", "E", mockTime.milliseconds()),
            new KeyValueTimestamp("E", "E:E", mockTime.milliseconds()))));
    }

    private static <K extends Comparable, V extends Comparable> int compare(final KeyValueTimestamp<K, V> o1,
                                                                            final KeyValueTimestamp<K, V> o2) {
        final int keyComparison = o1.key().compareTo(o2.key());
        if (keyComparison == 0) {
            final int valueComparison = o1.value().compareTo(o2.value());
            if (valueComparison == 0) {
                return Long.compare(o1.timestamp(), o2.timestamp());
            }
            return valueComparison;
        }
        return keyComparison;
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldReduceWindowed(final TestInfo testInfo) throws Exception {
        final long firstBatchTimestamp = mockTime.milliseconds();
        mockTime.sleep(1000);
        produceMessages(firstBatchTimestamp);
        final long secondBatchTimestamp = mockTime.milliseconds();
        produceMessages(secondBatchTimestamp);
        produceMessages(secondBatchTimestamp);

        final Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class, 500L);
        //noinspection deprecation
        groupedStream
                .windowedBy(TimeWindows.of(ofMillis(500L)))
                .reduce(reducer)
                .toStream()
                .to(outputTopic, Produced.with(windowedSerde, Serdes.String()));

        startStreams();

        final List<KeyValueTimestamp<Windowed<String>, String>> windowedOutput = receiveMessages(
            new TimeWindowedDeserializer<>(),
            new StringDeserializer(),
            String.class,
            15,
            testInfo);

        // read from ConsoleConsumer
        final String resultFromConsoleConsumer = readWindowedKeyedMessagesViaConsoleConsumer(
            new TimeWindowedDeserializer<String>(),
            new StringDeserializer(),
            String.class,
            15,
            true);

        final Comparator<KeyValueTimestamp<Windowed<String>, String>> comparator =
            Comparator.comparing((KeyValueTimestamp<Windowed<String>, String> o) -> o.key().key())
                .thenComparing(KeyValueTimestamp::value);

        windowedOutput.sort(comparator);
        final long firstBatchWindowStart = firstBatchTimestamp / 500 * 500;
        final long firstBatchWindowEnd = firstBatchWindowStart + 500;
        final long secondBatchWindowStart = secondBatchTimestamp / 500 * 500;
        final long secondBatchWindowEnd = secondBatchWindowStart + 500;

        final List<KeyValueTimestamp<Windowed<String>, String>> expectResult = Arrays.asList(
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(firstBatchWindowStart, firstBatchWindowEnd)), "A", firstBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(secondBatchWindowStart, secondBatchWindowEnd)), "A", secondBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(secondBatchWindowStart, secondBatchWindowEnd)), "A:A", secondBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(firstBatchWindowStart, firstBatchWindowEnd)), "B", firstBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(secondBatchWindowStart, secondBatchWindowEnd)), "B", secondBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(secondBatchWindowStart, secondBatchWindowEnd)), "B:B", secondBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(firstBatchWindowStart, firstBatchWindowEnd)), "C", firstBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(secondBatchWindowStart, secondBatchWindowEnd)), "C", secondBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(secondBatchWindowStart, secondBatchWindowEnd)), "C:C", secondBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(firstBatchWindowStart, firstBatchWindowEnd)), "D", firstBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(secondBatchWindowStart, secondBatchWindowEnd)), "D", secondBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(secondBatchWindowStart, secondBatchWindowEnd)), "D:D", secondBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(firstBatchWindowStart, firstBatchWindowEnd)), "E", firstBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(secondBatchWindowStart, secondBatchWindowEnd)), "E", secondBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(secondBatchWindowStart, secondBatchWindowEnd)), "E:E", secondBatchTimestamp)
        );
        assertThat(windowedOutput, is(expectResult));

        final Set<String> expectResultString = new HashSet<>(expectResult.size());
        for (final KeyValueTimestamp<Windowed<String>, String> eachRecord: expectResult) {
            expectResultString.add("CreateTime:" + eachRecord.timestamp() + ", "
                + eachRecord.key() + ", " + eachRecord.value());
        }

        // check every message is contained in the expect result
        final String[] allRecords = resultFromConsoleConsumer.split("\n");
        for (final String record: allRecords) {
            assertTrue(expectResultString.contains(record));
        }
    }

    @Test
    public void shouldAggregate(final TestInfo testInfo) throws Exception {
        produceMessages(mockTime.milliseconds());
        groupedStream.aggregate(
            initializer,
            aggregator,
            Materialized.as("aggregate-by-selected-key"))
            .toStream()
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.Integer()));

        startStreams();

        produceMessages(mockTime.milliseconds());

        final List<KeyValueTimestamp<String, Integer>> results = receiveMessages(
            new StringDeserializer(),
            new IntegerDeserializer(),
            10,
            testInfo);

        results.sort(KStreamAggregationIntegrationTest::compare);

        assertThat(results, is(Arrays.asList(
            new KeyValueTimestamp("A", 1, mockTime.milliseconds()),
            new KeyValueTimestamp("A", 2, mockTime.milliseconds()),
            new KeyValueTimestamp("B", 1, mockTime.milliseconds()),
            new KeyValueTimestamp("B", 2, mockTime.milliseconds()),
            new KeyValueTimestamp("C", 1, mockTime.milliseconds()),
            new KeyValueTimestamp("C", 2, mockTime.milliseconds()),
            new KeyValueTimestamp("D", 1, mockTime.milliseconds()),
            new KeyValueTimestamp("D", 2, mockTime.milliseconds()),
            new KeyValueTimestamp("E", 1, mockTime.milliseconds()),
            new KeyValueTimestamp("E", 2, mockTime.milliseconds())
        )));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldAggregateWindowed(final TestInfo testInfo) throws Exception {
        final long firstTimestamp = mockTime.milliseconds();
        mockTime.sleep(1000);
        produceMessages(firstTimestamp);
        final long secondTimestamp = mockTime.milliseconds();
        produceMessages(secondTimestamp);
        produceMessages(secondTimestamp);

        final Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class, 500L);
        //noinspection deprecation
        groupedStream.windowedBy(TimeWindows.of(ofMillis(500L)))
                .aggregate(
                        initializer,
                        aggregator,
                        Materialized.with(null, Serdes.Integer())
                )
                .toStream()
                .to(outputTopic, Produced.with(windowedSerde, Serdes.Integer()));

        startStreams();

        final List<KeyValueTimestamp<Windowed<String>, Integer>> windowedMessages = receiveMessagesWithTimestamp(
            new TimeWindowedDeserializer<>(new StringDeserializer(), 500L),
            new IntegerDeserializer(),
            String.class,
            15,
            testInfo);

        // read from ConsoleConsumer
        final String resultFromConsoleConsumer = readWindowedKeyedMessagesViaConsoleConsumer(
            new TimeWindowedDeserializer<String>(),
            new IntegerDeserializer(),
            String.class,
            15,
            true);

        final Comparator<KeyValueTimestamp<Windowed<String>, Integer>> comparator =
            Comparator.comparing((KeyValueTimestamp<Windowed<String>, Integer> o) -> o.key().key())
                .thenComparingInt(KeyValueTimestamp::value);
        windowedMessages.sort(comparator);

        final long firstWindowStart = firstTimestamp / 500 * 500;
        final long firstWindowEnd = firstWindowStart + 500;
        final long secondWindowStart = secondTimestamp / 500 * 500;
        final long secondWindowEnd = secondWindowStart + 500;

        final List<KeyValueTimestamp<Windowed<String>, Integer>> expectResult = Arrays.asList(
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(firstWindowStart, firstWindowEnd)), 1, firstTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(secondWindowStart, secondWindowEnd)), 1, secondTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(secondWindowStart, secondWindowEnd)), 2, secondTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(firstWindowStart, firstWindowEnd)), 1, firstTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(secondWindowStart, secondWindowEnd)), 1, secondTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(secondWindowStart, secondWindowEnd)), 2, secondTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(firstWindowStart, firstWindowEnd)), 1, firstTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(secondWindowStart, secondWindowEnd)), 1, secondTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(secondWindowStart, secondWindowEnd)), 2, secondTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(firstWindowStart, firstWindowEnd)), 1, firstTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(secondWindowStart, secondWindowEnd)), 1, secondTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(secondWindowStart, secondWindowEnd)), 2, secondTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(firstWindowStart, firstWindowEnd)), 1, firstTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(secondWindowStart, secondWindowEnd)), 1, secondTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(secondWindowStart, secondWindowEnd)), 2, secondTimestamp));

        assertThat(windowedMessages, is(expectResult));

        final Set<String> expectResultString = new HashSet<>(expectResult.size());
        for (final KeyValueTimestamp<Windowed<String>, Integer> eachRecord: expectResult) {
            expectResultString.add("CreateTime:" + eachRecord.timestamp() + ", " + eachRecord.key() + ", " + eachRecord.value());
        }

        // check every message is contained in the expect result
        final String[] allRecords = resultFromConsoleConsumer.split("\n");
        for (final String record: allRecords) {
            assertTrue(expectResultString.contains(record));
        }

    }

    private void shouldCountHelper(final TestInfo testInfo) throws Exception {
        startStreams();

        produceMessages(mockTime.milliseconds());

        final List<KeyValueTimestamp<String, Long>> results = receiveMessages(
            new StringDeserializer(),
            new LongDeserializer(),
            10,
            testInfo);
        results.sort(KStreamAggregationIntegrationTest::compare);

        assertThat(results, is(Arrays.asList(
            new KeyValueTimestamp("A", 1L, mockTime.milliseconds()),
            new KeyValueTimestamp("A", 2L, mockTime.milliseconds()),
            new KeyValueTimestamp("B", 1L, mockTime.milliseconds()),
            new KeyValueTimestamp("B", 2L, mockTime.milliseconds()),
            new KeyValueTimestamp("C", 1L, mockTime.milliseconds()),
            new KeyValueTimestamp("C", 2L, mockTime.milliseconds()),
            new KeyValueTimestamp("D", 1L, mockTime.milliseconds()),
            new KeyValueTimestamp("D", 2L, mockTime.milliseconds()),
            new KeyValueTimestamp("E", 1L, mockTime.milliseconds()),
            new KeyValueTimestamp("E", 2L, mockTime.milliseconds())
        )));
    }

    @Test
    public void shouldCount(final TestInfo testInfo) throws Exception {
        produceMessages(mockTime.milliseconds());

        groupedStream.count(Materialized.as("count-by-key"))
                .toStream()
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        shouldCountHelper(testInfo);
    }

    @Test
    public void shouldCountWithInternalStore(final TestInfo testInfo) throws Exception {
        produceMessages(mockTime.milliseconds());

        groupedStream.count()
                .toStream()
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        shouldCountHelper(testInfo);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldGroupByKey(final TestInfo testInfo) throws Exception {
        final long timestamp = mockTime.milliseconds();
        produceMessages(timestamp);
        produceMessages(timestamp);

        //noinspection deprecation
        stream.groupByKey(Grouped.with(Serdes.Integer(), Serdes.String()))
                .windowedBy(TimeWindows.of(ofMillis(500L)))
                .count()
                .toStream((windowedKey, value) -> windowedKey.key() + "@" + windowedKey.window().start()).to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        startStreams();

        final List<KeyValueTimestamp<String, Long>> results = receiveMessages(
            new StringDeserializer(),
            new LongDeserializer(),
            10,
            testInfo);
        results.sort(KStreamAggregationIntegrationTest::compare);

        final long window = timestamp / 500 * 500;
        assertThat(results, is(Arrays.asList(
            new KeyValueTimestamp("1@" + window, 1L, timestamp),
            new KeyValueTimestamp("1@" + window, 2L, timestamp),
            new KeyValueTimestamp("2@" + window, 1L, timestamp),
            new KeyValueTimestamp("2@" + window, 2L, timestamp),
            new KeyValueTimestamp("3@" + window, 1L, timestamp),
            new KeyValueTimestamp("3@" + window, 2L, timestamp),
            new KeyValueTimestamp("4@" + window, 1L, timestamp),
            new KeyValueTimestamp("4@" + window, 2L, timestamp),
            new KeyValueTimestamp("5@" + window, 1L, timestamp),
            new KeyValueTimestamp("5@" + window, 2L, timestamp)
        )));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldReduceSlidingWindows(final TestInfo testInfo) throws Exception {
        final long firstBatchTimestamp = mockTime.milliseconds();
        final long timeDifference = 500L;
        produceMessages(firstBatchTimestamp);
        final long secondBatchTimestamp = firstBatchTimestamp + timeDifference / 2;
        produceMessages(secondBatchTimestamp);
        final long thirdBatchTimestamp = firstBatchTimestamp + timeDifference - 100L;
        produceMessages(thirdBatchTimestamp);

        final Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class, timeDifference);
        //noinspection deprecation
        groupedStream
                .windowedBy(SlidingWindows.withTimeDifferenceAndGrace(ofMillis(timeDifference), ofMillis(2000L)))
                .reduce(reducer)
                .toStream()
                .to(outputTopic, Produced.with(windowedSerde, Serdes.String()));

        startStreams();

        final List<KeyValueTimestamp<Windowed<String>, String>> windowedOutput = receiveMessages(
                new TimeWindowedDeserializer<>(new StringDeserializer(), 500L),
                new StringDeserializer(),
                String.class,
                30,
                testInfo);

        final String resultFromConsoleConsumer = readWindowedKeyedMessagesViaConsoleConsumer(
                new TimeWindowedDeserializer<String>(),
                new StringDeserializer(),
                String.class,
                30,
                true);

        final Comparator<KeyValueTimestamp<Windowed<String>, String>> comparator =
                Comparator.comparing((KeyValueTimestamp<Windowed<String>, String> o) -> o.key().key())
                        .thenComparing(KeyValueTimestamp::value);

        windowedOutput.sort(comparator);
        final long firstBatchLeftWindowStart = firstBatchTimestamp - timeDifference;
        final long firstBatchLeftWindowEnd = firstBatchLeftWindowStart + timeDifference;
        final long firstBatchRightWindowStart = firstBatchTimestamp + 1;
        final long firstBatchRightWindowEnd = firstBatchRightWindowStart + timeDifference;

        final long secondBatchLeftWindowStart = secondBatchTimestamp - timeDifference;
        final long secondBatchLeftWindowEnd = secondBatchLeftWindowStart + timeDifference;
        final long secondBatchRightWindowStart = secondBatchTimestamp + 1;
        final long secondBatchRightWindowEnd = secondBatchRightWindowStart + timeDifference;

        final long thirdBatchLeftWindowStart = thirdBatchTimestamp - timeDifference;
        final long thirdBatchLeftWindowEnd = thirdBatchLeftWindowStart + timeDifference;

        final List<KeyValueTimestamp<Windowed<String>, String>> expectResult = Arrays.asList(
                // A @ firstBatchTimestamp left window created when A @ firstBatchTimestamp processed
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(firstBatchLeftWindowStart, firstBatchLeftWindowEnd)), "A", firstBatchTimestamp),
                // A @ firstBatchTimestamp right window created when A @ secondBatchTimestamp processed
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(firstBatchRightWindowStart, firstBatchRightWindowEnd)), "A", secondBatchTimestamp),
                // A @ secondBatchTimestamp right window created when A @ thirdBatchTimestamp processed
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(secondBatchRightWindowStart, secondBatchRightWindowEnd)), "A", thirdBatchTimestamp),
                // A @ secondBatchTimestamp left window created when A @ secondBatchTimestamp processed
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(secondBatchLeftWindowStart, secondBatchLeftWindowEnd)), "A:A", secondBatchTimestamp),
                // A @ firstBatchTimestamp right window updated when A @ thirdBatchTimestamp processed
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(firstBatchRightWindowStart, firstBatchRightWindowEnd)), "A:A", thirdBatchTimestamp),
                // A @ thirdBatchTimestamp left window created when A @ thirdBatchTimestamp processed
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(thirdBatchLeftWindowStart, thirdBatchLeftWindowEnd)), "A:A:A", thirdBatchTimestamp),

                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(firstBatchLeftWindowStart, firstBatchLeftWindowEnd)), "B", firstBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(firstBatchRightWindowStart, firstBatchRightWindowEnd)), "B", secondBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(secondBatchRightWindowStart, secondBatchRightWindowEnd)), "B", thirdBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(secondBatchLeftWindowStart, secondBatchLeftWindowEnd)), "B:B", secondBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(firstBatchRightWindowStart, firstBatchRightWindowEnd)), "B:B", thirdBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(thirdBatchLeftWindowStart, thirdBatchLeftWindowEnd)), "B:B:B", thirdBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(firstBatchLeftWindowStart, firstBatchLeftWindowEnd)), "C", firstBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(firstBatchRightWindowStart, firstBatchRightWindowEnd)), "C", secondBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(secondBatchRightWindowStart, secondBatchRightWindowEnd)), "C", thirdBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(secondBatchLeftWindowStart, secondBatchLeftWindowEnd)), "C:C", secondBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(firstBatchRightWindowStart, firstBatchRightWindowEnd)), "C:C", thirdBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(thirdBatchLeftWindowStart, thirdBatchLeftWindowEnd)), "C:C:C", thirdBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(firstBatchLeftWindowStart, firstBatchLeftWindowEnd)), "D", firstBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(firstBatchRightWindowStart, firstBatchRightWindowEnd)), "D", secondBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(secondBatchRightWindowStart, secondBatchRightWindowEnd)), "D", thirdBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(secondBatchLeftWindowStart, secondBatchLeftWindowEnd)), "D:D", secondBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(firstBatchRightWindowStart, firstBatchRightWindowEnd)), "D:D", thirdBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(thirdBatchLeftWindowStart, thirdBatchLeftWindowEnd)), "D:D:D", thirdBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(firstBatchLeftWindowStart, firstBatchLeftWindowEnd)), "E", firstBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(firstBatchRightWindowStart, firstBatchRightWindowEnd)), "E", secondBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(secondBatchRightWindowStart, secondBatchRightWindowEnd)), "E", thirdBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(secondBatchLeftWindowStart, secondBatchLeftWindowEnd)), "E:E", secondBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(firstBatchRightWindowStart, firstBatchRightWindowEnd)), "E:E", thirdBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(thirdBatchLeftWindowStart, thirdBatchLeftWindowEnd)), "E:E:E", thirdBatchTimestamp)
        );
        assertThat(windowedOutput, is(expectResult));

        final Set<String> expectResultString = new HashSet<>(expectResult.size());
        for (final KeyValueTimestamp<Windowed<String>, String> eachRecord: expectResult) {
            expectResultString.add("CreateTime:" + eachRecord.timestamp() + ", "
                    + eachRecord.key() + ", " + eachRecord.value());
        }

        // check every message is contained in the expect result
        final String[] allRecords = resultFromConsoleConsumer.split("\n");
        for (final String record: allRecords) {
            assertTrue(expectResultString.contains(record));
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldAggregateSlidingWindows(final TestInfo testInfo) throws Exception {
        final long firstBatchTimestamp = mockTime.milliseconds();
        final long timeDifference = 500L;
        produceMessages(firstBatchTimestamp);
        final long secondBatchTimestamp = firstBatchTimestamp + timeDifference / 2;
        produceMessages(secondBatchTimestamp);
        final long thirdBatchTimestamp = firstBatchTimestamp + timeDifference - 100L;
        produceMessages(thirdBatchTimestamp);

        final Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class, timeDifference);
        //noinspection deprecation
        groupedStream.windowedBy(SlidingWindows.withTimeDifferenceAndGrace(ofMillis(500L), ofMinutes(5)))
                .aggregate(
                        initializer,
                        aggregator,
                        Materialized.with(null, Serdes.Integer())
                )
                .toStream()
                .to(outputTopic, Produced.with(windowedSerde, Serdes.Integer()));

        startStreams();

        final List<KeyValueTimestamp<Windowed<String>, Integer>> windowedMessages = receiveMessagesWithTimestamp(
                new TimeWindowedDeserializer<>(),
                new IntegerDeserializer(),
                String.class,
                30,
                testInfo);

        // read from ConsoleConsumer
        final String resultFromConsoleConsumer = readWindowedKeyedMessagesViaConsoleConsumer(
                new TimeWindowedDeserializer<String>(),
                new IntegerDeserializer(),
                String.class,
                30,
                true);

        final Comparator<KeyValueTimestamp<Windowed<String>, Integer>> comparator =
                Comparator.comparing((KeyValueTimestamp<Windowed<String>, Integer> o) -> o.key().key())
                        .thenComparingInt(KeyValueTimestamp::value);
        windowedMessages.sort(comparator);

        final long firstBatchLeftWindowStart = firstBatchTimestamp - timeDifference;
        final long firstBatchLeftWindowEnd = firstBatchLeftWindowStart + timeDifference;
        final long firstBatchRightWindowStart = firstBatchTimestamp + 1;
        final long firstBatchRightWindowEnd = firstBatchRightWindowStart + timeDifference;

        final long secondBatchLeftWindowStart = secondBatchTimestamp - timeDifference;
        final long secondBatchLeftWindowEnd = secondBatchLeftWindowStart + timeDifference;
        final long secondBatchRightWindowStart = secondBatchTimestamp + 1;
        final long secondBatchRightWindowEnd = secondBatchRightWindowStart + timeDifference;

        final long thirdBatchLeftWindowStart = thirdBatchTimestamp - timeDifference;
        final long thirdBatchLeftWindowEnd = thirdBatchLeftWindowStart + timeDifference;

        final List<KeyValueTimestamp<Windowed<String>, Integer>> expectResult = Arrays.asList(
                // A @ firstBatchTimestamp left window created when A @ firstBatchTimestamp processed
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(firstBatchLeftWindowStart, firstBatchLeftWindowEnd)), 1, firstBatchTimestamp),
                // A @ firstBatchTimestamp right window created when A @ secondBatchTimestamp processed
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(firstBatchRightWindowStart, firstBatchRightWindowEnd)), 1, secondBatchTimestamp),
                // A @ secondBatchTimestamp right window created when A @ thirdBatchTimestamp processed
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(secondBatchRightWindowStart, secondBatchRightWindowEnd)), 1, thirdBatchTimestamp),
                // A @ secondBatchTimestamp left window created when A @ secondBatchTimestamp processed
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(secondBatchLeftWindowStart, secondBatchLeftWindowEnd)), 2, secondBatchTimestamp),
                // A @ firstBatchTimestamp right window updated when A @ thirdBatchTimestamp processed
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(firstBatchRightWindowStart, firstBatchRightWindowEnd)), 2, thirdBatchTimestamp),
                // A @ thirdBatchTimestamp left window created when A @ thirdBatchTimestamp processed
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(thirdBatchLeftWindowStart, thirdBatchLeftWindowEnd)), 3, thirdBatchTimestamp),

                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(firstBatchLeftWindowStart, firstBatchLeftWindowEnd)), 1, firstBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(firstBatchRightWindowStart, firstBatchRightWindowEnd)), 1, secondBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(secondBatchRightWindowStart, secondBatchRightWindowEnd)), 1, thirdBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(secondBatchLeftWindowStart, secondBatchLeftWindowEnd)), 2, secondBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(firstBatchRightWindowStart, firstBatchRightWindowEnd)), 2, thirdBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(thirdBatchLeftWindowStart, thirdBatchLeftWindowEnd)), 3, thirdBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(firstBatchLeftWindowStart, firstBatchLeftWindowEnd)), 1, firstBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(firstBatchRightWindowStart, firstBatchRightWindowEnd)), 1, secondBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(secondBatchRightWindowStart, secondBatchRightWindowEnd)), 1, thirdBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(secondBatchLeftWindowStart, secondBatchLeftWindowEnd)), 2, secondBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(firstBatchRightWindowStart, firstBatchRightWindowEnd)), 2, thirdBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("C", new TimeWindow(thirdBatchLeftWindowStart, thirdBatchLeftWindowEnd)), 3, thirdBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(firstBatchLeftWindowStart, firstBatchLeftWindowEnd)), 1, firstBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(firstBatchRightWindowStart, firstBatchRightWindowEnd)), 1, secondBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(secondBatchRightWindowStart, secondBatchRightWindowEnd)), 1, thirdBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(secondBatchLeftWindowStart, secondBatchLeftWindowEnd)), 2, secondBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(firstBatchRightWindowStart, firstBatchRightWindowEnd)), 2, thirdBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("D", new TimeWindow(thirdBatchLeftWindowStart, thirdBatchLeftWindowEnd)), 3, thirdBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(firstBatchLeftWindowStart, firstBatchLeftWindowEnd)), 1, firstBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(firstBatchRightWindowStart, firstBatchRightWindowEnd)), 1, secondBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(secondBatchRightWindowStart, secondBatchRightWindowEnd)), 1, thirdBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(secondBatchLeftWindowStart, secondBatchLeftWindowEnd)), 2, secondBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(firstBatchRightWindowStart, firstBatchRightWindowEnd)), 2, thirdBatchTimestamp),
                new KeyValueTimestamp<>(new Windowed<>("E", new TimeWindow(thirdBatchLeftWindowStart, thirdBatchLeftWindowEnd)), 3, thirdBatchTimestamp)
        );

        assertThat(windowedMessages, is(expectResult));

        final Set<String> expectResultString = new HashSet<>(expectResult.size());
        for (final KeyValueTimestamp<Windowed<String>, Integer> eachRecord: expectResult) {
            expectResultString.add("CreateTime:" + eachRecord.timestamp() + ", " + eachRecord.key() + ", " + eachRecord.value());
        }

        // check every message is contained in the expect result
        final String[] allRecords = resultFromConsoleConsumer.split("\n");
        for (final String record: allRecords) {
            assertTrue(expectResultString.contains(record));
        }

    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldCountSessionWindows() throws Exception {
        final long sessionGap = 5 * 60 * 1000L;
        final List<KeyValue<String, String>> t1Messages = Arrays.asList(new KeyValue<>("bob", "start"),
                                                                        new KeyValue<>("penny", "start"),
                                                                        new KeyValue<>("jo", "pause"),
                                                                        new KeyValue<>("emily", "pause"));

        final long t1 = mockTime.milliseconds() - TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                userSessionsStream,
                t1Messages,
                TestUtils.producerConfig(
                        CLUSTER.bootstrapServers(),
                        StringSerializer.class,
                        StringSerializer.class,
                        new Properties()),
                t1);
        final long t2 = t1 + (sessionGap / 2);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                userSessionsStream,
                Collections.singletonList(
                        new KeyValue<>("emily", "resume")
                ),
                TestUtils.producerConfig(
                        CLUSTER.bootstrapServers(),
                        StringSerializer.class,
                        StringSerializer.class,
                        new Properties()),
                t2);
        final long t3 = t1 + sessionGap + 1;
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                userSessionsStream,
                Arrays.asList(
                        new KeyValue<>("bob", "pause"),
                        new KeyValue<>("penny", "stop")
                ),
                TestUtils.producerConfig(
                        CLUSTER.bootstrapServers(),
                        StringSerializer.class,
                        StringSerializer.class,
                        new Properties()),
                t3);
        final long t4 = t3 + (sessionGap / 2);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                userSessionsStream,
                Arrays.asList(
                        new KeyValue<>("bob", "resume"), // bobs session continues
                        new KeyValue<>("jo", "resume")   // jo's starts new session
                ),
                TestUtils.producerConfig(
                        CLUSTER.bootstrapServers(),
                        StringSerializer.class,
                        StringSerializer.class,
                        new Properties()),
                t4);
        final long t5 = t4 - 1;
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            userSessionsStream,
            Collections.singletonList(
                new KeyValue<>("jo", "late")   // jo has late arrival
            ),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                StringSerializer.class,
                StringSerializer.class,
                new Properties()),
            t5);

        final Map<Windowed<String>, KeyValue<Long, Long>> results = new HashMap<>();
        final CountDownLatch latch = new CountDownLatch(13);

        //noinspection deprecation
        builder.stream(userSessionsStream, Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(SessionWindows.with(ofMillis(sessionGap)))
                .count()
                .toStream()
                .transform(() -> new Transformer<Windowed<String>, Long, KeyValue<Object, Object>>() {
                        private ProcessorContext context;

                        @Override
                        public void init(final ProcessorContext context) {
                            this.context = context;
                        }

                        @Override
                        public KeyValue<Object, Object> transform(final Windowed<String> key, final Long value) {
                            results.put(key, KeyValue.pair(value, context.timestamp()));
                            latch.countDown();
                            return null;
                        }

                        @Override
                        public void close() {}
                    });

        startStreams();
        latch.await(30, TimeUnit.SECONDS);

        assertThat(results.get(new Windowed<>("bob", new SessionWindow(t1, t1))), equalTo(KeyValue.pair(1L, t1)));
        assertThat(results.get(new Windowed<>("penny", new SessionWindow(t1, t1))), equalTo(KeyValue.pair(1L, t1)));
        assertThat(results.get(new Windowed<>("jo", new SessionWindow(t1, t1))), equalTo(KeyValue.pair(1L, t1)));
        assertThat(results.get(new Windowed<>("jo", new SessionWindow(t5, t4))), equalTo(KeyValue.pair(2L, t4)));
        assertThat(results.get(new Windowed<>("emily", new SessionWindow(t1, t2))), equalTo(KeyValue.pair(2L, t2)));
        assertThat(results.get(new Windowed<>("bob", new SessionWindow(t3, t4))), equalTo(KeyValue.pair(2L, t4)));
        assertThat(results.get(new Windowed<>("penny", new SessionWindow(t3, t3))), equalTo(KeyValue.pair(1L, t3)));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldReduceSessionWindows() throws Exception {
        final long sessionGap = 1000L; // something to do with time
        final List<KeyValue<String, String>> t1Messages = Arrays.asList(new KeyValue<>("bob", "start"),
                                                                        new KeyValue<>("penny", "start"),
                                                                        new KeyValue<>("jo", "pause"),
                                                                        new KeyValue<>("emily", "pause"));

        final long t1 = mockTime.milliseconds();
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                userSessionsStream,
                t1Messages,
                TestUtils.producerConfig(
                        CLUSTER.bootstrapServers(),
                        StringSerializer.class,
                        StringSerializer.class,
                        new Properties()),
                t1);
        final long t2 = t1 + (sessionGap / 2);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                userSessionsStream,
                Collections.singletonList(
                        new KeyValue<>("emily", "resume")
                ),
                TestUtils.producerConfig(
                        CLUSTER.bootstrapServers(),
                        StringSerializer.class,
                        StringSerializer.class,
                        new Properties()),
                t2);
        final long t3 = t1 + sessionGap + 1;
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                userSessionsStream,
                Arrays.asList(
                        new KeyValue<>("bob", "pause"),
                        new KeyValue<>("penny", "stop")
                ),
                TestUtils.producerConfig(
                        CLUSTER.bootstrapServers(),
                        StringSerializer.class,
                        StringSerializer.class,
                        new Properties()),
                t3);
        final long t4 = t3 + (sessionGap / 2);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                userSessionsStream,
                Arrays.asList(
                        new KeyValue<>("bob", "resume"), // bobs session continues
                        new KeyValue<>("jo", "resume")   // jo's starts new session
                ),
                TestUtils.producerConfig(
                        CLUSTER.bootstrapServers(),
                        StringSerializer.class,
                        StringSerializer.class,
                        new Properties()),
                t4);
        final long t5 = t4 - 1;
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            userSessionsStream,
            Collections.singletonList(
                new KeyValue<>("jo", "late")   // jo has late arrival
            ),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                StringSerializer.class,
                StringSerializer.class,
                new Properties()),
            t5);

        final Map<Windowed<String>, KeyValue<String, Long>> results = new HashMap<>();
        final CountDownLatch latch = new CountDownLatch(13);
        final String userSessionsStore = "UserSessionsStore";
        //noinspection deprecation
        builder.stream(userSessionsStream, Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(SessionWindows.with(ofMillis(sessionGap)))
                .reduce((value1, value2) -> value1 + ":" + value2, Materialized.as(userSessionsStore))
                .toStream()
            .transform(() -> new Transformer<Windowed<String>, String, KeyValue<Object, Object>>() {
                private ProcessorContext context;

                @Override
                public void init(final ProcessorContext context) {
                    this.context = context;
                }

                @Override
                public KeyValue<Object, Object> transform(final Windowed<String> key, final String value) {
                    results.put(key, KeyValue.pair(value, context.timestamp()));
                    latch.countDown();
                    return null;
                }

                @Override
                public void close() {}
            });

        startStreams();
        latch.await(30, TimeUnit.SECONDS);

        // verify correct data received
        assertThat(results.get(new Windowed<>("bob", new SessionWindow(t1, t1))), equalTo(KeyValue.pair("start", t1)));
        assertThat(results.get(new Windowed<>("penny", new SessionWindow(t1, t1))), equalTo(KeyValue.pair("start", t1)));
        assertThat(results.get(new Windowed<>("jo", new SessionWindow(t1, t1))), equalTo(KeyValue.pair("pause", t1)));
        assertThat(results.get(new Windowed<>("jo", new SessionWindow(t5, t4))), equalTo(KeyValue.pair("resume:late", t4)));
        assertThat(results.get(new Windowed<>("emily", new SessionWindow(t1, t2))), equalTo(KeyValue.pair("pause:resume", t2)));
        assertThat(results.get(new Windowed<>("bob", new SessionWindow(t3, t4))), equalTo(KeyValue.pair("pause:resume", t4)));
        assertThat(results.get(new Windowed<>("penny", new SessionWindow(t3, t3))), equalTo(KeyValue.pair("stop", t3)));

        // verify can query data via IQ
        final ReadOnlySessionStore<String, String> sessionStore =
            IntegrationTestUtils.getStore(userSessionsStore, kafkaStreams, QueryableStoreTypes.sessionStore());

        try (final KeyValueIterator<Windowed<String>, String> bob = sessionStore.fetch("bob")) {
            assertThat(bob.next(), equalTo(KeyValue.pair(new Windowed<>("bob", new SessionWindow(t1, t1)), "start")));
            assertThat(bob.next(), equalTo(KeyValue.pair(new Windowed<>("bob", new SessionWindow(t3, t4)), "pause:resume")));
            assertFalse(bob.hasNext());
        }
    }

    @Test
    public void shouldCountUnlimitedWindows() throws Exception {
        final long startTime = mockTime.milliseconds() - TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS) + 1;
        final long incrementTime = Duration.ofDays(1).toMillis();

        final long t1 = mockTime.milliseconds() - TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
        final List<KeyValue<String, String>> t1Messages = Arrays.asList(new KeyValue<>("bob", "start"),
                                                                        new KeyValue<>("penny", "start"),
                                                                        new KeyValue<>("jo", "pause"),
                                                                        new KeyValue<>("emily", "pause"));

        final Properties producerConfig = TestUtils.producerConfig(
            CLUSTER.bootstrapServers(),
            StringSerializer.class,
            StringSerializer.class,
            new Properties()
        );

        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            userSessionsStream,
            t1Messages,
            producerConfig,
            t1);

        final long t2 = t1 + incrementTime;
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            userSessionsStream,
            Collections.singletonList(
                new KeyValue<>("emily", "resume")
            ),
            producerConfig,
            t2);
        final long t3 = t2 + incrementTime;
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            userSessionsStream,
            Arrays.asList(
                new KeyValue<>("bob", "pause"),
                new KeyValue<>("penny", "stop")
            ),
            producerConfig,
            t3);

        final long t4 = t3 + incrementTime;
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            userSessionsStream,
            Arrays.asList(
                new KeyValue<>("bob", "resume"), // bobs session continues
                new KeyValue<>("jo", "resume")   // jo's starts new session
            ),
            producerConfig,
            t4);

        final Map<Windowed<String>, KeyValue<Long, Long>> results = new HashMap<>();
        final CountDownLatch latch = new CountDownLatch(5);

        builder.stream(userSessionsStream, Consumed.with(Serdes.String(), Serdes.String()))
               .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
               .windowedBy(UnlimitedWindows.of().startOn(ofEpochMilli(startTime)))
               .count()
               .toStream()
               .transform(() -> new Transformer<Windowed<String>, Long, KeyValue<Object, Object>>() {
                   private ProcessorContext context;

                   @Override
                   public void init(final ProcessorContext context) {
                       this.context = context;
                   }

                   @Override
                   public KeyValue<Object, Object> transform(final Windowed<String> key, final Long value) {
                       results.put(key, KeyValue.pair(value, context.timestamp()));
                       latch.countDown();
                       return null;
                   }

                   @Override
                   public void close() {}
               });
        startStreams();
        assertTrue(latch.await(30, TimeUnit.SECONDS));

        assertThat(results.get(new Windowed<>("bob", new UnlimitedWindow(startTime))), equalTo(KeyValue.pair(2L, t4)));
        assertThat(results.get(new Windowed<>("penny", new UnlimitedWindow(startTime))), equalTo(KeyValue.pair(1L, t3)));
        assertThat(results.get(new Windowed<>("jo", new UnlimitedWindow(startTime))), equalTo(KeyValue.pair(1L, t4)));
        assertThat(results.get(new Windowed<>("emily", new UnlimitedWindow(startTime))), equalTo(KeyValue.pair(1L, t2)));
    }


    private void produceMessages(final long timestamp) throws Exception {
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            streamOneInput,
            Arrays.asList(
                new KeyValue<>(1, "A"),
                new KeyValue<>(2, "B"),
                new KeyValue<>(3, "C"),
                new KeyValue<>(4, "D"),
                new KeyValue<>(5, "E")),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                StringSerializer.class,
                new Properties()),
            timestamp);
    }


    private void createTopics(final TestInfo testInfo) throws InterruptedException {
        final String safeTestName = safeUniqueTestName(getClass(), testInfo);
        streamOneInput = "stream-one-" + safeTestName;
        outputTopic = "output-" + safeTestName;
        userSessionsStream = "user-sessions-" + safeTestName;
        CLUSTER.createTopic(streamOneInput, 3, 1);
        CLUSTER.createTopics(userSessionsStream, outputTopic);
    }

    private void startStreams() {
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.start();
    }

    private <K, V> List<KeyValueTimestamp<K, V>> receiveMessages(final Deserializer<K> keyDeserializer,
                                                                 final Deserializer<V> valueDeserializer,
                                                                 final int numMessages,
                                                                 final TestInfo testInfo)
            throws Exception {

        return receiveMessages(keyDeserializer, valueDeserializer, null, numMessages, testInfo);
    }

    private <K, V> List<KeyValueTimestamp<K, V>> receiveMessages(final Deserializer<K> keyDeserializer,
                                                                 final Deserializer<V> valueDeserializer,
                                                                 final Class innerClass,
                                                                 final int numMessages,
                                                                 final TestInfo testInfo)
            throws Exception {

        final String safeTestName = safeUniqueTestName(getClass(), testInfo);
        final Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-" + safeTestName);
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass().getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass().getName());
        consumerProperties.put(StreamsConfig.WINDOW_SIZE_MS_CONFIG, 500L);
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

    private <K, V> List<KeyValueTimestamp<K, V>> receiveMessagesWithTimestamp(final Deserializer<K> keyDeserializer,
                                                                              final Deserializer<V> valueDeserializer,
                                                                              final Class innerClass,
                                                                              final int numMessages,
                                                                              final TestInfo testInfo) throws Exception {
        final String safeTestName = safeUniqueTestName(getClass(), testInfo);
        final Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-" + safeTestName);
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass().getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass().getName());
        consumerProperties.put(StreamsConfig.WINDOW_SIZE_MS_CONFIG, 500L);
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

    private <K, V> String readWindowedKeyedMessagesViaConsoleConsumer(final Deserializer<K> keyDeserializer,
                                                                      final Deserializer<V> valueDeserializer,
                                                                      final Class innerClass,
                                                                      final int numMessages,
                                                                      final boolean printTimestamp) {
        final ByteArrayOutputStream newConsole = new ByteArrayOutputStream();
        final PrintStream originalStream = System.out;
        try (final PrintStream newStream = new PrintStream(newConsole)) {
            System.setOut(newStream);

            final String keySeparator = ", ";
            // manually construct the console consumer argument array
            final String[] args = new String[] {
                "--bootstrap-server", CLUSTER.bootstrapServers(),
                "--from-beginning",
                "--property", "print.key=true",
                "--property", "print.timestamp=" + printTimestamp,
                "--topic", outputTopic,
                "--max-messages", String.valueOf(numMessages),
                "--property", "key.deserializer=" + keyDeserializer.getClass().getName(),
                "--property", "value.deserializer=" + valueDeserializer.getClass().getName(),
                "--property", "key.separator=" + keySeparator,
                "--property", "key.deserializer." + StreamsConfig.WINDOWED_INNER_CLASS_SERDE + "=" + Serdes.serdeFrom(innerClass).getClass().getName(),
                "--property", "key.deserializer.window.size.ms=500",
            };

            ConsoleConsumer.messageCount_$eq(0); //reset the message count
            ConsoleConsumer.run(new ConsoleConsumer.ConsumerConfig(args));
            newStream.flush();
            System.setOut(originalStream);
            return newConsole.toString();
        }
    }
}
