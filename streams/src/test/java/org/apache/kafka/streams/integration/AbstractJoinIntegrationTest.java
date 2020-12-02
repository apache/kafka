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
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 * Tests all available joins of Kafka Streams DSL.
 */
@Category({IntegrationTest.class})
@RunWith(value = Parameterized.class)
public abstract class AbstractJoinIntegrationTest {
    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(TestUtils.tempDirectory());

    @Parameterized.Parameters(name = "caching enabled = {0}")
    public static Collection<Object[]> data() {
        final List<Object[]> values = new ArrayList<>();
        for (final boolean cacheEnabled : Arrays.asList(true, false)) {
            values.add(new Object[]{cacheEnabled});
        }
        return values;
    }

    static String appID;

    private final MockTime time = new MockTime();
    private static final Long COMMIT_INTERVAL = 100L;
    static final Properties STREAMS_CONFIG = new Properties();
    static final String INPUT_TOPIC_RIGHT = "inputTopicRight";
    static final String INPUT_TOPIC_LEFT = "inputTopicLeft";
    static final String OUTPUT_TOPIC = "outputTopic";
    static final long ANY_UNIQUE_KEY = 0L;

    StreamsBuilder builder;

    private final List<Input<String>> input = Arrays.asList(
        new Input<>(INPUT_TOPIC_LEFT, null),
        new Input<>(INPUT_TOPIC_RIGHT, null),
        new Input<>(INPUT_TOPIC_LEFT, "A"),
        new Input<>(INPUT_TOPIC_RIGHT, "a"),
        new Input<>(INPUT_TOPIC_LEFT, "B"),
        new Input<>(INPUT_TOPIC_RIGHT, "b"),
        new Input<>(INPUT_TOPIC_LEFT, null),
        new Input<>(INPUT_TOPIC_RIGHT, null),
        new Input<>(INPUT_TOPIC_LEFT, "C"),
        new Input<>(INPUT_TOPIC_RIGHT, "c"),
        new Input<>(INPUT_TOPIC_RIGHT, null),
        new Input<>(INPUT_TOPIC_LEFT, null),
        new Input<>(INPUT_TOPIC_RIGHT, null),
        new Input<>(INPUT_TOPIC_RIGHT, "d"),
        new Input<>(INPUT_TOPIC_LEFT, "D")
    );

    final ValueJoiner<String, String, String> valueJoiner = (value1, value2) -> value1 + "-" + value2;

    final boolean cacheEnabled;

    AbstractJoinIntegrationTest(final boolean cacheEnabled) {
        this.cacheEnabled = cacheEnabled;
    }

    @BeforeClass
    public static void setupConfigsAndUtils() {
        STREAMS_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        STREAMS_CONFIG.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL);
    }

    void prepareEnvironment() throws InterruptedException {
        if (!cacheEnabled) {
            STREAMS_CONFIG.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        }

        STREAMS_CONFIG.put(StreamsConfig.STATE_DIR_CONFIG, testFolder.getRoot().getPath());
    }

    void runTestWithDriver(final List<List<TestRecord<Long, String>>> expectedResult) {
        runTestWithDriver(expectedResult, null);
    }

    void runTestWithDriver(final List<List<TestRecord<Long, String>>> expectedResult, final String storeName) {
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(STREAMS_CONFIG), STREAMS_CONFIG)) {
            final TestInputTopic<Long, String> right = driver.createInputTopic(INPUT_TOPIC_RIGHT, new LongSerializer(), new StringSerializer());
            final TestInputTopic<Long, String> left = driver.createInputTopic(INPUT_TOPIC_LEFT, new LongSerializer(), new StringSerializer());
            final TestOutputTopic<Long, String> outputTopic = driver.createOutputTopic(OUTPUT_TOPIC, new LongDeserializer(), new StringDeserializer());
            final Map<String, TestInputTopic<Long, String>> testInputTopicMap = new HashMap<>();

            testInputTopicMap.put(INPUT_TOPIC_RIGHT, right);
            testInputTopicMap.put(INPUT_TOPIC_LEFT, left);

            TestRecord<Long, String> expectedFinalResult = null;

            final long firstTimestamp = time.milliseconds();
            long eventTimestamp = firstTimestamp;
            final Iterator<List<TestRecord<Long, String>>> resultIterator = expectedResult.iterator();
            for (final Input<String> singleInputRecord : input) {
                testInputTopicMap.get(singleInputRecord.topic).pipeInput(singleInputRecord.record.key, singleInputRecord.record.value, ++eventTimestamp);

                final List<TestRecord<Long, String>> expected = resultIterator.next();
                if (expected != null) {
                    final List<TestRecord<Long, String>> updatedExpected = new LinkedList<>();
                    for (final TestRecord<Long, String> record : expected) {
                        updatedExpected.add(new TestRecord<>(record.key(), record.value(), null, firstTimestamp + record.timestamp()));
                    }

                    final List<TestRecord<Long, String>> output = outputTopic.readRecordsToList();
                    assertThat(output, equalTo(updatedExpected));
                    expectedFinalResult = updatedExpected.get(expected.size() - 1);
                }
            }

            if (storeName != null) {
                checkQueryableStore(storeName, expectedFinalResult, driver);
            }
        }
    }

    void runTestWithDriver(final TestRecord<Long, String> expectedFinalResult, final String storeName) throws InterruptedException {
        try (final TopologyTestDriver driver = new TopologyTestDriver(builder.build(STREAMS_CONFIG), STREAMS_CONFIG)) {
            final TestInputTopic<Long, String> right = driver.createInputTopic(INPUT_TOPIC_RIGHT, new LongSerializer(), new StringSerializer());
            final TestInputTopic<Long, String> left = driver.createInputTopic(INPUT_TOPIC_LEFT, new LongSerializer(), new StringSerializer());
            final TestOutputTopic<Long, String> outputTopic = driver.createOutputTopic(OUTPUT_TOPIC, new LongDeserializer(), new StringDeserializer());
            final Map<String, TestInputTopic<Long, String>> testInputTopicMap = new HashMap<>();

            testInputTopicMap.put(INPUT_TOPIC_RIGHT, right);
            testInputTopicMap.put(INPUT_TOPIC_LEFT, left);

            final long firstTimestamp = time.milliseconds();
            long eventTimestamp = firstTimestamp;

            for (final Input<String> singleInputRecord : input) {
                testInputTopicMap.get(singleInputRecord.topic).pipeInput(singleInputRecord.record.key, singleInputRecord.record.value, ++eventTimestamp);
            }

            final TestRecord<Long, String> updatedExpectedFinalResult =
                new TestRecord<>(
                    expectedFinalResult.key(),
                    expectedFinalResult.value(),
                    null,
                    firstTimestamp + expectedFinalResult.timestamp());

            final List<TestRecord<Long, String>> output = outputTopic.readRecordsToList();

            assertThat(output.get(output.size() - 1), equalTo(updatedExpectedFinalResult));

            if (storeName != null) {
                checkQueryableStore(storeName, updatedExpectedFinalResult, driver);
            }
        }
    }

    private void checkQueryableStore(final String queryableName, final TestRecord<Long, String> expectedFinalResult, final TopologyTestDriver driver) {
        final ReadOnlyKeyValueStore<Long, ValueAndTimestamp<String>> store = driver.getTimestampedKeyValueStore(queryableName);

        final KeyValueIterator<Long, ValueAndTimestamp<String>> all = store.all();
        final KeyValue<Long, ValueAndTimestamp<String>> onlyEntry = all.next();

        try {
            assertThat(onlyEntry.key, is(expectedFinalResult.key()));
            assertThat(onlyEntry.value.value(), is(expectedFinalResult.value()));
            assertThat(onlyEntry.value.timestamp(), is(expectedFinalResult.timestamp()));
            assertThat(all.hasNext(), is(false));
        } finally {
            all.close();
        }
    }

    private static final class Input<V> {
        String topic;
        KeyValue<Long, V> record;

        Input(final String topic, final V value) {
            this.topic = topic;
            record = KeyValue.pair(ANY_UNIQUE_KEY, value);
        }
    }
}
