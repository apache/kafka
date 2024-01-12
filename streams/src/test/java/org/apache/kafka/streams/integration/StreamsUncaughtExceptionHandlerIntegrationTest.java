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
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkObjectProperties;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.purgeLocalStreamsState;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitForApplicationState;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static java.util.Arrays.asList;

@Category(IntegrationTest.class)
@SuppressWarnings("deprecation") //Need to call the old handler, will remove those calls when the old handler is removed
public class StreamsUncaughtExceptionHandlerIntegrationTest {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(600);

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1, new Properties(), Collections.emptyList(), 0L, 0L);

    @BeforeClass
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }

    public static final Duration DEFAULT_DURATION = Duration.ofSeconds(30);

    @Rule
    public final TestName testName = new TestName();

    private final String testId = safeUniqueTestName(testName);
    private final String appId = "appId_" + testId;
    private final String inputTopic = "input" + testId;
    private final String inputTopic2 = "input2" + testId;
    private final String outputTopic = "output" + testId;
    private final String outputTopic2 = "output2" + testId;
    private final StreamsBuilder builder = new StreamsBuilder();
    private final List<String> processorValueCollector = new ArrayList<>();
    private static AtomicBoolean throwError = new AtomicBoolean(true);

    private final Properties properties = basicProps();

    private Properties basicProps() {
        return mkObjectProperties(
            mkMap(
                mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, appId),
                mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
                mkEntry(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2),
                mkEntry(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class),
                mkEntry(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class),
                mkEntry(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), 10000)
            )
        );
    }

    @Before
    public void setup() {
        IntegrationTestUtils.cleanStateBeforeTest(CLUSTER, inputTopic, inputTopic2, outputTopic, outputTopic2);
        final KStream<String, String> stream = builder.stream(inputTopic);
        stream.process(() -> new ShutdownProcessor(processorValueCollector), Named.as("process"));
    }

    @After
    public void teardown() throws IOException {
        purgeLocalStreamsState(properties);
    }

    @Test
    public void shouldShutdownThreadUsingOldHandler() throws Exception {
        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties)) {
            final AtomicInteger counter = new AtomicInteger(0);
            kafkaStreams.setUncaughtExceptionHandler((t, e) -> counter.incrementAndGet());

            startApplicationAndWaitUntilRunning(kafkaStreams);
            produceMessages(0L, inputTopic, "A");

            // should call the UncaughtExceptionHandler in current thread
            TestUtils.waitForCondition(() -> counter.get() == 1, "Handler was called 1st time");
            // should call the UncaughtExceptionHandler after rebalancing to another thread
            TestUtils.waitForCondition(() -> counter.get() == 2, DEFAULT_DURATION.toMillis(), "Handler was called 2nd time");
            // there is no threads running but the client is still in running
            waitForApplicationState(Collections.singletonList(kafkaStreams), KafkaStreams.State.RUNNING, DEFAULT_DURATION);

            assertThat(processorValueCollector.size(), equalTo(2));
        }
    }

    @Test
    public void shouldShutdownClient() throws Exception {
        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties)) {
            kafkaStreams.setUncaughtExceptionHandler((t, e) -> fail("should not hit old handler"));

            kafkaStreams.setUncaughtExceptionHandler(exception -> SHUTDOWN_CLIENT);

            startApplicationAndWaitUntilRunning(kafkaStreams);

            produceMessages(0L, inputTopic, "A");
            waitForApplicationState(Collections.singletonList(kafkaStreams), KafkaStreams.State.ERROR, DEFAULT_DURATION);

            assertThat(processorValueCollector.size(), equalTo(1));
        }
    }

    @Test
    public void shouldReplaceThreads() throws Exception {
        testReplaceThreads(2);
    }

    @Test
    public void shouldReplaceThreadsWithoutJavaHandler() throws Exception {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> fail("exception thrown"));
        testReplaceThreads(2);
    }

    @Test
    public void shouldReplaceSingleThread() throws Exception {
        testReplaceThreads(1);
    }

    @Test
    public void shouldShutdownMultipleThreadApplication() throws Exception {
        testShutdownApplication(2);
    }

    @Test
    public void shouldShutdownSingleThreadApplication() throws Exception {
        testShutdownApplication(1);
    }

    @Test
    public void shouldShutDownClientIfGlobalStreamThreadWantsToReplaceThread() throws Exception {
        builder.addGlobalStore(
            new KeyValueStoreBuilder<>(
                Stores.persistentKeyValueStore("globalStore"),
                Serdes.String(),
                Serdes.String(),
                CLUSTER.time
            ),
            inputTopic2,
            Consumed.with(Serdes.String(), Serdes.String()),
            () -> new ShutdownProcessor(processorValueCollector)
        );
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 0);

        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties)) {
            kafkaStreams.setUncaughtExceptionHandler((t, e) -> fail("should not hit old handler"));
            kafkaStreams.setUncaughtExceptionHandler(exception -> REPLACE_THREAD);

            startApplicationAndWaitUntilRunning(kafkaStreams);

            produceMessages(0L, inputTopic2, "A");
            waitForApplicationState(Collections.singletonList(kafkaStreams), KafkaStreams.State.ERROR, DEFAULT_DURATION);

            assertThat(processorValueCollector.size(), equalTo(1));
        }

    }

    @Test
    public void shouldEmitSameRecordAfterFailover() throws Exception {
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 300000L);
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);

        final AtomicBoolean shouldThrow = new AtomicBoolean(true);
        final StreamsBuilder builder = new StreamsBuilder();
        builder.table(inputTopic, Materialized.as("test-store"))
            .toStream()
            .map((key, value) -> {
                if (shouldThrow.compareAndSet(true, false)) {
                    throw new RuntimeException("Kaboom");
                } else {
                    return new KeyValue<>(key, value);
                }
            })
            .to(outputTopic);
        builder.stream(inputTopic2).to(outputTopic2);

        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties)) {
            kafkaStreams.setUncaughtExceptionHandler(exception -> StreamThreadExceptionResponse.REPLACE_THREAD);
            startApplicationAndWaitUntilRunning(kafkaStreams);

            IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                inputTopic,
                asList(
                    new KeyValue<>(1, "A"),
                    new KeyValue<>(1, "B")
                ),
                TestUtils.producerConfig(
                    CLUSTER.bootstrapServers(),
                    IntegerSerializer.class,
                    StringSerializer.class,
                    new Properties()),
                0L);

            IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                inputTopic2,
                asList(
                    new KeyValue<>(1, "A"),
                    new KeyValue<>(1, "B")
                ),
                TestUtils.producerConfig(
                    CLUSTER.bootstrapServers(),
                    IntegerSerializer.class,
                    StringSerializer.class,
                    new Properties()),
                0L);

            IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(
                TestUtils.consumerConfig(
                    CLUSTER.bootstrapServers(),
                    IntegerDeserializer.class,
                    StringDeserializer.class
                ),
                outputTopic,
                asList(
                    new KeyValue<>(1, "A"),
                    new KeyValue<>(1, "B")
                )
            );
            IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(
                TestUtils.consumerConfig(
                    CLUSTER.bootstrapServers(),
                    IntegerDeserializer.class,
                    StringDeserializer.class
                ),
                outputTopic2,
                asList(
                    new KeyValue<>(1, "A"),
                    new KeyValue<>(1, "B")
                )
            );
        }
    }

    private void produceMessages(final long timestamp, final String streamOneInput, final String msg) {
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            streamOneInput,
            Collections.singletonList(new KeyValue<>("1", msg)),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                StringSerializer.class,
                StringSerializer.class,
                new Properties()),
            timestamp);
    }

    private static class ShutdownProcessor extends org.apache.kafka.streams.processor.AbstractProcessor<String, String> {
        final List<String> valueList;

        ShutdownProcessor(final List<String> valueList) {
            this.valueList = valueList;
        }

        @Override
        public void process(final String key, final String value) {
            valueList.add(value + " " + context.taskId());
            if (throwError.get()) {
                throw new StreamsException(Thread.currentThread().getName());
            }
            throwError.set(true);
        }
    }

    private void testShutdownApplication(final int numThreads) throws Exception {
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numThreads);

        final Topology topology = builder.build();

        try (final KafkaStreams kafkaStreams1 = new KafkaStreams(topology, properties);
             final KafkaStreams kafkaStreams2 = new KafkaStreams(topology, properties)) {
            kafkaStreams1.setUncaughtExceptionHandler((t, e) -> fail("should not hit old handler"));
            kafkaStreams2.setUncaughtExceptionHandler((t, e) -> fail("should not hit old handler"));
            kafkaStreams1.setUncaughtExceptionHandler(exception -> SHUTDOWN_APPLICATION);
            kafkaStreams2.setUncaughtExceptionHandler(exception -> SHUTDOWN_APPLICATION);

            startApplicationAndWaitUntilRunning(asList(kafkaStreams1, kafkaStreams2));

            produceMessages(0L, inputTopic, "A");
            waitForApplicationState(asList(kafkaStreams1, kafkaStreams2), KafkaStreams.State.ERROR, DEFAULT_DURATION);

            assertThat(processorValueCollector.size(), equalTo(1));
        }
    }

    private void testReplaceThreads(final int numThreads) throws Exception {
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numThreads);
        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties)) {
            kafkaStreams.setUncaughtExceptionHandler((t, e) -> fail("should not hit old handler"));

            final AtomicInteger count = new AtomicInteger();
            kafkaStreams.setUncaughtExceptionHandler(exception -> {
                if (count.incrementAndGet() == numThreads) {
                    throwError.set(false);
                }
                return REPLACE_THREAD;
            });
            startApplicationAndWaitUntilRunning(kafkaStreams);

            produceMessages(0L, inputTopic, "A");
            TestUtils.waitForCondition(() -> count.get() == numThreads, "finished replacing threads");
            TestUtils.waitForCondition(() -> throwError.get(), "finished replacing threads");
            kafkaStreams.close();
            waitForApplicationState(Collections.singletonList(kafkaStreams), KafkaStreams.State.NOT_RUNNING, DEFAULT_DURATION);

            assertThat("All initial threads have failed and the replacement thread had processed on record",
                processorValueCollector.size(), equalTo(numThreads + 1));
        }
    }
}
