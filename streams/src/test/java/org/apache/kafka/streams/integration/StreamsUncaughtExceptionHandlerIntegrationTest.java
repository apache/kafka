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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
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
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitForApplicationState;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

@Category(IntegrationTest.class)
@SuppressWarnings("deprecation") //Need to call the old handler, will remove those calls when the old handler is removed
public class StreamsUncaughtExceptionHandlerIntegrationTest {
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1, new Properties(), 0L, 0L);
    public static final Duration DEFAULT_DURATION = Duration.ofSeconds(30);

    @Rule
    public TestName testName = new TestName();

    private static String inputTopic;
    private static StreamsBuilder builder;
    private static Properties properties;
    private static List<String> processorValueCollector;
    private static String appId = "";
    private static AtomicBoolean throwError = new AtomicBoolean(true);

    @Before
    public void setup() {
        final String testId = safeUniqueTestName(getClass(), testName);
        appId = "appId_" + testId;
        inputTopic = "input" + testId;
        IntegrationTestUtils.cleanStateBeforeTest(CLUSTER, inputTopic);

        builder  = new StreamsBuilder();

        processorValueCollector = new ArrayList<>();

        final KStream<String, String> stream = builder.stream(inputTopic);
        stream.process(() -> new ShutdownProcessor(processorValueCollector), Named.as("process"));

        properties  = mkObjectProperties(
            mkMap(
                mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, appId),
                mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
                mkEntry(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2),
                mkEntry(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class),
                mkEntry(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class)
            )
        );
    }

    @After
    public void teardown() throws IOException {
        purgeLocalStreamsState(properties);
    }

    @Test
    public void shouldShutdownThreadUsingOldHandler() throws InterruptedException {
        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties)) {
            final AtomicInteger counter = new AtomicInteger(0);
            kafkaStreams.setUncaughtExceptionHandler((t, e) -> counter.incrementAndGet());

            StreamsTestUtils.startKafkaStreamsAndWaitForRunningState(kafkaStreams);
            produceMessages(0L, inputTopic, "A");

            // should call the UncaughtExceptionHandler in current thread
            TestUtils.waitForCondition(() -> counter.get() == 1, "Handler was called 1st time");
            // should call the UncaughtExceptionHandler after rebalancing to another thread
            TestUtils.waitForCondition(() -> counter.get() == 2, DEFAULT_DURATION.toMillis(), "Handler was called 2nd time");
            // the stream should now turn into ERROR state after 2 threads are dead
            waitForApplicationState(Collections.singletonList(kafkaStreams), KafkaStreams.State.ERROR, DEFAULT_DURATION);

            assertThat(processorValueCollector.size(), equalTo(2));
        }
    }

    @Test
    public void shouldShutdownClient() throws InterruptedException {
        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties)) {
            kafkaStreams.setUncaughtExceptionHandler((t, e) -> fail("should not hit old handler"));

            kafkaStreams.setUncaughtExceptionHandler(exception -> SHUTDOWN_CLIENT);

            StreamsTestUtils.startKafkaStreamsAndWaitForRunningState(kafkaStreams);

            produceMessages(0L, inputTopic, "A");
            waitForApplicationState(Collections.singletonList(kafkaStreams), KafkaStreams.State.NOT_RUNNING, DEFAULT_DURATION);

            assertThat(processorValueCollector.size(), equalTo(1));
        }
    }

    @Test
    public void shouldReplaceThreads() throws InterruptedException {
        testReplaceThreads(2);
    }

    @Test
    public void shouldReplaceSingleThread() throws InterruptedException {
        testReplaceThreads(1);
    }

    @Test
    public void shouldShutdownMultipleThreadApplication() throws InterruptedException {
        testShutdownApplication(2);
    }

    @Test
    public void shouldShutdownSingleThreadApplication() throws InterruptedException {
        testShutdownApplication(1);
    }

    @Test
    public void shouldShutDownClientIfGlobalStreamThreadWantsToReplaceThread() throws InterruptedException {
        builder  = new StreamsBuilder();
        builder.addGlobalStore(
            new KeyValueStoreBuilder<>(
                Stores.persistentKeyValueStore("globalStore"),
                Serdes.String(),
                Serdes.String(),
                CLUSTER.time
            ),
            inputTopic,
            Consumed.with(Serdes.String(), Serdes.String()),
            () -> new ShutdownProcessor(processorValueCollector)
        );
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 0);

        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties)) {
            kafkaStreams.setUncaughtExceptionHandler((t, e) -> fail("should not hit old handler"));
            kafkaStreams.setUncaughtExceptionHandler(exception -> REPLACE_THREAD);

            StreamsTestUtils.startKafkaStreamsAndWaitForRunningState(kafkaStreams);

            produceMessages(0L, inputTopic, "A");
            waitForApplicationState(Collections.singletonList(kafkaStreams), KafkaStreams.State.NOT_RUNNING, DEFAULT_DURATION);

            assertThat(processorValueCollector.size(), equalTo(1));
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

    private static class ShutdownProcessor extends AbstractProcessor<String, String> {
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

    private void testShutdownApplication(final int numThreads) throws InterruptedException {
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numThreads);

        final Topology topology = builder.build();

        try (final KafkaStreams kafkaStreams1 = new KafkaStreams(topology, properties);
             final KafkaStreams kafkaStreams2 = new KafkaStreams(topology, properties)) {
            kafkaStreams1.setUncaughtExceptionHandler((t, e) -> fail("should not hit old handler"));
            kafkaStreams2.setUncaughtExceptionHandler((t, e) -> fail("should not hit old handler"));
            kafkaStreams1.setUncaughtExceptionHandler(exception -> SHUTDOWN_APPLICATION);
            kafkaStreams2.setUncaughtExceptionHandler(exception -> SHUTDOWN_APPLICATION);

            StreamsTestUtils.startKafkaStreamsAndWaitForRunningState(kafkaStreams1);
            StreamsTestUtils.startKafkaStreamsAndWaitForRunningState(kafkaStreams2);

            produceMessages(0L, inputTopic, "A");
            waitForApplicationState(Arrays.asList(kafkaStreams1, kafkaStreams2), KafkaStreams.State.ERROR, DEFAULT_DURATION);

            assertThat(processorValueCollector.size(), equalTo(1));
        }
    }

    private void testReplaceThreads(final int numThreads) throws InterruptedException {
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
            StreamsTestUtils.startKafkaStreamsAndWaitForRunningState(kafkaStreams);

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
