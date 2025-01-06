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
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.ThreadMetadata;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.test.TestUtils;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkObjectProperties;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.purgeLocalStreamsState;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Timeout(600)
@Tag("integration")
public class AdjustStreamThreadCountTest {
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    private final List<KafkaStreams.State> stateTransitionHistory = new ArrayList<>();
    private static String inputTopic;
    private static StreamsBuilder builder;
    private static Properties properties;
    private static String appId = "";
    public static final Duration DEFAULT_DURATION = Duration.ofSeconds(30);

    @BeforeEach
    public void setup(final TestInfo testInfo) {
        final String testId = safeUniqueTestName(testInfo);
        appId = "appId_" + testId;
        inputTopic = "input" + testId;
        IntegrationTestUtils.cleanStateBeforeTest(CLUSTER, inputTopic);

        builder = new StreamsBuilder();
        builder.stream(inputTopic);

        properties = mkObjectProperties(
            mkMap(
                mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, appId),
                mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
                mkEntry(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2),
                mkEntry(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class),
                mkEntry(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class),
                mkEntry(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000)
            )
        );
    }

    private void startStreamsAndWaitForRunning(final KafkaStreams kafkaStreams) throws InterruptedException {
        kafkaStreams.start();
        waitForRunning();
    }

    @AfterEach
    public void teardown() throws IOException {
        purgeLocalStreamsState(properties);
    }

    private void addStreamStateChangeListener(final KafkaStreams kafkaStreams) {
        kafkaStreams.setStateListener(
            (newState, oldState) -> stateTransitionHistory.add(newState)
        );
    }

    private void waitForRunning() throws InterruptedException {
        waitForCondition(
            () -> !stateTransitionHistory.isEmpty() &&
                stateTransitionHistory.get(stateTransitionHistory.size() - 1).equals(KafkaStreams.State.RUNNING),
            DEFAULT_DURATION.toMillis(),
            () -> String.format("Client did not transit to state %s in %d seconds",
                KafkaStreams.State.RUNNING, DEFAULT_DURATION.toMillis() / 1000)
        );
    }

    private void waitForTransitionFromRebalancingToRunning() throws InterruptedException {
        waitForRunning();

        final int historySize = stateTransitionHistory.size();
        assertThat("Client did not transit from REBALANCING to RUNNING. The observed state transitions are: " + stateTransitionHistory,
            historySize >= 2 &&
                stateTransitionHistory.get(historySize - 2).equals(KafkaStreams.State.REBALANCING) &&
                stateTransitionHistory.get(historySize - 1).equals(KafkaStreams.State.RUNNING), is(true));
    }

    @Test
    public void shouldAddStreamThread() throws Exception {
        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties)) {
            addStreamStateChangeListener(kafkaStreams);
            startStreamsAndWaitForRunning(kafkaStreams);

            final int oldThreadCount = kafkaStreams.metadataForLocalThreads().size();
            assertThat(kafkaStreams.metadataForLocalThreads().stream().map(t -> t.threadName().split("-StreamThread-")[1]).sorted().toArray(), equalTo(new String[] {"1", "2"}));

            stateTransitionHistory.clear();
            final Optional<String> name = kafkaStreams.addStreamThread();

            assertThat(name, not(Optional.empty()));
            TestUtils.waitForCondition(
                () -> kafkaStreams.metadataForLocalThreads().stream().sequential()
                    .map(ThreadMetadata::threadName).anyMatch(t -> t.equals(name.orElse(""))),
                "Wait for the thread to be added"
            );
            assertThat(kafkaStreams.metadataForLocalThreads().size(), equalTo(oldThreadCount + 1));
            assertThat(
                kafkaStreams
                    .metadataForLocalThreads()
                    .stream()
                    .map(t -> t.threadName().split("-StreamThread-")[1])
                    .sorted().toArray(),
                equalTo(new String[] {"1", "2", "3"})
            );

            waitForTransitionFromRebalancingToRunning();
        }
    }

    @Test
    public void shouldRemoveStreamThread() throws Exception {
        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties)) {
            addStreamStateChangeListener(kafkaStreams);
            startStreamsAndWaitForRunning(kafkaStreams);

            final int oldThreadCount = kafkaStreams.metadataForLocalThreads().size();
            stateTransitionHistory.clear();
            assertThat(kafkaStreams.removeStreamThread().get().split("-")[0], equalTo(appId));
            assertThat(kafkaStreams.metadataForLocalThreads().size(), equalTo(oldThreadCount - 1));

            waitForTransitionFromRebalancingToRunning();
        }
    }

    @Test
    public void shouldRemoveStreamThreadWithStaticMembership() throws Exception {
        properties.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "member-A");
        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties)) {
            addStreamStateChangeListener(kafkaStreams);
            startStreamsAndWaitForRunning(kafkaStreams);

            final int oldThreadCount = kafkaStreams.metadataForLocalThreads().size();
            stateTransitionHistory.clear();
            assertThat(kafkaStreams.removeStreamThread().get().split("-")[0], equalTo(appId));
            assertThat(kafkaStreams.metadataForLocalThreads().size(), equalTo(oldThreadCount - 1));

            waitForTransitionFromRebalancingToRunning();
        }
    }

    @Test
    public void shouldnNotRemoveStreamThreadWithinTimeout() throws Exception {
        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties)) {
            addStreamStateChangeListener(kafkaStreams);
            startStreamsAndWaitForRunning(kafkaStreams);
            assertThrows(TimeoutException.class, () -> kafkaStreams.removeStreamThread(Duration.ZERO.minus(DEFAULT_DURATION)));
        }
    }

    @Test
    public void shouldAddAndRemoveThreadsMultipleTimes() throws InterruptedException {
        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties)) {
            addStreamStateChangeListener(kafkaStreams);
            startStreamsAndWaitForRunning(kafkaStreams);

            final int oldThreadCount = kafkaStreams.metadataForLocalThreads().size();
            stateTransitionHistory.clear();

            final CountDownLatch latch = new CountDownLatch(2);
            final Thread one = adjustCountHelperThread(kafkaStreams, 4, latch);
            final Thread two = adjustCountHelperThread(kafkaStreams, 6, latch);
            two.start();
            one.start();
            latch.await(30, TimeUnit.SECONDS);
            assertThat(kafkaStreams.metadataForLocalThreads().size(), equalTo(oldThreadCount));

            waitForTransitionFromRebalancingToRunning();
        }
    }

    private Thread adjustCountHelperThread(final KafkaStreams kafkaStreams, final int count, final CountDownLatch latch) {
        return new Thread(() -> {
            for (int i = 0; i < count; i++) {
                kafkaStreams.addStreamThread();
                kafkaStreams.removeStreamThread();
            }
            latch.countDown();
        });
    }

    @Test
    public void testRebalanceHappensBeforeStreamThreadGetDown() throws Exception {
        try (final KafkaStreamsWrapper kafkaStreams = new KafkaStreamsWrapper(builder.build(), properties)) {
            addStreamStateChangeListener(kafkaStreams);
            startStreamsAndWaitForRunning(kafkaStreams);
            stateTransitionHistory.clear();

            final StreamThread thread = kafkaStreams.streamThreads().get(0);
            final StreamThread.StateListener listener = thread.getStateListener();
            final CountDownLatch latchBeforeDead = new CountDownLatch(1);
            thread.setStateListener((thread1, newState, oldState) -> {
                final StreamThread.State current = (StreamThread.State) newState;
                if (current == StreamThread.State.DEAD) {
                    try {
                        // block the pending shutdown thread to test whether other running thread
                        // can make kafka streams running
                        latchBeforeDead.await(DEFAULT_DURATION.toMillis(), TimeUnit.MILLISECONDS);
                    } catch (final InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                listener.onChange(thread1, newState, oldState);
            });
            final Optional<String> threadName = kafkaStreams.removeStreamThread();
            assertThat(threadName.isPresent(), Matchers.is(true));
            assertEquals(thread.getName(), threadName.get());
            waitForTransitionFromRebalancingToRunning();
            latchBeforeDead.countDown();
        }
    }

    @Test
    public void shouldAddAndRemoveStreamThreadsWhileKeepingNamesCorrect() throws Exception {
        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties)) {
            addStreamStateChangeListener(kafkaStreams);
            startStreamsAndWaitForRunning(kafkaStreams);

            int oldThreadCount = kafkaStreams.metadataForLocalThreads().size();
            stateTransitionHistory.clear();

            assertThat(
                kafkaStreams.metadataForLocalThreads()
                    .stream()
                    .map(t -> t.threadName().split("-StreamThread-")[1])
                    .sorted()
                    .toArray(),
                equalTo(new String[] {"1", "2"})
            );

            final Optional<String> name = kafkaStreams.addStreamThread();

            assertThat("New thread has index 3", "3".equals(name.get().split("-StreamThread-")[1]));
            TestUtils.waitForCondition(
                () -> kafkaStreams
                    .metadataForLocalThreads()
                    .stream().sequential()
                    .map(ThreadMetadata::threadName)
                    .anyMatch(t -> t.equals(name.get())),
                "Stream thread has not been added"
            );
            assertThat(kafkaStreams.metadataForLocalThreads().size(), equalTo(oldThreadCount + 1));
            assertThat(
                kafkaStreams
                    .metadataForLocalThreads()
                    .stream()
                    .map(t -> t.threadName().split("-StreamThread-")[1])
                    .sorted()
                    .toArray(),
                equalTo(new String[] {"1", "2", "3"})
            );
            waitForTransitionFromRebalancingToRunning();

            oldThreadCount = kafkaStreams.metadataForLocalThreads().size();
            stateTransitionHistory.clear();

            final Optional<String> removedThread = kafkaStreams.removeStreamThread();

            assertThat(removedThread, not(Optional.empty()));
            assertThat(kafkaStreams.metadataForLocalThreads().size(), equalTo(oldThreadCount - 1));
            waitForTransitionFromRebalancingToRunning();

            stateTransitionHistory.clear();

            final Optional<String> name2 = kafkaStreams.addStreamThread();

            assertThat(name2, not(Optional.empty()));
            TestUtils.waitForCondition(
                () -> kafkaStreams.metadataForLocalThreads().stream().sequential()
                    .map(ThreadMetadata::threadName).anyMatch(t -> t.equals(name2.orElse(""))),
                "Wait for the thread to be added"
            );
            assertThat(kafkaStreams.metadataForLocalThreads().size(), equalTo(oldThreadCount));
            assertThat(
                kafkaStreams
                    .metadataForLocalThreads()
                    .stream()
                    .map(t -> t.threadName().split("-StreamThread-")[1])
                    .sorted()
                    .toArray(),
                equalTo(new String[] {"1", "2", "3"})
            );

            assertThat("the new thread should have received the old threads name", name2.equals(removedThread));
            waitForTransitionFromRebalancingToRunning();
        }
    }

    @Test
    public void testConcurrentlyAccessThreads() throws InterruptedException {
        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties)) {
            addStreamStateChangeListener(kafkaStreams);
            startStreamsAndWaitForRunning(kafkaStreams);
            final int oldThreadCount = kafkaStreams.metadataForLocalThreads().size();
            final int threadCount = 5;
            final int loop = 3;
            final AtomicReference<Throwable> lastException = new AtomicReference<>();
            final ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            for (int threadIndex = 0; threadIndex < threadCount; ++threadIndex) {
                executor.execute(() -> {
                    try {
                        for (int i = 0; i < loop + 1; i++) {
                            if (kafkaStreams.addStreamThread().isEmpty())
                                throw new RuntimeException("failed to create stream thread");
                            kafkaStreams.metadataForLocalThreads();
                            if (kafkaStreams.removeStreamThread().isEmpty())
                                throw new RuntimeException("failed to delete a stream thread");
                        }
                    } catch (final Exception e) {
                        lastException.set(e);
                    }
                });
            }
            executor.shutdown();
            assertTrue(executor.awaitTermination(60, TimeUnit.SECONDS));
            assertNull(lastException.get());
            assertEquals(oldThreadCount, kafkaStreams.metadataForLocalThreads().size());
        }
    }

    @Test
    public void shouldResizeCacheAfterThreadRemovalTimesOut() throws InterruptedException {
        final long totalCacheBytes = 10L;
        final Properties props = new Properties();
        props.putAll(properties);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, totalCacheBytes);

        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props)) {
            addStreamStateChangeListener(kafkaStreams);
            startStreamsAndWaitForRunning(kafkaStreams);

            try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(KafkaStreams.class)) {
                assertThrows(TimeoutException.class, () -> kafkaStreams.removeStreamThread(Duration.ofSeconds(0)));

                for (final String log : appender.getMessages()) {
                    // all 10 bytes should be available for remaining thread
                    if (log.endsWith("Resizing thread cache due to thread removal, new cache size per thread is 10")) {
                        return;
                    }
                }
            }
        }
        fail();
    }

    @Test
    public void shouldResizeCacheAfterThreadReplacement() throws InterruptedException {
        final long totalCacheBytes = 10L;
        final Properties props = new Properties();
        props.putAll(properties);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, totalCacheBytes);

        final AtomicBoolean injectError = new AtomicBoolean(false);

        final StreamsBuilder builder  = new StreamsBuilder();
        final KStream<String, String> stream = builder.stream(inputTopic);
        stream.process(() -> new Processor<String, String, String, String>() {
            ProcessorContext<String, String> context;

            @Override
            public void init(final ProcessorContext<String, String> context) {
                this.context = context;
                context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
                    if (Thread.currentThread().getName().contains("StreamThread-1") && injectError.get()) {
                        injectError.set(false);
                        throw new RuntimeException("BOOM");
                    }
                });
            }

            @Override
            public void process(final Record<String, String> record) {
                context.forward(record);
            }
        });

        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props)) {
            addStreamStateChangeListener(kafkaStreams);
            kafkaStreams.setUncaughtExceptionHandler(e -> StreamThreadExceptionResponse.REPLACE_THREAD);
            startStreamsAndWaitForRunning(kafkaStreams);

            stateTransitionHistory.clear();
            try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister()) {
                injectError.set(true);
                waitForCondition(() -> !injectError.get(), "StreamThread did not hit and reset the injected error");

                waitForTransitionFromRebalancingToRunning();

                for (final String log : appender.getMessages()) {
                    // after we replace the thread there should be two remaining threads with 5 bytes each
                    if (log.endsWith("Adding StreamThread-3, there will now be 2 live threads and the new cache size per thread is 5")) {
                        return;
                    }
                }
            }
        }
        fail();
    }
}
