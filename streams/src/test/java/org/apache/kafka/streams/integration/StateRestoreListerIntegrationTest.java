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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.StateRestoreListener;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class StateRestoreListerIntegrationTest {
    private static final Duration RESTORATION_DELAY = Duration.ofSeconds(1);

    @Rule
    public Timeout globalTimeout = Timeout.seconds(600);

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(3);

    @BeforeClass
    public static void startCluster() throws Exception {
        CLUSTER.start();
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }

    private final MockTime mockTime = CLUSTER.time;

    private String inputTopic;

    private String outputTopic;

    private String safeTestName;

    private Properties baseConfiguration;

    private KafkaStreams kafkaStreams1;

    private KafkaStreams kafkaStreams2;

    @Rule
    public TestName testName = new TestName();

    private List<Properties> streamsConfigurations;

    @Before
    public void before() throws Exception {
        baseConfiguration = new Properties();
        streamsConfigurations = new ArrayList<>();

        safeTestName = safeUniqueTestName(getClass(), testName);

        inputTopic = "input-topic-" + safeTestName;
        outputTopic = "output-topic-" + safeTestName;

        CLUSTER.createTopic(inputTopic, 3, 1);
        CLUSTER.createTopic(outputTopic, 3, 1);

        final String applicationId = "app-" + safeTestName;
        baseConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        baseConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        baseConfiguration.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        baseConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        baseConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        baseConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());

        // Making restore consumer intentionally slow
        baseConfiguration.put(StreamsConfig.restoreConsumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), 10);
    }

    @After
    public void whenShuttingDown() throws Exception {
        Stream.of(kafkaStreams1, kafkaStreams2)
              .filter(Objects::nonNull)
              .forEach(KafkaStreams::close);

        IntegrationTestUtils.purgeLocalStreamsState(streamsConfigurations);
    }

    @Test
    public void shouldInvokeUserDefinedGlobalStateRestoreListener() throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(EARLIEST))
               .groupByKey()
               .reduce((oldVal, newVal) -> newVal)
               .toStream()
               .to(outputTopic);

        final List<KeyValue<Integer, Integer>> sampleData = IntStream.range(0, 100)
                                                                     .mapToObj(i -> new KeyValue<>(i, i))
                                                                     .collect(Collectors.toList());

        sendEvents(inputTopic, sampleData);

        final TestStateRestoreListener kafkaStreams1StateRestoreListener = new TestStateRestoreListener(RESTORATION_DELAY);
        kafkaStreams1 = startKafkaStreamsAndAwaitUntilRunning(builder, "ks-1", kafkaStreams1StateRestoreListener);

        validateReceivedMessages(sampleData, outputTopic);

        // Close kafkaStreams1 (with cleanup) and start it again to force the restoration of the state.
        kafkaStreams1.close(Duration.ofMillis(IntegrationTestUtils.DEFAULT_TIMEOUT));
        kafkaStreams1.cleanUp();
        kafkaStreams1 = startKafkaStreamsAndAwaitUntilRunning(builder, "ks-1", kafkaStreams1StateRestoreListener);

        assertTrue(kafkaStreams1StateRestoreListener.awaitUntilRestorationStarts());
        assertTrue(kafkaStreams1StateRestoreListener.awaitUntilBatchRestoredIsCalled());

        // Simulate a new instance joining in the middle of the restoration.
        // When this happens, some of the partitions that kafkaStreams1 was restoring will be migrated to kafkaStreams2,
        // and kafkaStreams1 must call StateRestoreListener#onRestoreSuspended.
        final TestStateRestoreListener kafkaStreams2StateRestoreListener = new TestStateRestoreListener(RESTORATION_DELAY);
        kafkaStreams2 = startKafkaStreamsAndAwaitUntilRunning(builder, "ks-2", kafkaStreams2StateRestoreListener);
        assertTrue(kafkaStreams1StateRestoreListener.awaitUntilRestorationSuspends());

        assertTrue(kafkaStreams2StateRestoreListener.awaitUntilRestorationStarts());

        assertTrue(kafkaStreams1StateRestoreListener.awaitUntilRestorationEnds());
        assertTrue(kafkaStreams2StateRestoreListener.awaitUntilRestorationEnds());
    }

    private void validateReceivedMessages(final List<KeyValue<Integer, Integer>> expectedRecords,
                                          final String outputTopic) throws Exception {
        final Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-" + safeTestName);
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.setProperty(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            IntegerDeserializer.class.getName()
        );
        consumerProperties.setProperty(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            IntegerDeserializer.class.getName()
        );

        IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(
            consumerProperties,
            outputTopic,
            expectedRecords
        );
    }

    private KafkaStreams startKafkaStreamsAndAwaitUntilRunning(final StreamsBuilder streamsBuilder,
                                                               final String instanceId,
                                                               final StateRestoreListener stateRestoreListener) {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.putAll(baseConfiguration);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath() + "-" + instanceId);
        streamsConfiguration.put(StreamsConfig.consumerPrefix(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG), instanceId);
        streamsConfigurations.add(streamsConfiguration);

        final KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamsConfiguration);

        kafkaStreams.setGlobalStateRestoreListener(stateRestoreListener);
        startKafkaStreamsAndAwaitUntilRunning(kafkaStreams);

        return kafkaStreams;
    }

    private void startKafkaStreamsAndAwaitUntilRunning(final KafkaStreams kafkaStreams) {
        final CountDownLatch latch = new CountDownLatch(1);

        kafkaStreams.setStateListener((newState, oldState) -> {
            if (KafkaStreams.State.RUNNING == newState) {
                latch.countDown();
            }
        });

        kafkaStreams.start();
    }

    private static final class TestStateRestoreListener implements StateRestoreListener {
        private final Duration onBatchRestoredSleepDuration;

        private final CountDownLatch onRestoreStartLatch = new CountDownLatch(1);
        private final CountDownLatch onRestoreEndLatch = new CountDownLatch(1);
        private final CountDownLatch onRestoreSuspendedLatch = new CountDownLatch(1);
        private final CountDownLatch onBatchRestoredLatch = new CountDownLatch(1);

        TestStateRestoreListener(final Duration onBatchRestoredSleepDuration) {
            this.onBatchRestoredSleepDuration = onBatchRestoredSleepDuration;
        }

        boolean awaitUntilRestorationStarts() throws InterruptedException {
            return awaitLatchWithTimeout(onRestoreStartLatch);
        }

        boolean awaitUntilRestorationSuspends() throws InterruptedException {
            return awaitLatchWithTimeout(onRestoreSuspendedLatch);
        }

        boolean awaitUntilRestorationEnds() throws InterruptedException {
            return awaitLatchWithTimeout(onRestoreEndLatch);
        }

        public boolean awaitUntilBatchRestoredIsCalled() throws InterruptedException {
            return awaitLatchWithTimeout(onBatchRestoredLatch);
        }

        @Override
        public void onRestoreStart(final TopicPartition topicPartition,
                                   final String storeName,
                                   final long startingOffset,
                                   final long endingOffset) {
            onRestoreStartLatch.countDown();
        }

        @Override
        public void onBatchRestored(final TopicPartition topicPartition,
                                    final String storeName,
                                    final long batchEndOffset,
                                    final long numRestored) {
            Utils.sleep(onBatchRestoredSleepDuration.toMillis());
            onBatchRestoredLatch.countDown();
        }

        @Override
        public void onRestoreEnd(final TopicPartition topicPartition,
                                 final String storeName,
                                 final long totalRestored) {
            onRestoreEndLatch.countDown();
        }

        @Override
        public void onRestoreSuspended(final TopicPartition topicPartition,
                                       final String storeName,
                                       final long totalRestored) {
            onRestoreSuspendedLatch.countDown();
        }

        private static boolean awaitLatchWithTimeout(final CountDownLatch latch) throws InterruptedException {
            return latch.await(IntegrationTestUtils.DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
        }
    }

    private void sendEvents(final String topic, final List<KeyValue<Integer, Integer>> events) {
        IntegrationTestUtils.produceKeyValuesSynchronously(
            topic,
            events,
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                IntegerSerializer.class,
                new Properties()
            ),
            mockTime
        );
    }
}
