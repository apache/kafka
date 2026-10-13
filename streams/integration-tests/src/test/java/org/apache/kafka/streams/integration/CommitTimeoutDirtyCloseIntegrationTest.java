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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.GroupProtocol;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TaskMetadata;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.test.faultproxy.FaultRule;
import org.apache.kafka.test.faultproxy.KafkaProtocolFaultProxy;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A commit whose EndTxn outlives {@code max.block.ms} twice (the commit itself, then the commit of the still-open
 * transaction in {@code TaskManager#handleCorruption}) leaves the producer owing a commit, so the abort in
 * {@code RecordCollectorImpl#closeDirty()} throws. The corrupted task is revived with a wiped store and, while it is
 * still restoring, a later commit on the same thread fails with {@code ILLEGAL_GENERATION}. The resulting
 * {@code TaskManager#handleLostAll()} closes the restoring task clean, which must not checkpoint changelog offsets
 * the record collector wrote before the failure.
 */
@Tag("integration")
@Timeout(600)
public class CommitTimeoutDirtyCloseIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(CommitTimeoutDirtyCloseIntegrationTest.class);
    private static final int NUM_PARTITIONS = 4;
    private static final int NUM_KEYS = 300;
    private static final String INPUT_TOPIC = "input";
    private static final String OUTPUT_TOPIC = "output";
    private static final String STORE = "store";
    private static final long PRODUCER_MAX_BLOCK_MS = 2_000L;
    // the timed-out commit and the handleCorruption commit both wait on the same pending EndTxn, so outlast both
    private static final Duration END_TXN_DELAY = Duration.ofMillis(3 * PRODUCER_MAX_BLOCK_MS);
    private static final Duration RESTORE_FETCH_DELAY = Duration.ofSeconds(2);
    private static final long WAIT_MS = 120_000L;

    private final Map<String, String> stateDirs = new HashMap<>();
    private final List<KafkaStreams> streamsToClose = new ArrayList<>();
    private EmbeddedKafkaCluster cluster;
    private KafkaProtocolFaultProxy proxy;
    private KafkaProducer<String, String> producer;
    private String appId;

    @BeforeEach
    public void setUp(final TestInfo testInfo) throws Exception {
        appId = "commit-timeout-dirty-close-" + safeUniqueTestName(testInfo);
        cluster = new EmbeddedKafkaCluster(1);
        cluster.start();
        cluster.createTopic(INPUT_TOPIC, NUM_PARTITIONS, 1);
        cluster.createTopic(OUTPUT_TOPIC, 1, 1);
        proxy = KafkaProtocolFaultProxy.inFrontOf(cluster.bootstrapServers());
        producer = new KafkaProducer<>(
            TestUtils.producerConfig(cluster.bootstrapServers(), StringSerializer.class, StringSerializer.class));
    }

    @AfterEach
    public void tearDown() {
        streamsToClose.forEach(streams -> streams.close(Duration.ofSeconds(30)));
        if (producer != null) {
            producer.close();
        }
        if (proxy != null) {
            proxy.close();
        }
        cluster.stop();
    }

    @Test
    public void shouldFullyRestoreStoreAfterAbortFailsOnTimedOutCommit() throws Exception {
        final RecordingRestoreListener instanceARestores = new RecordingRestoreListener();
        final KafkaStreams instanceA = streams("instance-a", instanceARestores);
        final KafkaStreams instanceB = streams("instance-b", null);
        IntegrationTestUtils.startApplicationAndWaitUntilRunning(List.of(instanceA, instanceB));
        TestUtils.waitForCondition(
            () -> activeTasks(instanceA).size() == 2 && activeTasks(instanceB).size() == 2,
            WAIT_MS,
            "the " + NUM_PARTITIONS + " tasks were never spread evenly over both instances"
        );
        // instance-a's thread owns both tasks: one gets corrupted, the other's commit later migrates the thread
        final List<TaskId> instanceATasks = activeTasks(instanceA);
        final TaskId corruptedTask = instanceATasks.get(0);
        final TaskId otherTask = instanceATasks.get(1);
        final String instanceAProducer = producerClientId(instanceA);

        final List<String> keys = new ArrayList<>();
        for (int i = 0; i < NUM_KEYS; i++) {
            keys.add("key-" + i);
        }
        produce(corruptedTask.partition(), keys);
        // read_committed, so the only EndTxn left to delay is the one for the trigger record
        waitForCommittedOutput();

        final FaultRule commitTimeout =
            proxy.delayOn(ApiKeys.END_TXN, END_TXN_DELAY).forClient(instanceAProducer).once();
        final FaultRule restoreDelay =
            proxy.delayOn(ApiKeys.FETCH, RESTORE_FETCH_DELAY).forClient(restoreConsumer("instance-a")).everyTime();
        instanceARestores.record();
        produce(corruptedTask.partition(), List.of("trigger"));

        TestUtils.waitForCondition(
            () -> instanceARestores.startingOffset() != -1L,
            WAIT_MS,
            "the store of " + corruptedTask + " was never restored after the commit timed out"
        );
        // a later commit acknowledges the late EndTxn; until then any send on this thread throws IllegalStateException
        final double committedBeforeAck = commitTimeNanos(instanceA, instanceAProducer);
        TestUtils.waitForCondition(
            () -> commitTimeNanos(instanceA, instanceAProducer) > committedBeforeAck,
            WAIT_MS,
            "instance-a never completed the commit whose EndTxn was delayed"
        );
        // instance-b has no state for the task, so restored progress makes instance-a the most caught-up client
        // and the assignor gives the task back to it after the migration
        TestUtils.waitForCondition(
            instanceARestores::restoredBatch,
            WAIT_MS,
            "instance-a never restored a batch of the wiped store of " + corruptedTask
        );
        final FaultRule generationMismatch = proxy.injectError(ApiKeys.TXN_OFFSET_COMMIT, Errors.ILLEGAL_GENERATION)
            .forClient(instanceAProducer)
            .once();
        produce(otherTask.partition(), List.of("other"));

        TestUtils.waitForCondition(
            instanceARestores::suspended,
            WAIT_MS,
            "instance-a's wiped store was never taken off the state updater before it finished restoring"
        );
        proxy.clearFaults();

        produce(corruptedTask.partition(), List.of("marker"));
        TestUtils.waitForCondition(
            () -> countPresentKeys(instanceA, List.of("marker")) == 1,
            WAIT_MS,
            "instance-a never processed the marker record for " + corruptedTask
        );
        final List<String> expectedKeys = new ArrayList<>(keys);
        expectedKeys.add("trigger");
        final int restoredKeys = countPresentKeys(instanceA, expectedKeys);

        assertAll(
            () -> assertEquals(
                1,
                commitTimeout.timesTriggered(),
                "the EndTxn delay should fire exactly once to time out the commit"
            ),
            () -> assertTrue(
                restoreDelay.timesTriggered() > 0,
                "the restore-fetch delay never matched " + restoreConsumer("instance-a")
                    + ", so the restore was never actually stalled; the internal restore-consumer client "
                    + "naming may have changed"
            ),
            () -> assertEquals(
                1,
                generationMismatch.timesTriggered(),
                "the ILLEGAL_GENERATION should fire exactly once to migrate instance-a's thread"
            ),
            () -> assertEquals(
                0L,
                instanceARestores.startingOffset(),
                "the store of " + corruptedTask + " should have been wiped and restored from the start of its changelog"
            ),
            () -> assertEquals(
                expectedKeys.size(),
                restoredKeys,
                "instance-a restored only " + restoredKeys + " of the " + expectedKeys.size() + " keys of "
                    + corruptedTask + " in " + STORE
            )
        );
    }

    private KafkaStreams streams(final String clientId, final StateRestoreListener restoreListener) {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .toTable(Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.String()))
            .toStream()
            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig(clientId));
        if (restoreListener != null) {
            streams.setGlobalStateRestoreListener(restoreListener);
        }
        streamsToClose.add(streams);
        return streams;
    }

    private Properties streamsConfig(final String clientId) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.CLIENT_ID_CONFIG, clientId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, proxy.bootstrapServers());
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDirs.computeIfAbsent(clientId, id -> TestUtils.tempDirectory().getPath()));
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0L);
        // scaled down with the data, so a wiped copy is not considered caught up
        props.put(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, 10L);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // a commit that outlives max.block.ms is what TaskExecutor turns into a TaskCorruptedException
        props.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_BLOCK_MS_CONFIG), PRODUCER_MAX_BLOCK_MS);
        // one changelog record per batch and small restore fetches keep the delayed restore consumer from catching up
        props.put(StreamsConfig.producerPrefix(ProducerConfig.BATCH_SIZE_CONFIG), 0);
        props.put(StreamsConfig.restoreConsumerPrefix(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG), 1024);
        props.put(StreamsConfig.restoreConsumerPrefix(ConsumerConfig.FETCH_MAX_BYTES_CONFIG), 1024);
        return props;
    }

    private void produce(final int partition, final List<String> keys) throws Exception {
        for (final String key : keys) {
            producer.send(new ProducerRecord<>(INPUT_TOPIC, partition, key, "value"),
                ((metadata, exception) -> {
                    if (exception != null) {
                        LOG.error("Failed to produce record", exception);
                    }
                }));
        }
    }

    private void waitForCommittedOutput() throws Exception {
        final Properties consumerConfig =
            TestUtils.consumerConfig(cluster.bootstrapServers(), StringDeserializer.class, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, OUTPUT_TOPIC, NUM_KEYS, WAIT_MS);
    }

    // only recorded once a commitTransaction() completes, not when it times out
    private static double commitTimeNanos(final KafkaStreams streams, final String producerClientId) {
        return (double) streams.metrics().entrySet().stream()
            .filter(e -> e.getKey().name().equals("txn-commit-time-ns-total")
                && producerClientId.equals(e.getKey().tags().get("client-id")))
            .map(e -> e.getValue().metricValue())
            .findFirst()
            .orElseThrow(() -> new AssertionError("no txn-commit-time-ns-total metric for " + producerClientId));
    }

    private static String producerClientId(final KafkaStreams streams) {
        final List<String> producerClientIds = streams.metadataForLocalThreads().stream()
            .flatMap(thread -> thread.producerClientIds().stream())
            .toList();
        assertEquals(1, producerClientIds.size(), "expected the single thread's producer, found " + producerClientIds);
        return producerClientIds.get(0);
    }

    private static List<TaskId> activeTasks(final KafkaStreams streams) {
        if (streams.state() != KafkaStreams.State.RUNNING) {
            return List.of();
        }
        return streams.metadataForLocalThreads().stream()
            .flatMap(thread -> thread.activeTasks().stream())
            .map(TaskMetadata::taskId)
            .sorted()
            .collect(Collectors.toList());
    }

    // stale queries reach the local copy of the store even while it is still restoring
    private static int countPresentKeys(final KafkaStreams streams, final List<String> keys) throws Exception {
        final ReadOnlyKeyValueStore<String, String> store =
            IntegrationTestUtils.getStore(STORE, streams, true, QueryableStoreTypes.keyValueStore());
        int present = 0;
        for (final String key : keys) {
            if (store.get(key) != null) {
                present++;
            }
        }
        return present;
    }

    private static String restoreConsumer(final String clientId) {
        return clientId + "-StateUpdater-1-restore-consumer";
    }

    // once recording, captures where the wiped store's restore starts, whether it restores a batch, and whether it is
    // suspended before finishing
    private static final class RecordingRestoreListener implements StateRestoreListener {
        private final AtomicLong startingOffset = new AtomicLong(-1L);
        private final AtomicBoolean suspended = new AtomicBoolean();
        private final AtomicBoolean restoredBatch = new AtomicBoolean();
        private volatile boolean recording;

        void record() {
            recording = true;
        }

        long startingOffset() {
            return startingOffset.get();
        }

        boolean suspended() {
            return suspended.get();
        }

        boolean restoredBatch() {
            return restoredBatch.get();
        }

        @Override
        public void onRestoreStart(final TopicPartition topicPartition,
                                   final String storeName,
                                   final long startingOffset,
                                   final long endingOffset) {
            if (recording) {
                this.startingOffset.compareAndSet(-1L, startingOffset);
            }
        }

        @Override
        public void onBatchRestored(final TopicPartition topicPartition,
                                    final String storeName,
                                    final long batchEndOffset,
                                    final long numRestored) {
            if (recording) {
                restoredBatch.set(true);
            }
        }

        @Override
        public void onRestoreEnd(final TopicPartition topicPartition, final String storeName, final long totalRestored) {
        }

        @Override
        public void onRestoreSuspended(final TopicPartition topicPartition, final String storeName, final long totalRestored) {
            if (recording) {
                suspended.set(true);
            }
        }
    }
}
