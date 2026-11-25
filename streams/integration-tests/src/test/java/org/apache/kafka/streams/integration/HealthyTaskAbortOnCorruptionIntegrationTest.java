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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.CloseOptions;
import org.apache.kafka.streams.GroupProtocol;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * One stream thread runs a stateless task 0_0 (input -> output) and a restoring stateful task 1_0. While the thread's
 * SyncGroup response is held, the restore of 1_0 fails with OFFSET_OUT_OF_RANGE, and closing 1_0 dirty aborts the
 * transaction holding 0_0's uncommitted outputs. Every input record below the committed input offset must have a
 * committed output.
 */
@Tag("integration")
@Timeout(120)
public class HealthyTaskAbortOnCorruptionIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(HealthyTaskAbortOnCorruptionIntegrationTest.class);
    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String STATE_INPUT = "state-input";
    private static final String STORE = "counts";
    private static final int STATE_KEYS = 200;
    private static final Duration RESTORE_FETCH_DELAY = Duration.ofMillis(300);
    private static final Duration SYNC_GROUP_HOLD = Duration.ofSeconds(4);
    private static final long WAIT_MS = 30_000L;

    private final List<KafkaStreams> streamsToClose = new ArrayList<>();
    // highest input offset 0_0 has processed, set on the stream thread
    private final AtomicLong lastProcessedInputOffset = new AtomicLong(-1L);
    private LogCaptureAppender appender;
    private EmbeddedKafkaCluster cluster;
    private KafkaProtocolFaultProxy proxy;
    private KafkaProducer<String, String> producer;
    private Admin admin;
    private String appId;

    @BeforeEach
    public void setUp(final TestInfo testInfo) throws Exception {
        appId = "healthy-task-abort-" + safeUniqueTestName(testInfo);
        cluster = new EmbeddedKafkaCluster(1);
        cluster.start();
        cluster.createTopic(INPUT, 1, 1);
        cluster.createTopic(OUTPUT, 1, 1);
        cluster.createTopic(STATE_INPUT, 1, 1);
        proxy = KafkaProtocolFaultProxy.inFrontOf(cluster.bootstrapServers());
        producer = new KafkaProducer<>(
            TestUtils.producerConfig(cluster.bootstrapServers(), StringSerializer.class, StringSerializer.class));
        admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers()));
    }

    @AfterEach
    public void tearDown() {
        streamsToClose.forEach(streams -> streams.close(Duration.ofSeconds(30)));
        if (appender != null) {
            appender.close();
        }
        if (producer != null) {
            producer.close();
        }
        if (admin != null) {
            admin.close();
        }
        if (proxy != null) {
            proxy.close();
        }
        cluster.stop();
    }

    @Test
    public void shouldNotCommitHealthyTaskInputPastAbortedOutputs() throws Exception {
        // Phase 1: build a changelog of STATE_KEYS records for 1_0, so restoring it in phase 2 takes many fetches.
        final KafkaStreams phase1 = streams("phase1", null);
        phase1.start();
        for (int i = 0; i < STATE_KEYS; i++) {
            producer.send(new ProducerRecord<>(STATE_INPUT, 0, "k-" + i, "v"));
        }
        producer.flush();
        TestUtils.waitForCondition(() -> committedOffset(new TopicPartition(STATE_INPUT, 0)) >= STATE_KEYS,
            WAIT_MS, "phase 1 never committed the state input");
        // Leave the group so phase 2 does not wait out phase 1's session timeout.
        phase1.close(CloseOptions.groupMembershipOperation(CloseOptions.GroupMembershipOperation.LEAVE_GROUP)
            .withTimeout(Duration.ofSeconds(30)));

        // Phase 2: a new state dir makes 1_0 restore from scratch, and the delayed restore fetches keep it restoring
        // while 0_0 processes on the same thread.
        final FaultRule restoreDelay = proxy.delayOn(ApiKeys.FETCH, RESTORE_FETCH_DELAY)
            .forClient(restoreConsumer()).everyTime();
        final RestoreTracker restores = new RestoreTracker();
        final KafkaStreams phase2 = streams("phase2", restores);

        final AtomicBoolean producing = new AtomicBoolean(true);
        final AtomicLong produced = new AtomicLong();
        final Thread inputProducer = new Thread(() -> {
            while (producing.get()) {
                producer.send(new ProducerRecord<>(INPUT, 0, "key", "v"));
                produced.incrementAndGet();
                Time.SYSTEM.sleep(2);
            }
        }, "input-producer");

        appender = LogCaptureAppender.createAndRegister();
        phase2.start();
        inputProducer.start();

        final TopicPartition input = new TopicPartition(INPUT, 0);
        TestUtils.waitForCondition(() -> restores.started.get() && committedOffset(input) > 200,
            WAIT_MS, "0_0 never committed while 1_0 was restoring");
        assertFalse(restores.ended.get(), "1_0 finished restoring before the fault could be injected");

        // Adding a thread triggers a rebalance. Holding thread-1's SyncGroup response keeps it mid-rebalance, where it
        // keeps processing 0_0 but skips commits.
        final FaultRule syncHold = proxy.delayOn(ApiKeys.SYNC_GROUP, SYNC_GROUP_HOLD)
            .forClient("phase2-StreamThread-1-consumer").once();
        phase2.addStreamThread();
        TestUtils.waitForCondition(() -> syncHold.timesTriggered() == 1, WAIT_MS,
            "thread-1's SyncGroup response was never held");

        // Commits are skipped until thread-1's SyncGroup completes, so once 0_0 has processed past the committed offset,
        // the open transaction holds outputs that the corruption will abort.
        final long committedAtHold = waitForCommittedOffset(input);
        TestUtils.waitForCondition(() -> lastProcessedInputOffset.get() >= committedAtHold, WAIT_MS,
            "0_0 processed nothing past committed offset " + committedAtHold + " during the rebalance");
        assertFalse(restores.ended.get(), "1_0 finished restoring before the corruption was injected");

        // While the rebalance is in progress, corrupt the restoring task.
        restoreDelay.remove();
        final FaultRule outOfRange = proxy.injectError(ApiKeys.FETCH, Errors.OFFSET_OUT_OF_RANGE)
            .forClient(restoreConsumer()).once();
        TestUtils.waitForCondition(() -> outOfRange.timesTriggered() == 1, WAIT_MS,
            "the restore fetch of 1_0 never got OFFSET_OUT_OF_RANGE");

        final long processedAtCorruption = lastProcessedInputOffset.get();
        TestUtils.waitForCondition(() -> committedOffset(input) > processedAtCorruption, WAIT_MS,
            "the app never committed past input offset " + processedAtCorruption + " after the corruption");
        producing.set(false);
        inputProducer.join();
        producer.flush();
        final long endOffset = produced.get();
        TestUtils.waitForCondition(() -> committedOffset(input) == endOffset, WAIT_MS,
            "the app never committed all " + endOffset + " input records");

        final long committed = waitForCommittedOffset(input);
        final Set<Long> inputOffsetsInOutput = inputOffsetsInOutput();
        final TreeSet<Long> missing = new TreeSet<>();
        for (long offset = 0; offset < committed; offset++) {
            if (!inputOffsetsInOutput.contains(offset)) {
                missing.add(offset);
            }
        }
        LOG.info("committed input offset {}, in output {}, missing {}", committed, inputOffsetsInOutput.size(), missing.size());

        assertAll(
            () -> assertTrue(corruptionHandledDuringRebalance(),
                "thread-1 did not handle the corruption of 1_0 between joining and syncing the group"),
            () -> assertEquals(0, missing.size(),
                "input offset " + committed + " was committed, but " + missing.size() + " input records have no "
                    + "committed output" + (missing.isEmpty() ? "" : " (offsets " + missing.first() + "-"
                    + missing.last() + ")"))
        );
    }

    private KafkaStreams streams(final String clientId, final StateRestoreListener restoreListener) {
        final StreamsBuilder builder = new StreamsBuilder();
        // 0_0: forwards each record with its input offset as the value, so each output can be matched to its input.
        builder.stream(INPUT, Consumed.with(Serdes.String(), Serdes.String()))
            .processValues(() -> new ContextualFixedKeyProcessor<String, String, String>() {
                @Override
                public void process(final FixedKeyRecord<String, String> record) {
                    final long offset = context().recordMetadata().orElseThrow().offset();
                    context().forward(record.withValue(Long.toString(offset)));
                    lastProcessedInputOffset.accumulateAndGet(offset, Math::max);
                }
            })
            .to(OUTPUT, Produced.with(Serdes.String(), Serdes.String()));
        // 1_0: stateful; its changelog restore is the one that gets corrupted.
        builder.stream(STATE_INPUT, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey()
            .count(Materialized.as(STORE));

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
        props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0L);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.mainConsumerPrefix(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG), 500);
        // One record per changelog batch and one batch per restore fetch, so the delayed restore lasts for many fetches.
        props.put(StreamsConfig.producerPrefix(ProducerConfig.BATCH_SIZE_CONFIG), 0);
        props.put(StreamsConfig.restoreConsumerPrefix(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG), 1);
        props.put(StreamsConfig.restoreConsumerPrefix(ConsumerConfig.FETCH_MAX_BYTES_CONFIG), 1);
        return props;
    }

    private long committedOffset(final TopicPartition partition) throws Exception {
        final OffsetAndMetadata offset = admin.listConsumerGroupOffsets(appId, new ListConsumerGroupOffsetsOptions().requireStable(true))
            .partitionsToOffsetAndMetadata().get().get(partition);
        return offset == null ? -1L : offset.offset();
    }

    private long waitForCommittedOffset(final TopicPartition partition) throws Exception {
        final AtomicLong offset = new AtomicLong(-1L);
        TestUtils.waitForCondition(() -> {
            offset.set(committedOffset(partition));
            return offset.get() >= 0;
        }, WAIT_MS, "no stable committed offset for " + partition);
        return offset.get();
    }

    private Set<Long> inputOffsetsInOutput() {
        final Properties config =
            TestUtils.consumerConfig(cluster.bootstrapServers(), StringDeserializer.class, StringDeserializer.class);
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        final Set<Long> offsets = new TreeSet<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            final TopicPartition output = new TopicPartition(OUTPUT, 0);
            consumer.assign(List.of(output));
            consumer.seekToBeginning(List.of(output));
            final long end = consumer.endOffsets(List.of(output)).get(output);
            while (consumer.position(output) < end) {
                for (final ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    offsets.add(Long.parseLong(record.value()));
                }
            }
        }
        return offsets;
    }

    // rebalanceInProgress is set before JoinGroup is sent and cleared after SyncGroup completes, so a corruption logged
    // between thread-1's "joined" and "synced" lines was handled while the flag was set.
    private boolean corruptionHandledDuringRebalance() {
        boolean joinedNotSynced = false;
        for (final String message : appender.getMessages()) {
            if (message.contains("clientId=phase2-StreamThread-1-consumer,")) {
                if (message.contains("Successfully joined group")) {
                    joinedNotSynced = true;
                } else if (message.contains("Successfully synced group")) {
                    joinedNotSynced = false;
                }
            } else if (message.contains("[phase2-StreamThread-1]")
                && message.contains("Detected the states of tasks [1_0] are corrupted")) {
                return joinedNotSynced;
            }
        }
        return false;
    }

    private static String restoreConsumer() {
        return "phase2" + "-StateUpdater-1-restore-consumer";
    }

    private static final class RestoreTracker implements StateRestoreListener {
        private final AtomicBoolean started = new AtomicBoolean();
        private final AtomicBoolean ended = new AtomicBoolean();

        @Override
        public void onRestoreStart(final TopicPartition topicPartition, final String storeName,
                                   final long startingOffset, final long endingOffset) {
            started.set(true);
        }

        @Override
        public void onBatchRestored(final TopicPartition topicPartition, final String storeName,
                                    final long batchEndOffset, final long numRestored) {
        }

        @Override
        public void onRestoreEnd(final TopicPartition topicPartition, final String storeName, final long totalRestored) {
            ended.set(true);
        }
    }
}
