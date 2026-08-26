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
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.StateDirectory;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration coverage for KAFKA-20940: confirms against a real broker that a checkpointed,
 * windowed-store restore with a large gap engages the probe and skips the expired prefix, and that
 * a small gap leaves the gate closed (a plain {@code seek(checkpoint + 1)} as before the change).
 */
@Tag("integration")
public class CheckpointProbeSkipIntegrationTest {

    private static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);
    private static final Duration WINDOW_SIZE = Duration.ofSeconds(1);
    private static final Duration RETENTION = Duration.ofSeconds(30);
    private static final long OLD_TIMESTAMP = System.currentTimeMillis() - Duration.ofMinutes(10).toMillis();

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void stopCluster() {
        CLUSTER.stop();
    }

    @Test
    public void shouldSkipPastCheckpointWhenGapIsLarge() throws Exception {
        // 20,100 changelog offsets vs. the 16,384 PROBE_MIN_CHECKPOINT_GAP: the gate passes, the
        // probe should fire, and the seek should land near offset 20,000 where the fresh tail begins.
        final long startOffset = runScenario(20_000, 100, 4L);
        assertThat(startOffset, greaterThan(19_000L));
    }

    @Test
    public void shouldNotSkipWhenGapIsSmall() throws Exception {
        // 60 changelog offsets, well under the 16,384 gate: the probe never runs, so restore is a
        // plain seek(checkpoint + 1) exactly as on trunk.
        final long startOffset = runScenario(50, 10, 4L);
        assertThat(startOffset, equalTo(5L));
    }

    /**
     * Runs one instance to populate a windowed-store changelog with {@code numOld} stale records
     * followed by {@code numFresh} live ones, force-rewrites its on-disk checkpoint down to
     * {@code checkpointOffset}, restarts, and returns the offset the restore listener reports as
     * the actual restore start for the changelog partition.
     */
    private long runScenario(final long numOld, final long numFresh, final long checkpointOffset) throws Exception {
        final String appId = "cp-skip-" + UUID.randomUUID();
        final String inputTopic = appId + "-input";
        final String storeName = "windowed-store";
        final String changelogTopic = appId + "-" + storeName + "-changelog";
        CLUSTER.createTopic(inputTopic, 1, 1);

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId).getPath());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500L);
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        // Short session/heartbeat timeout: the second instance reuses the same consumer group right
        // after the first closes, and the default 45s session.timeout.ms leaves it blocked in
        // REBALANCING waiting for the first instance's stale member to expire.
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 500);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 1000);

        final StreamsBuilder builder = new StreamsBuilder();
        final AtomicLong processed = new AtomicLong(0);
        builder.stream(inputTopic, Consumed.with(Serdes.Integer(), Serdes.Integer()))
            .groupByKey()
            .windowedBy(TimeWindows.ofSizeWithNoGrace(WINDOW_SIZE))
            .count(Materialized.<Integer, Long, WindowStore<Bytes, byte[]>>as(storeName).withRetention(RETENTION))
            .toStream()
            .foreach((windowedKey, count) -> processed.incrementAndGet());
        final Topology topology = builder.build();

        final KafkaStreams first = new KafkaStreams(topology, props);
        first.start();
        try {
            produce(inputTopic, numOld, OLD_TIMESTAMP);
            produce(inputTopic, numFresh, System.currentTimeMillis());
            waitForCondition(() -> processed.get() >= numOld + numFresh, TimeUnit.MINUTES.toMillis(2),
                () -> "only processed " + processed.get() + " of " + (numOld + numFresh) + " records");
        } finally {
            first.close(Duration.ofSeconds(30));
        }

        final StateDirectory stateDirectory = new StateDirectory(new StreamsConfig(props), new MockTime(), true, false);
        new OffsetCheckpoint(new File(stateDirectory.getOrCreateDirectoryForTask(new TaskId(0, 0)), ".checkpoint"))
            .write(Collections.singletonMap(new TopicPartition(changelogTopic, 0), checkpointOffset));

        final AtomicLong reportedStartOffset = new AtomicLong(-1);
        final CountDownLatch restoreStarted = new CountDownLatch(1);
        final KafkaStreams second = new KafkaStreams(topology, props);
        second.setGlobalStateRestoreListener(new StateRestoreListener() {
            @Override
            public void onRestoreStart(final TopicPartition topicPartition, final String store,
                                       final long startingOffset, final long endingOffset) {
                if (topicPartition.topic().equals(changelogTopic)) {
                    reportedStartOffset.set(startingOffset);
                    restoreStarted.countDown();
                }
            }

            @Override
            public void onBatchRestored(final TopicPartition topicPartition, final String store,
                                        final long batchEndOffset, final long numRestored) {
            }

            @Override
            public void onRestoreEnd(final TopicPartition topicPartition, final String store,
                                     final long totalRestored) {
            }
        });
        second.start();
        try {
            assertTrue(restoreStarted.await(30, TimeUnit.SECONDS), "restore never started for " + changelogTopic);
        } finally {
            second.close(Duration.ofSeconds(30));
        }
        return reportedStartOffset.get();
    }

    private void produce(final String topic, final long count, final long timestamp) {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        try (KafkaProducer<Integer, Integer> producer = new KafkaProducer<>(props)) {
            for (long i = 0; i < count; i++) {
                producer.send(new ProducerRecord<>(topic, 0, timestamp, 1, 1));
            }
            producer.flush();
        }
    }
}
