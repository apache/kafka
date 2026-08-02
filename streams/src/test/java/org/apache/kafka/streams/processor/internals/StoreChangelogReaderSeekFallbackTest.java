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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager.StateStoreMetadata;
import org.apache.kafka.test.MockStandbyUpdateListener;
import org.apache.kafka.test.MockStateRestoreListener;
import org.apache.kafka.test.StreamsTestUtils;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.streams.processor.internals.Task.TaskType.ACTIVE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Repro for the silent {@code seekToBeginning} fallbacks in the KAFKA-13499 /
 * PR #22115 windowed-restore optimisation ({@code seekNewPartitions} /
 * {@code seekByRetentionFromPolledRecords}).
 *
 * <p>Context: on the KIP-892/1035 soaks, the stack running 4.3 + #22115
 * ({@code TEST}) shows MORE restore {@code OffsetOutOfRangeException} than the
 * 4.3 baseline over an identical window (36 vs 21 events), with the entire
 * increase concentrated in the stream-stream join window stores (10 vs 3) — the
 * very stores whose retention (~2s) should make the optimisation work best.
 * The {@code KafkaException} fallback logs at WARN and is confirmed 0 in those
 * logs, so the suspicion falls on the fallbacks that log only at
 * {@code log.debug} and are therefore invisible in production:
 *
 * <ol>
 *   <li>the probe {@code poll(pollTime)} returning no records for a partition
 *       ({@code records.isEmpty()} at StoreChangelogReader:1127);</li>
 *   <li>{@code offsetsForTimes} returning {@code null} for a partition;</li>
 *   <li>{@code seekTimestamp <= 0}.</li>
 * </ol>
 *
 * <p>(1) is not covered by the three tests #22115 shipped — those all schedule a
 * record onto the probe poll, so they only exercise the populated path and the
 * {@code offsetsForTimes}-returns-null path.
 *
 * <p>These are <em>characterisation</em> tests: they assert what the code
 * currently does, so that the silent degradation is pinned down and visible. A
 * failure here means the model of the code in the soak write-up is wrong, not
 * that Kafka is.
 */
public class StoreChangelogReaderSeekFallbackTest {

    private static final long RETENTION_MS = Duration.ofSeconds(2).toMillis();
    private static final long LATEST_RECORD_TIMESTAMP = 10_000_000L;
    private static final long END_OFFSET = 100L;
    private static final long BEGINNING_OFFSET = 0L;
    private static final long OFFSET_FOR_TIMESTAMP = 42L;

    private final String storeName = "store";
    private final LogContext logContext = new LogContext("seek-fallback-repro ");
    private final StreamsConfig config =
        new StreamsConfig(StreamsTestUtils.getStreamsConfig("seek-fallback-repro"));
    private final MockTime time = new MockTime();
    private final MockStateRestoreListener callback = new MockStateRestoreListener();
    private final MockStandbyUpdateListener standbyListener = new MockStandbyUpdateListener();
    private final MockAdminClient adminClient = new MockAdminClient();

    /** Records which partitions {@code offsetsForTimes} was actually asked about. */
    private final List<TopicPartition> offsetsForTimesQueries = new ArrayList<>();
    /** Every set of partitions passed to {@code pause}, in order. */
    private final List<Set<TopicPartition>> pauseCalls = new ArrayList<>();
    private int pollCount;
    private int endOffsetsCount;

    private MockConsumer<byte[], byte[]> consumerReturning(final Map<TopicPartition, Long> offsets) {
        return new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name()) {
            @Override
            public synchronized Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
                    final Map<TopicPartition, Long> timestampsToSearch) {
                final Map<TopicPartition, OffsetAndTimestamp> result = new HashMap<>();
                timestampsToSearch.forEach((partition, timestamp) -> {
                    offsetsForTimesQueries.add(partition);
                    final Long offset = offsets.get(partition);
                    result.put(partition, offset == null ? null : new OffsetAndTimestamp(offset, timestamp));
                });
                return result;
            }

            @Override
            public synchronized ConsumerRecords<byte[], byte[]> poll(final Duration timeout) {
                pollCount++;
                return super.poll(timeout);
            }

            @Override
            public synchronized Map<TopicPartition, Long> endOffsets(final Collection<TopicPartition> partitions) {
                endOffsetsCount++;
                return super.endOffsets(partitions);
            }

            @Override
            public synchronized void pause(final Collection<TopicPartition> partitions) {
                pauseCalls.add(new HashSet<>(partitions));
                super.pause(partitions);
            }
        };
    }

    /** Same as {@link #windowedStateManagerFor} but non-windowed: retentionPeriod -1. */
    private ProcessorStateManager plainStateManagerFor(final TopicPartition partition, final TaskId taskId) {
        final StateStoreMetadata metadata = mock(StateStoreMetadata.class);
        final ProcessorStateManager stateManager = mock(ProcessorStateManager.class);
        final StateStore store = mock(StateStore.class);
        when(metadata.changelogPartition()).thenReturn(partition);
        when(metadata.store()).thenReturn(store);
        when(metadata.offset()).thenReturn(null);
        when(metadata.retentionPeriod()).thenReturn(-1L);
        when(store.name()).thenReturn(storeName);
        when(stateManager.storeMetadata(partition)).thenReturn(metadata);
        when(stateManager.taskType()).thenReturn(ACTIVE);
        when(stateManager.taskId()).thenReturn(taskId);
        return stateManager;
    }

    /** A windowed (finite-retention) store with no checkpoint — the #22115 path. */
    private ProcessorStateManager windowedStateManagerFor(final TopicPartition partition, final TaskId taskId) {
        final StateStoreMetadata metadata = mock(StateStoreMetadata.class);
        final ProcessorStateManager stateManager = mock(ProcessorStateManager.class);
        final StateStore store = mock(StateStore.class);
        when(metadata.changelogPartition()).thenReturn(partition);
        when(metadata.store()).thenReturn(store);
        when(metadata.offset()).thenReturn(null);
        when(metadata.retentionPeriod()).thenReturn(RETENTION_MS);
        when(store.name()).thenReturn(storeName);
        when(stateManager.storeMetadata(partition)).thenReturn(metadata);
        when(stateManager.taskType()).thenReturn(ACTIVE);
        when(stateManager.taskId()).thenReturn(taskId);
        return stateManager;
    }

    private ConsumerRecord<byte[], byte[]> recordAtHead(final TopicPartition partition) {
        return new ConsumerRecord<>(
            partition.topic(), partition.partition(), END_OFFSET - 1,
            LATEST_RECORD_TIMESTAMP, TimestampType.CREATE_TIME,
            0, 0, new byte[0], new byte[0],
            new RecordHeaders(), Optional.empty());
    }

    /**
     * Baseline / control: when the probe poll DOES return a record, the
     * optimisation works and the consumer lands on the offsetsForTimes result.
     * This is #22115's happy path, restated here so the contrast below is
     * unambiguous within one test class.
     */
    @Test
    public void probePollWithARecordSeeksByTimestamp() {
        final TopicPartition tp = new TopicPartition("topic", 0);
        final MockConsumer<byte[], byte[]> consumer =
            consumerReturning(Collections.singletonMap(tp, OFFSET_FOR_TIMESTAMP));
        consumer.updateBeginningOffsets(Collections.singletonMap(tp, BEGINNING_OFFSET));
        consumer.updateEndOffsets(Collections.singletonMap(tp, END_OFFSET));
        adminClient.updateEndOffsets(Collections.singletonMap(tp, END_OFFSET));

        // a record IS available on the probe poll
        consumer.schedulePollTask(() -> consumer.addRecord(recordAtHead(tp)));

        final TaskId taskId = new TaskId(0, 0);
        final StoreChangelogReader reader = new StoreChangelogReader(
            time, config, logContext, adminClient, consumer, callback, standbyListener);
        reader.register(tp, windowedStateManagerFor(tp, taskId));
        reader.restore(Collections.singletonMap(taskId, mock(Task.class)));

        assertEquals(OFFSET_FOR_TIMESTAMP, consumer.position(tp),
            "with a record on the probe poll the optimisation should seek by timestamp");
        assertTrue(offsetsForTimesQueries.contains(tp),
            "offsetsForTimes should have been consulted");
    }

    /**
     * THE REPRO. Identical setup, except the probe {@code poll(pollTime)} returns
     * nothing for the partition. {@code records.isEmpty()} is then true, so
     * {@code seekByRetentionFromPolledRecords} adds the partition to
     * {@code seekToBeginningPartitions} and the restore starts from log-start —
     * exactly what un-patched 4.3 does — while logging only at {@code debug}.
     *
     * <p>Note {@code offsetsForTimes} is never even consulted, so a broker that
     * would have answered perfectly well is not asked.
     */
    @Test
    public void probePollWithNoRecordsSilentlyFallsBackToBeginning() {
        final TopicPartition tp = new TopicPartition("topic", 0);
        final MockConsumer<byte[], byte[]> consumer =
            consumerReturning(Collections.singletonMap(tp, OFFSET_FOR_TIMESTAMP));
        consumer.updateBeginningOffsets(Collections.singletonMap(tp, BEGINNING_OFFSET));
        consumer.updateEndOffsets(Collections.singletonMap(tp, END_OFFSET));
        adminClient.updateEndOffsets(Collections.singletonMap(tp, END_OFFSET));

        // deliberately NO schedulePollTask: the probe poll comes back empty,
        // which is what a 100ms poll.ms does under load with many partitions.

        final TaskId taskId = new TaskId(0, 0);
        final StoreChangelogReader reader = new StoreChangelogReader(
            time, config, logContext, adminClient, consumer, callback, standbyListener);
        reader.register(tp, windowedStateManagerFor(tp, taskId));
        reader.restore(Collections.singletonMap(taskId, mock(Task.class)));

        assertEquals(BEGINNING_OFFSET, consumer.position(tp),
            "an empty probe poll silently degrades the optimisation to seekToBeginning");
        assertTrue(offsetsForTimesQueries.isEmpty(),
            "offsetsForTimes is not even consulted when the probe poll returns nothing");
    }

    /**
     * The soak-shaped case: several windowed partitions are initialised in one
     * batch and share a SINGLE probe poll. A record is available for only one of
     * them, so that one is optimised and the rest fall back to log-start.
     *
     * <p>On the soak a task carries 9-12 stores, most of them windowed, all
     * prepared together — so partial population of one {@code poll(100ms)} is the
     * expected case rather than an edge case.
     */
    @Test
    public void partitionsMissingFromASharedProbePollFallBackWhileOthersOptimise() {
        final TopicPartition populated = new TopicPartition("populated", 0);
        final TopicPartition starved = new TopicPartition("starved", 0);

        final Map<TopicPartition, Long> forTimes = new HashMap<>();
        forTimes.put(populated, OFFSET_FOR_TIMESTAMP);
        forTimes.put(starved, OFFSET_FOR_TIMESTAMP);
        final MockConsumer<byte[], byte[]> consumer = consumerReturning(forTimes);

        final Map<TopicPartition, Long> beginnings = new HashMap<>();
        final Map<TopicPartition, Long> ends = new HashMap<>();
        for (final TopicPartition tp : List.of(populated, starved)) {
            beginnings.put(tp, BEGINNING_OFFSET);
            ends.put(tp, END_OFFSET);
        }
        consumer.updateBeginningOffsets(beginnings);
        consumer.updateEndOffsets(ends);
        adminClient.updateEndOffsets(ends);

        // only ONE of the two partitions yields a record on the shared probe poll
        consumer.schedulePollTask(() -> consumer.addRecord(recordAtHead(populated)));

        final TaskId populatedTask = new TaskId(0, 0);
        final TaskId starvedTask = new TaskId(0, 1);
        final StoreChangelogReader reader = new StoreChangelogReader(
            time, config, logContext, adminClient, consumer, callback, standbyListener);
        reader.register(populated, windowedStateManagerFor(populated, populatedTask));
        reader.register(starved, windowedStateManagerFor(starved, starvedTask));

        final Map<TaskId, Task> tasks = new HashMap<>();
        tasks.put(populatedTask, mock(Task.class));
        tasks.put(starvedTask, mock(Task.class));
        reader.restore(tasks);

        assertEquals(OFFSET_FOR_TIMESTAMP, consumer.position(populated),
            "the partition with a record on the shared poll is optimised");
        assertEquals(BEGINNING_OFFSET, consumer.position(starved),
            "the partition absent from the same poll silently restores from log-start");
    }

    /**
     * Quantifies what the windowed path costs even when it ends up at exactly the
     * same offset as the un-patched code. A windowed no-checkpoint partition that
     * falls back to log-start still pays an {@code endOffsets()} broker call and a
     * probe {@code poll(poll.ms)} that a non-windowed partition does not.
     *
     * <p>This is the candidate explanation for {@code TEST} showing MORE OOORE
     * than 4-3 rather than the same: the destination is identical, but the restore
     * starts later, and at log-start there is zero margin before retention laps
     * the position.
     */
    @Test
    public void windowedFallbackStillPaysAnExtraPollAndBrokerCallVersusNonWindowed() {
        final TopicPartition tp = new TopicPartition("topic", 0);

        // --- non-windowed control: straight to seekToBeginning, no probe ---
        final MockConsumer<byte[], byte[]> plain = consumerReturning(Collections.emptyMap());
        plain.updateBeginningOffsets(Collections.singletonMap(tp, BEGINNING_OFFSET));
        plain.updateEndOffsets(Collections.singletonMap(tp, END_OFFSET));
        adminClient.updateEndOffsets(Collections.singletonMap(tp, END_OFFSET));
        final TaskId plainTask = new TaskId(0, 0);
        final StoreChangelogReader plainReader = new StoreChangelogReader(
            time, config, logContext, adminClient, plain, callback, standbyListener);
        plainReader.register(tp, plainStateManagerFor(tp, plainTask));
        plainReader.restore(Collections.singletonMap(plainTask, mock(Task.class)));
        final int plainPolls = pollCount;
        final int plainEndOffsets = endOffsetsCount;

        // --- windowed, probe poll empty so it lands on the SAME offset ---
        pollCount = 0;
        endOffsetsCount = 0;
        final MockConsumer<byte[], byte[]> windowed =
            consumerReturning(Collections.singletonMap(tp, OFFSET_FOR_TIMESTAMP));
        windowed.updateBeginningOffsets(Collections.singletonMap(tp, BEGINNING_OFFSET));
        windowed.updateEndOffsets(Collections.singletonMap(tp, END_OFFSET));
        final TaskId windowedTask = new TaskId(0, 1);
        final StoreChangelogReader windowedReader = new StoreChangelogReader(
            time, config, logContext, adminClient, windowed, callback, standbyListener);
        windowedReader.register(tp, windowedStateManagerFor(tp, windowedTask));
        windowedReader.restore(Collections.singletonMap(windowedTask, mock(Task.class)));

        assertEquals(BEGINNING_OFFSET, windowed.position(tp),
            "sanity: the windowed fallback lands on the same offset as the plain path");
        assertTrue(pollCount > plainPolls,
            "the windowed path burns an extra probe poll to reach the same offset "
                + "(windowed=" + pollCount + " plain=" + plainPolls + ")");
        assertTrue(endOffsetsCount > plainEndOffsets,
            "the windowed path makes an extra endOffsets broker call to reach the same offset "
                + "(windowed=" + endOffsetsCount + " plain=" + plainEndOffsets + ")");
    }

    /**
     * Blast radius. {@code seekNewPartitions} calls
     * {@code restoreConsumer.pause(allAssigned)} — not just the new windowed
     * partitions — so initialising one windowed store pauses every partition
     * already mid-restore on that StateUpdater thread for the duration of the
     * probe.
     *
     * <p>On the soak a task carries 9-12 stores and there are several tasks per
     * thread, so this stall lands on in-flight restores repeatedly. Combined with
     * the zero margin of a log-start restore, that is how the optimisation could
     * make failures more likely rather than less.
     */
    @Test
    public void initialisingAWindowedPartitionPausesPartitionsAlreadyRestoring() {
        final TopicPartition alreadyRestoring = new TopicPartition("already", 0);
        final TopicPartition newlyWindowed = new TopicPartition("newly", 0);

        final MockConsumer<byte[], byte[]> consumer =
            consumerReturning(Collections.singletonMap(newlyWindowed, OFFSET_FOR_TIMESTAMP));
        final Map<TopicPartition, Long> beginnings = new HashMap<>();
        final Map<TopicPartition, Long> ends = new HashMap<>();
        for (final TopicPartition tp : List.of(alreadyRestoring, newlyWindowed)) {
            beginnings.put(tp, BEGINNING_OFFSET);
            ends.put(tp, END_OFFSET);
        }
        consumer.updateBeginningOffsets(beginnings);
        consumer.updateEndOffsets(ends);
        adminClient.updateEndOffsets(ends);

        final StoreChangelogReader reader = new StoreChangelogReader(
            time, config, logContext, adminClient, consumer, callback, standbyListener);

        // first partition is registered and restoring
        final TaskId firstTask = new TaskId(0, 0);
        reader.register(alreadyRestoring, plainStateManagerFor(alreadyRestoring, firstTask));
        reader.restore(Collections.singletonMap(firstTask, mock(Task.class)));

        pauseCalls.clear();

        // now a NEW windowed partition joins, triggering seekNewPartitions
        final TaskId secondTask = new TaskId(0, 1);
        reader.register(newlyWindowed, windowedStateManagerFor(newlyWindowed, secondTask));
        final Map<TaskId, Task> tasks = new HashMap<>();
        tasks.put(firstTask, mock(Task.class));
        tasks.put(secondTask, mock(Task.class));
        reader.restore(tasks);

        final boolean pausedTheInnocentPartition = pauseCalls.stream()
            .anyMatch(paused -> paused.contains(alreadyRestoring));
        assertTrue(pausedTheInnocentPartition,
            "initialising a windowed partition should have paused the already-restoring "
                + "partition too; pause calls were " + pauseCalls);
    }
}
