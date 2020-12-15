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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskCorruptedException;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager.StateStoreMetadata;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.processor.internals.ClientUtils.fetchCommittedOffsets;

/**
 * ChangelogReader is created and maintained by the stream thread and used for both updating standby tasks and
 * restoring active tasks. It manages the restore consumer, including its assigned partitions, when to pause / resume
 * these partitions, etc.
 * <p>
 * The reader also maintains the source of truth for restoration state: only active tasks restoring changelog could
 * be completed, while standby tasks updating changelog would always be in restoring state after being initialized.
 */
public class StoreChangelogReader implements ChangelogReader {
    private static final long RESTORE_LOG_INTERVAL_MS = 10_000L;
    private long lastRestoreLogTime = 0L;

    enum ChangelogState {
        // registered but need to be initialized (i.e. set its starting, end, limit offsets)
        REGISTERED("REGISTERED"),

        // initialized and restoring
        RESTORING("RESTORING", 0),

        // completed restoring (only for active restoring task, standby task should never be completed)
        COMPLETED("COMPLETED", 1);

        public final String name;
        private final List<Integer> prevStates;

        ChangelogState(final String name, final Integer... prevStates) {
            this.name = name;
            this.prevStates = Arrays.asList(prevStates);
        }
    }

    // NOTE we assume that the changelog reader is used only for either
    //   1) restoring active task or
    //   2) updating standby task at a given time,
    // but never doing both
    enum ChangelogReaderState {
        ACTIVE_RESTORING("ACTIVE_RESTORING"),

        STANDBY_UPDATING("STANDBY_UPDATING");

        public final String name;

        ChangelogReaderState(final String name) {
            this.name = name;
        }
    }

    static class ChangelogMetadata {

        private final StateStoreMetadata storeMetadata;

        private final ProcessorStateManager stateManager;

        private ChangelogState changelogState;

        private long totalRestored;

        // the end offset beyond which records should not be applied (yet) to restore the states
        //
        // for both active restoring tasks and standby updating tasks, it is defined as:
        //    * log-end-offset if the changelog is not piggy-backed with source topic
        //    * min(log-end-offset, committed-offset) if the changelog is piggy-backed with source topic
        //
        // the log-end-offset only needs to be updated once and only need to be for active tasks since for standby
        // tasks it would never "complete" based on the end-offset;
        //
        // the committed-offset needs to be updated periodically for those standby tasks
        //
        // NOTE we do not book keep the current offset since we leverage state manager as its source of truth
        private Long restoreEndOffset;

        // buffer records polled by the restore consumer;
        private final List<ConsumerRecord<byte[], byte[]>> bufferedRecords;

        // the limit index (exclusive) inside the buffered records beyond which should not be used to restore
        // either due to limit offset (standby) or committed end offset (active)
        private int bufferedLimitIndex;

        private ChangelogMetadata(final StateStoreMetadata storeMetadata, final ProcessorStateManager stateManager) {
            this.changelogState = ChangelogState.REGISTERED;
            this.storeMetadata = storeMetadata;
            this.stateManager = stateManager;
            this.restoreEndOffset = null;
            this.totalRestored = 0L;

            this.bufferedRecords = new ArrayList<>();
            this.bufferedLimitIndex = 0;
        }

        private void clear() {
            this.bufferedRecords.clear();
        }

        private void transitTo(final ChangelogState newState) {
            if (newState.prevStates.contains(changelogState.ordinal())) {
                changelogState = newState;
            } else {
                throw new IllegalStateException("Invalid transition from " + changelogState + " to " + newState);
            }
        }

        @Override
        public String toString() {
            final Long currentOffset = storeMetadata.offset();
            return changelogState + " " + stateManager.taskType() +
                " (currentOffset " + currentOffset + ", endOffset " + restoreEndOffset + ")";
        }

        // for testing only below
        ChangelogState state() {
            return changelogState;
        }

        long totalRestored() {
            return totalRestored;
        }

        Long endOffset() {
            return restoreEndOffset;
        }

        List<ConsumerRecord<byte[], byte[]>> bufferedRecords() {
            return bufferedRecords;
        }

        int bufferedLimitIndex() {
            return bufferedLimitIndex;
        }
    }

    private final static long DEFAULT_OFFSET_UPDATE_MS = Duration.ofMinutes(5L).toMillis();

    private ChangelogReaderState state;

    private final Time time;
    private final Logger log;
    private final Duration pollTime;
    private final long updateOffsetIntervalMs;

    // 1) we keep adding partitions to restore consumer whenever new tasks are registered with the state manager;
    // 2) we do not unassign partitions when we switch between standbys and actives, we just pause / resume them;
    // 3) we only remove an assigned partition when the corresponding task is being removed from the thread.
    private final Consumer<byte[], byte[]> restoreConsumer;
    private final StateRestoreListener stateRestoreListener;

    // source of the truth of the current registered changelogs;
    // NOTE a changelog would only be removed when its corresponding task
    // is being removed from the thread; otherwise it would stay in this map even after completed
    private final Map<TopicPartition, ChangelogMetadata> changelogs;

    // the changelog reader only need the main consumer to get committed offsets for source changelog partitions
    // to update offset limit for standby tasks;
    private Consumer<byte[], byte[]> mainConsumer;

    // the changelog reader needs the admin client to list end offsets
    private final Admin adminClient;

    private long lastUpdateOffsetTime;

    void setMainConsumer(final Consumer<byte[], byte[]> consumer) {
        this.mainConsumer = consumer;
    }

    public StoreChangelogReader(final Time time,
                                final StreamsConfig config,
                                final LogContext logContext,
                                final Admin adminClient,
                                final Consumer<byte[], byte[]> restoreConsumer,
                                final StateRestoreListener stateRestoreListener) {
        this.time = time;
        this.log = logContext.logger(StoreChangelogReader.class);
        this.state = ChangelogReaderState.ACTIVE_RESTORING;
        this.adminClient = adminClient;
        this.restoreConsumer = restoreConsumer;
        this.stateRestoreListener = stateRestoreListener;

        this.pollTime = Duration.ofMillis(config.getLong(StreamsConfig.POLL_MS_CONFIG));
        this.updateOffsetIntervalMs = config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG) == Long.MAX_VALUE ?
            DEFAULT_OFFSET_UPDATE_MS : config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG);
        this.lastUpdateOffsetTime = 0L;

        this.changelogs = new HashMap<>();
    }

    private static String recordEndOffset(final Long endOffset) {
        return endOffset == null ? "UNKNOWN (since it is for standby task)" : endOffset.toString();
    }

    private boolean hasRestoredToEnd(final ChangelogMetadata metadata) {
        final Long endOffset = metadata.restoreEndOffset;
        if (endOffset == null) {
            // end offset is not initialized meaning that it is from a standby task,
            // this should never happen since we only call this function for active task in restoring phase
            throw new IllegalStateException("End offset for changelog " + metadata + " is unknown when deciding " +
                "if it has completed restoration, this should never happen.");
        } else if (endOffset == 0) {
            // this is a special case, meaning there's nothing to be restored since the changelog has no data
            // OR the changelog is a source topic and there's no committed offset
            return true;
        } else if (metadata.bufferedRecords.isEmpty()) {
            // NOTE there are several corner cases that we need to consider:
            //  1) the end / committed offset returned from the consumer is the last offset + 1
            //  2) there could be txn markers as the last record if EOS is enabled at the producer
            //
            // It is possible that: the last record's offset == last txn marker offset - 1 == end / committed offset - 2
            //
            // So we make the following decision:
            //  1) if all the buffered records have been applied, then we compare the end offset with the
            //     current consumer's position, which is the "next" record to fetch, bypassing the txn marker already
            //  2) if not all the buffered records have been applied, then it means we are restricted by the end offset,
            //     and the consumer's position is likely already ahead of that end offset. Then we just need to check
            //     the first record in the remaining buffer and see if that record is no smaller than the end offset.
            final TopicPartition partition = metadata.storeMetadata.changelogPartition();
            try {
                return restoreConsumer.position(partition) >= endOffset;
            } catch (final TimeoutException timeoutException) {
                // re-throw to trigger `task.timeout.ms`
                throw timeoutException;
            } catch (final KafkaException e) {
                // this also includes InvalidOffsetException, which should not happen under normal
                // execution, hence it is also okay to wrap it as fatal StreamsException
                throw new StreamsException("Restore consumer get unexpected error trying to get the position " +
                    " of " + partition, e);
            }
        } else {
            return metadata.bufferedRecords.get(0).offset() >= endOffset;
        }
    }

    // Once some new tasks are created, we transit to restore them and pause on the existing standby tasks. It is
    // possible that when newly created tasks are created the changelog reader are still restoring existing
    // active tasks, and hence this function is idempotent and can be called multiple times.
    //
    // NOTE: even if the newly created tasks do not need any restoring, we still first transit to this state and then
    // immediately transit back -- there's no overhead of transiting back and forth but simplifies the logic a lot.
    @Override
    public void enforceRestoreActive() {
        if (state != ChangelogReaderState.ACTIVE_RESTORING) {
            log.debug("Transiting to restore active tasks: {}", changelogs);
            lastRestoreLogTime = 0L;

            // pause all partitions that are for standby tasks from the restore consumer
            pauseChangelogsFromRestoreConsumer(standbyRestoringChangelogs());

            state = ChangelogReaderState.ACTIVE_RESTORING;
        }
    }

    // Only after we've completed restoring all active tasks we'll then move back to resume updating standby tasks.
    // This function is NOT idempotent: if it is already in updating standby tasks mode, we should not call it again.
    //
    // NOTE: we do not clear completed active restoring changelogs or remove partitions from restore consumer either
    // upon completing them but only pause the corresponding partitions; the changelog metadata / partitions would only
    // be cleared when the corresponding task is being removed from the thread. In other words, the restore consumer
    // should contain all changelogs that are RESTORING or COMPLETED
    @Override
    public void transitToUpdateStandby() {
        if (state != ChangelogReaderState.ACTIVE_RESTORING) {
            throw new IllegalStateException(
                "The changelog reader is not restoring active tasks (is " + state + ") while trying to " +
                    "transit to update standby tasks: " + changelogs
            );
        }

        log.debug("Transiting to update standby tasks: {}", changelogs);

        // resume all standby restoring changelogs from the restore consumer
        resumeChangelogsFromRestoreConsumer(standbyRestoringChangelogs());

        state = ChangelogReaderState.STANDBY_UPDATING;
    }

    /**
     * Since it is shared for multiple tasks and hence multiple state managers, the registration would take its
     * corresponding state manager as well for restoring.
     */
    @Override
    public void register(final TopicPartition partition, final ProcessorStateManager stateManager) {
        final StateStoreMetadata storeMetadata = stateManager.storeMetadata(partition);
        if (storeMetadata == null) {
            throw new IllegalStateException("Cannot find the corresponding state store metadata for changelog " +
                partition);
        }

        final ChangelogMetadata changelogMetadata = new ChangelogMetadata(storeMetadata, stateManager);

        // initializing limit offset to 0L for standby changelog to effectively disable any restoration until it is updated
        if (stateManager.taskType() == Task.TaskType.STANDBY && stateManager.changelogAsSource(partition)) {
            changelogMetadata.restoreEndOffset = 0L;
        }

        if (changelogs.putIfAbsent(partition, changelogMetadata) != null) {
            throw new IllegalStateException("There is already a changelog registered for " + partition +
                ", this should not happen: " + changelogs);
        }
    }

    private ChangelogMetadata restoringChangelogByPartition(final TopicPartition partition) {
        final ChangelogMetadata changelogMetadata = changelogs.get(partition);
        if (changelogMetadata == null) {
            throw new IllegalStateException("The corresponding changelog restorer for " + partition +
                " does not exist, this should not happen.");
        }
        if (changelogMetadata.changelogState != ChangelogState.RESTORING) {
            throw new IllegalStateException("The corresponding changelog restorer for " + partition +
                " has already transited to completed state, this should not happen.");
        }

        return changelogMetadata;
    }

    private Set<ChangelogMetadata> registeredChangelogs() {
        return changelogs.values().stream()
            .filter(metadata -> metadata.changelogState == ChangelogState.REGISTERED)
            .collect(Collectors.toSet());
    }

    private Set<TopicPartition> restoringChangelogs() {
        return changelogs.values().stream()
            .filter(metadata -> metadata.changelogState == ChangelogState.RESTORING)
            .map(metadata -> metadata.storeMetadata.changelogPartition())
            .collect(Collectors.toSet());
    }

    private Set<TopicPartition> activeRestoringChangelogs() {
        return changelogs.values().stream()
            .filter(metadata -> metadata.changelogState == ChangelogState.RESTORING &&
                metadata.stateManager.taskType() == Task.TaskType.ACTIVE)
            .map(metadata -> metadata.storeMetadata.changelogPartition())
            .collect(Collectors.toSet());
    }

    private Set<TopicPartition> standbyRestoringChangelogs() {
        return changelogs.values().stream()
            .filter(metadata -> metadata.changelogState == ChangelogState.RESTORING &&
                metadata.stateManager.taskType() == Task.TaskType.STANDBY)
            .map(metadata -> metadata.storeMetadata.changelogPartition())
            .collect(Collectors.toSet());
    }

    private boolean allChangelogsCompleted() {
        return changelogs.values().stream()
            .allMatch(metadata -> metadata.changelogState == ChangelogState.COMPLETED);
    }

    @Override
    public Set<TopicPartition> completedChangelogs() {
        return changelogs.values().stream()
            .filter(metadata -> metadata.changelogState == ChangelogState.COMPLETED)
            .map(metadata -> metadata.storeMetadata.changelogPartition())
            .collect(Collectors.toSet());
    }

    // 1. if there are any registered changelogs that needs initialization, try to initialize them first;
    // 2. if all changelogs have finished, return early;
    // 3. if there are any restoring changelogs, try to read from the restore consumer and process them.
    @Override
    public void restore(final Map<TaskId, Task> tasks) {
        initializeChangelogs(tasks, registeredChangelogs());

        if (!activeRestoringChangelogs().isEmpty() && state == ChangelogReaderState.STANDBY_UPDATING) {
            throw new IllegalStateException("Should not be in standby updating state if there are still un-completed active changelogs");
        }

        if (allChangelogsCompleted()) {
            log.debug("Finished restoring all changelogs {}", changelogs.keySet());
            return;
        }

        final Set<TopicPartition> restoringChangelogs = restoringChangelogs();
        if (!restoringChangelogs.isEmpty()) {
            final ConsumerRecords<byte[], byte[]> polledRecords;

            try {
                // for restoring active and updating standby we may prefer different poll time
                // in order to make sure we call the main consumer#poll in time.
                // TODO: once we move ChangelogReader to a separate thread this may no longer be a concern
                polledRecords = restoreConsumer.poll(state == ChangelogReaderState.STANDBY_UPDATING ? Duration.ZERO : pollTime);

                // TODO (?) If we cannot fetch records during restore, should we trigger `task.timeout.ms` ?
                // TODO (?) If we cannot fetch records for standby task, should we trigger `task.timeout.ms` ?
            } catch (final InvalidOffsetException e) {
                log.warn("Encountered " + e.getClass().getName() +
                    " fetching records from restore consumer for partitions " + e.partitions() + ", it is likely that " +
                    "the consumer's position has fallen out of the topic partition offset range because the topic was " +
                    "truncated or compacted on the broker, marking the corresponding tasks as corrupted and re-initializing" +
                    " it later.", e);

                final Map<TaskId, Collection<TopicPartition>> taskWithCorruptedChangelogs = new HashMap<>();
                for (final TopicPartition partition : e.partitions()) {
                    final TaskId taskId = changelogs.get(partition).stateManager.taskId();
                    taskWithCorruptedChangelogs.computeIfAbsent(taskId, k -> new HashSet<>()).add(partition);
                }
                throw new TaskCorruptedException(taskWithCorruptedChangelogs, e);
            } catch (final KafkaException e) {
                throw new StreamsException("Restore consumer get unexpected error polling records.", e);
            }

            for (final TopicPartition partition : polledRecords.partitions()) {
                bufferChangelogRecords(restoringChangelogByPartition(partition), polledRecords.records(partition));
            }

            for (final TopicPartition partition : restoringChangelogs) {
                // even if some partition do not have any accumulated data, we still trigger
                // restoring since some changelog may not need to restore any at all, and the
                // restore to end check needs to be executed still.
                // TODO: we always try to restore as a batch when some records are accumulated, which may result in
                //       small batches; this can be optimized in the future, e.g. wait longer for larger batches.
                final TaskId taskId = changelogs.get(partition).stateManager.taskId();
                try {
                    if (restoreChangelog(changelogs.get(partition))) {
                        tasks.get(taskId).clearTaskTimeout();
                    }
                } catch (final TimeoutException timeoutException) {
                    tasks.get(taskId).maybeInitTaskTimeoutOrThrow(
                        time.milliseconds(),
                        timeoutException
                    );
                }
            }

            maybeUpdateLimitOffsetsForStandbyChangelogs(tasks);

            maybeLogRestorationProgress();
        }
    }

    private void maybeLogRestorationProgress() {
        if (state == ChangelogReaderState.ACTIVE_RESTORING) {
            if (time.milliseconds() - lastRestoreLogTime > RESTORE_LOG_INTERVAL_MS) {
                final Set<TopicPartition> topicPartitions = activeRestoringChangelogs();
                if (!topicPartitions.isEmpty()) {
                    final StringBuilder builder = new StringBuilder().append("Restoration in progress for ")
                                                                     .append(topicPartitions.size())
                                                                     .append(" partitions.");
                    for (final TopicPartition partition : topicPartitions) {
                        final ChangelogMetadata changelogMetadata = restoringChangelogByPartition(partition);
                        builder.append(" {")
                               .append(partition)
                               .append(": ")
                               .append("position=")
                               .append(getPositionString(partition, changelogMetadata))
                               .append(", end=")
                               .append(changelogMetadata.restoreEndOffset)
                               .append(", totalRestored=")
                               .append(changelogMetadata.totalRestored)
                               .append("}");
                    }
                    log.info(builder.toString());
                    lastRestoreLogTime = time.milliseconds();
                }
            }
        } else {
            lastRestoreLogTime = 0L;
        }
    }

    private static String getPositionString(final TopicPartition partition,
                                            final ChangelogMetadata changelogMetadata) {
        final ProcessorStateManager stateManager = changelogMetadata.stateManager;
        final Long offsets = stateManager.changelogOffsets().get(partition);
        return offsets == null ? "unknown" : String.valueOf(offsets);
    }

    private void maybeUpdateLimitOffsetsForStandbyChangelogs(final Map<TaskId, Task> tasks) {
        // we only consider updating the limit offset for standbys if we are not restoring active tasks
        if (state == ChangelogReaderState.STANDBY_UPDATING &&
            updateOffsetIntervalMs < time.milliseconds() - lastUpdateOffsetTime) {

            // when the interval has elapsed we should try to update the limit offset for standbys reading from
            // a source changelog with the new committed offset, unless there are no buffered records since 
            // we only need the limit when processing new records
            // for other changelog partitions we do not need to update limit offset at all since we never need to
            // check when it completes based on limit offset anyways: the end offset would keep increasing and the
            // standby never need to stop
            final Set<TopicPartition> changelogsWithLimitOffsets = changelogs.entrySet().stream()
                .filter(entry -> entry.getValue().stateManager.taskType() == Task.TaskType.STANDBY &&
                    entry.getValue().stateManager.changelogAsSource(entry.getKey()))
                .map(Map.Entry::getKey).collect(Collectors.toSet());

            for (final TopicPartition partition : changelogsWithLimitOffsets) {
                if (!changelogs.get(partition).bufferedRecords().isEmpty()) {
                    updateLimitOffsetsForStandbyChangelogs(committedOffsetForChangelogs(tasks, changelogsWithLimitOffsets));
                    break;
                }
            }
        }
    }

    private void bufferChangelogRecords(final ChangelogMetadata changelogMetadata, final List<ConsumerRecord<byte[], byte[]>> records) {
        // update the buffered records and limit index with the fetched records
        for (final ConsumerRecord<byte[], byte[]> record : records) {
            // filter polled records for null-keys and also possibly update buffer limit index
            if (record.key() == null) {
                log.warn("Read changelog record with null key from changelog {} at offset {}, " +
                    "skipping it for restoration", changelogMetadata.storeMetadata.changelogPartition(), record.offset());
            } else {
                changelogMetadata.bufferedRecords.add(record);
                final long offset = record.offset();
                if (changelogMetadata.restoreEndOffset == null || offset < changelogMetadata.restoreEndOffset) {
                    changelogMetadata.bufferedLimitIndex = changelogMetadata.bufferedRecords.size();
                }
            }
        }
    }

    /**
     * restore a changelog with its buffered records if there's any; for active changelogs also check if
     * it has completed the restoration and can transit to COMPLETED state and trigger restore callbacks
     */
    private boolean restoreChangelog(final ChangelogMetadata changelogMetadata) {
        final ProcessorStateManager stateManager = changelogMetadata.stateManager;
        final StateStoreMetadata storeMetadata = changelogMetadata.storeMetadata;
        final TopicPartition partition = storeMetadata.changelogPartition();
        final String storeName = storeMetadata.store().name();
        final int numRecords = changelogMetadata.bufferedLimitIndex;

        boolean madeProgress = false;

        if (numRecords != 0) {
            madeProgress = true;

            final List<ConsumerRecord<byte[], byte[]>> records = changelogMetadata.bufferedRecords.subList(0, numRecords);
            stateManager.restore(storeMetadata, records);

            // NOTE here we use removeRange of ArrayList in order to achieve efficiency with range shifting,
            // otherwise one-at-a-time removal or addition would be very costly; if all records are restored
            // then we can further optimize to save the array-shift but just set array elements to null
            if (numRecords < changelogMetadata.bufferedRecords.size()) {
                records.clear();
            } else {
                changelogMetadata.bufferedRecords.clear();
            }

            final Long currentOffset = storeMetadata.offset();
            log.trace("Restored {} records from changelog {} to store {}, end offset is {}, current offset is {}",
                partition, storeName, numRecords, recordEndOffset(changelogMetadata.restoreEndOffset), currentOffset);

            changelogMetadata.bufferedLimitIndex = 0;
            changelogMetadata.totalRestored += numRecords;

            // do not trigger restore listener if we are processing standby tasks
            if (changelogMetadata.stateManager.taskType() == Task.TaskType.ACTIVE) {
                try {
                    stateRestoreListener.onBatchRestored(partition, storeName, currentOffset, numRecords);
                } catch (final Exception e) {
                    throw new StreamsException("State restore listener failed on batch restored", e);
                }
            }
        }

        // we should check even if there's nothing restored, but do not check completed if we are processing standby tasks
        if (changelogMetadata.stateManager.taskType() == Task.TaskType.ACTIVE && hasRestoredToEnd(changelogMetadata)) {
            madeProgress = true;

            log.info("Finished restoring changelog {} to store {} with a total number of {} records",
                partition, storeName, changelogMetadata.totalRestored);

            changelogMetadata.transitTo(ChangelogState.COMPLETED);
            pauseChangelogsFromRestoreConsumer(Collections.singleton(partition));

            try {
                stateRestoreListener.onRestoreEnd(partition, storeName, changelogMetadata.totalRestored);
            } catch (final Exception e) {
                throw new StreamsException("State restore listener failed on restore completed", e);
            }
        }

        return madeProgress;
    }

    private Set<Task> getTasksFromPartitions(final Map<TaskId, Task> tasks,
                                             final Set<TopicPartition> partitions) {
        final Set<Task> result = new HashSet<>();
        for (final TopicPartition partition : partitions) {
            result.add(tasks.get(changelogs.get(partition).stateManager.taskId()));
        }
        return result;
    }

    private void clearTaskTimeout(final Set<Task> tasks) {
        tasks.forEach(Task::clearTaskTimeout);
    }

    private void maybeInitTaskTimeoutOrThrow(final Set<Task> tasks,
                                             final Exception cause) {
        final long now = time.milliseconds();
        tasks.forEach(t -> t.maybeInitTaskTimeoutOrThrow(now, cause));
    }

    private Map<TopicPartition, Long> committedOffsetForChangelogs(final Map<TaskId, Task> tasks,
                                                                   final Set<TopicPartition> partitions) {
        final Map<TopicPartition, Long> committedOffsets;
        try {
            committedOffsets = fetchCommittedOffsets(partitions, mainConsumer);
            clearTaskTimeout(getTasksFromPartitions(tasks, partitions));
        } catch (final TimeoutException timeoutException) {
            log.debug("Could not fetch all committed offsets for {}, will retry in the next run loop", partitions);
            maybeInitTaskTimeoutOrThrow(getTasksFromPartitions(tasks, partitions), timeoutException);
            return Collections.emptyMap();
        }
        lastUpdateOffsetTime = time.milliseconds();
        return committedOffsets;
    }

    private Map<TopicPartition, Long> endOffsetForChangelogs(final Map<TaskId, Task> tasks,
                                                             final Set<TopicPartition> partitions) {
        if (partitions.isEmpty()) {
            return Collections.emptyMap();
        }

        try {
            final ListOffsetsResult result = adminClient.listOffsets(
                    partitions.stream().collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.latest())),
                    new ListOffsetsOptions(IsolationLevel.READ_UNCOMMITTED)
            );

            final Map<TopicPartition,  ListOffsetsResult.ListOffsetsResultInfo> resultPerPartition = result.all().get();
            clearTaskTimeout(getTasksFromPartitions(tasks, partitions));

            return resultPerPartition.entrySet().stream().collect(
                    Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().offset())
            );
        } catch (final TimeoutException | InterruptedException | ExecutionException retriableException) {
            log.debug("Could not fetch all end offsets for {}, will retry in the next run loop", partitions);
            maybeInitTaskTimeoutOrThrow(getTasksFromPartitions(tasks, partitions), retriableException);
            return Collections.emptyMap();
        } catch (final KafkaException e) {
            throw new StreamsException(String.format("Failed to retrieve end offsets for %s", partitions), e);
        }
    }

    private void updateLimitOffsetsForStandbyChangelogs(final Map<TopicPartition, Long> committedOffsets) {
        for (final ChangelogMetadata metadata : changelogs.values()) {
            final TopicPartition partition = metadata.storeMetadata.changelogPartition();
            if (metadata.stateManager.taskType() == Task.TaskType.STANDBY &&
                metadata.stateManager.changelogAsSource(partition) &&
                committedOffsets.containsKey(partition)) {

                final Long newLimit = committedOffsets.get(partition);
                final Long previousLimit = metadata.restoreEndOffset;

                if (previousLimit != null && previousLimit > newLimit) {
                    throw new IllegalStateException("Offset limit should monotonically increase, but was reduced for partition " +
                        partition + ". New limit: " + newLimit + ". Previous limit: " + previousLimit);
                }

                metadata.restoreEndOffset = newLimit;

                // update the limit index for buffered records
                while (metadata.bufferedLimitIndex < metadata.bufferedRecords.size() &&
                    metadata.bufferedRecords.get(metadata.bufferedLimitIndex).offset() < metadata.restoreEndOffset)
                    metadata.bufferedLimitIndex++;
            }
        }
    }

    private void initializeChangelogs(final Map<TaskId, Task> tasks,
                                      final Set<ChangelogMetadata> newPartitionsToRestore) {
        if (newPartitionsToRestore.isEmpty()) {
            return;
        }

        // for active changelogs, we need to find their end offset before transit to restoring
        // if the changelog is on source topic, then its end offset should be the minimum of
        // its committed offset and its end offset; for standby tasks that use source topics
        // as changelogs, we want to initialize their limit offsets as committed offsets as well
        final Set<TopicPartition> newPartitionsToFindEndOffset = new HashSet<>();
        final Set<TopicPartition> newPartitionsToFindCommittedOffset = new HashSet<>();

        for (final ChangelogMetadata metadata : newPartitionsToRestore) {
            final TopicPartition partition = metadata.storeMetadata.changelogPartition();

            // TODO K9113: when TaskType.GLOBAL is added we need to modify this
            if (metadata.stateManager.taskType() == Task.TaskType.ACTIVE) {
                newPartitionsToFindEndOffset.add(partition);
            }

            if (metadata.stateManager.changelogAsSource(partition)) {
                newPartitionsToFindCommittedOffset.add(partition);
            }
        }

        // NOTE we assume that all requested partitions will be included in the returned map for both end/committed
        // offsets, i.e., it would not return partial result and would timeout if some of the results cannot be found
        final Map<TopicPartition, Long> endOffsets = endOffsetForChangelogs(tasks, newPartitionsToFindEndOffset);
        final Map<TopicPartition, Long> committedOffsets = committedOffsetForChangelogs(tasks, newPartitionsToFindCommittedOffset);

        for (final TopicPartition partition : newPartitionsToFindEndOffset) {
            final ChangelogMetadata changelogMetadata = changelogs.get(partition);
            final Long endOffset = endOffsets.get(partition);
            final Long committedOffset = newPartitionsToFindCommittedOffset.contains(partition) ?
                committedOffsets.get(partition) : Long.valueOf(Long.MAX_VALUE);

            if (endOffset != null && committedOffset != null) {
                if (changelogMetadata.restoreEndOffset != null) {
                    throw new IllegalStateException("End offset for " + partition +
                        " should only be initialized once. Existing value: " + changelogMetadata.restoreEndOffset +
                        ", new value: (" + endOffset + ", " + committedOffset + ")");
                }

                changelogMetadata.restoreEndOffset = Math.min(endOffset, committedOffset);

                log.debug("End offset for changelog {} initialized as {}.", partition, changelogMetadata.restoreEndOffset);
            } else {
                if (!newPartitionsToRestore.remove(changelogMetadata)) {
                    throw new IllegalStateException("New changelogs to restore " + newPartitionsToRestore +
                        " does not contain the one looking for end offset: " + partition + ", this should not happen.");
                }

                log.info("End offset for changelog {} cannot be found; will retry in the next time.", partition);
            }
        }

        // try initialize limit offsets for standby tasks for the first time
        if (!committedOffsets.isEmpty()) {
            updateLimitOffsetsForStandbyChangelogs(committedOffsets);
        }

        // add new partitions to the restore consumer and transit them to restoring state
        addChangelogsToRestoreConsumer(newPartitionsToRestore.stream().map(metadata -> metadata.storeMetadata.changelogPartition())
            .collect(Collectors.toSet()));

        newPartitionsToRestore.forEach(metadata -> metadata.transitTo(ChangelogState.RESTORING));

        // if it is in the active restoring mode, we immediately pause those standby changelogs
        // here we just blindly pause all (including the existing and newly added)
        if (state == ChangelogReaderState.ACTIVE_RESTORING) {
            pauseChangelogsFromRestoreConsumer(standbyRestoringChangelogs());
        }

        // prepare newly added partitions of the restore consumer by setting their starting position
        prepareChangelogs(newPartitionsToRestore);
    }

    private void addChangelogsToRestoreConsumer(final Set<TopicPartition> partitions) {
        final Set<TopicPartition> assignment = new HashSet<>(restoreConsumer.assignment());

        // the current assignment should not contain any of the new partitions
        if (assignment.removeAll(partitions)) {
            throw new IllegalStateException("The current assignment " + restoreConsumer.assignment() + " " +
                "already contains some of the new partitions " + partitions);
        }
        assignment.addAll(partitions);
        restoreConsumer.assign(assignment);

        log.debug("Added partitions {} to the restore consumer, current assignment is {}", partitions, assignment);
    }

    private void pauseChangelogsFromRestoreConsumer(final Collection<TopicPartition> partitions) {
        final Set<TopicPartition> assignment = new HashSet<>(restoreConsumer.assignment());

        // the current assignment should contain all the partitions to pause
        if (!assignment.containsAll(partitions)) {
            throw new IllegalStateException("The current assignment " + assignment + " " +
                "does not contain some of the partitions " + partitions + " for pausing.");
        }
        restoreConsumer.pause(partitions);

        log.debug("Paused partitions {} from the restore consumer", partitions);
    }

    private void removeChangelogsFromRestoreConsumer(final Collection<TopicPartition> partitions) {
        final Set<TopicPartition> assignment = new HashSet<>(restoreConsumer.assignment());

        // the current assignment should contain all the partitions to remove
        if (!assignment.containsAll(partitions)) {
            throw new IllegalStateException("The current assignment " + assignment + " " +
                "does not contain some of the partitions " + partitions + " for removing.");
        }
        assignment.removeAll(partitions);
        restoreConsumer.assign(assignment);
    }

    private void resumeChangelogsFromRestoreConsumer(final Collection<TopicPartition> partitions) {
        final Set<TopicPartition> assignment = new HashSet<>(restoreConsumer.assignment());

        // the current assignment should contain all the partitions to resume
        if (!assignment.containsAll(partitions)) {
            throw new IllegalStateException("The current assignment " + assignment + " " +
                "does not contain some of the partitions " + partitions + " for resuming.");
        }
        restoreConsumer.resume(partitions);

        log.debug("Resumed partitions {} from the restore consumer", partitions);
    }

    private void prepareChangelogs(final Set<ChangelogMetadata> newPartitionsToRestore) {
        // separate those who do not have the current offset loaded from checkpoint
        final Set<TopicPartition> newPartitionsWithoutStartOffset = new HashSet<>();

        for (final ChangelogMetadata changelogMetadata : newPartitionsToRestore) {
            final StateStoreMetadata storeMetadata = changelogMetadata.storeMetadata;
            final TopicPartition partition = storeMetadata.changelogPartition();
            final Long currentOffset = storeMetadata.offset();
            final Long endOffset = changelogs.get(partition).restoreEndOffset;

            if (currentOffset != null) {
                // the current offset is the offset of the last record, so we should set the position
                // as that offset + 1 as the "next" record to fetch; seek is not a blocking call so
                // there's nothing to capture
                restoreConsumer.seek(partition, currentOffset + 1);

                log.debug("Start restoring changelog partition {} from current offset {} to end offset {}.",
                    partition, currentOffset, recordEndOffset(endOffset));
            } else {
                log.debug("Start restoring changelog partition {} from the beginning offset to end offset {} " +
                    "since we cannot find current offset.", partition, recordEndOffset(endOffset));

                newPartitionsWithoutStartOffset.add(partition);
            }
        }

        // optimization: batch all seek-to-beginning offsets in a single request
        //               seek is not a blocking call so there's nothing to capture
        if (!newPartitionsWithoutStartOffset.isEmpty()) {
            restoreConsumer.seekToBeginning(newPartitionsWithoutStartOffset);
        }

        // do not trigger restore listener if we are processing standby tasks
        for (final ChangelogMetadata changelogMetadata : newPartitionsToRestore) {
            if (changelogMetadata.stateManager.taskType() == Task.TaskType.ACTIVE) {
                final StateStoreMetadata storeMetadata = changelogMetadata.storeMetadata;
                final TopicPartition partition = storeMetadata.changelogPartition();
                final String storeName = storeMetadata.store().name();

                long startOffset = 0L;
                try {
                    startOffset = restoreConsumer.position(partition);
                } catch (final TimeoutException swallow) {
                    // if we cannot find the starting position at the beginning, just use the default 0L
                } catch (final KafkaException e) {
                    // this also includes InvalidOffsetException, which should not happen under normal
                    // execution, hence it is also okay to wrap it as fatal StreamsException
                    throw new StreamsException("Restore consumer get unexpected error trying to get the position " +
                        " of " + partition, e);
                }

                try {
                    stateRestoreListener.onRestoreStart(partition, storeName, startOffset, changelogMetadata.restoreEndOffset);
                } catch (final Exception e) {
                    throw new StreamsException("State restore listener failed on batch restored", e);
                }
            }
        }
    }

    @Override
    public void unregister(final Collection<TopicPartition> revokedChangelogs) {
        // Only changelogs that are initialized have been added to the restore consumer's assignment
        final List<TopicPartition> revokedInitializedChangelogs = new ArrayList<>();

        for (final TopicPartition partition : revokedChangelogs) {
            final ChangelogMetadata changelogMetadata = changelogs.remove(partition);
            if (changelogMetadata != null) {
                if (!changelogMetadata.state().equals(ChangelogState.REGISTERED)) {
                    revokedInitializedChangelogs.add(partition);
                }

                changelogMetadata.clear();
            } else {
                log.debug("Changelog partition {} could not be found," +
                    " it could be already cleaned up during the handling" +
                    " of task corruption and never restore again", partition);
            }
        }

        removeChangelogsFromRestoreConsumer(revokedInitializedChangelogs);
    }

    @Override
    public void clear() {
        for (final ChangelogMetadata changelogMetadata : changelogs.values()) {
            changelogMetadata.clear();
        }
        changelogs.clear();

        try {
            restoreConsumer.unsubscribe();
        } catch (final KafkaException e) {
            throw new StreamsException("Restore consumer get unexpected error unsubscribing", e);
        }
    }

    @Override
    public boolean isEmpty() {
        return changelogs.isEmpty();
    }

    @Override
    public String toString() {
        return "StoreChangelogReader: " + changelogs + "\n";
    }

    // for testing only
    ChangelogMetadata changelogMetadata(final TopicPartition partition) {
        return changelogs.get(partition);
    }

    ChangelogReaderState state() {
        return state;
    }
}
