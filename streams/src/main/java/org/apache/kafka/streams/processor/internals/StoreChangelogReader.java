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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class StoreChangelogReader implements ChangelogReader {

    enum ChangelogState {
        // registered but need to be initialized
        REGISTERED("REGISTERED"),

        // initialized and restoring
        RESTORING("RESTORING"),

        // completed restoring (only for active restoring task)
        COMPLETED("COMPLETED");

        public final String name;

        ChangelogState(final String name) {
            this.name = name;
        }
    }

    private class ChangelogMetadata {

        private TopicPartition changelogPartition;

        private ChangelogState changelogState;

        private ProcessorStateManager stateManager;

        private Long restoreEndOffset; // only for active restoring task (for standby it is null)

        private ChangelogMetadata(final TopicPartition changelogPartition, final ProcessorStateManager stateManager) {
            this.stateManager = stateManager;
            this.changelogPartition = changelogPartition;
            this.changelogState = ChangelogState.REGISTERED;
            this.restoreEndOffset = null;
        }
    }

    private final Logger log;
    private final Duration pollTime;
    private final ProcessorStateManager stateManager;
    private final Consumer<byte[], byte[]> restoreConsumer;
    private final StateRestoreListener stateRestoreListener;
    private final Map<TopicPartition, Long> restoreToOffsets = new HashMap<>();
    private final Map<TopicPartition, StateRestorer> stateRestorers = new HashMap<>();
    private final Set<TopicPartition> needsRestoring = new HashSet<>();
    private final Set<TopicPartition> needsInitializing = new HashSet<>();
    private final Set<TopicPartition> completedRestorers = new HashSet<>();

    private final Map<TopicPartition, ChangelogMetadata> changelogs;

    // if some partition metadata does not exist yet then it is highly likely that fetching
    // for their end offsets would timeout; thus we maintain a global partition information
    // and only try to fetch for the corresponding end offset after we know that this
    // partition already exists from the broker-side metadata: this is an optimization.
    private final Map<String, List<PartitionInfo>> partitionInfo = new HashMap<>();


    public StoreChangelogReader(final StreamsConfig config,
                                final LogContext logContext,
                                final ProcessorStateManager stateManager,
                                final Consumer<byte[], byte[]> restoreConsumer,
                                final StateRestoreListener stateRestoreListener) {
        this.log = logContext.logger(getClass());
        this.stateManager = stateManager;
        this.restoreConsumer = restoreConsumer;
        this.stateRestoreListener = stateRestoreListener;
        this.pollTime = Duration.ofMillis(config.getLong(StreamsConfig.POLL_MS_CONFIG));

        this.changelogs = new HashMap<>();
    }

    @Override
    public void register(final StateRestorer restorer, final ProcessorStateManager stateManager) {
        if (!stateRestorers.containsKey(restorer.partition())) {
            restorer.setUserRestoreListener(stateRestoreListener);
            stateRestorers.put(restorer.partition(), restorer);

            log.trace("Added restorer for changelog {}", restorer.partition());
        } else {
            log.debug("Skip re-adding restorer for changelog {}", restorer.partition());
        }

        needsInitializing.add(restorer.partition());

        if (changelogs.putIfAbsent(restorer.partition(), new ChangelogMetadata(restorer.partition())) != null) {
            throw new IllegalStateException("There is already a changelog registered for " + restorer.partition() +
                ", this should not happen.");
        }
    }

    private Set<TopicPartition> registeredChangelogs() {
        return changelogs.values().stream()
            .filter(metadata -> metadata.changelogState == ChangelogState.REGISTERED)
            .map(metadata -> metadata.changelogPartition)
            .collect(Collectors.toSet());
    }

    private boolean hasChangelogsToInit() {
        return !registeredChangelogs().isEmpty();
    }

    private Set<TopicPartition> restoringChangelogs() {
        return changelogs.values().stream()
            .filter(metadata -> metadata.changelogState == ChangelogState.RESTORING)
            .map(metadata -> metadata.changelogPartition)
            .collect(Collectors.toSet());
    }

    private boolean hasChangelogsToRestore() {
        return !restoringChangelogs().isEmpty();
    }

    private boolean allChangelogsCompleted() {
        return changelogs.values().stream()
            .allMatch(metadata -> metadata.changelogState == ChangelogState.COMPLETED);
    }

    public Collection<TopicPartition> restore(final RestoringTasks active) {
        // 1. if there are any registered changelogs that needs initialization, try to initialize them first;
        // 2. if all changelogs have finished, return early;
        // 3. if there are any restoring changelogs, try to read from the restore consumer and process them.

        final Set<TopicPartition> registeredChangelogs = registeredChangelogs();
        if (!registeredChangelogs.isEmpty()) {
            initialize(registeredChangelogs, active);
        }

        if (allChangelogsCompleted()) {
            log.info("Finished restoring all active tasks");
            restoreConsumer.unsubscribe();
            return changelogs.keySet();
        }

        if (hasChangelogsToRestore()) {
            final ConsumerRecords<byte[], byte[]> polledRecords = restoreConsumer.poll(pollTime);

            for (final TopicPartition partition : polledRecords.partitions()) {
                final ChangelogMetadata changelogMetadata = changelogs.get(partition);
                if (changelogMetadata == null) {
                    throw new IllegalStateException("The corresponding changelog restorer for " + partition +
                        " does not exist, this should not happen.");
                }

                if (changelogMetadata.changelogState != ChangelogState.RESTORING) {
                    throw new IllegalStateException("The corresponding changelog restorer for " + partition +
                        " has already transited to completed state, this should not happen.");
                }

                final List<ConsumerRecord<byte[], byte[]>> records = polledRecords.records(partition);
            }

            for (final TopicPartition partition : needsRestoring) {
                final StateRestorer restorer = stateRestorers.get(partition);
                final long pos = processNext(polledRecords.records(partition), restorer, restoreToOffsets.get(partition));
                restorer.setRestoredOffset(pos);
                if (restorer.hasCompleted(pos, restoreToOffsets.get(partition))) {
                    restorer.restoreDone();
                    restoreToOffsets.remove(partition);
                    completedRestorers.add(partition);
                }
            }
        }

        try {
            final ConsumerRecords<byte[], byte[]> records = restoreConsumer.poll(pollTime);

            for (final TopicPartition partition : needsRestoring) {
                final StateRestorer restorer = stateRestorers.get(partition);
                final long pos = processNext(records.records(partition), restorer, restoreToOffsets.get(partition));
                restorer.setRestoredOffset(pos);
                if (restorer.hasCompleted(pos, restoreToOffsets.get(partition))) {
                    restorer.restoreDone();
                    restoreToOffsets.remove(partition);
                    completedRestorers.add(partition);
                }
            }
        } catch (final InvalidOffsetException recoverableException) {
            log.warn("Restoring StreamTasks failed. Deleting StreamTasks stores to recreate from scratch.", recoverableException);
            final Set<TopicPartition> partitions = recoverableException.partitions();
            for (final TopicPartition partition : partitions) {
                final StreamTask task = active.restoringTaskFor(partition);
                log.info("Reinitializing StreamTask {} for changelog {}", task, partition);

                needsInitializing.remove(partition);
                needsRestoring.remove(partition);

                final StateRestorer restorer = stateRestorers.get(partition);
                restorer.setCheckpointOffset(StateRestorer.NO_CHECKPOINT);
                task.reinitializeStateStoresForPartitions(recoverableException.partitions());
            }
            restoreConsumer.seekToBeginning(partitions);
        }

        needsRestoring.removeAll(completedRestorers);

        checkForCompletedRestoration();

        return completedRestorers;
    }

    private boolean hasPartition(final TopicPartition topicPartition) {
        final List<PartitionInfo> partitions = partitionInfo.get(topicPartition.topic());

        if (partitions == null) {
            return false;
        }

        for (final PartitionInfo partition : partitions) {
            if (partition.partition() == topicPartition.partition()) {
                return true;
            }
        }

        return false;
    }

    private boolean restoredToEnd(final ChangelogMetadata changelogMetadata) {
        if (changelogMetadata.changelogState != ChangelogState.RESTORING) {
            throw new IllegalStateException("Should never check a non-restoring changelog if it has restored to end");
        }

        final Long endOffset = changelogMetadata.restoreEndOffset;
        final Long currentOffset = changelogMetadata.stateManager.changelogOffsets().get(changelogMetadata.changelogPartition);

        if (endOffset == null) {
            // end offset is not initialized meaning that it is from a standby task (i.e. it should never end)
            return false;
        } else if (currentOffset == null) {
            // current offset is not initialized meaning there's no checkpointed offset,
            // we would start restoring from beginning and it does not end yet
            return false;
        } else if (currentOffset < endOffset) {
            return false;
        } else {
            return true;
        }
    }

    private void initialize(final Set<TopicPartition> registeredChangelogs, final RestoringTasks active) {
        // once a task has been initialized to restoring, we would update the restore
        // consumer to add their corresponding partitions; if not all tasks can be initialized
        // we need to update the

        // if the partition information is empty we first try to refresh it before trying to find which partitions
        // exist for end offset retrieving; this is a one-time thing.
        if (partitionInfo.isEmpty())
            refreshChangelogInfo();

        final Set<TopicPartition> initializableChangelogs = registeredChangelogs.stream()
            .filter(this::hasPartition).collect(Collectors.toSet());

        // try to fetch end offsets for the initializable changelogs and remove any partitions
        // where we already have all of the data
        final Map<TopicPartition, Long> endOffsets;
        try {
            endOffsets = restoreConsumer.endOffsets(initializableChangelogs);
        } catch (final TimeoutException e) {
            // if timeout exception gets thrown we just give up this time and retry in the next run loop
            log.debug("Could not fetch all end offsets for {}, will retry in the next run loop", initializableChangelogs);
            return;
        }

        endOffsets.forEach((partition, endOffset) -> {
            if (endOffset != null) {
                final ChangelogMetadata changelogMetadata = changelogs.get(partition);
                changelogMetadata.restoreEndOffset = endOffset;
                changelogMetadata.changelogState = ChangelogState.RESTORING;
            } else {
                log.info("End offset cannot be found form the returned metadata; removing this partition from the current run loop");
                initializableChangelogs.remove(partition);
            }
        });

        final Iterator<TopicPartition> iter = initializableChangelogs.iterator();
        while (iter.hasNext()) {
            final TopicPartition topicPartition = iter.next();
            final Long restoreOffset = restoreToOffsets.get(topicPartition);
            final StateRestorer restorer = stateRestorers.get(topicPartition);

            if (restorer.checkpoint() >= restoreOffset) {
                restorer.setRestoredOffset(restorer.checkpoint());
                iter.remove();
                completedRestorers.add(topicPartition);
            } else if (restoreOffset == 0) {
                restorer.setRestoredOffset(0);
                iter.remove();
                completedRestorers.add(topicPartition);
            } else {
                restorer.setEndingOffset(restoreOffset);
            }
            needsInitializing.remove(topicPartition);
        }

        // set up restorer for those initializable
        if (!initializable.isEmpty()) {
            startRestoration(initializable, active);
        }

        // first refresh the changelog partition information from brokers, since initialize is only called when
        // the needsInitializing map is not empty, meaning we do not know the metadata for some of them yet
    }



    private void startRestoration(final Set<TopicPartition> initialized, final RestoringTasks active) {
        log.debug("Start restoring state stores from changelog topics {}", initialized);

        final Set<TopicPartition> assignment = new HashSet<>(restoreConsumer.assignment());
        assignment.addAll(initialized);
        restoreConsumer.assign(assignment);

        final List<StateRestorer> needsPositionUpdate = new ArrayList<>();

        for (final TopicPartition partition : initialized) {
            final StateRestorer restorer = stateRestorers.get(partition);
            if (restorer.checkpoint() != StateRestorer.NO_CHECKPOINT) {
                log.trace("Found checkpoint {} from changelog {} for store {}.", restorer.checkpoint(), partition, restorer.storeName());

                restoreConsumer.seek(partition, restorer.checkpoint());
                logRestoreOffsets(partition,
                        restorer.checkpoint(),
                        restoreToOffsets.get(partition));
                restorer.setStartingOffset(restoreConsumer.position(partition));

                log.debug("Calling restorer for partition {}", partition);
                restorer.restoreStarted();
            } else {
                log.trace("Did not find checkpoint from changelog {} for store {}, rewinding to beginning.", partition, restorer.storeName());

                restoreConsumer.seekToBeginning(Collections.singletonList(partition));
                needsPositionUpdate.add(restorer);
            }
        }

        for (final StateRestorer restorer : needsPositionUpdate) {
            final TopicPartition partition = restorer.partition();

            // If checkpoint does not exist it means the task was not shutdown gracefully before;
            // and in this case if EOS is turned on we should wipe out the state and re-initialize the task
            final StreamTask task = active.restoringTaskFor(partition);
            if (task.isEosEnabled()) {
                log.info("No checkpoint found for task {} state store {} changelog {} with EOS turned on. " +
                        "Reinitializing the task and restore its state from the beginning.", task.id, restorer.storeName(), partition);

                needsInitializing.remove(partition);
                initialized.remove(partition);
                restorer.setCheckpointOffset(restoreConsumer.position(partition));

                task.reinitializeStateStoresForPartitions(Collections.singleton(partition));
            } else {
                log.info("Restoring task {}'s state store {} from beginning of the changelog {} ", task.id, restorer.storeName(), partition);

                final long position = restoreConsumer.position(restorer.partition());
                logRestoreOffsets(restorer.partition(),
                        position,
                        restoreToOffsets.get(restorer.partition()));
                restorer.setStartingOffset(position);
                restorer.restoreStarted();
            }
        }

        needsRestoring.addAll(initialized);
    }

    private void logRestoreOffsets(final TopicPartition partition,
                                   final long startingOffset,
                                   final Long endOffset) {
        log.debug("Restoring partition {} from offset {} to endOffset {}",
                  partition,
                  startingOffset,
                  endOffset);
    }

    private void refreshChangelogInfo() {
        partitionInfo.clear();
        try {
            partitionInfo.putAll(restoreConsumer.listTopics());
        } catch (final TimeoutException e) {
            log.debug("Could not fetch topic metadata within the timeout, will retry in the next run loop");
        }
    }

    @Override
    public Map<TopicPartition, Long> restoredOffsets() {
        final Map<TopicPartition, Long> restoredOffsets = new HashMap<>();
        for (final Map.Entry<TopicPartition, StateRestorer> entry : stateRestorers.entrySet()) {
            final StateRestorer restorer = entry.getValue();
            if (restorer.isPersistent()) {
                restoredOffsets.put(entry.getKey(), restorer.restoredOffset());
            }
        }
        return restoredOffsets;
    }

    @Override
    public void remove(final List<TopicPartition> revokedPartitions) {
        for (final TopicPartition partition : revokedPartitions) {
            partitionInfo.remove(partition.topic());
            stateRestorers.remove(partition);
            needsRestoring.remove(partition);
            restoreToOffsets.remove(partition);
            needsInitializing.remove(partition);
            completedRestorers.remove(partition);
        }
    }

    @Override
    public void clear() {
        partitionInfo.clear();
        stateRestorers.clear();
        needsRestoring.clear();
        restoreToOffsets.clear();
        needsInitializing.clear();
        completedRestorers.clear();
    }

    @Override
    public boolean isEmpty() {
        return stateRestorers.isEmpty()
            && needsRestoring.isEmpty()
            && restoreToOffsets.isEmpty()
            && needsInitializing.isEmpty()
            && completedRestorers.isEmpty();
    }

    @Override
    public String toString() {
        return "RestoreToOffset: " + restoreToOffsets + "\n" +
               "StateRestorers: " + stateRestorers + "\n" +
               "NeedsRestoring: " + needsRestoring + "\n" +
               "NeedsInitializing: " + needsInitializing + "\n" +
               "CompletedRestorers: " + completedRestorers + "\n";
    }

    private long processNext(final List<ConsumerRecord<byte[], byte[]>> records,
                             final StateRestorer restorer,
                             final Long endOffset) {
        final List<ConsumerRecord<byte[], byte[]>> restoreRecords = new ArrayList<>();
        long nextPosition = -1;
        final int numberRecords = records.size();
        int numberRestored = 0;
        long lastRestoredOffset = -1;
        for (final ConsumerRecord<byte[], byte[]> record : records) {
            final long offset = record.offset();
            if (restorer.hasCompleted(offset, endOffset)) {
                nextPosition = record.offset();
                break;
            }
            lastRestoredOffset = offset;
            numberRestored++;
            if (record.key() != null) {
                restoreRecords.add(record);
            }
        }


        // if we have changelog topic then we should have restored all records in the list
        // otherwise if we did not fully restore to that point we need to set nextPosition
        // to the position of the restoreConsumer and we'll cause a TaskMigratedException exception
        if (nextPosition == -1 || (restorer.offsetLimit() == Long.MAX_VALUE && numberRecords != numberRestored)) {
            nextPosition = restoreConsumer.position(restorer.partition());
        }

        if (!restoreRecords.isEmpty()) {
            stateManager.restore(restorer.partition(), restoreRecords);
            restorer.restoreBatchCompleted(lastRestoredOffset, records.size());

            log.trace("Restored from {} to {} with {} records, ending offset is {}, next starting position is {}",
                    restorer.partition(), restorer.storeName(), records.size(), lastRestoredOffset, nextPosition);
        }

        return nextPosition;
    }

    private boolean checkForCompletedRestoration() {
        if (needsRestoring.isEmpty()) {
            log.info("Finished restoring all active tasks");
            restoreConsumer.unsubscribe();
            return true;
        }
        return false;
    }
}
