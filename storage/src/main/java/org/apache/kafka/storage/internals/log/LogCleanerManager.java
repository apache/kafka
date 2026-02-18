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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.storage.internals.checkpoint.OffsetCheckpointFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.server.util.LockUtils.inLock;

/**
 * This class manages the state (see {@link LogCleaningState}) of each partition being cleaned.
 * <ul>
 * <li>1. None                    : No cleaning state in a TopicPartition. In this state, it can become LogCleaningInProgress
 *                                or LogCleaningPaused(1). Valid previous state are LogCleaningInProgress and LogCleaningPaused(1)</li>
 * <li>2. LogCleaningInProgress   : The cleaning is currently in progress. In this state, it can become None when log cleaning is finished
 *                                or become LogCleaningAborted. Valid previous state is None.</li>
 * <li>3. LogCleaningAborted      : The cleaning abort is requested. In this state, it can become LogCleaningPaused(1).
 *                                Valid previous state is LogCleaningInProgress.</li>
 * <li>4-a. LogCleaningPaused(1)  : The cleaning is paused once. No log cleaning can be done in this state.
 *                                In this state, it can become None or LogCleaningPaused(2).
 *                                Valid previous state is None, LogCleaningAborted or LogCleaningPaused(2).</li>
 * <li>4-b. LogCleaningPaused(i)  : The cleaning is paused i times where i>= 2. No log cleaning can be done in this state.
 *                                In this state, it can become LogCleaningPaused(i-1) or LogCleaningPaused(i+1).
 *                                Valid previous state is LogCleaningPaused(i-1) or LogCleaningPaused(i+1).</li>
 * </ul>
 */
public class LogCleanerManager {
    public static final String OFFSET_CHECKPOINT_FILE = "cleaner-offset-checkpoint";

    private static final Logger LOG = LoggerFactory.getLogger("kafka.log.LogCleaner");

    private static final String UNCLEANABLE_PARTITIONS_COUNT_METRIC_NAME = "uncleanable-partitions-count";
    private static final String UNCLEANABLE_BYTES_METRIC_NAME = "uncleanable-bytes";
    private static final String MAX_DIRTY_PERCENT_METRIC_NAME = "max-dirty-percent";
    private static final String TIME_SINCE_LAST_RUN_MS_METRIC_NAME = "time-since-last-run-ms";

    // Visible for testing
    public static final Set<String> GAUGE_METRIC_NAME_NO_TAG = Set.of(MAX_DIRTY_PERCENT_METRIC_NAME, TIME_SINCE_LAST_RUN_MS_METRIC_NAME);

    // For compatibility, metrics are defined to be under `kafka.log.LogCleanerManager` class
    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup("kafka.log", "LogCleanerManager");

    /**
     * The set of logs currently being cleaned.
     */
    private final Map<TopicPartition, LogCleaningState> inProgress = new HashMap<>();

    /**
     * The set of uncleanable partitions (partitions that have raised an unexpected error during cleaning)
     * for each log directory.
     */
    private final Map<String, Set<TopicPartition>> uncleanablePartitions = new HashMap<>();

    /**
     * A global lock used to control all access to the in-progress set and the offset checkpoints.
     */
    private final Lock lock = new ReentrantLock();

    /**
     * For coordinating the pausing and the cleaning of a partition.
     */
    private final Condition pausedCleaningCond = lock.newCondition();

    private final Map<String, List<Map<String, String>>> gaugeMetricNameWithTag = new HashMap<>();

    private final ConcurrentMap<TopicPartition, UnifiedLog> logs;

    /**
     * The offset checkpoints holding the last cleaned point for each log.
     */
    private volatile Map<File, OffsetCheckpointFile> checkpoints;

    private volatile double dirtiestLogCleanableRatio;
    private volatile long timeOfLastRun;

    @SuppressWarnings({"this-escape"})
    public LogCleanerManager(
            List<File> logDirs,
            ConcurrentMap<TopicPartition, UnifiedLog> logs,
            LogDirFailureChannel logDirFailureChannel
    ) {
        this.logs = logs;
        checkpoints = logDirs.stream()
                .collect(Collectors.toMap(
                        dir -> dir,
                        dir -> {
                            try {
                                return new OffsetCheckpointFile(new File(dir, OFFSET_CHECKPOINT_FILE), logDirFailureChannel);
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                ));

        registerMetrics(logDirs);
    }

    private void registerMetrics(List<File> logDirs) {
        // gauges for tracking the number of partitions marked as uncleanable for each log directory
        for (File dir : logDirs) {
            Map<String, String> metricTag = Map.of("logDirectory", dir.getAbsolutePath());
            metricsGroup.newGauge(
                    UNCLEANABLE_PARTITIONS_COUNT_METRIC_NAME,
                    () -> inLock(lock, () -> uncleanablePartitions.getOrDefault(dir.getAbsolutePath(), Set.of()).size()),
                    metricTag
            );

            gaugeMetricNameWithTag
                    .computeIfAbsent(UNCLEANABLE_PARTITIONS_COUNT_METRIC_NAME, k -> new ArrayList<>())
                    .add(metricTag);
        }

        // gauges for tracking the number of uncleanable bytes from uncleanable partitions for each log directory
        for (File dir : logDirs) {
            Map<String, String> metricTag = Map.of("logDirectory", dir.getAbsolutePath());
            metricsGroup.newGauge(
                    UNCLEANABLE_BYTES_METRIC_NAME,
                    () -> inLock(lock, () -> {
                        Set<TopicPartition> partitions = uncleanablePartitions.get(dir.getAbsolutePath());

                        if (partitions == null) {
                            return 0;
                        } else {
                            Map<TopicPartition, Long> lastClean = allCleanerCheckpoints();
                            long now = Time.SYSTEM.milliseconds();
                            return partitions.stream()
                                    .mapToLong(tp -> {
                                        UnifiedLog log = logs.get(tp);
                                        if (log != null) {
                                            Optional<Long> lastCleanOffset = Optional.of(lastClean.get(tp));
                                            try {
                                                OffsetsToClean offsetsToClean = cleanableOffsets(log, lastCleanOffset, now);
                                                return calculateCleanableBytes(log, offsetsToClean.firstDirtyOffset(),
                                                        offsetsToClean.firstUncleanableDirtyOffset()).getValue();
                                            } catch (IOException e) {
                                                throw new RuntimeException(e);
                                            }
                                        } else {
                                            return 0L;
                                        }
                                    }).sum();
                        }
                    }),
                    metricTag
            );

            gaugeMetricNameWithTag
                    .computeIfAbsent(UNCLEANABLE_BYTES_METRIC_NAME, k -> new ArrayList<>())
                    .add(metricTag);
        }

        // a gauge for tracking the cleanable ratio of the dirtiest log
        dirtiestLogCleanableRatio = 0.0;
        metricsGroup.newGauge(MAX_DIRTY_PERCENT_METRIC_NAME, () -> (int) (100 * dirtiestLogCleanableRatio));

        // a gauge for tracking the time since the last log cleaner run, in milliseconds
        timeOfLastRun = Time.SYSTEM.milliseconds();
        metricsGroup.newGauge(TIME_SINCE_LAST_RUN_MS_METRIC_NAME, () -> Time.SYSTEM.milliseconds() - timeOfLastRun);
    }

    public Map<String, List<Map<String, String>>> gaugeMetricNameWithTag() {
        return gaugeMetricNameWithTag;
    }

    /**
     * @return the position processed for all logs.
     */
    public Map<TopicPartition, Long> allCleanerCheckpoints() {
        return inLock(lock, () -> checkpoints.values().stream()
                .flatMap(checkpoint -> {
                    try {
                        return checkpoint.read().entrySet().stream();
                    } catch (KafkaStorageException e) {
                        LOG.error("Failed to access checkpoint file {} in dir {}",
                                checkpoint.file().getName(), checkpoint.file().getParentFile().getAbsolutePath(), e);
                        return Stream.empty();
                    }
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    /**
     * For testing only. Get the cleaning state of the partition.
     */
    Optional<LogCleaningState> cleaningState(TopicPartition tp) {
        return inLock(lock, () -> Optional.ofNullable(inProgress.get(tp)));
    }

    /**
     * For testing only. Set the cleaning state of the partition.
     */
    void setCleaningState(TopicPartition tp, LogCleaningState state) {
        inLock(lock, () -> inProgress.put(tp, state));
    }

    /**
     * Choose the log to clean next and add it to the in-progress set. We recompute this
     * each time from the full set of logs to allow logs to be dynamically added to the pool of logs
     * the log manager maintains.
     */
    public Optional<LogToClean> grabFilthiestCompactedLog(Time time, PreCleanStats preCleanStats) {
        return inLock(lock, () -> {
            long now = time.milliseconds();
            timeOfLastRun = now;
            Map<TopicPartition, Long> lastClean = allCleanerCheckpoints();

            List<LogToClean> dirtyLogs = logs.entrySet().stream()
                    .filter(entry -> entry.getValue().config().compact &&
                            !inProgress.containsKey(entry.getKey()) &&
                            !isUncleanablePartition(entry.getValue(), entry.getKey())
                    )
                    .map(entry -> {
                                // create a LogToClean instance for each
                                TopicPartition topicPartition = entry.getKey();
                                UnifiedLog log = entry.getValue();
                                try {
                                    Long lastCleanOffset = lastClean.get(topicPartition);
                                    OffsetsToClean offsetsToClean = cleanableOffsets(log, Optional.ofNullable(lastCleanOffset), now);
                                    // update checkpoint for logs with invalid checkpointed offsets
                                    if (offsetsToClean.forceUpdateCheckpoint) {
                                        updateCheckpoints(log.parentDirFile(), Optional.of(Map.entry(topicPartition, offsetsToClean.firstDirtyOffset)), Optional.empty());
                                    }
                                    long compactionDelayMs = maxCompactionDelay(log, offsetsToClean.firstDirtyOffset, now);
                                    preCleanStats.updateMaxCompactionDelay(compactionDelayMs);

                                    return new LogToClean(log, offsetsToClean.firstDirtyOffset,
                                            offsetsToClean.firstUncleanableDirtyOffset, compactionDelayMs > 0);
                                } catch (Throwable e) {
                                    throw new LogCleaningException(log, "Failed to calculate log cleaning stats for partition " + topicPartition, e);
                                }
                            }
                    )
                    .filter(ltc -> ltc.totalBytes() > 0) // skip any empty logs
                    .toList();

            dirtiestLogCleanableRatio = dirtyLogs.isEmpty()
                    ? 0
                    : dirtyLogs.stream()
                        .mapToDouble(LogToClean::cleanableRatio)
                        .max()
                        .orElse(0.0);
            // and must meet the minimum threshold for dirty byte ratio or have some bytes required to be compacted
            List<LogToClean> cleanableLogs = dirtyLogs.stream()
                    .filter(ltc -> (ltc.needCompactionNow() && ltc.cleanableBytes() > 0) || ltc.cleanableRatio() > ltc.log().config().minCleanableRatio)
                    .toList();

            if (cleanableLogs.isEmpty()) {
                return Optional.empty();
            } else {
                preCleanStats.recordCleanablePartitions(cleanableLogs.size());
                LogToClean filthiest = cleanableLogs.stream()
                        .max(Comparator.comparingDouble(LogToClean::cleanableRatio))
                        .orElseThrow(() -> new IllegalStateException("No filthiest log found"));

                inProgress.put(filthiest.topicPartition(), LogCleaningState.LOG_CLEANING_IN_PROGRESS);
                return Optional.of(filthiest);
            }
        });
    }

    /**
     * Pause logs cleaning for logs that do not have compaction enabled
     * and do not have other deletion or compaction in progress.
     * This is to handle potential race between retention and cleaner threads when users
     * switch topic configuration between compacted and non-compacted topic.
     *
     * @return retention logs that have log cleaning successfully paused
     */
    public List<Map.Entry<TopicPartition, UnifiedLog>> pauseCleaningForNonCompactedPartitions() {
        return inLock(lock, () -> {
            List<Map.Entry<TopicPartition, UnifiedLog>> deletableLogs = logs.entrySet().stream()
                    .filter(entry -> !entry.getValue().config().compact) // pick non-compacted logs
                    .filter(entry -> !inProgress.containsKey(entry.getKey())) // skip any logs already in-progress
                    .collect(Collectors.toList());

            deletableLogs.forEach(entry -> inProgress.put(entry.getKey(), LogCleaningState.logCleaningPaused(1)));

            return deletableLogs;
        });
    }

    /**
     * Find any logs that have compaction enabled. Mark them as being cleaned
     * Include logs without delete enabled, as they may have segments
     * that precede the start offset.
     */
    public Map<TopicPartition, UnifiedLog> deletableLogs() {
        return inLock(lock, () -> {
            Map<TopicPartition, UnifiedLog> toClean = logs.entrySet().stream()
                    .filter(entry -> {
                        TopicPartition topicPartition = entry.getKey();
                        UnifiedLog log = entry.getValue();
                        return !inProgress.containsKey(topicPartition) && log.config().compact &&
                                !isUncleanablePartition(log, topicPartition);
                    })
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            toClean.forEach((partition, log) -> inProgress.put(partition, LogCleaningState.LOG_CLEANING_IN_PROGRESS));
            return toClean;
        });
    }

    /**
     * Abort the cleaning of a particular partition, if it's in progress. This call blocks until the cleaning of
     * the partition is aborted.
     * This is implemented by first abortAndPausing and then resuming the cleaning of the partition.
     */
    public void abortCleaning(TopicPartition topicPartition) {
        inLock(lock, () -> {
            abortAndPauseCleaning(topicPartition);
            resumeCleaning(Set.of(topicPartition));
            return null;
        });
    }

    /**
     * Abort the cleaning of a particular partition if it's in progress, and pause any future cleaning of this partition.
     * This call blocks until the cleaning of the partition is aborted and paused.
     * <ol>
     * <li>If the partition is not in progress, mark it as paused.</li>
     * <li>Otherwise, first mark the state of the partition as aborted.</li>
     * <li>The cleaner thread checks the state periodically and if it sees the state of the partition is aborted, it
     *    throws a LogCleaningAbortedException to stop the cleaning task.</li>
     * <li>When the cleaning task is stopped, doneCleaning() is called, which sets the state of the partition as paused.</li>
     * <li>abortAndPauseCleaning() waits until the state of the partition is changed to paused.</li>
     * <li>If the partition is already paused, a new call to this function
     *    will increase the paused count by one.</li>
     * </ol>
     */
    public void abortAndPauseCleaning(TopicPartition topicPartition) {
        inLock(lock, () -> {
            LogCleaningState state = inProgress.get(topicPartition);

            if (state == null) {
                inProgress.put(topicPartition, LogCleaningState.logCleaningPaused(1));
            } else if (state == LogCleaningState.LOG_CLEANING_IN_PROGRESS) {
                inProgress.put(topicPartition, LogCleaningState.LOG_CLEANING_ABORTED);
            } else if (state instanceof LogCleaningState.LogCleaningPaused logCleaningPaused) {
                inProgress.put(topicPartition, LogCleaningState.logCleaningPaused(logCleaningPaused.pausedCount() + 1));
            } else {
                throw new IllegalStateException("Compaction for partition " + topicPartition +
                        " cannot be aborted and paused since it is in " + state + " state.");
            }

            while (!isCleaningInStatePaused(topicPartition)) {
                try {
                    pausedCleaningCond.await(100, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            return null;
        });
    }

    /**
     * Resume the cleaning of paused partitions.
     * Each call of this function will undo one pause.
     */
    public void resumeCleaning(Set<TopicPartition> topicPartitions) {
        inLock(lock, () -> {
            topicPartitions.forEach(topicPartition -> {
                LogCleaningState state = inProgress.get(topicPartition);

                if (state == null) {
                    throw new IllegalStateException("Compaction for partition " + topicPartition + " cannot be resumed since it is not paused.");
                }

                if (state instanceof LogCleaningState.LogCleaningPaused logCleaningPaused) {
                    if (logCleaningPaused.pausedCount() == 1) {
                        inProgress.remove(topicPartition);
                    } else if (logCleaningPaused.pausedCount() > 1) {
                        inProgress.put(topicPartition, LogCleaningState.logCleaningPaused(logCleaningPaused.pausedCount() - 1));
                    }
                } else {
                    throw new IllegalStateException("Compaction for partition " + topicPartition +
                            " cannot be resumed since it is in " + state + " state.");
                }
            });

            return null;
        });
    }

    /**
     * Check if the cleaning for a partition is in a particular state. The caller is expected to hold lock while making the call.
     */
    private boolean isCleaningInState(TopicPartition topicPartition, LogCleaningState expectedState) {
        LogCleaningState state = inProgress.get(topicPartition);

        if (state == null) {
            return false;
        } else {
            return state == expectedState;
        }
    }

    /**
     * Check if the cleaning for a partition is paused. The caller is expected to hold lock while making the call.
     */
    private boolean isCleaningInStatePaused(TopicPartition topicPartition) {
        LogCleaningState state = inProgress.get(topicPartition);

        if (state == null) {
            return false;
        } else {
            return state instanceof LogCleaningState.LogCleaningPaused;
        }
    }

    /**
     * Check if the cleaning for a partition is aborted. If so, throw an exception.
     */
    public void checkCleaningAborted(TopicPartition topicPartition) {
        inLock(lock, () -> {
            if (isCleaningInState(topicPartition, LogCleaningState.LOG_CLEANING_ABORTED)) {
                throw new LogCleaningAbortedException();
            }
            return null;
        });
    }

    /**
     * Update checkpoint file, adding or removing partitions if necessary.
     *
     * @param dataDir                The File object to be updated
     * @param partitionToUpdateOrAdd The [TopicPartition, Long] map entry to be updated. pass "Optional.empty" if doing remove, not add
     * @param partitionToRemove      The TopicPartition to be removed
     */
    public void updateCheckpoints(
            File dataDir,
            Optional<Map.Entry<TopicPartition, Long>> partitionToUpdateOrAdd,
            Optional<TopicPartition> partitionToRemove
    ) {
        inLock(lock, () -> {
            OffsetCheckpointFile checkpoint = checkpoints.get(dataDir);
            if (checkpoint != null) {
                try {
                    Map<TopicPartition, Long> currentCheckpoint = checkpoint.read().entrySet().stream()
                            .filter(entry -> logs.containsKey(entry.getKey()))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                    Map<TopicPartition, Long> updatedCheckpoint = new HashMap<>(currentCheckpoint);

                    // Remove the partition offset if present
                    partitionToRemove.ifPresent(updatedCheckpoint::remove);

                    // Update or add the partition offset if present
                    partitionToUpdateOrAdd.ifPresent(entry -> updatedCheckpoint.put(entry.getKey(), entry.getValue()));

                    // Write back the updated checkpoint
                    checkpoint.write(updatedCheckpoint);
                } catch (KafkaStorageException e) {
                    LOG.error("Failed to access checkpoint file {} in dir {}",
                            checkpoint.file().getName(), checkpoint.file().getParentFile().getAbsolutePath(), e);
                }
            }

            return null;
        });
    }

    /**
     * Alter the checkpoint directory for the topicPartition, to remove the data in sourceLogDir, and add the data in destLogDir.
     */
    public void alterCheckpointDir(TopicPartition topicPartition, File sourceLogDir, File destLogDir) {
        inLock(lock, () -> {
            try {
                Optional<Long> offsetOpt = Optional.ofNullable(checkpoints.get(sourceLogDir))
                        .flatMap(checkpoint -> Optional.ofNullable(checkpoint.read().get(topicPartition)));

                offsetOpt.ifPresent(offset -> {
                    LOG.debug("Removing the partition offset data in checkpoint file for '{}' from {} directory.",
                            topicPartition, sourceLogDir.getAbsoluteFile());
                    updateCheckpoints(sourceLogDir, Optional.empty(), Optional.of(topicPartition));

                    LOG.debug("Adding the partition offset data in checkpoint file for '{}' to {} directory.",
                            topicPartition, destLogDir.getAbsoluteFile());
                    updateCheckpoints(destLogDir, Optional.of(Map.entry(topicPartition, offset)), Optional.empty());
                });
            } catch (KafkaStorageException e) {
                LOG.error("Failed to access checkpoint file in dir {}", sourceLogDir.getAbsolutePath(), e);
            }

            Set<TopicPartition> logUncleanablePartitions = uncleanablePartitions.getOrDefault(sourceLogDir.toString(), Collections.emptySet());
            if (logUncleanablePartitions.contains(topicPartition)) {
                logUncleanablePartitions.remove(topicPartition);
                markPartitionUncleanable(destLogDir.toString(), topicPartition);
            }

            return null;
        });
    }

    /**
     * Stop cleaning logs in the provided directory.
     *
     * @param dir the absolute path of the log dir
     */
    public void handleLogDirFailure(String dir) {
        LOG.warn("Stopping cleaning logs in dir {}", dir);
        inLock(lock, () -> {
            checkpoints = checkpoints.entrySet().stream()
                    .filter(entry -> !entry.getKey().getAbsolutePath().equals(dir))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            return null;
        });
    }

    /**
     * Truncate the checkpointed offset for the given partition if its checkpointed offset is larger than the given offset.
     */
    public void maybeTruncateCheckpoint(File dataDir, TopicPartition topicPartition, long offset) {
        inLock(lock, () -> {
            if (logs.get(topicPartition).config().compact) {
                OffsetCheckpointFile checkpoint = checkpoints.get(dataDir);
                if (checkpoint != null) {
                    Map<TopicPartition, Long> existing = checkpoint.read();
                    if (existing.getOrDefault(topicPartition, 0L) > offset) {
                        existing.put(topicPartition, offset);
                        checkpoint.write(existing);
                    }
                }
            }

            return null;
        });
    }

    /**
     * Save out the endOffset and remove the given log from the in-progress set, if not aborted.
     */
    public void doneCleaning(TopicPartition topicPartition, File dataDir, long endOffset) {
        inLock(lock, () -> {
            LogCleaningState state = inProgress.get(topicPartition);

            if (state == null) {
                throw new IllegalStateException("State for partition " + topicPartition + " should exist.");
            } else if (state == LogCleaningState.LOG_CLEANING_IN_PROGRESS) {
                updateCheckpoints(dataDir, Optional.of(Map.entry(topicPartition, endOffset)), Optional.empty());
                inProgress.remove(topicPartition);
            } else if (state == LogCleaningState.LOG_CLEANING_ABORTED) {
                inProgress.put(topicPartition, LogCleaningState.logCleaningPaused(1));
                pausedCleaningCond.signalAll();
            } else {
                throw new IllegalStateException("In-progress partition " + topicPartition + " cannot be in " + state + " state.");
            }

            return null;
        });
    }

    public void doneDeleting(List<TopicPartition> topicPartitions) {
        inLock(lock, () -> {
            topicPartitions.forEach(topicPartition -> {
                LogCleaningState logCleaningState = inProgress.get(topicPartition);

                if (logCleaningState == null) {
                    throw new IllegalStateException("State for partition " + topicPartition + " should exist.");
                } else if (logCleaningState == LogCleaningState.LOG_CLEANING_IN_PROGRESS) {
                    inProgress.remove(topicPartition);
                } else if (logCleaningState == LogCleaningState.LOG_CLEANING_ABORTED) {
                    inProgress.put(topicPartition, LogCleaningState.logCleaningPaused(1));
                    pausedCleaningCond.signalAll();
                } else {
                    throw new IllegalStateException("In-progress partition " + topicPartition + " cannot be in " + logCleaningState + " state.");
                }
            });

            return null;
        });
    }

    /**
     * For testing only. Returns an immutable set of the uncleanable partitions for a given log directory.
     * Only used for testing.
     */
    public Set<TopicPartition> uncleanablePartitions(String logDir) {
        return inLock(lock, () -> {
            Set<TopicPartition> partitions = uncleanablePartitions.get(logDir);
            return partitions != null ? Set.copyOf(partitions) : Set.of();
        });
    }

    public void markPartitionUncleanable(String logDir, TopicPartition partition) {
        inLock(lock, () -> {
            Set<TopicPartition> partitions = uncleanablePartitions.computeIfAbsent(logDir, dir -> new HashSet<>());
            partitions.add(partition);

            return null;
        });
    }

    private boolean isUncleanablePartition(UnifiedLog log, TopicPartition topicPartition) {
        return inLock(lock, () -> Optional.ofNullable(uncleanablePartitions.get(log.parentDir()))
                .map(partitions -> partitions.contains(topicPartition))
                .orElse(false)
        );
    }

    public void maintainUncleanablePartitions() {
        // Remove deleted partitions from uncleanablePartitions
        inLock(lock, () -> {
            // Remove deleted partitions
            uncleanablePartitions.values().forEach(partitions ->
                    partitions.removeIf(partition -> !logs.containsKey(partition)));

            // Remove entries with empty partition set.
            uncleanablePartitions.entrySet().removeIf(entry -> entry.getValue().isEmpty());

            return null;
        });
    }

    public void removeMetrics() {
        GAUGE_METRIC_NAME_NO_TAG.forEach(metricsGroup::removeMetric);
        gaugeMetricNameWithTag.forEach((metricName, tags) ->
                tags.forEach(tag -> metricsGroup.removeMetric(metricName, tag)));
        gaugeMetricNameWithTag.clear();
    }

    private static boolean isCompactAndDelete(UnifiedLog log) {
        return log.config().compact && log.config().delete;
    }

    /**
     * Get max delay between the time when log is required to be compacted as determined
     * by maxCompactionLagMs and the current time.
     */
    private static long maxCompactionDelay(UnifiedLog log, long firstDirtyOffset, long now) {
        List<LogSegment> dirtyNonActiveSegments = log.nonActiveLogSegmentsFrom(firstDirtyOffset);
        Stream<Long> firstBatchTimestamps = log.getFirstBatchTimestampForSegments(dirtyNonActiveSegments).stream()
                .filter(timestamp -> timestamp > 0);

        long earliestDirtySegmentTimestamp = firstBatchTimestamps.min(Comparator.naturalOrder()).orElse(Long.MAX_VALUE);

        long maxCompactionLagMs = Math.max(log.config().maxCompactionLagMs, 0L);
        long cleanUntilTime = now - maxCompactionLagMs;

        return earliestDirtySegmentTimestamp < cleanUntilTime ? cleanUntilTime - earliestDirtySegmentTimestamp : 0L;
    }

    /**
     * Returns the range of dirty offsets that can be cleaned.
     *
     * @param log             the log
     * @param lastCleanOffset the last checkpointed offset
     * @param now             the current time in milliseconds of the cleaning operation
     * @return OffsetsToClean containing offsets for cleanable portion of log and whether the log checkpoint needs updating
     * @throws IOException    if an I/O error occurs
     */
    // Visible for testing
    static OffsetsToClean cleanableOffsets(UnifiedLog log, Optional<Long> lastCleanOffset, long now) throws IOException {
        // If the log segments are abnormally truncated and hence the checkpointed offset is no longer valid;
        // reset to the log starting offset and log the error

        long logStartOffset = log.logStartOffset();
        long checkpointDirtyOffset = lastCleanOffset.orElse(logStartOffset);

        long firstDirtyOffset;
        boolean forceUpdateCheckpoint;

        if (checkpointDirtyOffset < logStartOffset) {
            // Don't bother with the warning if compact and delete are enabled.
            if (!isCompactAndDelete(log))
                LOG.warn("Resetting first dirty offset of {} to log start offset {} since the checkpointed offset {} is invalid.",
                        log.name(), logStartOffset, checkpointDirtyOffset);

            firstDirtyOffset = logStartOffset;
            forceUpdateCheckpoint = true;
        } else if (checkpointDirtyOffset > log.logEndOffset()) {
            // The dirty offset has gotten ahead of the log end offset. This could happen if there was data
            // corruption at the end of the log. We conservatively assume that the full log needs cleaning.
            LOG.warn("The last checkpoint dirty offset for partition {} is {}, " +
                            "which is larger than the log end offset {}. Resetting to the log start offset {}.",
                    log.name(), checkpointDirtyOffset, log.logEndOffset(), logStartOffset);

            firstDirtyOffset = logStartOffset;
            forceUpdateCheckpoint = true;
        } else {
            firstDirtyOffset = checkpointDirtyOffset;
            forceUpdateCheckpoint = false;
        }

        long minCompactionLagMs = Math.max(log.config().compactionLagMs, 0L);

        // Find the first segment that cannot be cleaned. We cannot clean past:
        // 1. The active segment
        // 2. The last stable offset (including the high watermark)
        // 3. Any segments closer to the head of the log than the minimum compaction lag time
        long firstUncleanableDirtyOffset = Stream.of(
                        // we do not clean beyond the last stable offset
                        Optional.of(log.lastStableOffset()),

                        // the active segment is always uncleanable
                        Optional.of(log.activeSegment().baseOffset()),

                        // the first segment whose largest message timestamp is within a minimum time lag from now
                        minCompactionLagMs > 0 ? findFirstUncleanableSegment(log, firstDirtyOffset, now, minCompactionLagMs) : Optional.<Long>empty()
                )
                .flatMap(Optional::stream)
                .min(Long::compare)
                .orElseThrow(() -> new IllegalStateException("No uncleanable offset found"));

        LOG.debug("Finding range of cleanable offsets for log={}. Last clean offset={} " +
                        "now={} => firstDirtyOffset={} firstUncleanableOffset={} activeSegment.baseOffset={}",
                log.name(), lastCleanOffset, now, firstDirtyOffset, firstUncleanableDirtyOffset, log.activeSegment().baseOffset());

        return new OffsetsToClean(firstDirtyOffset, Math.max(firstDirtyOffset, firstUncleanableDirtyOffset), forceUpdateCheckpoint);
    }

    /**
     * Given the first dirty offset and an uncleanable offset, calculates the total cleanable bytes for this log.
     *
     * @return the biggest uncleanable offset and the total amount of cleanable bytes
     */
    public static Map.Entry<Long, Long> calculateCleanableBytes(UnifiedLog log, long firstDirtyOffset, long uncleanableOffset) {
        List<LogSegment> nonActiveSegments = log.nonActiveLogSegmentsFrom(uncleanableOffset);
        LogSegment firstUncleanableSegment = nonActiveSegments.isEmpty() ? log.activeSegment() : nonActiveSegments.get(0);
        long firstUncleanableOffset = firstUncleanableSegment.baseOffset();
        long cleanableBytes = log.logSegments(Math.min(firstDirtyOffset, firstUncleanableOffset), firstUncleanableOffset).stream()
                .mapToLong(LogSegment::size)
                .sum();

        return Map.entry(firstUncleanableOffset, cleanableBytes);
    }

    private static Optional<Long> findFirstUncleanableSegment(UnifiedLog log, long firstDirtyOffset, long now, long minCompactionLagMs) throws IOException {
        List<LogSegment> dirtyNonActiveSegments = log.nonActiveLogSegmentsFrom(firstDirtyOffset);

        for (LogSegment segment : dirtyNonActiveSegments) {
            boolean isUncleanable = segment.largestTimestamp() > now - minCompactionLagMs;

            LOG.debug("Checking if log segment may be cleaned: log='{}' segment.baseOffset={} " +
                            "segment.largestTimestamp={}; now - compactionLag={}; is uncleanable={}",
                    log.name(), segment.baseOffset(), segment.largestTimestamp(), now - minCompactionLagMs, isUncleanable);

            if (isUncleanable) {
                return Optional.of(segment.baseOffset());
            }
        }

        return Optional.empty();
    }

    /**
     * Helper class for the range of cleanable dirty offsets of a log and whether to update the checkpoint associated with
     * the log.
     *
     * @param firstDirtyOffset            the lower (inclusive) offset to begin cleaning from
     * @param firstUncleanableDirtyOffset the upper(exclusive) offset to clean to
     * @param forceUpdateCheckpoint       whether to update the checkpoint associated with this log. if true, checkpoint should be
     *                                    reset to firstDirtyOffset
     */
    record OffsetsToClean(long firstDirtyOffset, long firstUncleanableDirtyOffset, boolean forceUpdateCheckpoint) {
    }
}
