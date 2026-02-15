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
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.config.BrokerReconfigurable;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.util.ShutdownableThread;
import org.apache.kafka.storage.internals.utils.Throttler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.DigestException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The cleaner is responsible for removing obsolete records from logs which have the "compact" retention strategy.
 * A message with key K and offset O is obsolete if there exists a message with key K and offset O' such that O < O'.
 * <p>
 * Each log can be thought of being split into two sections of segments: a "clean" section which has previously been cleaned followed by a
 * "dirty" section that has not yet been cleaned. The dirty section is further divided into the "cleanable" section followed by an "uncleanable" section.
 * The uncleanable section is excluded from cleaning. The active log segment is always uncleanable. If there is a
 * compaction lag time set, segments whose largest message timestamp is within the compaction lag time of the cleaning operation are also uncleanable.
 * <p>
 * The cleaning is carried out by a pool of background threads. Each thread chooses the dirtiest log that has the "compact" retention policy
 * and cleans that. The dirtiness of the log is guessed by taking the ratio of bytes in the dirty section of the log to the total bytes in the log.
 * <p>
 * To clean a log the cleaner first builds a mapping of key=>last_offset for the dirty section of the log. See {@link OffsetMap} for details of
 * the implementation of the mapping.
 * <p>
 * Once the key=>last_offset map is built, the log is cleaned by recopying each log segment but omitting any key that appears in the offset map with a
 * higher offset than what is found in the segment (i.e. messages with a key that appears in the dirty section of the log).
 * <p>
 * To avoid segments shrinking to very small sizes with repeated cleanings we implement a rule by which if we will merge successive segments when
 * doing a cleaning if their log and index size are less than the maximum log and index size prior to the clean beginning.
 * <p>
 * Cleaned segments are swapped into the log as they become available.
 * <p>
 * One nuance that the cleaner must handle is log truncation. If a log is truncated while it is being cleaned the cleaning of that log is aborted.
 * <p>
 * Messages with null payload are treated as deletes for the purpose of log compaction. This means that they receive special treatment by the cleaner.
 * The cleaner will only retain delete records for a period of time to avoid accumulating space indefinitely. This period of time is configurable on a per-topic
 * basis and is measured from the time the segment enters the clean portion of the log (at which point any prior message with that key has been removed).
 * Delete markers in the clean section of the log that are older than this time will not be retained when log segments are being recopied as part of cleaning.
 * This time is tracked by setting the base timestamp of a record batch with delete markers when the batch is recopied in the first cleaning that encounters
 * it. The relative timestamps of the records in the batch are also modified when recopied in this cleaning according to the new base timestamp of the batch.
 * <p>
 * Note that cleaning is more complicated with the idempotent/transactional producer capabilities. The following
 * are the key points:
 * <p>
 * <ol>
 * <li>In order to maintain sequence number continuity for active producers, we always retain the last batch
 *    from each producerId, even if all the records from the batch have been removed. The batch will be removed
 *    once the producer either writes a new batch or is expired due to inactivity.</li>
 * <li>We do not clean beyond the last stable offset. This ensures that all records observed by the cleaner have
 *    been decided (i.e. committed or aborted). In particular, this allows us to use the transaction index to
 *    collect the aborted transactions ahead of time.</li>
 * <li>Records from aborted transactions are removed by the cleaner immediately without regard to record keys.</li>
 * <li>Transaction markers are retained until all record batches from the same transaction have been removed and
 *    a sufficient amount of time has passed to reasonably ensure that an active consumer wouldn't consume any
 *    data from the transaction prior to reaching the offset of the marker. This follows the same logic used for
 *    tombstone deletion.</li>
 * </ol>
 */
public class LogCleaner implements BrokerReconfigurable {
    private static final Logger LOG = LoggerFactory.getLogger(LogCleaner.class);

    public static final Set<String> RECONFIGURABLE_CONFIGS = Set.of(
            CleanerConfig.LOG_CLEANER_THREADS_PROP,
            CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP,
            CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_PROP,
            CleanerConfig.LOG_CLEANER_IO_BUFFER_SIZE_PROP,
            ServerConfigs.MESSAGE_MAX_BYTES_CONFIG,
            CleanerConfig.LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_PROP,
            CleanerConfig.LOG_CLEANER_BACKOFF_MS_PROP
    );

    // Visible for test
    public static final String MAX_BUFFER_UTILIZATION_PERCENT_METRIC_NAME = "max-buffer-utilization-percent";
    public static final String MAX_CLEAN_TIME_METRIC_NAME = "max-clean-time-secs";
    public static final String MAX_COMPACTION_DELAY_METRICS_NAME = "max-compaction-delay-secs";

    private static final String CLEANER_RECOPY_PERCENT_METRIC_NAME = "cleaner-recopy-percent";
    private static final String DEAD_THREAD_COUNT_METRIC_NAME = "DeadThreadCount";

    // Visible for test
    public static final Set<String> METRIC_NAMES = Set.of(
            MAX_BUFFER_UTILIZATION_PERCENT_METRIC_NAME,
            CLEANER_RECOPY_PERCENT_METRIC_NAME,
            MAX_CLEAN_TIME_METRIC_NAME,
            MAX_COMPACTION_DELAY_METRICS_NAME,
            DEAD_THREAD_COUNT_METRIC_NAME
    );

    // For compatibility, metrics are defined to be under `kafka.log.LogCleaner` class
    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup("kafka.log", "LogCleaner");

    /**
     * For managing the state of partitions being cleaned.
     */
    private final LogCleanerManager cleanerManager;

    /**
     * A throttle used to limit the I/O of all the cleaner threads to a user-specified maximum rate.
     */
    private final Throttler throttler;
    private final ConcurrentMap<TopicPartition, UnifiedLog> logs;
    private final LogDirFailureChannel logDirFailureChannel;
    private final Time time;
    private final List<CleanerThread> cleaners = new ArrayList<>();

    /**
     * Log cleaner configuration which may be dynamically updated.
     */
    private volatile CleanerConfig config;

    /**
     * @param initialConfig Initial configuration parameters for the cleaner. Actual config may be dynamically updated.
     * @param logDirs The directories where offset checkpoints reside
     * @param logs The map of logs
     * @param logDirFailureChannel The channel used to add offline log dirs that may be encountered when cleaning the log
     * @param time A way to control the passage of time
     */
    @SuppressWarnings("this-escape")
    public LogCleaner(CleanerConfig initialConfig,
                      List<File> logDirs,
                      ConcurrentMap<TopicPartition, UnifiedLog> logs,
                      LogDirFailureChannel logDirFailureChannel,
                      Time time) {
        config = initialConfig;
        this.logs = logs;
        this.logDirFailureChannel = logDirFailureChannel;
        cleanerManager = new LogCleanerManager(logDirs, logs, this.logDirFailureChannel);
        this.time = time;
        throttler = new Throttler(config.maxIoBytesPerSecond, 300, "cleaner-io", "bytes", this.time);

        registerMetrics();
    }

    private void registerMetrics() {
        /* a metric to track the maximum utilization of any thread's buffer in the last cleaning */
        metricsGroup.newGauge(MAX_BUFFER_UTILIZATION_PERCENT_METRIC_NAME,
                () -> (int) (maxOverCleanerThreads(t -> t.lastStats.bufferUtilization) * 100));

        /* a metric to track the recopy rate of each thread's last cleaning */
        metricsGroup.newGauge(CLEANER_RECOPY_PERCENT_METRIC_NAME, () -> {
            List<CleanerStats> stats = cleaners.stream().map(t -> t.lastStats).toList();
            double recopyRate = (double) stats.stream().mapToLong(stat -> stat.bytesWritten).sum() /
                    Math.max(stats.stream().mapToLong(stat -> stat.bytesRead).sum(), 1);
            return (int) (100 * recopyRate);
        });

        /* a metric to track the maximum cleaning time for the last cleaning from each thread */
        metricsGroup.newGauge(MAX_CLEAN_TIME_METRIC_NAME, () -> (int) maxOverCleanerThreads(t -> t.lastStats.elapsedSecs()));

        // a metric to track delay between the time when a log is required to be compacted
        // as determined by max compaction lag and the time of last cleaner run.
        metricsGroup.newGauge(MAX_COMPACTION_DELAY_METRICS_NAME,
                () -> (int) (maxOverCleanerThreads(t -> (double) t.lastPreCleanStats.maxCompactionDelayMs()) / 1000));

        metricsGroup.newGauge(DEAD_THREAD_COUNT_METRIC_NAME, this::deadThreadCount);
    }

    /**
     * Start the background cleaner threads.
     */
    public void startup() {
        if (config.numThreads < 1) {
            LOG.warn("Invalid value for `log.cleaner.threads`: must be >= 1 starting from Kafka 5.0 since log cleaner" +
                    " is always enabled.");
        }

        LOG.info("Starting the log cleaner");
        IntStream.range(0, config.numThreads).forEach(i -> {
            try {
                CleanerThread cleaner = new CleanerThread(i);
                cleaners.add(cleaner);
                cleaner.start();
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Stop the background cleaner threads.
     */
    private void shutdownCleaners() {
        LOG.info("Shutting down the log cleaner.");
        cleaners.forEach(thread -> {
            try {
                thread.shutdown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        cleaners.clear();
    }

    /**
     * Stop the background cleaner threads.
     */
    public void shutdown() {
        try {
            shutdownCleaners();
        } finally {
            removeMetrics();
        }
    }

    /**
     * Remove metrics.
     */
    public void removeMetrics() {
        METRIC_NAMES.forEach(metricsGroup::removeMetric);
        cleanerManager.removeMetrics();
    }

    /**
     * @return A set of configs that is reconfigurable in LogCleaner
     */
    @Override
    public Set<String> reconfigurableConfigs() {
        return RECONFIGURABLE_CONFIGS;
    }

    /**
     * Validate the new cleaner threads num is reasonable.
     *
     * @param newConfig A submitted new AbstractConfig instance that contains new cleaner config
     */
    @Override
    public void validateReconfiguration(AbstractConfig newConfig) {
        int numThreads = new CleanerConfig(newConfig).numThreads;
        int currentThreads = config.numThreads;
        if (numThreads < 1)
            throw new ConfigException("Log cleaner threads should be at least 1");
        if (numThreads < currentThreads / 2)
            throw new ConfigException("Log cleaner threads cannot be reduced to less than half the current value " + currentThreads);
        if (numThreads > currentThreads * 2)
            throw new ConfigException("Log cleaner threads cannot be increased to more than double the current value " + currentThreads);

    }

    /**
     * Reconfigure log clean config. The will:
     * <ol>
     *   <li>update desiredRatePerSec in Throttler with logCleanerIoMaxBytesPerSecond, if necessary</li>
     *   <li>stop current log cleaners and create new ones.</li>
     * </ol>
     * That ensures that if any of the cleaners had failed, new cleaners are created to match the new config.
     *
     * @param oldConfig the old log cleaner config
     * @param newConfig the new log cleaner config reconfigured
     */
    @Override
    public void reconfigure(AbstractConfig oldConfig, AbstractConfig newConfig) {
        config = new CleanerConfig(newConfig);

        double maxIoBytesPerSecond = config.maxIoBytesPerSecond;
        if (maxIoBytesPerSecond != oldConfig.getDouble(CleanerConfig.LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_PROP)) {
            LOG.info("Updating logCleanerIoMaxBytesPerSecond: {}", maxIoBytesPerSecond);
            throttler.updateDesiredRatePerSec(maxIoBytesPerSecond);
        }
        // call shutdownCleaners() instead of shutdown to avoid unnecessary deletion of metrics
        shutdownCleaners();
        startup();
    }

    /**
     *  Abort the cleaning of a particular partition, if it's in progress. This call blocks until the cleaning of
     *  the partition is aborted.
     *
     *  @param topicPartition The topic and partition to abort cleaning
     */
    public void abortCleaning(TopicPartition topicPartition) {
        cleanerManager.abortCleaning(topicPartition);
    }

    /**
     * Update checkpoint file to remove partitions if necessary.
     *
     * @param dataDir The data dir to be updated if necessary
     * @param partitionToRemove The topicPartition to be removed
     */
    public void updateCheckpoints(File dataDir, Optional<TopicPartition> partitionToRemove) {
        cleanerManager.updateCheckpoints(dataDir, Optional.empty(), partitionToRemove);
    }

    /**
     * Alter the checkpoint directory for the `topicPartition`, to remove the data in `sourceLogDir`, and add the data in `destLogDir`.
     * Generally occurs when the disk balance ends and replaces the previous file with the future file.
     *
     * @param topicPartition The topic and partition to alter checkpoint
     * @param sourceLogDir The source log dir to remove checkpoint
     * @param destLogDir The dest log dir to remove checkpoint
     */
    public void alterCheckpointDir(TopicPartition topicPartition, File sourceLogDir, File destLogDir) {
        cleanerManager.alterCheckpointDir(topicPartition, sourceLogDir, destLogDir);
    }

    /**
     * Stop cleaning logs in the provided directory when handling log dir failure.
     *
     * @param dir     the absolute path of the log dir
     */
    public void handleLogDirFailure(String dir) {
        cleanerManager.handleLogDirFailure(dir);
    }

    /**
     * Truncate cleaner offset checkpoint for the given partition if its checkpoint offset is larger than the given offset.
     *
     * @param dataDir The data dir to be truncated if necessary
     * @param topicPartition The topic and partition to truncate checkpoint offset
     * @param offset The given offset to be compared
     */
    public void maybeTruncateCheckpoint(File dataDir, TopicPartition topicPartition, long offset) {
        cleanerManager.maybeTruncateCheckpoint(dataDir, topicPartition, offset);
    }

    /**
     *  Abort the cleaning of a particular partition if it's in progress, and pause any future cleaning of this partition.
     *  This call blocks until the cleaning of the partition is aborted and paused.
     *
     *  @param topicPartition The topic and partition to abort and pause cleaning
     */
    public void abortAndPauseCleaning(TopicPartition topicPartition) {
        cleanerManager.abortAndPauseCleaning(topicPartition);
    }

    /**
     *  Resume the cleaning of paused partitions.
     *
     *  @param topicPartitions The collection of topicPartitions to be resumed cleaning
     */
    public void resumeCleaning(Set<TopicPartition> topicPartitions) {
        cleanerManager.resumeCleaning(topicPartitions);
    }

    /**
     * For testing, a way to know when work has completed. This method waits until the
     * cleaner has processed up to the given offset on the specified topic/partition.
     *
     * @param topicPartition The topic and partition to be cleaned
     * @param offset The first dirty offset that the cleaner doesn't have to clean
     * @param maxWaitMs The maximum time in ms to wait for cleaner
     *
     * @return A boolean indicating whether the work has completed before timeout
     */
    public boolean awaitCleaned(TopicPartition topicPartition, long offset, long maxWaitMs) throws InterruptedException {
        long remainingWaitMs = maxWaitMs;
        while (!isCleaned(topicPartition, offset) && remainingWaitMs > 0) {
            long sleepTime = Math.min(100, remainingWaitMs);
            Thread.sleep(sleepTime);
            remainingWaitMs -= sleepTime;
        }
        return isCleaned(topicPartition, offset);
    }

    private boolean isCleaned(TopicPartition topicPartition, long offset) {
        return Optional.ofNullable(cleanerManager.allCleanerCheckpoints().get(topicPartition))
                .map(checkpoint -> checkpoint >= offset)
                .orElse(false);
    }

    /**
     * To prevent race between retention and compaction,
     * retention threads need to make this call to obtain:
     *
     * @return A map of log partitions that retention threads can safely work on
     */
    public Map<TopicPartition, UnifiedLog> pauseCleaningForNonCompactedPartitions() {
        return cleanerManager.pauseCleaningForNonCompactedPartitions().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * @param f to compute the result
     * @return the max value or 0 if there is no cleaner
     */
    public double maxOverCleanerThreads(Function<CleanerThread, Double> f) {
        return cleaners.stream()
                .mapToDouble(f::apply)
                .max()
                .orElse(0.0d);
    }

    public int deadThreadCount() {
        return (int) cleaners.stream()
                .filter(ShutdownableThread::isThreadFailed)
                .count();
    }

    // Only for testing
    public LogCleanerManager cleanerManager() {
        return cleanerManager;
    }

    // Only for testing
    public Throttler throttler() {
        return throttler;
    }

    // Only for testing
    public ConcurrentMap<TopicPartition, UnifiedLog> logs() {
        return logs;
    }

    // Only for testing
    public List<CleanerThread> cleaners() {
        return cleaners;
    }

    // Only for testing
    public KafkaMetricsGroup metricsGroup() {
        return metricsGroup;
    }

    // Only for testing
    public CleanerConfig currentConfig() {
        return config;
    }

    // Only for testing
    public int cleanerCount() {
        return cleaners.size();
    }

    /**
     * The cleaner threads do the actual log cleaning. Each thread processes does its cleaning repeatedly by
     * choosing the dirtiest log, cleaning it, and then swapping in the cleaned segments.
     */
    public final class CleanerThread extends ShutdownableThread {
        private final Logger logger = new LogContext(logPrefix).logger(CleanerThread.class);

        private final Cleaner cleaner;

        private volatile CleanerStats lastStats = new CleanerStats(Time.SYSTEM);
        private volatile PreCleanStats lastPreCleanStats = new PreCleanStats();

        public CleanerThread(int threadId) throws NoSuchAlgorithmException {
            super("kafka-log-cleaner-thread-" + threadId, false);

            cleaner = new Cleaner(
                    threadId,
                    new SkimpyOffsetMap((int) Math.min(config.dedupeBufferSize / config.numThreads, Integer.MAX_VALUE), config.hashAlgorithm()),
                    config.ioBufferSize / config.numThreads / 2,
                    config.maxMessageSize,
                    config.dedupeBufferLoadFactor,
                    throttler,
                    time,
                    this::checkDone
            );

            if (config.dedupeBufferSize / config.numThreads > Integer.MAX_VALUE) {
                logger.warn("Cannot use more than 2G of cleaner buffer space per cleaner thread, ignoring excess buffer space...");
            }
        }

        /**
         *  Check if the cleaning for a partition is aborted. If so, throw an exception.
         *
         *  @param topicPartition The topic and partition to check
         */
        private void checkDone(TopicPartition topicPartition) {
            if (!isRunning()) {
                throw new ThreadShutdownException();
            }

            cleanerManager.checkCleaningAborted(topicPartition);
        }

        /**
         * The main loop for the cleaner thread.
         * Clean a log if there is a dirty log available, otherwise sleep for a bit.
         */
        @Override
        public void doWork() {
            boolean cleaned = tryCleanFilthiestLog();
            if (!cleaned) {
                try {
                    pause(config.backoffMs, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            cleanerManager.maintainUncleanablePartitions();
        }

        public CleanerStats lastStats() {
            return lastStats;
        }

        public void setLastStats(CleanerStats lastStats) {
            this.lastStats = lastStats;
        }

        public PreCleanStats lastPreCleanStats() {
            return lastPreCleanStats;
        }

        /**
         * Cleans a log if there is a dirty log available.
         *
         * @return whether a log was cleaned
         */
        private boolean tryCleanFilthiestLog() {
            try {
                return cleanFilthiestLog();
            } catch (LogCleaningException e) {
                logger.warn("Unexpected exception thrown when cleaning log {}. Marking its partition ({}) as uncleanable", e.log, e.log.topicPartition(), e);
                cleanerManager.markPartitionUncleanable(e.log.parentDir(), e.log.topicPartition());

                return false;
            }
        }

        private boolean cleanFilthiestLog() throws LogCleaningException {
            PreCleanStats preCleanStats = new PreCleanStats();
            Optional<LogToClean> ltc = cleanerManager.grabFilthiestCompactedLog(time, preCleanStats);
            boolean cleaned;

            if (ltc.isEmpty()) {
                cleaned = false;
            } else {
                // there's a log, clean it
                this.lastPreCleanStats = preCleanStats;
                LogToClean cleanable = ltc.get();
                try {
                    cleanLog(cleanable);
                    cleaned = true;
                } catch (ThreadShutdownException e) {
                    throw e;
                } catch (Exception e) {
                    throw new LogCleaningException(cleanable.log(), e.getMessage(), e);
                }
            }

            Map<TopicPartition, UnifiedLog> deletable = cleanerManager.deletableLogs();
            try {
                deletable.forEach((topicPartition, log) -> {
                    try {
                        log.deleteOldSegments();
                    } catch (ThreadShutdownException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new LogCleaningException(log, e.getMessage(), e);
                    }
                });
            } finally {
                cleanerManager.doneDeleting(deletable.keySet().stream().toList());
            }

            return cleaned;
        }

        private void cleanLog(LogToClean cleanable) throws DigestException {
            long startOffset = cleanable.firstDirtyOffset();
            long endOffset = startOffset;
            try {
                Map.Entry<Long, CleanerStats> entry = cleaner.clean(cleanable);
                endOffset = entry.getKey();
                recordStats(cleaner.id(), cleanable.log().name(), startOffset, endOffset, entry.getValue());
            } catch (LogCleaningAbortedException ignored) {
                // task can be aborted, let it go.
            } catch (KafkaStorageException ignored) {
                // partition is already offline. let it go.
            } catch (IOException e) {
                String logDirectory = cleanable.log().parentDir();
                String msg = String.format("Failed to clean up log for %s in dir %s due to IOException", cleanable.topicPartition(), logDirectory);
                logDirFailureChannel.maybeAddOfflineLogDir(logDirectory, msg, e);
            } finally {
                cleanerManager.doneCleaning(cleanable.topicPartition(), cleanable.log().parentDirFile(), endOffset);
            }
        }

        /**
         * Log out statistics on a single run of the cleaner.
         *
         * @param id The cleaner thread id
         * @param name The cleaned log name
         * @param from The cleaned offset that is the first dirty offset to begin
         * @param to The cleaned offset that is the first not cleaned offset to end
         * @param stats The statistics for this round of cleaning
         */
        private void recordStats(int id, String name, long from, long to, CleanerStats stats) {
            this.lastStats = stats;
            String message = String.format("%n\tLog cleaner thread %d cleaned log %s (dirty section = [%d, %d])%n", id, name, from, to) +
                        String.format("\t%,.1f MB of log processed in %,.1f seconds (%,.1f MB/sec).%n", mb(stats.bytesRead),
                                stats.elapsedSecs(),
                                mb(stats.bytesRead / stats.elapsedSecs())) +
                        String.format("\tIndexed %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n", mb(stats.mapBytesRead),
                                stats.elapsedIndexSecs(),
                                mb(stats.mapBytesRead) / stats.elapsedIndexSecs(),
                                100 * stats.elapsedIndexSecs() / stats.elapsedSecs()) +
                        String.format("\tBuffer utilization: %.1f%%%n", 100 * stats.bufferUtilization) +
                        String.format("\tCleaned %,.1f MB in %.1f seconds (%,.1f Mb/sec, %.1f%% of total time)%n", mb(stats.bytesRead),
                                stats.elapsedSecs() - stats.elapsedIndexSecs(),
                                mb(stats.bytesRead) / (stats.elapsedSecs() - stats.elapsedIndexSecs()), 100 * (stats.elapsedSecs() - stats.elapsedIndexSecs()) / stats.elapsedSecs()) +
                        String.format("\tStart size: %,.1f MB (%,d messages)%n", mb(stats.bytesRead), stats.messagesRead) +
                        String.format("\tEnd size: %,.1f MB (%,d messages)%n", mb(stats.bytesWritten), stats.messagesWritten) +
                        String.format("\t%.1f%% size reduction (%.1f%% fewer messages)%n", 100.0 * (1.0 - Long.valueOf(stats.bytesWritten).doubleValue() / stats.bytesRead),
                                100.0 * (1.0 - Long.valueOf(stats.messagesWritten).doubleValue() / stats.messagesRead));
            logger.info(message);
            if (lastPreCleanStats.delayedPartitions() > 0) {
                logger.info("\tCleanable partitions: {}, Delayed partitions: {}, max delay: {}",
                        lastPreCleanStats.cleanablePartitions(), lastPreCleanStats.delayedPartitions(), lastPreCleanStats.maxCompactionDelayMs());
            }
            if (stats.invalidMessagesRead > 0) {
                logger.warn("\tFound {} invalid messages during compaction.", stats.invalidMessagesRead);
            }
        }

        private double mb(double bytes) {
            return bytes / (1024 * 1024);
        }
    }
}
