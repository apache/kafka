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

import org.apache.kafka.common.DirectoryId;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InconsistentTopicIdException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.LogDirNotFoundException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.Exit;
import org.apache.kafka.common.utils.internals.KafkaThread;
import org.apache.kafka.metadata.ConfigRepository;
import org.apache.kafka.metadata.properties.MetaProperties;
import org.apache.kafka.metadata.properties.MetaPropertiesEnsemble;
import org.apache.kafka.metadata.properties.PropertiesUtils;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.util.FileLock;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.storage.internals.checkpoint.CleanShutdownFileHandler;
import org.apache.kafka.storage.internals.checkpoint.OffsetCheckpointFile;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class LogManager {

    private static final Logger LOG = LoggerFactory.getLogger(LogManager.class);
    public static final String LOCK_FILE_NAME = ".lock";
    public static final String RECOVERY_POINT_CHECKPOINT_FILE = "recovery-point-offset-checkpoint";
    public static final String LOG_START_OFFSET_CHECKPOINT_FILE = "log-start-offset-checkpoint";

    // Changing the package or class name may cause incompatibility with existing code and metrics configuration
    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup("kafka.log", "LogManager");

    private final Object logCreationOrDeletionLock = new Object();
    private final ConcurrentHashMap<TopicPartition, UnifiedLog> currentLogs = new ConcurrentHashMap<>();
    // Future logs are put in the directory with "-future" suffix. Future log is created when user wants to move replica
    // from one log directory to another log directory on the same broker. The directory of the future log will be renamed
    // to replace the current log of the partition after the future log catches up with the current log
    private final ConcurrentHashMap<TopicPartition, UnifiedLog> futureLogs = new ConcurrentHashMap<>();
    private final List<File> logDirs;
    private final ConfigRepository configRepository;
    private final LogConfig initialDefaultConfig;
    private final CleanerConfig cleanerConfig;
    private final long flushCheckMs;
    private final long flushRecoveryOffsetCheckpointMs;
    private final long flushStartOffsetCheckpointMs;
    private final long retentionCheckMs;
    private final int maxTransactionTimeoutMs;
    private final ProducerStateManagerConfig producerStateManagerConfig;
    private final int producerIdExpirationCheckIntervalMs;
    private final Scheduler scheduler;
    private final BrokerTopicStats brokerTopicStats;
    private final LogDirFailureChannel logDirFailureChannel;
    private final Time time;
    private final boolean remoteStorageSystemEnable;
    private final long initialTaskDelayMs;
    private final LogCleanerBuilder cleanerFactory;

    // Each element in the queue contains the log object to be deleted and the time it is scheduled for deletion.
    private final LinkedBlockingQueue<Map.Entry<UnifiedLog, Long>> logsToBeDeleted = new LinkedBlockingQueue<>();

    // A map that stores the preferred log dir for each partition
    private final ConcurrentHashMap<TopicPartition, String> preferredLogDirs = new ConcurrentHashMap<>();

    // A map that stores hadCleanShutdown flag for each log dir.
    private final ConcurrentHashMap<String, Boolean> hadCleanShutdownFlags = new ConcurrentHashMap<>();

    // A map that tells whether all logs in a log dir had been loaded or not at startup time.
    private final ConcurrentHashMap<String, Boolean> loadLogsCompletedFlags = new ConcurrentHashMap<>();

    // This map contains all partitions whose logs are getting loaded and initialized. If log configuration
    // of these partitions get updated at the same time, the corresponding entry in this map is set to "true",
    // which triggers a config reload after initialization is finished (to get the latest config value).
    // See KAFKA-8813 for more detail on the race condition
    private final ConcurrentHashMap<TopicPartition, Boolean> partitionsInitializing = new ConcurrentHashMap<>();

    private final ConcurrentLinkedQueue<File> liveLogDirs;
    private final ConcurrentHashMap<String, Uuid> directoryIds;
    private final List<FileLock> dirLocks;

    private volatile Set<String> cordonedLogDirs = new HashSet<>();
    private volatile LogConfig currentDefaultConfig;
    private volatile int numRecoveryThreadsPerDataDir;
    private volatile Map<File, OffsetCheckpointFile> recoveryPointCheckpoints;
    private volatile Map<File, OffsetCheckpointFile> logStartOffsetCheckpoints;
    private volatile LogCleaner cleaner;

    /**
     * Interface to pass the values required to build a LogCleaner instance
     */
    public interface LogCleanerBuilder {

        /**
         * Build a LogCleaner
         *
         * @param config Initial configuration parameters for the cleaner. Actual config may be dynamically updated.
         * @param logDirs The directories where offset checkpoints reside
         * @param logs The map of logs
         * @param logDirFailureChannel The channel used to add offline log dirs that may be encountered when cleaning the log
         * @param time A way to control the passage of time
         */
        LogCleaner build(CleanerConfig config, List<File> logDirs, ConcurrentMap<TopicPartition, UnifiedLog> logs, LogDirFailureChannel logDirFailureChannel, Time time);
    }

    /**
     * Interface to decouple LogManager and TopicsImage. TopicsImage is in the metadata module which depends on storage.
     */
    public interface DirectoryForBrokerPartition {

        /**
         * Retrieve the log dir Uuid hosting the specified topic-partition on the specified broker
         * @param topicId The topic Uuid
         * @param partition The partitionId
         * @param brokerId The brokerId
         * @return The log dir Uuid
         */
        Uuid get(Uuid topicId, int partition, int brokerId);
    }

    /**
     * The entry point to the kafka log management subsystem. The log manager is responsible for log creation, retrieval, and cleaning.
     * All read and write operations are delegated to the individual log instances.
     *
     * The log manager maintains logs in one or more directories. New logs are created in the data directory
     * with the fewest logs. No attempt is made to move partitions after the fact or balance based on
     * size or I/O rate.
     *
     * A background thread handles log retention by periodically truncating excess log segments.
     *
     * This class is thread-safe.
     */
    @SuppressWarnings("this-escape")
    public LogManager(List<File> logDirs,
                      List<File> initialOfflineDirs,
                      ConfigRepository configRepository,
                      LogConfig initialDefaultConfig,
                      CleanerConfig cleanerConfig,
                      int numRecoveryThreadsPerDataDir,
                      long flushCheckMs,
                      long flushRecoveryOffsetCheckpointMs,
                      long flushStartOffsetCheckpointMs,
                      long retentionCheckMs,
                      int maxTransactionTimeoutMs,
                      ProducerStateManagerConfig producerStateManagerConfig,
                      int producerIdExpirationCheckIntervalMs,
                      Scheduler scheduler,
                      BrokerTopicStats brokerTopicStats,
                      LogDirFailureChannel logDirFailureChannel,
                      Time time,
                      boolean remoteStorageSystemEnable,
                      long initialTaskDelayMs,
                      LogCleanerBuilder cleanerBuilder) throws IOException {
        this.logDirs = logDirs;
        this.configRepository = configRepository;
        this.initialDefaultConfig = initialDefaultConfig;
        this.cleanerConfig = cleanerConfig;
        this.numRecoveryThreadsPerDataDir = numRecoveryThreadsPerDataDir;
        this.flushCheckMs = flushCheckMs;
        this.flushRecoveryOffsetCheckpointMs = flushRecoveryOffsetCheckpointMs;
        this.flushStartOffsetCheckpointMs = flushStartOffsetCheckpointMs;
        this.retentionCheckMs = retentionCheckMs;
        this.maxTransactionTimeoutMs = maxTransactionTimeoutMs;
        this.producerStateManagerConfig = producerStateManagerConfig;
        this.producerIdExpirationCheckIntervalMs = producerIdExpirationCheckIntervalMs;
        this.scheduler = scheduler;
        this.brokerTopicStats = brokerTopicStats;
        this.logDirFailureChannel = logDirFailureChannel;
        this.time = time;
        this.remoteStorageSystemEnable = remoteStorageSystemEnable;
        this.initialTaskDelayMs = initialTaskDelayMs;
        this.cleanerFactory = cleanerBuilder;
        liveLogDirs = createAndValidateLogDirs(logDirs, initialOfflineDirs);
        currentDefaultConfig = initialDefaultConfig;
        dirLocks = lockLogDirs(liveLogDirs);
        directoryIds = loadDirectoryIds(liveLogDirs);

        recoveryPointCheckpoints = new HashMap<>();
        logStartOffsetCheckpoints = new HashMap<>();
        for (File dir : liveLogDirs) {
            recoveryPointCheckpoints.put(dir, new OffsetCheckpointFile(new File(dir, RECOVERY_POINT_CHECKPOINT_FILE), logDirFailureChannel));
            logStartOffsetCheckpoints.put(dir, new OffsetCheckpointFile(new File(dir, LOG_START_OFFSET_CHECKPOINT_FILE), logDirFailureChannel));
        }

        metricsGroup.newGauge("OfflineLogDirectoryCount", () -> offlineLogDirs().size());
        metricsGroup.newGauge("CordonedLogDirectoryCount", () -> cordonedLogDirs().size());
        for (File dir : logDirs) {
            metricsGroup.newGauge("LogDirectoryOffline",
                    () -> liveLogDirs.contains(dir) ? 0 : 1,
                    Map.of("logDirectory", dir.getAbsolutePath()));
            metricsGroup.newGauge("LogDirectoryCordoned",
                    () -> cordonedLogDirs.contains(dir.getAbsolutePath()) ? 1 : 0,
                    Map.of("logDirectory", dir.getAbsolutePath()));
        }
    }

    public boolean hasOfflineLogDirs() {
        return !offlineLogDirs().isEmpty();
    }

    public boolean onlineLogDirId(Uuid uuid) {
        return directoryIds.containsValue(uuid);
    }

    public ProducerStateManagerConfig producerStateManagerConfig() {
        return producerStateManagerConfig;
    }

    public void updateCordonedLogDirs(Set<String> newCordonedLogDirs) {
        cordonedLogDirs = newCordonedLogDirs;
    }

    public Set<String> cordonedLogDirs() {
        return cordonedLogDirs;
    }

    public void reconfigureDefaultLogConfig(LogConfig logConfig) {
        this.currentDefaultConfig = logConfig;
    }

    public Collection<File> liveLogDirs() {
        return liveLogDirs.size() == logDirs.size() ? logDirs : liveLogDirs;
    }

    public LogCleaner cleaner() {
        return cleaner;
    }

    public Map<String, Uuid> directoryIds() {
        return Map.copyOf(directoryIds);
    }

    private Set<File> offlineLogDirs() {
        Set<File> result = new HashSet<>(logDirs);
        result.removeAll(liveLogDirs);
        return result;
    }

    // Only for testing
    public LogConfig initialDefaultConfig() {
        return initialDefaultConfig;
    }

    // Only for testing
    public int maxTransactionTimeoutMs() {
        return maxTransactionTimeoutMs;
    }

    // Only for testing
    public long initialTaskDelayMs() {
        return initialTaskDelayMs;
    }

    // Only for testing
    public int producerIdExpirationCheckIntervalMs() {
        return producerIdExpirationCheckIntervalMs;
    }

    // Only for testing
    public LogConfig currentDefaultConfig() {
        return currentDefaultConfig;
    }

    // Only for testing
    public List<FileLock> dirLocks() {
        return dirLocks;
    }

    // Only for testing
    public ConcurrentHashMap<TopicPartition, Boolean> partitionsInitializing() {
        return partitionsInitializing;
    }

    // Only for testing
    public boolean hasLogsToBeDeleted() {
        return !logsToBeDeleted.isEmpty();
    }

    /**
     * Create and check validity of the given directories that are not in the given offline directories, specifically:
     * <ol>
     * <li> Ensure that there are no duplicates in the directory list
     * <li> Create each directory if it doesn't exist
     * <li> Check that each path is a readable directory
     * </ol>
     */
    private ConcurrentLinkedQueue<File> createAndValidateLogDirs(List<File> dirs, List<File> initialOfflineDirs) {
        ConcurrentLinkedQueue<File> liveLogDirs = new ConcurrentLinkedQueue<>();
        Set<String> canonicalPaths = new HashSet<>();

        for (File dir : dirs) {
            try {
                if (initialOfflineDirs.contains(dir)) {
                    throw new IOException("Failed to load " + dir.getAbsolutePath() + " during broker startup");
                }
                if (!dir.exists()) {
                    LOG.info("Log directory {} not found, creating it.", dir.getAbsolutePath());
                    boolean created = dir.mkdirs();
                    if (!created) {
                        throw new IOException("Failed to create data directory " + dir.getAbsolutePath());
                    }
                    Utils.flushDir(dir.toPath().toAbsolutePath().normalize().getParent());
                }
                if (!dir.isDirectory() || !dir.canRead()) {
                    throw new IOException(dir.getAbsolutePath() + " is not a readable log directory.");
                }

                // getCanonicalPath() throws IOException if a file system query fails or if the path is invalid (e.g. contains
                // the Nul character). Since there's no easy way to distinguish between the two cases, we treat them the same
                // and mark the log directory as offline.
                if (!canonicalPaths.add(dir.getCanonicalPath())) {
                    throw new KafkaException("Duplicate log directory found: " + dirsToString(dirs));
                }

                liveLogDirs.add(dir);
            } catch (IOException ioe) {
                logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath(), "Failed to create or validate data directory " + dir.getAbsolutePath(), ioe);
            }
        }
        if (liveLogDirs.isEmpty()) {
            LOG.error("Shutdown broker because none of the specified log dirs from {} can be created or validated", dirsToString(dirs));
            Exit.halt(1);
        }

        return liveLogDirs;
    }

    public void resizeRecoveryThreadPool(int newSize) {
        LOG.info("Resizing recovery thread pool size for each data dir from {} to {}", numRecoveryThreadsPerDataDir, newSize);
        numRecoveryThreadsPerDataDir = newSize;
    }

    private Set<TopicPartition> removeOfflineLogs(String dir, ConcurrentMap<TopicPartition, UnifiedLog> logs) {
        Set<TopicPartition> offlineTopicPartitions = logs.entrySet().stream()
                .filter(entry -> entry.getValue().parentDir().equals(dir))
                .map(Map.Entry::getKey)
                .collect(Collectors.toUnmodifiableSet());
        offlineTopicPartitions.forEach(topicPartition -> {
            Optional<UnifiedLog> removedLog = removeLogAndMetrics(logs, topicPartition);
            removedLog.ifPresent(UnifiedLog::closeHandlers);
        });

        return offlineTopicPartitions;
    }

    private String dirsToString(Collection<File> dirs) {
        return dirs.stream().map(File::toString).collect(Collectors.joining(", "));
    }

    /**
     * The log directory failure handler. It will stop log cleaning in that directory.
     *
     * @param dir the absolute path of the log directory
     */
    public void handleLogDirFailure(String dir) {
        LOG.warn("Stopping serving logs in dir {}", dir);
        synchronized (logCreationOrDeletionLock)  {
            liveLogDirs.remove(new File(dir));
            directoryIds.remove(dir);
            if (liveLogDirs.isEmpty()) {
                LOG.error("Shutdown broker because all log dirs in {} have failed", dirsToString(logDirs));
                Exit.halt(1);
            }

            recoveryPointCheckpoints = recoveryPointCheckpoints.entrySet().stream()
                    .filter(entry -> !entry.getKey().getAbsolutePath().equals(dir))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            logStartOffsetCheckpoints = logStartOffsetCheckpoints.entrySet().stream()
                    .filter(entry -> !entry.getKey().getAbsolutePath().equals(dir))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            if (cleaner != null) {
                cleaner.handleLogDirFailure(dir);
            }

            Set<TopicPartition> offlineCurrentTopicPartitions = removeOfflineLogs(dir, currentLogs);
            Set<TopicPartition> offlineFutureTopicPartitions = removeOfflineLogs(dir, futureLogs);

            LOG.warn("Logs for partitions {} are offline and logs for future partitions {} are offline due to failure on log directory {}",
                    offlineCurrentTopicPartitions.stream().map(Object::toString).collect(Collectors.joining(",")),
                    offlineFutureTopicPartitions.stream().map(Object::toString).collect(Collectors.joining(",")),
                    dir);
            for (FileLock lock : dirLocks) {
                if (lock.file().getParent().equals(dir)) {
                    Utils.swallow(LOG, lock::destroy);
                }
            }
        }
    }

    /**
     * Lock all the given directories
     */
    private List<FileLock> lockLogDirs(Collection<File> dirs) {
        return dirs.stream().flatMap(dir -> {
            try {
                FileLock lock = new FileLock(new File(dir, LOCK_FILE_NAME));
                if (!lock.tryLock()) {
                    throw new KafkaException("Failed to acquire lock on file .lock in " + lock.file().getParent() +
                            ". A Kafka instance in another process or thread is using this directory.");
                }
                return Stream.of(lock);
            } catch (IOException ioe) {
                logDirFailureChannel.maybeAddOfflineLogDir(dir.getAbsolutePath(), "Disk error while locking directory " + dir, ioe);
                return Stream.empty();
            }
        }).collect(Collectors.toList());
    }

    /**
     * Retrieves the Uuid for the directory, given its absolute path.
     */
    public Optional<Uuid> directoryId(String dir) {
        return Optional.ofNullable(directoryIds.get(dir));
    }

    /**
     * Retrieves the absolute path for the directory, given its Uuid
     */
    public Optional<String> directoryPath(Uuid uuid) {
        for (Map.Entry<String, Uuid> entry : directoryIds.entrySet()) {
            if (entry.getValue().equals(uuid)) {
                return Optional.of(entry.getKey());
            }
        }
        return Optional.empty();
    }

    /**
     * Determine directory ID for each directory with a meta.properties.
     * If meta.properties does not include a directory ID, one is generated and persisted back to meta.properties.
     * Directories without a meta.properties don't get a directory ID assigned.
     */
    private ConcurrentHashMap<String, Uuid> loadDirectoryIds(Collection<File> logDirs) {
        ConcurrentHashMap<String, Uuid> result = new ConcurrentHashMap<>();
        logDirs.forEach(logDir -> {
            try {
                Properties props = PropertiesUtils.readPropertiesFile(
                        new File(logDir, MetaPropertiesEnsemble.META_PROPERTIES_NAME).getAbsolutePath());
                MetaProperties metaProps = new MetaProperties.Builder(props).build();
                metaProps.directoryId().ifPresent(directoryId -> result.put(logDir.getAbsolutePath(), directoryId));
            } catch (NoSuchFileException nsfe) {
                LOG.info("No meta.properties file found in {}.", logDir);
            } catch (IOException ioe) {
                logDirFailureChannel.maybeAddOfflineLogDir(logDir.getAbsolutePath(), "Disk error while loading ID " + logDir, ioe);
            }
        });
        return result;
    }

    private void addLogToBeDeleted(UnifiedLog log) {
        this.logsToBeDeleted.add(Map.entry(log, time.milliseconds()));
    }

    // Visible for testing
    public UnifiedLog loadLog(File logDir,
                               boolean hadCleanShutdown,
                               Map<TopicPartition, Long> recoveryPoints,
                               Map<TopicPartition, Long> logStartOffsets,
                               LogConfig defaultConfig,
                               Map<String, LogConfig> topicConfigOverrides,
                               ConcurrentMap<String, Integer> numRemainingSegments,
                               Function<UnifiedLog, Boolean> isStray) throws IOException {
        TopicPartition topicPartition = UnifiedLog.parseTopicPartitionName(logDir);
        LogConfig config = topicConfigOverrides.getOrDefault(topicPartition.topic(), defaultConfig);
        long logRecoveryPoint = recoveryPoints.getOrDefault(topicPartition, 0L);
        long logStartOffset = logStartOffsets.getOrDefault(topicPartition, 0L);

        UnifiedLog log = UnifiedLog.create(
            logDir,
            config,
            logStartOffset,
            logRecoveryPoint,
            scheduler,
            brokerTopicStats,
            time,
            maxTransactionTimeoutMs,
            producerStateManagerConfig,
            producerIdExpirationCheckIntervalMs,
            logDirFailureChannel,
            hadCleanShutdown,
            Optional.empty(),
            numRemainingSegments,
            remoteStorageSystemEnable,
            LogOffsetsListener.NO_OP_OFFSETS_LISTENER);

        if (logDir.getName().endsWith(UnifiedLog.DELETE_DIR_SUFFIX)) {
            addLogToBeDeleted(log);
        } else if (logDir.getName().endsWith(UnifiedLog.STRAY_DIR_SUFFIX)) {
            LOG.warn("Loaded stray log: {}", logDir);
        } else if (isStray.apply(log)) {
            // We are unable to prevent a topic from being recreated before every replica has been deleted.
            // Broker with an offline directory may be unable to detect it still holds a to-be-deleted replica,
            // and can create a conflicting topic partition for a new incarnation of the topic in one of the remaining online directories.
            // So upon a restart in which the offline directory is back online we need to clean up the old replica directory.
            log.renameDir(UnifiedLog.logStrayDirName(log.topicPartition()), false);
            LOG.warn("Log in {} marked stray and renamed to {}", logDir.getAbsolutePath(), log.dir().getAbsolutePath());
        } else {
            UnifiedLog previous = log.isFuture()
                    ? this.futureLogs.put(topicPartition, log)
                    : this.currentLogs.put(topicPartition, log);

            if (previous != null) {
                if (log.isFuture()) {
                    throw new IllegalStateException("Duplicate log directories found: " + log.dir().getAbsolutePath() + ", " + previous.dir().getAbsolutePath());
                } else {
                    throw new IllegalStateException("Duplicate log directories for " + topicPartition + " are found in both " + log.dir().getAbsolutePath() +
                            " and " + previous.dir().getAbsolutePath() + ". It is likely because log directory failure happened while broker was " +
                            "replacing current replica with future replica. Recover broker from this failure by manually deleting one of the two directories " +
                            "for this partition. It is recommended to delete the partition in the log directory that is known to have failed recently.");
                }
            }
        }

        return log;
    }

    // factory class for naming the log recovery threads used in metrics
    private static class LogRecoveryThreadFactory implements ThreadFactory {

        String dirPath;
        AtomicInteger threadNum = new AtomicInteger(0);

        LogRecoveryThreadFactory(String dirPath) {
            this.dirPath = dirPath;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            return KafkaThread.nonDaemon(logRecoveryThreadName(dirPath, threadNum.getAndIncrement()), runnable);
        }
    }

    // create a unique log recovery thread name for each log dir as the format: prefix-dirPath-threadNum, ex: "log-recovery-/tmp/kafkaLogs-0"
    private static String logRecoveryThreadName(String dirPath, int threadNum) {
        return "log-recovery" + "-" + dirPath + "-" + threadNum;
    }

    // Visible for testing
    public int decNumRemainingLogs(ConcurrentMap<String, Integer> numRemainingLogs, String path) {
        assert path != null : "path cannot be null to update remaining logs metric.";
        return numRemainingLogs.compute(path, (k, oldVal) -> oldVal - 1);
    }

    private void handleIOException(Set<Map.Entry<String, IOException>> offlineDirs, String logDirAbsolutePath, IOException e) {
        offlineDirs.add(Map.entry(logDirAbsolutePath, e));
        LOG.error("Error while loading log dir {}", logDirAbsolutePath, e);
    }

    /**
     * Recover and load all logs in the given data directories
     */
    public void loadLogs(LogConfig defaultConfig, Map<String, LogConfig> topicConfigOverrides, Function<UnifiedLog, Boolean> isStray) throws Exception {
        LOG.info("Loading logs from log dirs {}", liveLogDirs);
        long startMs = time.hiResClockMs();
        List<ExecutorService> threadPools = new ArrayList<>();
        Set<Map.Entry<String, IOException>> offlineDirs = new HashSet<>();
        List<List<Future<?>>> jobs = new ArrayList<>();
        int numTotalLogs = 0;
        // log dir path -> number of Remaining logs map for remainingLogsToRecover metric
        ConcurrentMap<String, Integer> numRemainingLogs = new ConcurrentHashMap<>();
        // log recovery thread name -> number of remaining segments map for remainingSegmentsToRecover metric
        ConcurrentMap<String, Integer> numRemainingSegments = new ConcurrentHashMap<>();

        List<String> uncleanLogDirs = new ArrayList<>();
        for (File dir : liveLogDirs) {
            String logDirAbsolutePath = dir.getAbsolutePath();
            final AtomicBoolean hadCleanShutdown = new AtomicBoolean(false);
            try {
                ExecutorService pool = Executors.newFixedThreadPool(numRecoveryThreadsPerDataDir,
                        new LogRecoveryThreadFactory(logDirAbsolutePath));
                threadPools.add(pool);

                CleanShutdownFileHandler cleanShutdownFileHandler = new CleanShutdownFileHandler(dir.getPath());
                if (cleanShutdownFileHandler.exists()) {
                    // Cache the clean shutdown status and use that for rest of log loading workflow. Delete the CleanShutdownFile
                    // so that if broker crashes while loading the log, it is considered hard shutdown during the next boot up. KAFKA-10471
                    cleanShutdownFileHandler.delete();
                    hadCleanShutdown.set(true);
                }
                hadCleanShutdownFlags.put(logDirAbsolutePath, hadCleanShutdown.get());

                final Map<TopicPartition, Long> recoveryPoints = new HashMap<>();
                try {
                    recoveryPoints.putAll(recoveryPointCheckpoints.get(dir).read());
                } catch (Exception e) {
                    LOG.warn("Error occurred while reading recovery-point-offset-checkpoint file of directory " +
                                    "{}, resetting the recovery checkpoint to 0", logDirAbsolutePath, e);
                }

                final Map<TopicPartition, Long> logStartOffsets = new HashMap<>();
                try {
                    logStartOffsets.putAll(logStartOffsetCheckpoints.get(dir).read());
                } catch (Exception e) {
                    LOG.warn("Error occurred while reading log-start-offset-checkpoint file of directory " +
                                    "{}, resetting to the base offset of the first segment", logDirAbsolutePath, e);
                }
                List<File> logsToLoad = Arrays.stream(Optional.ofNullable(dir.listFiles()).orElse(new File[]{}))
                        .filter(logDir -> {
                            if (!logDir.isDirectory()) return false;
                            // Ignore remote-log-index-cache directory as that is index cache maintained by tiered storage subsystem
                            // but not any topic-partition dir.
                            if (logDir.getName().equals(RemoteIndexCache.DIR_NAME)) return false;
                            return !UnifiedLog.parseTopicPartitionName(logDir).topic().equals(Topic.CLUSTER_METADATA_TOPIC_NAME);
                        })
                        .toList();
                numTotalLogs += logsToLoad.size();
                numRemainingLogs.put(logDirAbsolutePath, logsToLoad.size());
                loadLogsCompletedFlags.put(logDirAbsolutePath, logsToLoad.isEmpty());

                if (logsToLoad.isEmpty()) {
                    LOG.info("No logs found to be loaded in {}", logDirAbsolutePath);
                } else if (hadCleanShutdown.get()) {
                    LOG.info("Skipping recovery of {} logs from {} since clean shutdown file was found", logsToLoad.size(), logDirAbsolutePath);
                } else {
                    LOG.info("Recovering {} logs from {} since no clean shutdown file was found", logsToLoad.size(), logDirAbsolutePath);
                    uncleanLogDirs.add(logDirAbsolutePath);
                }

                List<Runnable> jobsForDir = logsToLoad.stream().map(logDir -> (Runnable) () -> {
                    LOG.debug("Loading log {}", logDir);
                    Optional<UnifiedLog> log = Optional.empty();
                    long logLoadStartMs = time.hiResClockMs();
                    try {
                        log = Optional.of(loadLog(logDir, hadCleanShutdown.get(), recoveryPoints, logStartOffsets,
                                defaultConfig, topicConfigOverrides, numRemainingSegments, isStray));
                    } catch (IOException ioe) {
                        handleIOException(offlineDirs, logDirAbsolutePath, ioe);
                    } catch (KafkaStorageException kse) {
                        // KafkaStorageException might be thrown, ex: during writing LeaderEpochFileCache
                        // And while converting IOException to KafkaStorageException, we've already handled the exception.
                        // So only throw if it's not the case here.
                        if (!(kse.getCause() instanceof IOException)) {
                            throw kse;
                        }
                    } finally {
                        long logLoadDurationMs = time.hiResClockMs() - logLoadStartMs;
                        int remainingLogs = decNumRemainingLogs(numRemainingLogs, logDirAbsolutePath);
                        int currentNumLoaded = logsToLoad.size() - remainingLogs;
                        if (log.isPresent()) {
                            UnifiedLog loadedLog = log.get();
                            LOG.info("Completed load of {} with {} segments, local-log-start-offset {} and log-end-offset {} in {}ms ({}/{} completed in {})",
                                    loadedLog, loadedLog.numberOfSegments(), loadedLog.localLogStartOffset(), loadedLog.logEndOffset(), logLoadDurationMs, currentNumLoaded, logsToLoad.size(), logDirAbsolutePath);
                        } else {
                            LOG.info("Error while loading logs in {} in {}ms ({}/{} completed in {})", logDir, logLoadDurationMs, currentNumLoaded, logsToLoad.size(), logDirAbsolutePath);
                        }

                        if (remainingLogs == 0) {
                            // loadLog is completed for all logs under the logDir, mark it.
                            loadLogsCompletedFlags.put(logDirAbsolutePath, true);
                        }
                    }
                }).toList();

                jobs.add(jobsForDir.stream().map(pool::submit).collect(Collectors.toList()));
            } catch (IOException e) {
                handleIOException(offlineDirs, logDirAbsolutePath, e);
            }
        }

        try {
            addLogRecoveryMetrics(numRemainingLogs, numRemainingSegments);
            for (List<Future<?>> dirJobs : jobs) {
                for (Future<?> job : dirJobs) {
                    job.get();
                }
            }

            offlineDirs.forEach(entry ->
                logDirFailureChannel.maybeAddOfflineLogDir(entry.getKey(), "Error while loading log dir " + entry.getKey(), entry.getValue())
            );
        } catch (ExecutionException e) {
            LOG.error("There was an error in one of the threads during logs loading", e.getCause());
            throw (Exception) e.getCause();
        } finally {
            removeLogRecoveryMetrics();
            threadPools.forEach(ExecutorService::shutdown);
        }

        long elapsedMs = time.hiResClockMs() - startMs;
        String printedUncleanLogDirs = uncleanLogDirs.isEmpty() ? "" : " (unclean log dirs = " + uncleanLogDirs + ")";
        LOG.info("Loaded {} logs in {}ms{}", numTotalLogs, elapsedMs, printedUncleanLogDirs);
    }

    // Visible for testing
    public void addLogRecoveryMetrics(ConcurrentMap<String, Integer> numRemainingLogs, ConcurrentMap<String, Integer> numRemainingSegments) {
        LOG.debug("Adding log recovery metrics");
        for (File dir : logDirs) {
            metricsGroup.newGauge("remainingLogsToRecover", () -> numRemainingLogs.get(dir.getAbsolutePath()),
                    Map.of("dir", dir.getAbsolutePath()));
            for (int i = 0; i < numRecoveryThreadsPerDataDir; i++) {
                String threadName = logRecoveryThreadName(dir.getAbsolutePath(), i);
                LinkedHashMap<String, String> tags = new LinkedHashMap<>();
                tags.put("dir", dir.getAbsolutePath());
                tags.put("threadNum", String.valueOf(i));
                metricsGroup.newGauge("remainingSegmentsToRecover", () -> numRemainingSegments.get(threadName), tags);
            }
        }
    }

    // Visible for testing
    public void removeLogRecoveryMetrics() {
        LOG.debug("Removing log recovery metrics");
        for (File dir : logDirs) {
            metricsGroup.removeMetric("remainingLogsToRecover", Map.of("dir", dir.getAbsolutePath()));
            for (int i = 0; i < numRecoveryThreadsPerDataDir; i++) {
                LinkedHashMap<String, String> tags = new LinkedHashMap<>();
                tags.put("dir", dir.getAbsolutePath());
                tags.put("threadNum", String.valueOf(i));
                metricsGroup.removeMetric("remainingSegmentsToRecover", tags);
            }
        }
    }

    // Visible for testing
    public void startup(Set<String> topicNames) throws Exception {
        startup(topicNames, l -> false);
    }

    /**
     *  Start the background threads to flush logs and do log cleanup
     */
    public void startup(Set<String> topicNames, Function<UnifiedLog, Boolean> isStray) throws Exception {
        // ensure consistency between default config and overrides
        LogConfig defaultConfig = currentDefaultConfig;
        startupWithConfigOverrides(defaultConfig, fetchTopicConfigOverrides(defaultConfig, topicNames), isStray);
    }

    // Visible for testing
    public Map<String, LogConfig> fetchTopicConfigOverrides(LogConfig defaultConfig, Set<String> topicNames) {
        Map<String, LogConfig> topicConfigOverrides = new  HashMap<>();
        Map<String, Object> defaultProps = defaultConfig.originals();
        topicNames.forEach(topicName -> {
            Properties overrides = configRepository.topicConfig(topicName);
            // save memory by only including configs for topics with overrides
            if (!overrides.isEmpty()) {
                LogConfig logConfig = LogConfig.fromProps(defaultProps, overrides);
                topicConfigOverrides.put(topicName, logConfig);
            }
        });
        return topicConfigOverrides;
    }

    private LogConfig fetchLogConfig(String topicName) {
        // ensure consistency between default config and overrides
        LogConfig defaultConfig = currentDefaultConfig;
        Iterator<LogConfig> config = fetchTopicConfigOverrides(defaultConfig, Set.of(topicName)).values().iterator();
        return config.hasNext() ? config.next() : defaultConfig;
    }

    private void startupWithConfigOverrides(LogConfig defaultConfig,
                                            Map<String, LogConfig> topicConfigOverrides,
                                            Function<UnifiedLog, Boolean> isStray) throws Exception {
        loadLogs(defaultConfig, topicConfigOverrides, isStray); // this could take a while if shutdown was not clean

        // Schedule the cleanup task to delete old logs
        if (scheduler != null) {
            LOG.info("Starting log cleanup with a period of {} ms.", retentionCheckMs);
            scheduler.schedule("kafka-log-retention",
                    this::cleanupLogs,
                    initialTaskDelayMs,
                    retentionCheckMs);
            LOG.info("Starting log flusher with a default period of {} ms.", flushCheckMs);
            scheduler.schedule("kafka-log-flusher",
                    this::flushDirtyLogs,
                    initialTaskDelayMs,
                    flushCheckMs);
            scheduler.schedule("kafka-recovery-point-checkpoint",
                    this::checkpointLogRecoveryOffsets,
                    initialTaskDelayMs,
                    flushRecoveryOffsetCheckpointMs);
            scheduler.schedule("kafka-log-start-offset-checkpoint",
                    this::checkpointLogStartOffsets,
                    initialTaskDelayMs,
                    flushStartOffsetCheckpointMs);
            scheduler.scheduleOnce("kafka-delete-logs", // will be rescheduled after each delete logs with a dynamic period
                    this::deleteLogs,
                    initialTaskDelayMs);
        }
        if (cleanerConfig.enableCleaner) {
            cleaner = cleanerFactory.build(cleanerConfig, liveLogDirs.stream().toList(), currentLogs, logDirFailureChannel, time);
            cleaner.startup();
        } else {
            LOG.warn("The config `log.cleaner.enable` is deprecated and will be removed in Kafka 5.0. Starting from Kafka 5.0, the log cleaner will always be enabled, and this config will be ignored.");

        }
    }

    // Only for testing
    public void shutdown() throws IOException {
        shutdown(-1L);
    }

    /**
     * Close all the logs
     */
    public void shutdown(long brokerEpoch) throws IOException {
        LOG.info("Shutting down.");

        metricsGroup.removeMetric("OfflineLogDirectoryCount");
        for (File dir : logDirs) {
            metricsGroup.removeMetric("LogDirectoryOffline", Map.of("logDirectory", dir.getAbsolutePath()));
        }

        List<ExecutorService> threadPools = new ArrayList<>();
        Map<File, List<Future<?>>> jobs = new HashMap<>();

        // stop the cleaner first
        if (cleaner != null) {
            Utils.swallow(LOG, () -> cleaner.shutdown());
        }

        Map<String, Map<TopicPartition, UnifiedLog>> localLogsByDir = logsByDir();

        // close logs in each dir
        for (File dir : liveLogDirs) {
            LOG.debug("Flushing and closing logs at {}", dir);

            ExecutorService pool = Executors.newFixedThreadPool(numRecoveryThreadsPerDataDir,
                    runnable -> KafkaThread.nonDaemon("log-closing-" + dir.getAbsolutePath(), runnable));
            threadPools.add(pool);

            Collection<UnifiedLog> logs = logsInDir(localLogsByDir, dir).values();

            List<Runnable> jobsForDir = logs.stream().map(log -> {
                // flush the log to ensure latest possible recovery point
                return (Runnable) () -> {
                    // flush the log to ensure latest possible recovery point
                    log.flush(true);
                    log.close();
                };
            }).toList();

            jobs.put(dir, jobsForDir.stream().map(pool::submit).collect(Collectors.toList()));
        }

        try {
            jobs.forEach((dir, dirJobs) -> {
                if (waitForAllToComplete(dirJobs,
                        e -> LOG.warn("There was an error in one of the threads during LogManager shutdown", e.getCause()))) {
                    Map<TopicPartition, UnifiedLog> logs = logsInDir(localLogsByDir, dir);

                    // update the last flush point
                    LOG.debug("Updating recovery points at {}", dir);
                    checkpointRecoveryOffsetsInDir(dir, logs);

                    LOG.debug("Updating log start offsets at {}", dir);
                    checkpointLogStartOffsetsInDir(dir, logs);

                    // mark that the shutdown was clean by creating marker file for log dirs that:
                    //  1. had clean shutdown marker file; or
                    //  2. had no clean shutdown marker file, but all logs under it have been recovered at startup time
                    String logDirAbsolutePath = dir.getAbsolutePath();
                    if (hadCleanShutdownFlags.getOrDefault(logDirAbsolutePath, false) ||
                            loadLogsCompletedFlags.getOrDefault(logDirAbsolutePath, false)) {
                        CleanShutdownFileHandler cleanShutdownFileHandler = new CleanShutdownFileHandler(dir.getPath());
                        LOG.debug("Writing clean shutdown marker at {} with broker epoch={}", dir, brokerEpoch);
                        Utils.swallow(LOG, () -> cleanShutdownFileHandler.write(brokerEpoch));
                    }
                }
            });
        } finally {
            threadPools.forEach(ExecutorService::shutdown);
            // regardless of whether the close succeeded, we need to unlock the data directories
            for (FileLock lock : dirLocks) {
                lock.destroy();
            }
        }

        LOG.info("Shutdown complete.");
    }

    /**
     * Truncate the partition logs to the specified offsets and checkpoint the recovery point to this offset
     *
     * @param partitionOffsets Partition logs that need to be truncated
     * @param isFuture True iff the truncation should be performed on the future log of the specified partitions
     */
    public void truncateTo(Map<TopicPartition, Long> partitionOffsets, boolean isFuture) {
        List<UnifiedLog> affectedLogs = new ArrayList<>();
        for (Map.Entry<TopicPartition, Long> entry : partitionOffsets.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            long truncateOffset = entry.getValue();
            UnifiedLog log = isFuture
                ? futureLogs.get(topicPartition)
                : currentLogs.get(topicPartition);
            // If the log does not exist, skip it
            if (log != null) {
                // May need to abort and pause the cleaning of the log, and resume after truncation is done.
                boolean needToStopCleaner = truncateOffset < log.activeSegment().baseOffset();
                if (needToStopCleaner && !isFuture) {
                    abortAndPauseCleaning(topicPartition);
                }
                try {
                    if (log.truncateTo(truncateOffset)) {
                        affectedLogs.add(log);
                    }
                    if (needToStopCleaner && !isFuture) {
                        maybeTruncateCleanerCheckpointToActiveSegmentBaseOffset(log, topicPartition);
                    }
                } finally {
                    if (needToStopCleaner && !isFuture) {
                        resumeCleaning(topicPartition);
                    }
                }
            }
        }

        affectedLogs.stream()
                .map(UnifiedLog::parentDirFile)
                .distinct()
                .forEach(this::checkpointRecoveryOffsetsInDir);
    }

    /**
     * Delete all data in a partition and start the log at the new offset
     *
     * @param topicPartition The partition whose log needs to be truncated
     * @param newOffset The new offset to start the log with
     * @param isFuture True iff the truncation should be performed on the future log of the specified partition
     * @param logStartOffsetOpt The log start offset to set for the log. If None, the new offset will be used.
     */
    public void truncateFullyAndStartAt(TopicPartition topicPartition,
                                 long newOffset,
                                 boolean isFuture,
                                 Optional<Long> logStartOffsetOpt) {
        UnifiedLog log = isFuture
            ? futureLogs.get(topicPartition)
            : currentLogs.get(topicPartition);
        // If the log does not exist, skip it
        if (log != null) {
            // Abort and pause the cleaning of the log, and resume after truncation is done.
            if (!isFuture) {
                abortAndPauseCleaning(topicPartition);
            }
            try {
                log.truncateFullyAndStartAt(newOffset, logStartOffsetOpt);
                if (!isFuture) {
                    maybeTruncateCleanerCheckpointToActiveSegmentBaseOffset(log, topicPartition);
                }
            } finally {
                if (!isFuture) {
                    resumeCleaning(topicPartition);
                }
            }
            checkpointRecoveryOffsetsInDir(log.parentDirFile());
        }
    }

    /**
     * Write out the current recovery point for all logs to a text file in the log directory
     * to avoid recovering the whole log on startup.
     */
    // Visible for testing
    public void checkpointLogRecoveryOffsets() {
        Map<String, Map<TopicPartition, UnifiedLog>> logsByDirCached = logsByDir();
        liveLogDirs.forEach(logDir -> {
            Map<TopicPartition, UnifiedLog> logsToCheckpoint = logsInDir(logsByDirCached, logDir);
            checkpointRecoveryOffsetsInDir(logDir, logsToCheckpoint);
        });
    }

    /**
     * Write out the current log start offset for all logs to a text file in the log directory
     * to avoid exposing data that have been deleted by DeleteRecordsRequest
     */
    public void checkpointLogStartOffsets() {
        Map<String, Map<TopicPartition, UnifiedLog>> logsByDirCached = logsByDir();
        liveLogDirs.forEach(logDir ->
            checkpointLogStartOffsetsInDir(logDir, logsInDir(logsByDirCached, logDir))
        );
    }

    /**
     * Checkpoint recovery offsets for all the logs in logDir.
     *
     * @param logDir the directory in which the logs to be checkpointed are
     */
    // Visible for testing
    public void checkpointRecoveryOffsetsInDir(File logDir) {
        checkpointRecoveryOffsetsInDir(logDir, logsInDir(logDir));
    }

    /**
     * Checkpoint recovery offsets for all the provided logs.
     *
     * @param logDir the directory in which the logs are
     * @param logsToCheckpoint the logs to be checkpointed
     */
    private void checkpointRecoveryOffsetsInDir(File logDir, Map<TopicPartition, UnifiedLog> logsToCheckpoint) {
        try {
            OffsetCheckpointFile checkpoint = recoveryPointCheckpoints.get(logDir);
            if (checkpoint != null) {
                Map<TopicPartition, Long> recoveryOffsets = logsToCheckpoint.entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> entry.getValue().recoveryPoint()
                        ));
                // checkpoint.write calls Utils.atomicMoveWithFallback, which flushes the parent
                // directory and guarantees crash consistency.
                checkpoint.write(recoveryOffsets);
            }
        } catch (KafkaStorageException kse) {
            LOG.error("Disk error while writing recovery offsets checkpoint in directory {}: {}", logDir, kse.getMessage());
        }
    }

    /**
     * Checkpoint log start offsets for all the provided logs in the provided directory.
     *
     * @param logDir the directory in which logs are checkpointed
     * @param logsToCheckpoint the logs to be checkpointed
     */
    private void checkpointLogStartOffsetsInDir(File logDir, Map<TopicPartition, UnifiedLog> logsToCheckpoint) {
        try {
            OffsetCheckpointFile checkpoint = logStartOffsetCheckpoints.get(logDir);
            if (checkpoint != null) {
                Map<TopicPartition, Long> logStartOffsets = logsToCheckpoint.entrySet().stream()
                        .filter(entry -> {
                            UnifiedLog log = entry.getValue();
                            List<LogSegment> segments = log.logSegments();
                            return log.remoteLogEnabled() || log.logStartOffset() > segments.get(0).baseOffset();
                        })
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> entry.getValue().logStartOffset()
                        ));
                checkpoint.write(logStartOffsets);
            }
        } catch (KafkaStorageException kse) {
            LOG.error("Disk error while writing log start offsets checkpoint in directory {}: {}", logDir, kse.getMessage());
        }
    }

    /**
     * Update the preferred log dir for the partition
     *
     * @param topicPartition The partition to update
     * @param logDir The logDir should be an absolute path
     */
    public void maybeUpdatePreferredLogDir(TopicPartition topicPartition, String logDir) {
        // Do not cache the preferred log directory if either the current log or the future log for this partition exists in the specified logDir
        boolean currentLogInDir = getLog(topicPartition)
                .map(log -> log.parentDir().equals(logDir))
                .orElse(false);
        boolean futureLogInDir = getLog(topicPartition, true)
                .map(log -> log.parentDir().equals(logDir))
                .orElse(false);
        if (!currentLogInDir && !futureLogInDir)
            preferredLogDirs.put(topicPartition, logDir);
    }

    /**
     * Abort and pause cleaning of the provided partition and log a message about it.
     */
    public void abortAndPauseCleaning(TopicPartition topicPartition) {
        if (cleaner != null) {
            cleaner.abortAndPauseCleaning(topicPartition);
            LOG.info("The cleaning for partition {} is aborted and paused", topicPartition);
        }
    }

    /**
     * Abort cleaning of the provided partition and log a message about it.
     */
    // Visible for testing
    public void abortCleaning(TopicPartition topicPartition) {
        if (cleaner != null) {
            cleaner.abortCleaning(topicPartition);
            LOG.info("The cleaning for partition {} is aborted", topicPartition);
        }
    }

    /**
     * Resume cleaning of the provided partition and log a message about it.
     */
    public void resumeCleaning(TopicPartition topicPartition) {
        if (cleaner != null) {
            cleaner.resumeCleaning(Set.of(topicPartition));
            LOG.info("Cleaning for partition {} is resumed", topicPartition);
        }
    }

    /**
     * Truncate the cleaner's checkpoint to the based offset of the active segment of
     * the provided log.
     */
    private void maybeTruncateCleanerCheckpointToActiveSegmentBaseOffset(UnifiedLog log, TopicPartition topicPartition) {
        if (cleaner != null) {
            cleaner.maybeTruncateCheckpoint(log.parentDirFile(), topicPartition, log.activeSegment().baseOffset());
        }
    }

    /**
     * Get the log if it exists, otherwise return None
     *
     * @param topicPartition the partition of the log
     */
    public Optional<UnifiedLog> getLog(TopicPartition topicPartition) {
        return getLog(topicPartition, false);
    }

    /**
     * Get the log if it exists, otherwise return None
     *
     * @param topicPartition the partition of the log
     * @param isFuture True if the future log of the specified partition should be returned
     */
    public Optional<UnifiedLog> getLog(TopicPartition topicPartition, boolean isFuture) {
        return isFuture
            ? Optional.ofNullable(futureLogs.get(topicPartition))
            : Optional.ofNullable(currentLogs.get(topicPartition));
    }

    /**
     * Method to indicate that logs are getting initialized for the partition passed in as argument.
     * This method should always be followed by {@link #finishedInitializingLog} to indicate that log
     * initialization is done.
     */
    public void initializingLog(TopicPartition topicPartition) {
        partitionsInitializing.put(topicPartition, false);
    }

    /**
     * Mark the partition configuration for all partitions that are getting initialized for topic
     * as dirty. That will result in reloading of configuration once initialization is done.
     */
    // Visible for testing
    public void topicConfigUpdated(String topic) {
        partitionsInitializing.keySet().stream()
                .filter(tp -> tp.topic().equals(topic))
                .forEach(topicPartition ->
                    partitionsInitializing.replace(topicPartition, false, true));
    }

    /**
     * Update the configuration of the provided topic.
     */
    public void updateTopicConfig(String topic,
                                  Properties newTopicConfig,
                                  boolean isRemoteLogStorageSystemEnabled,
                                  boolean wasRemoteLogEnabled) {
        topicConfigUpdated(topic);
        List<UnifiedLog> logs = logsByTopic(topic);
        LogConfig newLogConfig = LogConfig.fromProps(currentDefaultConfig.originals(), newTopicConfig);
        boolean isRemoteLogStorageEnabled = newLogConfig.remoteStorageEnable();
        // We would like to validate the configuration no matter whether the logs have materialised on disk or not.
        // Otherwise, we risk someone creating a tiered-topic, disabling Tiered Storage cluster-wide and the check
        // failing since the logs for the topic are non-existent.
        LogConfig.validateRemoteStorageOnlyIfSystemEnabled(newLogConfig.values(), isRemoteLogStorageSystemEnabled, true);
        LogConfig.validateTurningOffRemoteStorageWithDelete(newLogConfig.values(), wasRemoteLogEnabled, isRemoteLogStorageEnabled);
        LogConfig.validateRetentionConfigsWhenRemoteCopyDisabled(newLogConfig.values(), isRemoteLogStorageEnabled);
        if (!logs.isEmpty()) {
            logs.forEach(log -> {
                LogConfig oldLogConfig = log.updateConfig(newLogConfig);
                if (oldLogConfig.compact && !newLogConfig.compact) {
                    abortCleaning(log.topicPartition());
                }
            });
        }
    }

    /**
     * Mark all in progress partitions having dirty configuration if broker configuration is updated.
     */
    public void brokerConfigUpdated() {
        partitionsInitializing.keySet().forEach(topicPartition ->
                partitionsInitializing.replace(topicPartition, false, true)
        );
    }

    /**
     * Method to indicate that the log initialization for the partition passed in as argument is
     * finished. This method should follow a call to {@link #initializingLog}.
     *
     * It will retrieve the topic configs a second time if they were updated while the
     * relevant log was being loaded.
     */
    public void finishedInitializingLog(TopicPartition topicPartition, Optional<UnifiedLog> maybeLog) {
        Boolean removedValue = partitionsInitializing.remove(topicPartition);
        if (removedValue != null && removedValue)
            maybeLog.ifPresent(l -> l.updateConfig(fetchLogConfig(topicPartition.topic())));
    }

    // Only for testing
    public UnifiedLog getOrCreateLog(TopicPartition topicPartition, Optional<Uuid> topicId) throws IOException {
        return getOrCreateLog(topicPartition, false, false, topicId, Optional.empty());
    }

    // Only for testing
    public UnifiedLog getOrCreateLog(TopicPartition topicPartition, boolean isNew, boolean isFuture, Optional<Uuid> topicId) throws IOException {
        return getOrCreateLog(topicPartition, isNew, isFuture, topicId, Optional.empty());
    }

    /**
     * If the log already exists, just return a copy of the existing log
     * Otherwise if isNew=true or if there is no offline log directory, create a log for the given topic and the given partition
     * Otherwise throw KafkaStorageException
     *
     * @param topicPartition The partition whose log needs to be returned or created
     * @param isNew Whether the replica should have existed on the broker or not
     * @param isFuture True if the future log of the specified partition should be returned or created
     * @param topicId The topic ID of the partition's topic
     * @param targetLogDirectoryId The directory Id that should host the partition's topic.
     *                             The next selected directory will be picked up if it None or equal {@link DirectoryId#UNASSIGNED}.
     *                             The method assumes provided Id belong to online directory.
     * @throws KafkaStorageException if isNew=false, log is not found in the cache and there is offline log directory on the broker
     * @throws InconsistentTopicIdException if the topic ID in the log does not match the topic ID provided
     */
    public UnifiedLog getOrCreateLog(TopicPartition topicPartition,
                                     boolean isNew,
                                     boolean isFuture,
                                     Optional<Uuid> topicId,
                                     Optional<Uuid> targetLogDirectoryId) throws IOException {
        synchronized (logCreationOrDeletionLock) {
            Optional<UnifiedLog> logOpt = getLog(topicPartition, isFuture);
            UnifiedLog log = logOpt.isPresent()
                    ? logOpt.get()
                    : createLog(topicPartition, isNew, isFuture, topicId, targetLogDirectoryId);

            // Ensure topic IDs are consistent
            topicId.ifPresent(id ->
                log.topicId().ifPresent(logTopicId -> {
                    if (!id.equals(logTopicId))
                        throw new InconsistentTopicIdException("Tried to assign topic ID " + id + " to log for topic partition " + topicPartition + "," +
                                "but log already contained topic ID " + logTopicId);
                })
            );
            return log;
        }
    }

    private UnifiedLog createLog(TopicPartition topicPartition,
                                 boolean isNew,
                                 boolean isFuture,
                                 Optional<Uuid> topicId,
                                 Optional<Uuid> targetLogDirectoryId) throws IOException {
        // create the log if it has not already been created in another thread
        if (!isNew && !offlineLogDirs().isEmpty())
            throw new KafkaStorageException("Can not create log for " + topicPartition + " because log directories " +
                    dirsToString(offlineLogDirs()) + " are offline");

        // Determine preferred log directory
        String preferredLogDir;
        if (targetLogDirectoryId.isPresent() &&
                !Set.of(DirectoryId.UNASSIGNED, DirectoryId.LOST).contains(targetLogDirectoryId.get()) &&
                !preferredLogDirs.containsKey(topicPartition)) {
            // If partition is configured with both targetLogDirectoryId and preferredLogDirs, then
            // preferredLogDirs will be respected, otherwise targetLogDirectoryId will be respected
            preferredLogDir = directoryPath(targetLogDirectoryId.get()).orElse(null);
        } else {
            preferredLogDir = preferredLogDirs.get(topicPartition);
        }

        if (isFuture) {
            if (preferredLogDir == null) {
                throw new IllegalStateException("Can not create the future log for " + topicPartition + " without having a preferred log directory");
            } else if (getLog(topicPartition).get().parentDir().equals(preferredLogDir)) {
                throw new IllegalStateException("Can not create the future log for " + topicPartition + " in the current log directory of this partition");
            }
        }

        List<File> logDirs = preferredLogDir != null
                ? List.of(new File(preferredLogDir))
                : nextLogDirs();

        String logDirName = isFuture
                ? UnifiedLog.logFutureDirName(topicPartition)
                : UnifiedLog.logDirName(topicPartition);

        // Try each directory until one succeeds
        File logDir = null;
        KafkaStorageException lastException = null;
        for (File dir : logDirs) {
            try {
                logDir = createLogDirectory(dir, logDirName);
                break;
            } catch (KafkaStorageException e) {
                lastException = e;
            }
        }
        if (logDir == null) {
            throw new KafkaStorageException("No log directories available. Tried " + dirsToString(logDirs), lastException);
        }

        LogConfig config = fetchLogConfig(topicPartition.topic());
        UnifiedLog newLog = UnifiedLog.create(
                logDir,
                config,
                0L,
                0L,
                scheduler,
                brokerTopicStats,
                time,
                maxTransactionTimeoutMs,
                producerStateManagerConfig,
                producerIdExpirationCheckIntervalMs,
                logDirFailureChannel,
                true,
                topicId,
                new ConcurrentHashMap<>(),
                remoteStorageSystemEnable,
                LogOffsetsListener.NO_OP_OFFSETS_LISTENER);

        if (isFuture) {
            futureLogs.put(topicPartition, newLog);
        } else {
            currentLogs.put(topicPartition, newLog);
        }

        LOG.info("Created log for partition {} in {} with properties {}", topicPartition, logDir, config.overriddenConfigsAsLoggableString());
        // Remove the preferred log dir since it has already been satisfied
        preferredLogDirs.remove(topicPartition);

        return newLog;
    }

    /**
     * Create the log dir
     * @param logDir The log dir path
     * @param logDirName The log dir name
     */
    // Visible for testing
    public File createLogDirectory(File logDir, String logDirName) {
        String logDirPath = logDir.getAbsolutePath();
        if (isLogDirOnline(logDirPath)) {
            File dir = new File(logDirPath, logDirName);
            try {
                Files.createDirectories(dir.toPath());
                return dir;
            } catch (IOException ioe) {
                String msg = "Error while creating log for " + logDirName + " in dir " + logDirPath;
                logDirFailureChannel.maybeAddOfflineLogDir(logDirPath, msg, ioe);
                LOG.warn(msg, ioe);
                throw new KafkaStorageException(msg, ioe);
            }
        } else {
            throw new KafkaStorageException("Can not create log " + logDirName + " because log directory " + logDirPath + " is offline");
        }
    }

    private long nextDeleteDelayMs(long fileDeleteDelayMs) {
        if (!logsToBeDeleted.isEmpty()) {
            Map.Entry<UnifiedLog, Long> entry = logsToBeDeleted.peek();
            return entry.getValue() + fileDeleteDelayMs - time.milliseconds();
        } else {
            // avoid the case: fileDeleteDelayMs is 0 with empty logsToBeDeleted
            // in this case, logsToBeDeleted.take() will block forever
            return Math.max(fileDeleteDelayMs, 1);
        }
    }

    /**
     *  Delete logs marked for deletion. Delete all logs for which `currentDefaultConfig.fileDeleteDelayMs`
     *  has elapsed after the delete was scheduled. Logs for which this interval has not yet elapsed will be
     *  considered for deletion in the next iteration of `deleteLogs`. The next iteration will be executed
     *  after the remaining time for the first log that is not deleted. If there are no more `logsToBeDeleted`,
     *  `deleteLogs` will be executed after `max(currentDefaultConfig.fileDeleteDelayMs, 1)`.
     */
    private void deleteLogs() {
        long fileDeleteDelayMs = currentDefaultConfig.fileDeleteDelayMs;
        long nextDelayMs = nextDeleteDelayMs(fileDeleteDelayMs);
        try {
            while (nextDelayMs <= 0) {
                Map.Entry<UnifiedLog, Long> entry = logsToBeDeleted.take();
                UnifiedLog removedLog = entry.getKey();
                if (removedLog != null) {
                    try {
                        removedLog.delete();
                        LOG.info("Deleted log for partition {} in {}.", removedLog.topicPartition(), removedLog.dir().getAbsolutePath());
                    } catch (KafkaStorageException kse) {
                        LOG.error("Exception while deleting {} in dir {}.", removedLog, removedLog.parentDir(), kse);
                    }
                }
                nextDelayMs = nextDeleteDelayMs(fileDeleteDelayMs);
            }
        } catch (Throwable e) {
            LOG.error("Exception in kafka-delete-logs thread.", e);
        } finally {
            try {
                scheduler.scheduleOnce("kafka-delete-logs", this::deleteLogs, nextDelayMs);
            } catch (Throwable e) {
                // No errors should occur unless scheduler has been shutdown
                LOG.error("Failed to schedule next delete in kafka-delete-logs thread", e);
            }
        }
    }

    /**
     * Attempts to recover abandoned future logs if we can find a current matching log
     */
    public void recoverAbandonedFutureLogs(int brokerId, DirectoryForBrokerPartition tpToDirectoryUuid) throws IOException {
        Map<UnifiedLog, Optional<UnifiedLog>> abandonedFutureLogs = findAbandonedFutureLogs(brokerId, tpToDirectoryUuid);
        for (Map.Entry<UnifiedLog, Optional<UnifiedLog>> entry : abandonedFutureLogs.entrySet()) {
            UnifiedLog futureLog = entry.getKey();
            Optional<UnifiedLog> currentLog = entry.getValue();
            TopicPartition tp = futureLog.topicPartition();
            // We invoke abortAndPauseCleaning here because log cleaner runs asynchronously and replaceCurrentWithFutureLog
            // invokes resumeCleaning which requires log cleaner's internal state to have a key for the given topic partition.
            abortAndPauseCleaning(tp);

            if (currentLog.isPresent()) {
                LOG.info("Attempting to recover abandoned future log for {} at {} and removing {}", tp, futureLog, currentLog.get());
            } else {
                LOG.info("Attempting to recover abandoned future log for {} at {}", tp, futureLog);
            }
            replaceCurrentWithFutureLog(currentLog, futureLog, false);
            LOG.info("Successfully recovered abandoned future log for {}", tp);
        }
    }

    private Map<UnifiedLog, Optional<UnifiedLog>> findAbandonedFutureLogs(int brokerId, DirectoryForBrokerPartition tpToDirectoryUuid) {
        return futureLogs.values().stream()
                .flatMap(futureLog -> {
                    Uuid topicId = futureLog.topicId().orElseThrow(() ->
                            new RuntimeException("The log dir " + futureLog + " does not have a topic ID, " +
                                    "which is not allowed when running in KRaft mode."));
                    int partitionId = futureLog.topicPartition().partition();

                    // Get the directory assigned by the controller for this partition
                    Uuid assignedDirectory = tpToDirectoryUuid.get(topicId, partitionId, brokerId);
                    if (assignedDirectory == null) {
                        return Stream.empty();
                    }

                    // Check if the future log is in the assigned directory
                    Optional<Uuid> futureLogDirectoryId = directoryId(futureLog.parentDir());
                    if (futureLogDirectoryId.isEmpty() || !futureLogDirectoryId.get().equals(assignedDirectory)) {
                        return Stream.empty();
                    }

                    // Find the matching current log (if it exists)
                    Optional<UnifiedLog> currentLog = Optional.ofNullable(currentLogs.get(futureLog.topicPartition()))
                            .filter(log -> {
                                Optional<Uuid> currentLogTopicId = log.topicId();
                                return currentLogTopicId.isPresent() && currentLogTopicId.get().equals(topicId);
                            });

                    return Stream.of(Map.entry(futureLog, currentLog));
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Mark the partition directory in the source log directory for deletion and
     * rename the future log of this partition in the destination log directory to be the current log
     *
     * @param topicPartition TopicPartition that needs to be swapped
     */
    public void replaceCurrentWithFutureLog(TopicPartition topicPartition) throws IOException {
        synchronized (logCreationOrDeletionLock)  {
            UnifiedLog sourceLog = currentLogs.get(topicPartition);
            UnifiedLog destLog = futureLogs.get(topicPartition);

            if (sourceLog == null) {
                throw new KafkaStorageException("The current replica for " + topicPartition + " is offline");
            }
            if (destLog == null) {
                throw new KafkaStorageException("The future replica for " + topicPartition + " is offline");
            }

            LOG.info("Attempting to replace current log {} with {} for {}", sourceLog, destLog, topicPartition);
            replaceCurrentWithFutureLog(Optional.of(sourceLog), destLog, true);
            LOG.info("The current replica is successfully replaced with the future replica for {}", topicPartition);
        }
    }

    private void replaceCurrentWithFutureLog(Optional<UnifiedLog> sourceLog, UnifiedLog destLog, boolean updateHighWatermark) throws IOException {
        TopicPartition topicPartition = destLog.topicPartition();

        destLog.renameDir(UnifiedLog.logDirName(topicPartition), true);
        // the metrics tags still contain "future", so we have to remove it.
        // we will add metrics back after sourceLog remove the metrics
        destLog.removeLogMetrics();
        if (updateHighWatermark && sourceLog.isPresent()) {
            destLog.updateHighWatermark(sourceLog.get().highWatermark());
        }

        // Now that future replica has been successfully renamed to be the current replica
        // Update the cached map and log cleaner as appropriate.
        futureLogs.remove(topicPartition);
        currentLogs.put(topicPartition, destLog);
        if (cleaner != null) {
            sourceLog.ifPresent(srcLog ->
                cleaner.alterCheckpointDir(topicPartition, srcLog.parentDirFile(), destLog.parentDirFile())
            );
            resumeCleaning(topicPartition);
        }

        try {
            sourceLog.ifPresent(srcLog -> {
                srcLog.renameDir(UnifiedLog.logDeleteDirName(topicPartition), true);
                // Now that replica in source log directory has been successfully renamed for deletion,
                // update checkpoint files and enqueue this log to be deleted.
                // Note: We intentionally do NOT close the log here to avoid race conditions where concurrent
                // operations (e.g., log flusher, fetch requests) might encounter ClosedChannelException.
                // The log will be deleted asynchronously by the background delete-logs thread.
                // File handles are intentionally left open; Unix semantics allow the renamed files
                // to remain accessible until all handles are closed.
                File logDir = srcLog.parentDirFile();
                Map<TopicPartition, UnifiedLog> logsToCheckpoint = logsInDir(logDir);
                checkpointRecoveryOffsetsInDir(logDir, logsToCheckpoint);
                checkpointLogStartOffsetsInDir(logDir, logsToCheckpoint);
                srcLog.removeLogMetrics();
                addLogToBeDeleted(srcLog);
            });
            destLog.newMetrics();
        } catch (KafkaStorageException kse) {
            // If sourceLog's log directory is offline, we need close its handlers here.
            // handleLogDirFailure() will not close handlers of sourceLog because it has been removed from currentLogs map
            sourceLog.ifPresent(srcLog -> {
                srcLog.closeHandlers();
                srcLog.removeLogMetrics();
            });
            throw kse;
        }
    }

    // Only for testing
    public Optional<UnifiedLog> asyncDelete(TopicPartition topicPartition) {
        return asyncDelete(topicPartition, false, true, false);
    }

    /**
     * Rename the directory of the given topic-partition "logdir" as "logdir.uuid.delete" and
     * add it in the queue for deletion.
     *
     * @param topicPartition TopicPartition that needs to be deleted
     * @param isFuture True if the future log of the specified partition should be deleted
     * @param checkpoint True if checkpoints must be written
     * @param isStray True is the partition is stray
     * @return the removed log
     */
    public Optional<UnifiedLog> asyncDelete(TopicPartition topicPartition, boolean isFuture, boolean checkpoint, boolean isStray) {
        Optional<UnifiedLog> removedLogOpt;
        synchronized (logCreationOrDeletionLock)  {
            removedLogOpt = removeLogAndMetrics(isFuture ? futureLogs : currentLogs, topicPartition);
        }
        if (removedLogOpt.isPresent()) {
            UnifiedLog removedLog = removedLogOpt.get();
            // We need to wait until there is no more cleaning task on the log to be deleted before actually deleting it.
            if (cleaner != null && !isFuture) {
                cleaner.abortCleaning(topicPartition);
                if (checkpoint) {
                    cleaner.updateCheckpoints(removedLog.parentDirFile(), Optional.of(topicPartition));
                }
            }
            if (isStray) {
                // Move aside stray partitions, don't delete them
                removedLog.renameDir(UnifiedLog.logStrayDirName(topicPartition), false);
                LOG.warn("Log for partition {} is marked as stray and renamed to {}", removedLog.topicPartition(), removedLog.dir().getAbsolutePath());
            } else {
                removedLog.renameDir(UnifiedLog.logDeleteDirName(topicPartition), false);
                addLogToBeDeleted(removedLog);
                LOG.info("Log for partition {} is renamed to {} and is scheduled for deletion", removedLog.topicPartition(), removedLog.dir().getAbsolutePath());
            }
            if (checkpoint) {
                File logDir = removedLog.parentDirFile();
                Map<TopicPartition, UnifiedLog> logsToCheckpoint = logsInDir(logDir);
                checkpointRecoveryOffsetsInDir(logDir, logsToCheckpoint);
                checkpointLogStartOffsetsInDir(logDir, logsToCheckpoint);
            }
        } else {
            if (!offlineLogDirs().isEmpty()) {
                throw new KafkaStorageException("Failed to delete log for " + (isFuture ? "future " : " ") + topicPartition + " because it may be in one of the offline directories " +
                        dirsToString(offlineLogDirs()));
            }
        }
        return removedLogOpt;
    }

    /**
     * Rename the directories of the given topic-partitions and add them in the queue for
     * deletion. Checkpoints are updated once all the directories have been renamed.
     *
     * @param topicPartitions The set of topic-partitions to delete asynchronously
     * @param isStray True if the topic-partitions are strays
     * @param errorHandler The error handler that will be called when an exception for a particular
     *                     topic-partition is raised
     */
    public void asyncDelete(Set<TopicPartition> topicPartitions,
                            boolean isStray,
                            BiConsumer<TopicPartition, Throwable> errorHandler) {
        Set<File> logDirs = new HashSet<>();
        topicPartitions.forEach(topicPartition -> {
            try {
                getLog(topicPartition).ifPresent(log -> {
                    logDirs.add(log.parentDirFile());
                    asyncDelete(topicPartition, false, false, isStray);
                });
                getLog(topicPartition, true).ifPresent(log -> {
                    logDirs.add(log.parentDirFile());
                    asyncDelete(topicPartition, true, false, isStray);
                });
            } catch (Throwable e) {
                errorHandler.accept(topicPartition, e);
            }
        });

        Map<String, Map<TopicPartition, UnifiedLog>> logsByDirCached = logsByDir();
        logDirs.forEach(logDir -> {
            if (cleaner != null) cleaner.updateCheckpoints(logDir, Optional.empty());
            Map<TopicPartition, UnifiedLog> logsToCheckpoint = logsInDir(logsByDirCached, logDir);
            checkpointRecoveryOffsetsInDir(logDir, logsToCheckpoint);
            checkpointLogStartOffsetsInDir(logDir, logsToCheckpoint);
        });
    }

    /**
     * Provides the full ordered list of suggested directories for the next partition.
     * Currently, this is done by calculating the number of partitions in each directory and then sorting the
     * data directories by fewest partitions.
     *
     * <p>It's possible replicas are assigned to this broker right before all its log directories are cordoned.
     * In that case, pick the first available directory.
     */
    // Visible for testing
    public List<File> nextLogDirs() {
        if (liveLogDirs.size() == 1) {
            return List.of(liveLogDirs.peek());
        } else {
            // count the number of logs in each parent directory (including 0 for empty directories)
            Map<String, Long> logCounts = allLogs().stream()
                    .collect(Collectors.groupingBy(UnifiedLog::parentDir, Collectors.counting()));

            Map<String, Long> zeros = new LinkedHashMap<>();
            liveLogDirs.forEach(dir -> zeros.put(dir.getPath(), 0L));

            Map<String, Long> dirCounts = new LinkedHashMap<>(zeros);
            dirCounts.putAll(logCounts);
            dirCounts.entrySet().removeIf(entry -> cordonedLogDirs.contains(entry.getKey()));

            if (dirCounts.isEmpty()) {
                // all log directories are cordoned, choose the first live directory
                return List.of(liveLogDirs.peek());
            } else {
                // choose the directory with the least logs in it
                return dirCounts.entrySet().stream()
                        .sorted(Map.Entry.comparingByValue())
                        .map(entry -> new File(entry.getKey()))
                        .collect(Collectors.toList());
            }
        }
    }

    /**
     * Delete any eligible logs. Return the number of segments deleted.
     * Only consider logs that are not compacted.
     */
    private void cleanupLogs() {
        LOG.debug("Beginning log cleanup...");
        AtomicInteger total = new AtomicInteger(0);
        long startMs = time.milliseconds();

        // clean current logs.
        Map<TopicPartition, UnifiedLog> deletableLogs;
        if (cleaner != null) {
            // prevent cleaner from working on same partitions when changing cleanup policy
            deletableLogs = cleaner.pauseCleaningForNonCompactedPartitions();
        } else {
            deletableLogs = currentLogs.entrySet().stream()
                    .filter(e -> !e.getValue().config().compact)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        try {
            for (Map.Entry<TopicPartition, UnifiedLog> entry : deletableLogs.entrySet()) {
                TopicPartition topicPartition = entry.getKey();
                UnifiedLog log = entry.getValue();
                LOG.debug("Garbage collecting '{}'", log.name());
                total.addAndGet(log.deleteOldSegments());

                UnifiedLog futureLog = futureLogs.get(topicPartition);
                if (futureLog != null) {
                    // clean future logs
                    LOG.debug("Garbage collecting future log '{}'", futureLog.name());
                    total.addAndGet(futureLog.deleteOldSegments());
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            if (cleaner != null) {
                cleaner.resumeCleaning(deletableLogs.keySet());
            }
        }

        LOG.debug("Log cleanup completed. {} files deleted in {} seconds", total, (time.milliseconds() - startMs) / 1000);
    }

    /**
     * Get all the partition logs
     */
    public Set<UnifiedLog> allLogs() {
        return Stream.of(currentLogs.values(), futureLogs.values())
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    /**
     * Get the UnifiedLogs for the specified topic
     */
    public List<UnifiedLog> logsByTopic(String topic) {
        return Stream.of(currentLogs.entrySet(), futureLogs.entrySet())
                .flatMap(Collection::stream)
                .filter(entry -> entry.getKey().topic().equals(topic))
                .map(Map.Entry::getValue)
                .toList();
    }

    /**
     * Map of log dir to logs by topic and partitions in that dir
     */
    private Map<String, Map<TopicPartition, UnifiedLog>> logsByDir() {
        // This code is called often by checkpoint processes and is written in a way that reduces
        // allocations and CPU with many topic partitions.
        // When changing this code please measure the changes with org.apache.kafka.jmh.server.CheckpointBench
        Map<String, Map<TopicPartition, UnifiedLog>> byDir = new HashMap<>();
        currentLogs.forEach((tp, log) ->
            byDir.computeIfAbsent(log.parentDir(), k -> new HashMap<>()).put(tp, log)
        );
        futureLogs.forEach((tp, log) ->
            byDir.computeIfAbsent(log.parentDir(), k -> new HashMap<>()).put(tp, log)
        );
        return byDir;
    }

    private Map<TopicPartition, UnifiedLog> logsInDir(File dir) {
        return logsByDir().getOrDefault(dir.getAbsolutePath(), Map.of());
    }

    private Map<TopicPartition, UnifiedLog> logsInDir(Map<String, Map<TopicPartition, UnifiedLog>> cachedLogsByDir, File dir) {
        return cachedLogsByDir.getOrDefault(dir.getAbsolutePath(), Map.of());
    }

    // logDir should be an absolute path
    public boolean isLogDirOnline(String logDir) {
        // The logDir should be an absolute path
        if (logDirs.stream().noneMatch(f -> f.getAbsolutePath().equals(logDir))) {
            throw new LogDirNotFoundException("Log dir " + logDir + " is not found in the config.");
        }
        return liveLogDirs.contains(new File(logDir));
    }

    /**
     * Flush any log which has exceeded its flush interval and has unwritten messages.
     */
    private void flushDirtyLogs() {
        LOG.debug("Checking for dirty logs to flush...");

        Stream.of(currentLogs.entrySet(), futureLogs.entrySet()).flatMap(Collection::stream).forEach(entry -> {
            TopicPartition topicPartition = entry.getKey();
            UnifiedLog log = entry.getValue();
            try {
                long timeSinceLastFlush = time.milliseconds() - log.lastFlushTime();
                LOG.debug("Checking if flush is needed on {} flush interval {} last flushed {} time since last flush: {}", topicPartition.topic(), log.config().flushMs, log.lastFlushTime(), timeSinceLastFlush);
                if (timeSinceLastFlush >= log.config().flushMs) {
                    log.flush(false);
                }
            } catch (Throwable e) {
                LOG.error("Error flushing topic {}", topicPartition.topic(), e);
            }
        });
    }

    private Optional<UnifiedLog> removeLogAndMetrics(ConcurrentMap<TopicPartition, UnifiedLog> logs, TopicPartition tp) {
        UnifiedLog removedLog = logs.remove(tp);
        if (removedLog != null) {
            removedLog.removeLogMetrics();
            return Optional.of(removedLog);
        } else {
            return Optional.empty();
        }
    }

    /**
     * Retrieve the broker epoch from the shutdown file if it exists, return empty otherwise
     */
    public OptionalLong readBrokerEpochFromCleanShutdownFiles() {
        // Verify whether all the log dirs have the same broker epoch in their clean shutdown files. If there is any dir not
        // live, fail the broker epoch check.
        if (liveLogDirs.size() < logDirs.size()) {
            return OptionalLong.empty();
        }
        long brokerEpoch = -1L;
        for (File dir : liveLogDirs) {
            CleanShutdownFileHandler cleanShutdownFileHandler = new CleanShutdownFileHandler(dir.getPath());
            OptionalLong currentBrokerEpoch = cleanShutdownFileHandler.read();
            if (currentBrokerEpoch.isEmpty()) {
                LOG.info("Unable to read the broker epoch in {}.", dir);
                return OptionalLong.empty();
            }
            if (brokerEpoch != -1 && currentBrokerEpoch.getAsLong() != brokerEpoch) {
                LOG.info("Found different broker epochs in {}. Other={} vs current={}.", dir, brokerEpoch, currentBrokerEpoch);
                return OptionalLong.empty();
            }
            brokerEpoch = currentBrokerEpoch.getAsLong();
        }
        return OptionalLong.of(brokerEpoch);
    }

    /**
     * Wait for all jobs to complete
     * @param jobs The jobs
     * @param callback This will be called to handle the exception caused by each Future#get
     * @return true if all pass. Otherwise, false
     */
    public static boolean waitForAllToComplete(List<Future<?>> jobs, Consumer<Throwable> callback) {
        List<Future<?>> failed = new ArrayList<>();
        for (Future<?> job : jobs) {
            try {
                job.get();
            } catch (Exception e) {
                callback.accept(e);
                failed.add(job);
            }
        }
        return failed.isEmpty();
    }

    /**
     * Returns true if the given log should not be on the current broker according to the metadata.
     *
     * @param replicas       The replicas hosting the partition
     * @param brokerId       The ID of the current broker.
     * @param log            The log object to check
     * @return true if the log should not exist on the broker, false otherwise.
     */
    public static boolean isStrayReplica(List<Integer> replicas, int brokerId, UnifiedLog log) {
        if (replicas.isEmpty()) {
            LOG.info("Found stray log dir {}: the topicId {} does not exist in the metadata image.", log, log.topicId().get());
            return true;
        }
        if (!replicas.contains(brokerId)) {
            LOG.info("Found stray log dir {}: the current replica assignment {} does not contain the local brokerId {}.",
                    log, replicas.stream().map(String::valueOf).collect(Collectors.joining(", ", "[", "]")), brokerId);
            return true;
        } else {
            return false;
        }
    }
}
