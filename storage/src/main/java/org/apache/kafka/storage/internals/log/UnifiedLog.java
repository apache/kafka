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

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.InconsistentTopicIdException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.InvalidTxnStateException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.errors.RecordBatchTooLargeException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.DescribeProducersResponseData;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.RecordValidationStats;
import org.apache.kafka.common.record.RecordVersion;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.PrimitiveRef;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.common.TransactionVersion;
import org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.record.BrokerCompressionType;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.UnexpectedAppendOffsetException;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.storage.internals.checkpoint.LeaderEpochCheckpointFile;
import org.apache.kafka.storage.internals.checkpoint.PartitionMetadataFile;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.log.metrics.BrokerTopicMetrics;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse.UNDEFINED_EPOCH_OFFSET;

/**
 * A log which presents a unified view of local and tiered log segments.
 *
 * <p>The log consists of tiered and local segments with the tiered portion of the log being optional. There could be an
 * overlap between the tiered and local segments. The active segment is always guaranteed to be local. If tiered segments
 * are present, they always appear at the beginning of the log, followed by an optional region of overlap, followed by the local
 * segments including the active segment.
 *
 * <p>NOTE: this class handles state and behavior specific to tiered segments as well as any behavior combining both tiered
 * and local segments. The state and behavior specific to local segments are handled by the encapsulated LocalLog instance.
 */
public class UnifiedLog implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(UnifiedLog.class);

    public static final String LOG_FILE_SUFFIX = LogFileUtils.LOG_FILE_SUFFIX;
    public static final String INDEX_FILE_SUFFIX = LogFileUtils.INDEX_FILE_SUFFIX;
    public static final String TIME_INDEX_FILE_SUFFIX = LogFileUtils.TIME_INDEX_FILE_SUFFIX;
    public static final String TXN_INDEX_FILE_SUFFIX = LogFileUtils.TXN_INDEX_FILE_SUFFIX;
    public static final String CLEANED_FILE_SUFFIX = LogFileUtils.CLEANED_FILE_SUFFIX;
    public static final String SWAP_FILE_SUFFIX = LogFileUtils.SWAP_FILE_SUFFIX;
    public static final String DELETE_DIR_SUFFIX = LogFileUtils.DELETE_DIR_SUFFIX;
    public static final String STRAY_DIR_SUFFIX = LogFileUtils.STRAY_DIR_SUFFIX;
    public static final long UNKNOWN_OFFSET = LocalLog.UNKNOWN_OFFSET;

    // For compatibility, metrics are defined to be under `Log` class
    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup("kafka.log", "Log");

    /* A lock that guards all modifications to the log */
    private final Object lock = new Object();
    private final Map<String, Map<String, String>> metricNames = new HashMap<>();

    // localLog The LocalLog instance containing non-empty log segments recovered from disk
    private final LocalLog localLog;
    private final BrokerTopicStats brokerTopicStats;
    private final ProducerStateManager producerStateManager;
    private final boolean remoteStorageSystemEnable;
    private final ScheduledFuture<?> producerExpireCheck;
    private final int producerIdExpirationCheckIntervalMs;
    private final String logIdent;
    private final Logger logger;
    private final Logger futureTimestampLogger;
    private final LogValidator.MetricsRecorder validatorMetricsRecorder;

    /* The earliest offset which is part of an incomplete transaction. This is used to compute the
     * last stable offset (LSO) in ReplicaManager. Note that it is possible that the "true" first unstable offset
     * gets removed from the log (through record or segment deletion). In this case, the first unstable offset
     * will point to the log start offset, which may actually be either part of a completed transaction or not
     * part of a transaction at all. However, since we only use the LSO for the purpose of restricting the
     * read_committed consumer to fetching decided data (i.e. committed, aborted, or non-transactional), this
     * temporary abuse seems justifiable and saves us from scanning the log after deletion to find the first offsets
     * of each ongoing transaction in order to compute a new first unstable offset. It is possible, however,
     * that this could result in disagreement between replicas depending on when they began replicating the log.
     * In the worst case, the LSO could be seen by a consumer to go backwards.
     */
    private volatile Optional<LogOffsetMetadata> firstUnstableOffsetMetadata = Optional.empty();
    private volatile Optional<PartitionMetadataFile> partitionMetadataFile = Optional.empty();
    // This is the offset(inclusive) until which segments are copied to the remote storage.
    private volatile long highestOffsetInRemoteStorage = -1L;

    /* Keep track of the current high watermark in order to ensure that segments containing offsets at or above it are
     * not eligible for deletion. This means that the active segment is only eligible for deletion if the high watermark
     * equals the log end offset (which may never happen for a partition under consistent load). This is needed to
     * prevent the log start offset (which is exposed in fetch responses) from getting ahead of the high watermark.
     */
    private volatile LogOffsetMetadata highWatermarkMetadata;
    private volatile long localLogStartOffset;
    private volatile long logStartOffset;
    private volatile LeaderEpochFileCache leaderEpochCache;
    private volatile Optional<Uuid> topicId;
    private volatile LogOffsetsListener logOffsetsListener;

    /**
     * A log which presents a unified view of local and tiered log segments.
     *
     * <p>The log consists of tiered and local segments with the tiered portion of the log being optional. There could be an
     * overlap between the tiered and local segments. The active segment is always guaranteed to be local. If tiered segments
     * are present, they always appear at the beginning of the log, followed by an optional region of overlap, followed by the local
     * segments including the active segment.
     *
     * <p>NOTE: this class handles state and behavior specific to tiered segments as well as any behavior combining both tiered
     * and local segments. The state and behavior specific to local segments are handled by the encapsulated LocalLog instance.
     *
     * @param logStartOffset The earliest offset allowed to be exposed to kafka client.
     *                       The logStartOffset can be updated by :
     *                       - user's DeleteRecordsRequest
     *                       - broker's log retention
     *                       - broker's log truncation
     *                       - broker's log recovery
     *                       The logStartOffset is used to decide the following:
     *                       - Log deletion. LogSegment whose nextOffset <= log's logStartOffset can be deleted.
     *                         It may trigger log rolling if the active segment is deleted.
     *                       - Earliest offset of the log in response to ListOffsetRequest. To avoid OffsetOutOfRange exception after user seeks to earliest offset,
     *                         we make sure that logStartOffset <= log's highWatermark
     *                       Other activities such as log cleaning are not affected by logStartOffset.
     * @param localLog The LocalLog instance containing non-empty log segments recovered from disk
     * @param brokerTopicStats Container for Broker Topic Yammer Metrics
     * @param producerIdExpirationCheckIntervalMs How often to check for producer ids which need to be expired
     * @param leaderEpochCache The LeaderEpochFileCache instance (if any) containing state associated
     *                         with the provided logStartOffset and nextOffsetMetadata
     * @param producerStateManager The ProducerStateManager instance containing state associated with the provided segments
     * @param topicId optional Uuid to specify the topic ID for the topic if it exists. Should only be specified when
     *                first creating the log through Partition.makeLeader or Partition.makeFollower. When reloading a log,
     *                this field will be populated by reading the topic ID value from partition.metadata if it exists.
     * @param remoteStorageSystemEnable flag to indicate whether the system level remote log storage is enabled or not.
     * @param logOffsetsListener listener invoked when the high watermark is updated
     */
    @SuppressWarnings({"this-escape"})
    public UnifiedLog(long logStartOffset,
                      LocalLog localLog,
                      BrokerTopicStats brokerTopicStats,
                      int producerIdExpirationCheckIntervalMs,
                      LeaderEpochFileCache leaderEpochCache,
                      ProducerStateManager producerStateManager,
                      Optional<Uuid> topicId,
                      boolean remoteStorageSystemEnable,
                      LogOffsetsListener logOffsetsListener) throws IOException {
        this.logStartOffset = logStartOffset;
        this.localLog = localLog;
        this.brokerTopicStats = brokerTopicStats;
        this.producerIdExpirationCheckIntervalMs = producerIdExpirationCheckIntervalMs;
        this.leaderEpochCache = leaderEpochCache;
        this.producerStateManager = producerStateManager;
        this.topicId = topicId;
        this.remoteStorageSystemEnable = remoteStorageSystemEnable;
        this.logOffsetsListener = logOffsetsListener;

        this.logIdent = "[UnifiedLog partition=" + topicPartition() + ", dir=" + parentDir() + "] ";
        this.logger = new LogContext(logIdent).logger(UnifiedLog.class);
        this.futureTimestampLogger = new LogContext(logIdent).logger("LogFutureTimestampLogger");
        this.highWatermarkMetadata = new LogOffsetMetadata(logStartOffset);
        this.localLogStartOffset = logStartOffset;
        this.producerExpireCheck = scheduler().schedule("PeriodicProducerExpirationCheck", () -> removeExpiredProducers(time().milliseconds()),
                producerIdExpirationCheckIntervalMs, producerIdExpirationCheckIntervalMs);
        this.validatorMetricsRecorder = UnifiedLog.newValidatorMetricsRecorder(brokerTopicStats.allTopicsStats());

        initializePartitionMetadata();
        updateLogStartOffset(logStartOffset);
        updateLocalLogStartOffset(Math.max(logStartOffset, localLog.segments().firstSegmentBaseOffset().orElse(0L)));
        if (!remoteLogEnabled())
            this.logStartOffset = localLogStartOffset;
        maybeIncrementFirstUnstableOffset();
        initializeTopicId();

        logOffsetsListener.onHighWatermarkUpdated(highWatermarkMetadata.messageOffset);
        newMetrics();
    }

    /**
     * Create a new UnifiedLog instance
     * @param dir dir The directory in which log segments are created.
     * @param config The log configuration settings
     * @param logStartOffset The checkpoint of the log start offset
     * @param recoveryPoint The checkpoint of the offset at which to begin the recovery
     * @param scheduler The thread pool scheduler used for background actions
     * @param brokerTopicStats Container for Broker Topic Yammer Metrics
     * @param time The time instance used for checking the clock
     * @param maxTransactionTimeoutMs The timeout in milliseconds for transactions
     * @param producerStateManagerConfig The configuration for creating the ProducerStateManager instance
     * @param producerIdExpirationCheckIntervalMs How often to check for producer ids which need to be expired
     * @param logDirFailureChannel The LogDirFailureChannel instance to asynchronously handle Log dir failure
     * @param lastShutdownClean Boolean flag to indicate whether the associated log previously had a clean shutdown
     * @param topicId optional Uuid to specify the topic ID for the topic if it exists
     * @throws IOException if an I/O error occurs
     */
    public static UnifiedLog create(File dir,
                                    LogConfig config,
                                    long logStartOffset,
                                    long recoveryPoint,
                                    Scheduler scheduler,
                                    BrokerTopicStats brokerTopicStats,
                                    Time time,
                                    int maxTransactionTimeoutMs,
                                    ProducerStateManagerConfig producerStateManagerConfig,
                                    int producerIdExpirationCheckIntervalMs,
                                    LogDirFailureChannel logDirFailureChannel,
                                    boolean lastShutdownClean,
                                    Optional<Uuid> topicId) throws IOException {
        return create(dir,
                config,
                logStartOffset,
                recoveryPoint,
                scheduler,
                brokerTopicStats,
                time,
                maxTransactionTimeoutMs,
                producerStateManagerConfig,
                producerIdExpirationCheckIntervalMs,
                logDirFailureChannel,
                lastShutdownClean,
                topicId,
                new ConcurrentHashMap<>(),
                false,
                LogOffsetsListener.NO_OP_OFFSETS_LISTENER);
    }

    /**
     * Create a new UnifiedLog instance
     * @param dir dir The directory in which log segments are created.
     * @param config The log configuration settings
     * @param logStartOffset The checkpoint of the log start offset
     * @param recoveryPoint The checkpoint of the offset at which to begin the recovery
     * @param scheduler The thread pool scheduler used for background actions
     * @param brokerTopicStats Container for Broker Topic Yammer Metrics
     * @param time The time instance used for checking the clock
     * @param maxTransactionTimeoutMs The timeout in milliseconds for transactions
     * @param producerStateManagerConfig The configuration for creating the ProducerStateManager instance
     * @param producerIdExpirationCheckIntervalMs How often to check for producer ids which need to be expired
     * @param logDirFailureChannel The LogDirFailureChannel instance to asynchronously handle Log dir failure
     * @param lastShutdownClean Boolean flag to indicate whether the associated log previously had a clean shutdown
     * @param topicId Optional Uuid to specify the topic ID for the topic if it exists
     * @param numRemainingSegments The remaining segments to be recovered in this log keyed by recovery thread name
     * @param remoteStorageSystemEnable Boolean flag to indicate whether the system level remote log storage is enabled or not.
     * @param logOffsetsListener listener invoked when the high watermark is updated
     * @throws IOException if an I/O error occurs
     */
    public static UnifiedLog create(File dir,
                                    LogConfig config,
                                    long logStartOffset,
                                    long recoveryPoint,
                                    Scheduler scheduler,
                                    BrokerTopicStats brokerTopicStats,
                                    Time time,
                                    int maxTransactionTimeoutMs,
                                    ProducerStateManagerConfig producerStateManagerConfig,
                                    int producerIdExpirationCheckIntervalMs,
                                    LogDirFailureChannel logDirFailureChannel,
                                    boolean lastShutdownClean,
                                    Optional<Uuid> topicId,
                                    ConcurrentMap<String, Integer> numRemainingSegments,
                                    boolean remoteStorageSystemEnable,
                                    LogOffsetsListener logOffsetsListener) throws IOException {
        // create the log directory if it doesn't exist
        Files.createDirectories(dir.toPath());
        TopicPartition topicPartition = UnifiedLog.parseTopicPartitionName(dir);
        LogSegments segments = new LogSegments(topicPartition);
        // The created leaderEpochCache will be truncated by LogLoader if necessary
        // so it is guaranteed that the epoch entries will be correct even when on-disk
        // checkpoint was stale (due to async nature of LeaderEpochFileCache#truncateFromStart/End).
        LeaderEpochFileCache leaderEpochCache = UnifiedLog.createLeaderEpochCache(
                dir,
                topicPartition,
                logDirFailureChannel,
                Optional.empty(),
                scheduler);
        ProducerStateManager producerStateManager = new ProducerStateManager(
                topicPartition,
                dir,
                maxTransactionTimeoutMs,
                producerStateManagerConfig,
                time);
        boolean isRemoteLogEnabled = UnifiedLog.isRemoteLogEnabled(remoteStorageSystemEnable, config, topicPartition.topic());
        LoadedLogOffsets offsets = new LogLoader(
                dir,
                topicPartition,
                config,
                scheduler,
                time,
                logDirFailureChannel,
                lastShutdownClean,
                segments,
                logStartOffset,
                recoveryPoint,
                leaderEpochCache,
                producerStateManager,
                numRemainingSegments,
                isRemoteLogEnabled
                ).load();
        LocalLog localLog = new LocalLog(
                dir,
                config,
                segments,
                offsets.recoveryPoint(),
                offsets.nextOffsetMetadata(),
                scheduler,
                time,
                topicPartition,
                logDirFailureChannel);
        return new UnifiedLog(offsets.logStartOffset(),
                localLog,
                brokerTopicStats,
                producerIdExpirationCheckIntervalMs,
                leaderEpochCache,
                producerStateManager,
                topicId,
                remoteStorageSystemEnable,
                logOffsetsListener);
    }

    public long localLogStartOffset() {
        return localLogStartOffset;
    }

    public LeaderEpochFileCache leaderEpochCache() {
        return leaderEpochCache;
    }

    public long logStartOffset() {
        return logStartOffset;
    }

    public long highestOffsetInRemoteStorage() {
        return highestOffsetInRemoteStorage;
    }

    public Optional<PartitionMetadataFile> partitionMetadataFile() {
        return partitionMetadataFile;
    }

    public Optional<Uuid> topicId() {
        return topicId;
    }

    public File dir() {
        return localLog.dir();
    }

    public String parentDir() {
        return localLog.parentDir();
    }

    public File parentDirFile() {
        return localLog.parentDirFile();
    }

    public String name() {
        return localLog.name();
    }

    public long recoveryPoint() {
        return localLog.recoveryPoint();
    }

    public TopicPartition topicPartition() {
        return localLog.topicPartition();
    }

    public LogDirFailureChannel logDirFailureChannel() {
        return localLog.logDirFailureChannel();
    }

    public LogConfig config() {
        return localLog.config();
    }

    public boolean remoteLogEnabled() {
        return UnifiedLog.isRemoteLogEnabled(remoteStorageSystemEnable, config(), topicPartition().topic());
    }

    public ScheduledFuture<?> producerExpireCheck() {
        return producerExpireCheck;
    }

    public int producerIdExpirationCheckIntervalMs() {
        return producerIdExpirationCheckIntervalMs;
    }

    public void updateLogStartOffsetFromRemoteTier(long remoteLogStartOffset) {
        if (!remoteLogEnabled()) {
            logger.error("Ignoring the call as the remote log storage is disabled");
            return;
        }
        maybeIncrementLogStartOffset(remoteLogStartOffset, LogStartOffsetIncrementReason.SegmentDeletion);
    }

    // visible for testing
    public void updateLocalLogStartOffset(long offset) throws IOException {
        localLogStartOffset = offset;
        if (highWatermark() < offset) {
            updateHighWatermark(offset);
        }
        if (recoveryPoint() < offset) {
            localLog.updateRecoveryPoint(offset);
        }
    }

    public void setLogOffsetsListener(LogOffsetsListener listener) {
        logOffsetsListener = listener;
    }

    /**
     * Initialize topic ID information for the log by maintaining the partition metadata file and setting the in-memory _topicId.
     * Set _topicId based on a few scenarios:
     *   - Recover topic ID if present. Ensure we do not try to assign a provided topicId that is inconsistent
     *     with the ID on file.
     *   - If we were provided a topic ID when creating the log and one does not yet exist
     *     set _topicId and write to the partition metadata file.
     */
    private void initializeTopicId() {
        PartitionMetadataFile partMetadataFile = partitionMetadataFile.orElseThrow(() ->
                new KafkaException("The partitionMetadataFile should have been initialized"));

        if (partMetadataFile.exists()) {
            Uuid fileTopicId = partMetadataFile.read().topicId();
            if (topicId.filter(x -> !x.equals(fileTopicId)).isPresent()) {
                throw new InconsistentTopicIdException("Tried to assign topic ID " + topicId + " to log for topic partition " + topicPartition() + "," +
                        "but log already contained topic ID " + fileTopicId);
            }
            topicId = Optional.of(fileTopicId);
        } else {
            topicId.ifPresent(partMetadataFile::record);
            scheduler().scheduleOnce("flush-metadata-file", this::maybeFlushMetadataFile);
        }
    }

    public LogConfig updateConfig(LogConfig newConfig) {
        LogConfig oldConfig = localLog.config();
        localLog.updateConfig(newConfig);
        return oldConfig;
    }

    public long highWatermark() {
        return highWatermarkMetadata.messageOffset;
    }

    public ProducerStateManager producerStateManager() {
        return producerStateManager;
    }

    private Time time() {
        return localLog.time();
    }

    private Scheduler scheduler() {
        return localLog.scheduler();
    }

    /**
     * Update the high watermark to a new offset. The new high watermark will be lower-bounded by the log start offset
     * and upper-bounded by the log end offset.
     *
     * <p>This is intended to be called by the leader when initializing the high watermark.
     *
     * @param hw the suggested new value for the high watermark
     * @return the updated high watermark offset
     */
    public long updateHighWatermark(long hw) throws IOException {
        return updateHighWatermark(new LogOffsetMetadata(hw));
    }

    /**
     * Update high watermark with offset metadata. The new high watermark will be lower-bounded by the log start offset
     * and upper-bounded by the log end offset.
     *
     * @param highWatermarkMetadata the suggested high watermark with offset metadata
     * @return the updated high watermark offset
     */
    public long updateHighWatermark(LogOffsetMetadata highWatermarkMetadata) throws IOException {
        LogOffsetMetadata endOffsetMetadata = localLog.logEndOffsetMetadata();
        LogOffsetMetadata newHighWatermarkMetadata = highWatermarkMetadata.messageOffset < logStartOffset
            ? new LogOffsetMetadata(logStartOffset)
            : highWatermarkMetadata.messageOffset >= endOffsetMetadata.messageOffset
                ? endOffsetMetadata
                : highWatermarkMetadata;

        updateHighWatermarkMetadata(newHighWatermarkMetadata);
        return newHighWatermarkMetadata.messageOffset;
    }

    /**
     * Update the high watermark to a new value if and only if it is larger than the old value. It is
     * an error to update to a value which is larger than the log end offset.
     *
     * <p>This method is intended to be used by the leader to update the high watermark after follower
     * fetch offsets have been updated.
     *
     * @return the old high watermark, if updated by the new value
     */
    public Optional<LogOffsetMetadata> maybeIncrementHighWatermark(LogOffsetMetadata newHighWatermark) throws IOException {
        if (newHighWatermark.messageOffset > logEndOffset()) {
            throw new IllegalArgumentException("High watermark " + newHighWatermark + " update exceeds current " +
                    "log end offset " + localLog.logEndOffsetMetadata());
        }

        synchronized (lock) {
            LogOffsetMetadata oldHighWatermark = fetchHighWatermarkMetadata();
            // Ensure that the high watermark increases monotonically. We also update the high watermark when the new
            // offset metadata is on a newer segment, which occurs whenever the log is rolled to a new segment.
            if (oldHighWatermark.messageOffset < newHighWatermark.messageOffset ||
                    (oldHighWatermark.messageOffset == newHighWatermark.messageOffset && oldHighWatermark.onOlderSegment(newHighWatermark))) {
                updateHighWatermarkMetadata(newHighWatermark);
                return Optional.of(oldHighWatermark);
            } else {
                return Optional.empty();
            }
        }
    }

    /**
     * Update high watermark with a new value. The new high watermark will be lower-bounded by the log start offset
     * and upper-bounded by the log end offset.
     *
     * <p>This method is intended to be used by the follower to update its high watermark after
     * replication from the leader.
     *
     * @return the new high watermark if the high watermark changed, None otherwise.
     */
    public Optional<Long> maybeUpdateHighWatermark(long hw) throws IOException {
        synchronized (lock) {
            LogOffsetMetadata oldHighWatermark = highWatermarkMetadata;
            long newHighWatermark = updateHighWatermark(new LogOffsetMetadata(hw));
            return (newHighWatermark == oldHighWatermark.messageOffset)
                    ? Optional.empty()
                    : Optional.of(newHighWatermark);
        }
    }


    /**
     * Get the offset and metadata for the current high watermark. If offset metadata is not
     * known, this will do a lookup in the index and cache the result.
     */
    private LogOffsetMetadata fetchHighWatermarkMetadata() throws IOException {
        localLog.checkIfMemoryMappedBufferClosed();
        LogOffsetMetadata offsetMetadata = highWatermarkMetadata;
        if (offsetMetadata.messageOffsetOnly()) {
            synchronized (lock) {
                LogOffsetMetadata fullOffset = maybeConvertToOffsetMetadata(highWatermark());
                updateHighWatermarkMetadata(fullOffset);
                return fullOffset;
            }
        } else {
            return offsetMetadata;
        }
    }

    private void updateHighWatermarkMetadata(LogOffsetMetadata newHighWatermark) throws IOException {
        if (newHighWatermark.messageOffset < 0) {
            throw new IllegalArgumentException("High watermark offset should be non-negative");
        }

        synchronized (lock)  {
            if (newHighWatermark.messageOffset < highWatermarkMetadata.messageOffset) {
                logger.warn("Non-monotonic update of high watermark from {} to {}", highWatermarkMetadata, newHighWatermark);
            }
            highWatermarkMetadata = newHighWatermark;
            producerStateManager.onHighWatermarkUpdated(newHighWatermark.messageOffset);
            logOffsetsListener.onHighWatermarkUpdated(newHighWatermark.messageOffset);
            maybeIncrementFirstUnstableOffset();
        }
        logger.trace("Setting high watermark {}", newHighWatermark);
    }

    /**
     * Get the first unstable offset. Unlike the last stable offset, which is always defined,
     * the first unstable offset only exists if there are transactions in progress.
     *
     * @return the first unstable offset, if it exists
     */
    public Optional<Long> firstUnstableOffset() {
        return firstUnstableOffsetMetadata.map(uom -> uom.messageOffset);
    }

    private LogOffsetMetadata fetchLastStableOffsetMetadata() throws IOException {
        localLog.checkIfMemoryMappedBufferClosed();

        // cache the current high watermark and the first unstable offset metadata to avoid a concurrent update
        // invalidating the range check breaking the isPresent check
        LogOffsetMetadata highWatermarkMetadata = fetchHighWatermarkMetadata();
        Optional<LogOffsetMetadata> firstUnstableOffsetMetadataCopy = firstUnstableOffsetMetadata;
        if (firstUnstableOffsetMetadataCopy.isPresent() && firstUnstableOffsetMetadataCopy.get().messageOffset < highWatermarkMetadata.messageOffset) {
            LogOffsetMetadata lom = firstUnstableOffsetMetadataCopy.get();
            if (lom.messageOffsetOnly()) {
                synchronized (lock) {
                    LogOffsetMetadata fullOffset = maybeConvertToOffsetMetadata(lom.messageOffset);
                    firstUnstableOffsetMetadata = Optional.of(fullOffset);
                    return fullOffset;
                }
            } else {
                return lom;
            }
        } else {
            return highWatermarkMetadata;
        }
    }

    /**
     * The last stable offset (LSO) is defined as the first offset such that all lower offsets have been "decided."
     * Non-transactional messages are considered decided immediately, but transactional messages are only decided when
     * the corresponding COMMIT or ABORT marker is written. This implies that the last stable offset will be equal
     * to the high watermark if there are no transactional messages in the log. Note also that the LSO cannot advance
     * beyond the high watermark.
     */
    public long lastStableOffset() {
        // cache the first unstable offset metadata to avoid a concurrent update breaking the isPresent check
        Optional<LogOffsetMetadata> firstUnstableOffsetMetadataCopy = firstUnstableOffsetMetadata;
        if (firstUnstableOffsetMetadataCopy.isPresent() && firstUnstableOffsetMetadataCopy.get().messageOffset < highWatermark()) {
            return firstUnstableOffsetMetadataCopy.get().messageOffset;
        } else {
            return highWatermark();
        }
    }

    public long lastStableOffsetLag() {
        return highWatermark() - lastStableOffset();
    }

    /**
     * Fully materialize and return an offset snapshot including segment position info. This method will update
     * the LogOffsetMetadata for the high watermark and last stable offset if they are message-only. Throws an
     * offset out of range error if the segment info cannot be loaded.
     */
    public LogOffsetSnapshot fetchOffsetSnapshot() throws IOException {
        LogOffsetMetadata lastStable = fetchLastStableOffsetMetadata();
        LogOffsetMetadata highWatermark = fetchHighWatermarkMetadata();
        return new LogOffsetSnapshot(
                logStartOffset,
                localLog.logEndOffsetMetadata(),
                highWatermark,
                lastStable
        );
    }

    public void newMetrics() {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("topic", topicPartition().topic());
        tags.put("partition", String.valueOf(topicPartition().partition()));
        if (isFuture()) {
            tags.put("is-future", "true");
        }
        metricsGroup.newGauge(LogMetricNames.NUM_LOG_SEGMENTS, this::numberOfSegments, tags);
        metricsGroup.newGauge(LogMetricNames.LOG_START_OFFSET, this::logStartOffset, tags);
        metricsGroup.newGauge(LogMetricNames.LOG_END_OFFSET, this::logEndOffset, tags);
        metricsGroup.newGauge(LogMetricNames.SIZE, this::size, tags);
        metricNames.put(LogMetricNames.NUM_LOG_SEGMENTS, tags);
        metricNames.put(LogMetricNames.LOG_START_OFFSET, tags);
        metricNames.put(LogMetricNames.LOG_END_OFFSET, tags);
        metricNames.put(LogMetricNames.SIZE, tags);
    }

    public void removeExpiredProducers(long currentTimeMs) {
        synchronized (lock) {
            producerStateManager.removeExpiredProducers(currentTimeMs);
        }
    }

    public void loadProducerState(long lastOffset) throws IOException {
        synchronized (lock) {
            rebuildProducerState(lastOffset, producerStateManager);
            maybeIncrementFirstUnstableOffset();
            updateHighWatermark(localLog.logEndOffsetMetadata());
        }
    }

    private void initializePartitionMetadata() {
        synchronized (lock) {
            File partitionMetadata = PartitionMetadataFile.newFile(dir());
            partitionMetadataFile = Optional.of(new PartitionMetadataFile(partitionMetadata, logDirFailureChannel()));
        }
    }

    private void maybeFlushMetadataFile() {
        partitionMetadataFile.ifPresent(PartitionMetadataFile::maybeFlush);
    }

    /** Only used for ZK clusters when we update and start using topic IDs on existing topics */
    public void assignTopicId(Uuid topicId) {
        if (this.topicId.isPresent()) {
            Uuid currentId = this.topicId.get();
            if (!currentId.equals(topicId)) {
                throw new InconsistentTopicIdException("Tried to assign topic ID " + topicId + " to log for topic partition " + topicPartition() +
                        ", but log already contained topic ID " + currentId);
            }
        } else {
            this.topicId = Optional.of(topicId);
            partitionMetadataFile.ifPresentOrElse(
                file -> {
                    if (!file.exists()) {
                        file.record(topicId);
                        scheduler().scheduleOnce("flush-metadata-file", this::maybeFlushMetadataFile);
                    }
                },
                () -> logger.warn("The topic id {} will not be persisted to the partition metadata file since the partition is deleted", topicId)
            );
        }
    }

    private void reinitializeLeaderEpochCache() throws IOException {
        synchronized (lock) {
            leaderEpochCache = UnifiedLog.createLeaderEpochCache(
                    dir(), topicPartition(), logDirFailureChannel(), Optional.of(leaderEpochCache), scheduler());
        }
    }

    private void updateHighWatermarkWithLogEndOffset() throws IOException {
        // Update the high watermark in case it has gotten ahead of the log end offset following a truncation
        // or if a new segment has been rolled and the offset metadata needs to be updated.
        if (highWatermark() >= localLog.logEndOffset()) {
            updateHighWatermarkMetadata(localLog.logEndOffsetMetadata());
        }
    }

    private void updateLogStartOffset(long offset) throws IOException {
        logStartOffset = offset;
        if (highWatermark() < offset) {
            updateHighWatermark(offset);
        }
        if (localLog.recoveryPoint() < offset) {
            localLog.updateRecoveryPoint(offset);
        }
    }

    public void updateHighestOffsetInRemoteStorage(long offset) {
        if (!remoteLogEnabled()) {
            logger.warn("Unable to update the highest offset in remote storage with offset {} since remote storage is not enabled. The existing highest offset is {}.", offset, highestOffsetInRemoteStorage());
        } else if (offset > highestOffsetInRemoteStorage()) {
            highestOffsetInRemoteStorage = offset;
        }
    }

    // Rebuild producer state until lastOffset. This method may be called from the recovery code path, and thus must be
    // free of all side effects, i.e. it must not update any log-specific state.
    private void rebuildProducerState(long lastOffset, ProducerStateManager producerStateManager) throws IOException {
        synchronized (lock) {
            localLog.checkIfMemoryMappedBufferClosed();
            UnifiedLog.rebuildProducerState(producerStateManager, localLog.segments(), logStartOffset, lastOffset, time(), false, logIdent);
        }
    }

    public boolean hasLateTransaction(long currentTimeMs) {
        return producerStateManager.hasLateTransaction(currentTimeMs);
    }

    public int producerIdCount() {
        return producerStateManager.producerIdCount();
    }

    public List<DescribeProducersResponseData.ProducerState> activeProducers() {
        synchronized (lock) {
            return producerStateManager.activeProducers().entrySet().stream().map(entry -> {
                long producerId = entry.getKey();
                ProducerStateEntry state = entry.getValue();
                return new DescribeProducersResponseData.ProducerState()
                        .setProducerId(producerId)
                        .setProducerEpoch(state.producerEpoch())
                        .setLastSequence(state.lastSeq())
                        .setLastTimestamp(state.lastTimestamp())
                        .setCoordinatorEpoch(state.coordinatorEpoch())
                        .setCurrentTxnStartOffset(state.currentTxnFirstOffset().orElse(-1L));
            }).toList();
        }
    }

    public Map<Long, Integer> activeProducersWithLastSequence() {
        synchronized (lock) {
            Map<Long, Integer> result = new HashMap<>();
            producerStateManager.activeProducers().forEach((producerId, producerIdEntry) ->
                    result.put(producerId, producerIdEntry.lastSeq())
            );
            return result;
        }
    }

    public Map<Long, LastRecord> lastRecordsOfActiveProducers() {
        synchronized (lock) {
            Map<Long, LastRecord> result = new HashMap<>();
            producerStateManager.activeProducers().forEach((producerId, producerIdEntry) -> {
                Optional<Long> lastDataOffset = (producerIdEntry.lastDataOffset() >= 0)
                    ? Optional.of(producerIdEntry.lastDataOffset())
                    : Optional.empty();
                LastRecord lastRecord = new LastRecord(
                        lastDataOffset.map(OptionalLong::of).orElseGet(OptionalLong::empty),
                        producerIdEntry.producerEpoch());
                result.put(producerId, lastRecord);
            });
            return result;
        }
    }

    /**
     * Maybe create and return the VerificationGuard for the given producer ID if the transaction is not yet ongoing.
     * Creation starts the verification process. Otherwise, return the sentinel VerificationGuard.
     */
    public VerificationGuard maybeStartTransactionVerification(long producerId, int sequence, short epoch, boolean supportsEpochBump) {
        synchronized (lock) {
            // Check if the producer epoch is lower than the stored one, and reject early if it is
            ProducerStateEntry entry = producerStateManager.activeProducers().get(producerId);
            if (entry != null && epoch < entry.producerEpoch()) {
                String message = "Epoch of producer " + producerId + " is " + epoch + ", " +
                        "which is smaller than the last seen epoch " + entry.producerEpoch();
                throw new InvalidProducerEpochException(message);
            }

            if (hasOngoingTransaction(producerId, epoch)) {
                return VerificationGuard.SENTINEL;
            } else {
                return maybeCreateVerificationGuard(producerId, sequence, epoch, supportsEpochBump);
            }
        }
    }

    /**
     * Maybe create the VerificationStateEntry for the given producer ID -- always return the VerificationGuard
     */
    private VerificationGuard maybeCreateVerificationGuard(long producerId, int sequence, short epoch, boolean supportsEpochBump) {
        synchronized (lock) {
            return producerStateManager.maybeCreateVerificationStateEntry(producerId, sequence, epoch, supportsEpochBump).verificationGuard();
        }
    }

    /**
     * If an VerificationStateEntry is present for the given producer ID, return its VerificationGuard, otherwise, return the
     * sentinel VerificationGuard.
     */
    // visible for testing
    public VerificationGuard verificationGuard(long producerId) {
        synchronized (lock) {
            VerificationStateEntry entry = producerStateManager.verificationStateEntry(producerId);
            return (entry != null)
                ? entry.verificationGuard()
                : VerificationGuard.SENTINEL;
        }
    }

    /**
     * Return true if the given producer ID has a transaction ongoing.
     * Note, if the incoming producer epoch is newer than the stored one, the transaction may have finished.
     */
    // visible for testing
    public boolean hasOngoingTransaction(long producerId, short producerEpoch) {
        synchronized (lock) {
            ProducerStateEntry entry = producerStateManager.activeProducers().get(producerId);
            return entry != null && entry.currentTxnFirstOffset().isPresent() && entry.producerEpoch() == producerEpoch;
        }
    }

    /**
     * The number of segments in the log.
     */
    public int numberOfSegments() {
        return localLog.segments().numberOfSegments();
    }

    /**
     * Close this log.
     * The memory mapped buffer for index files of this log will be left open until the log is deleted.
     */
    @Override
    public void close() {
        logger.debug("Closing log");
        synchronized (lock) {
            logOffsetsListener = LogOffsetsListener.NO_OP_OFFSETS_LISTENER;
            maybeFlushMetadataFile();
            localLog.checkIfMemoryMappedBufferClosed();
            producerExpireCheck.cancel(true);
            maybeHandleIOException(
                    () -> "Error while renaming dir for " + topicPartition() + " in dir " + dir().getParent(),
                    () -> {
                        // We take a snapshot at the last written offset to hopefully avoid the need to scan the log
                        // after restarting and to ensure that we cannot inadvertently hit the upgrade optimization
                        // (the clean shutdown file is written after the logs are all closed).
                        producerStateManager.takeSnapshot();
                        return null;
                    });
            localLog.close();
        }
    }

    /**
     * Rename the directory of the local log. If the log's directory is being renamed for async deletion due to a
     * StopReplica request, then the shouldReinitialize parameter should be set to false, otherwise it should be set to true.
     *
     * @param name The new name that this log's directory is being renamed to
     * @param shouldReinitialize Whether the log's metadata should be reinitialized after renaming
     * @throws KafkaStorageException if rename fails
     */
    public void renameDir(String name, boolean shouldReinitialize) {
        synchronized (lock) {
            maybeHandleIOException(
                    () -> "Error while renaming dir for " + topicPartition() + " in log dir " + dir().getParent(),
                    () -> {
                        // Flush partitionMetadata file before initializing again
                        maybeFlushMetadataFile();
                        if (localLog.renameDir(name)) {
                            producerStateManager.updateParentDir(dir());
                            if (shouldReinitialize) {
                                // re-initialize leader epoch cache so that LeaderEpochCheckpointFile.checkpoint can correctly reference
                                // the checkpoint file in renamed log directory
                                reinitializeLeaderEpochCache();
                                initializePartitionMetadata();
                            } else {
                                leaderEpochCache.clear();
                                partitionMetadataFile = Optional.empty();
                            }
                        }
                        return null;
                    });
        }
    }

    /**
     * Close file handlers used by this log but don't write to disk. This is called if the log directory is offline
     */
    public void closeHandlers() {
        logger.debug("Closing handlers");
        synchronized (lock) {
            localLog.closeHandlers();
        }
    }

    /**
     * Append this message set to the active segment of the local log, assigning offsets and Partition Leader Epochs
     *
     * @param records The records to append
     * @param leaderEpoch the epoch of the replica appending
     */
    public LogAppendInfo appendAsLeader(MemoryRecords records, int leaderEpoch) throws IOException {
        return appendAsLeader(records, leaderEpoch, AppendOrigin.CLIENT, RequestLocal.noCaching(), VerificationGuard.SENTINEL, TransactionVersion.TV_UNKNOWN);
    }

    /**
     * Append this message set to the active segment of the local log, assigning offsets and Partition Leader Epochs
     *
     * @param records The records to append
     * @param leaderEpoch the epoch of the replica appending
     * @param origin Declares the origin of the append which affects required validations
     */
    public LogAppendInfo appendAsLeader(MemoryRecords records, int leaderEpoch, AppendOrigin origin) throws IOException {
        return appendAsLeader(records, leaderEpoch, origin, RequestLocal.noCaching(), VerificationGuard.SENTINEL, TransactionVersion.TV_UNKNOWN);
    }

    /**
     * Append this message set to the active segment of the local log, assigning offsets and Partition Leader Epochs
     *
     * @param records The records to append
     * @param leaderEpoch the epoch of the replica appending
     * @param origin Declares the origin of the append which affects required validations
     * @param requestLocal request local instance
     * @param verificationGuard verification guard for transaction verification
     * @param transactionVersion the transaction version for the records (1 for TV1, 2 for TV2, etc.).
     *                           Defaults to TV_UNKNOWN (-1) to force explicit specification.
     *                           Used for epoch validation of transaction markers (KIP-1228).
     * @throws KafkaStorageException If the append fails due to an I/O error.
     * @return Information about the appended messages including the first and last offset.
     */
    public LogAppendInfo appendAsLeader(MemoryRecords records,
                                        int leaderEpoch,
                                        AppendOrigin origin,
                                        RequestLocal requestLocal,
                                        VerificationGuard verificationGuard,
                                        short transactionVersion) {
        boolean validateAndAssignOffsets = origin != AppendOrigin.RAFT_LEADER;
        return append(records, origin, validateAndAssignOffsets, leaderEpoch, Optional.of(requestLocal),
            verificationGuard, false, RecordBatch.CURRENT_MAGIC_VALUE, transactionVersion);
    }

    /**
     * Even though we always write to disk with record version v2 since Apache Kafka 4.0, older record versions may have
     * been persisted to disk before that. In order to test such scenarios, we need the ability to append with older
     * record versions. This method exists for that purpose and hence it should only be used from test code.
     *
     * @see UnifiedLog#appendAsLeader
     */
    public LogAppendInfo appendAsLeaderWithRecordVersion(MemoryRecords records, int leaderEpoch, RecordVersion recordVersion) {
        return append(records, AppendOrigin.CLIENT, true, leaderEpoch, Optional.of(RequestLocal.noCaching()),
                VerificationGuard.SENTINEL, false, recordVersion.value, TransactionVersion.TV_UNKNOWN);
    }

    /**
     * Append this message set to the active segment of the local log without assigning offsets or Partition Leader Epochs
     *
     * @param records The records to append
     * @param leaderEpoch the epoch of the replica appending
     * @throws KafkaStorageException If the append fails due to an I/O error.
     * @return Information about the appended messages including the first and last offset.
     */
    public LogAppendInfo appendAsFollower(MemoryRecords records, int leaderEpoch) {
        return append(records,
                      AppendOrigin.REPLICATION,
                      false,
                      leaderEpoch,
                      Optional.empty(),
                      VerificationGuard.SENTINEL,
                      true,
                      RecordBatch.CURRENT_MAGIC_VALUE,
                      TransactionVersion.TV_UNKNOWN);
    }

    /**
     * Append this message set to the active segment of the local log, rolling over to a fresh segment if necessary.
     *
     * <p>This method will generally be responsible for assigning offsets to the messages,
     * however if the assignOffsets=false flag is passed we will only check that the existing offsets are valid.
     *
     * @param records The log records to append
     * @param origin Declares the origin of the append which affects required validations
     * @param validateAndAssignOffsets Should the log assign offsets to this message set or blindly apply what it is given
     * @param leaderEpoch The partition's leader epoch which will be applied to messages when offsets are assigned on the leader
     * @param requestLocal The request local instance if validateAndAssignOffsets is true
     * @param verificationGuard verification guard for transaction verification
     * @param ignoreRecordSize true to skip validation of record size.
     * @param toMagic the record version magic value
     * @param transactionVersion the transaction version for the records (1 for TV1, 2 for TV2, etc.).
     *                           Defaults to TV_UNKNOWN (-1) to force explicit specification.
     *                           Used for epoch validation of transaction markers (KIP-1228).
     *
     * @throws KafkaStorageException If the append fails due to an I/O error.
     * @throws OffsetsOutOfOrderException If out of order offsets found in 'records'
     * @throws UnexpectedAppendOffsetException If the first or last offset in append is less than next offset
     * @return Information about the appended messages including the first and last offset.
     */
    private LogAppendInfo append(MemoryRecords records,
                                 AppendOrigin origin,
                                 boolean validateAndAssignOffsets,
                                 int leaderEpoch,
                                 Optional<RequestLocal> requestLocal,
                                 VerificationGuard verificationGuard,
                                 boolean ignoreRecordSize,
                                 byte toMagic,
                                 short transactionVersion) {
        // We want to ensure the partition metadata file is written to the log dir before any log data is written to disk.
        // This will ensure that any log data can be recovered with the correct topic ID in the case of failure.
        maybeFlushMetadataFile();

        LogAppendInfo appendInfo = analyzeAndValidateRecords(records, origin, ignoreRecordSize, !validateAndAssignOffsets, leaderEpoch);

        // return if we have no valid messages or if this is a duplicate of the last appended entry
        if (appendInfo.validBytes() <= 0) {
            return appendInfo;
        } else {
            // trim any invalid bytes or partial messages before appending it to the on-disk log
            final MemoryRecords trimmedRecords = trimInvalidBytes(records, appendInfo);
            // they are valid, insert them in the log
            synchronized (lock)  {
                return maybeHandleIOException(
                        () -> "Error while appending records to " + topicPartition() + " in dir " + dir().getParent(),
                        () -> {
                            MemoryRecords validRecords = trimmedRecords;
                            localLog.checkIfMemoryMappedBufferClosed();
                            if (validateAndAssignOffsets) {
                                // assign offsets to the message set
                                PrimitiveRef.LongRef offset = PrimitiveRef.ofLong(localLog.logEndOffset());
                                appendInfo.setFirstOffset(offset.value);
                                Compression targetCompression = BrokerCompressionType.targetCompression(config().compression, appendInfo.sourceCompression());
                                LogValidator validator = new LogValidator(validRecords,
                                        topicPartition(),
                                        time(),
                                        appendInfo.sourceCompression(),
                                        targetCompression,
                                        config().compact,
                                        toMagic,
                                        config().messageTimestampType,
                                        config().messageTimestampBeforeMaxMs,
                                        config().messageTimestampAfterMaxMs,
                                        leaderEpoch,
                                        origin
                                );
                                LogValidator.ValidationResult validateAndOffsetAssignResult = validator.validateMessagesAndAssignOffsets(offset,
                                        validatorMetricsRecorder,
                                        requestLocal.orElseThrow(() -> new IllegalArgumentException(
                                                "requestLocal should be defined if assignOffsets is true")).bufferSupplier());

                                validRecords = validateAndOffsetAssignResult.validatedRecords();
                                appendInfo.setMaxTimestamp(validateAndOffsetAssignResult.maxTimestampMs());
                                appendInfo.setLastOffset(offset.value - 1);
                                appendInfo.setRecordValidationStats(validateAndOffsetAssignResult.recordValidationStats());
                                if (config().messageTimestampType == TimestampType.LOG_APPEND_TIME) {
                                    appendInfo.setLogAppendTime(validateAndOffsetAssignResult.logAppendTimeMs());
                                }

                                // re-validate message sizes if there's a possibility that they have changed (due to re-compression or message
                                // format conversion)
                                if (!ignoreRecordSize && validateAndOffsetAssignResult.messageSizeMaybeChanged()) {
                                    validRecords.batches().forEach(batch -> {
                                        if (batch.sizeInBytes() > config().maxMessageSize()) {
                                            // we record the original message set size instead of the trimmed size
                                            // to be consistent with pre-compression bytesRejectedRate recording
                                            brokerTopicStats.topicStats(topicPartition().topic()).bytesRejectedRate().mark(records.sizeInBytes());
                                            brokerTopicStats.allTopicsStats().bytesRejectedRate().mark(records.sizeInBytes());
                                            throw new RecordTooLargeException("Message batch size is " + batch.sizeInBytes() + " bytes in append to" +
                                                    "partition " + topicPartition() + " which exceeds the maximum configured size of " + config().maxMessageSize() + ".");
                                        }
                                    });
                                }
                            } else {
                                // we are taking the offsets we are given
                                if (appendInfo.firstOrLastOffsetOfFirstBatch() < localLog.logEndOffset()) {
                                    // we may still be able to recover if the log is empty
                                    // one example: fetching from log start offset on the leader which is not batch aligned,
                                    // which may happen as a result of AdminClient#deleteRecords()
                                    boolean hasFirstOffset = appendInfo.firstOffset() != UnifiedLog.UNKNOWN_OFFSET;
                                    long firstOffset = hasFirstOffset ? appendInfo.firstOffset() : records.batches().iterator().next().baseOffset();

                                    String firstOrLast = hasFirstOffset ? "First offset" : "Last offset of the first batch";
                                    List<String> offsets = new ArrayList<>();
                                    for (Record record : records.records()) {
                                        offsets.add(String.valueOf(record.offset()));
                                        if (offsets.size() == 10) break;
                                    }
                                    throw new UnexpectedAppendOffsetException(
                                            "Unexpected offset in append to " + topicPartition() + ". " + firstOrLast + " " +
                                                    appendInfo.firstOrLastOffsetOfFirstBatch() + " is less than the next offset " + localLog.logEndOffset() + ". " +
                                                    "First 10 offsets in append: " + String.join(", ", offsets) + ", last offset in" +
                                                    " append: " + appendInfo.lastOffset() + ". Log start offset = " + logStartOffset,
                                            firstOffset, appendInfo.lastOffset());
                                }
                            }

                            // update the epoch cache with the epoch stamped onto the message by the leader
                            validRecords.batches().forEach(batch -> {
                                if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
                                    assignEpochStartOffset(batch.partitionLeaderEpoch(), batch.baseOffset());
                                } else {
                                    // In partial upgrade scenarios, we may get a temporary regression to the message format. In
                                    // order to ensure the safety of leader election, we clear the epoch cache so that we revert
                                    // to truncation by high watermark after the next leader election.
                                    if (leaderEpochCache.nonEmpty()) {
                                        logger.warn("Clearing leader epoch cache after unexpected append with message format v{}", batch.magic());
                                        leaderEpochCache.clearAndFlush();
                                    }
                                }
                            });

                            // check messages size does not exceed config.segmentSize
                            if (validRecords.sizeInBytes() > config().segmentSize()) {
                                throw new RecordBatchTooLargeException("Message batch size is " + validRecords.sizeInBytes() + " bytes in append " +
                                        "to partition " + topicPartition() + ", which exceeds the maximum configured segment size of " + config().segmentSize() + ".");
                            }

                            // maybe roll the log if this segment is full
                            LogSegment segment = maybeRoll(validRecords.sizeInBytes(), appendInfo);

                            LogOffsetMetadata logOffsetMetadata = new LogOffsetMetadata(
                                    appendInfo.firstOrLastOffsetOfFirstBatch(),
                                    segment.baseOffset(),
                                    segment.size());

                            // now that we have valid records, offsets assigned, and timestamps updated, we need to
                            // validate the idempotent/transactional state of the producers and collect some metadata
                            AnalyzeAndValidateProducerStateResult result = analyzeAndValidateProducerState(
                                logOffsetMetadata, validRecords, origin, verificationGuard, transactionVersion
                            );

                            if (result.maybeDuplicate.isPresent()) {
                                BatchMetadata duplicate = result.maybeDuplicate.get();
                                appendInfo.setFirstOffset(duplicate.firstOffset());
                                appendInfo.setLastOffset(duplicate.lastOffset());
                                appendInfo.setLogAppendTime(duplicate.timestamp());
                                appendInfo.setLogStartOffset(logStartOffset);
                                logger.trace("Duplicate batch detected, returning AppendInfo from duplicate batch with last offset: {}, first offset: {}, next offset: {}, skipped messages: {}",
                                        appendInfo.lastOffset(), appendInfo.firstOffset(), localLog.logEndOffset(), validRecords);
                            } else {
                                // Append the records, and increment the local log end offset immediately after the append because a
                                // write to the transaction index below may fail, and we want to ensure that the offsets
                                // of future appends still grow monotonically. The resulting transaction index inconsistency
                                // will be cleaned up after the log directory is recovered. Note that the end offset of the
                                // ProducerStateManager will not be updated and the last stable offset will not advance
                                // if the append to the transaction index fails.
                                localLog.append(appendInfo.lastOffset(), validRecords);
                                updateHighWatermarkWithLogEndOffset();

                                // update the producer state
                                result.updatedProducers.values().forEach(producerStateManager::update);

                                // update the transaction index with the true last stable offset. The last offset visible
                                // to consumers using READ_COMMITTED will be limited by this value and the high watermark.
                                for (CompletedTxn completedTxn : result.completedTxns) {
                                    long lastStableOffset = producerStateManager.lastStableOffset(completedTxn);
                                    segment.updateTxnIndex(completedTxn, lastStableOffset);
                                    producerStateManager.completeTxn(completedTxn);
                                }

                                // always update the last producer id map offset so that the snapshot reflects the current offset
                                // even if there isn't any idempotent data being written
                                producerStateManager.updateMapEndOffset(appendInfo.lastOffset() + 1);

                                // update the first unstable offset (which is used to compute LSO)
                                maybeIncrementFirstUnstableOffset();

                                logger.trace("Appended message set with last offset: {}, first offset: {}, next offset: {}, and messages: {}",
                                        appendInfo.lastOffset(), appendInfo.firstOffset(), localLog.logEndOffset(), validRecords);

                                if (localLog.unflushedMessages() >= config().flushInterval) flush(false);
                            }
                            return appendInfo;
                        });
            }
        }
    }

    public void assignEpochStartOffset(int leaderEpoch, long startOffset) {
        leaderEpochCache.assign(leaderEpoch, startOffset);
    }

    public Optional<Integer> latestEpoch() {
        return leaderEpochCache.latestEpoch();
    }

    public Optional<OffsetAndEpoch> endOffsetForEpoch(int leaderEpoch) {
        Map.Entry<Integer, Long> entry = leaderEpochCache.endOffsetFor(leaderEpoch, logEndOffset());
        int foundEpoch = entry.getKey();
        long foundOffset = entry.getValue();
        if (foundOffset == UNDEFINED_EPOCH_OFFSET) {
            return Optional.empty();
        } else {
            return Optional.of(new OffsetAndEpoch(foundOffset, foundEpoch));
        }
    }

    private void maybeIncrementFirstUnstableOffset() throws IOException {
        synchronized (lock) {
            localLog.checkIfMemoryMappedBufferClosed();

            Optional<LogOffsetMetadata> updatedFirstUnstableOffset = producerStateManager.firstUnstableOffset();
            if (updatedFirstUnstableOffset.isPresent() &&
                    (updatedFirstUnstableOffset.get().messageOffsetOnly() || updatedFirstUnstableOffset.get().messageOffset < logStartOffset)) {
                long offset = Math.max(updatedFirstUnstableOffset.get().messageOffset, logStartOffset);
                updatedFirstUnstableOffset = Optional.of(maybeConvertToOffsetMetadata(offset));
            }

            if (updatedFirstUnstableOffset != this.firstUnstableOffsetMetadata) {
                logger.debug("First unstable offset updated to {}", updatedFirstUnstableOffset);
                this.firstUnstableOffsetMetadata = updatedFirstUnstableOffset;
            }
        }
    }

    public void maybeIncrementLocalLogStartOffset(long newLocalLogStartOffset, LogStartOffsetIncrementReason reason) {
        synchronized (lock) {
            if (newLocalLogStartOffset > localLogStartOffset()) {
                localLogStartOffset = newLocalLogStartOffset;
                logger.info("Incremented local log start offset to {} due to reason {}", localLogStartOffset(), reason);
            }
        }
    }

    /**
     * Increment the log start offset if the provided offset is larger.
     *
     * <p>If the log start offset changed, then this method also update a few key offset such that
     * `logStartOffset <= logStableOffset <= highWatermark`. The leader epoch cache is also updated
     * such that all the offsets referenced in that component point to valid offset in this log.
     *
     * @throws OffsetOutOfRangeException if the log start offset is greater than the high watermark
     * @return true if the log start offset was updated; otherwise false
     */
    public boolean maybeIncrementLogStartOffset(long newLogStartOffset, LogStartOffsetIncrementReason reason) {
        // We don't have to write the log start offset to log-start-offset-checkpoint immediately.
        // The deleteRecordsOffset may be lost only if all in-sync replicas of this broker are shutdown
        // in an unclean manner within log.flush.start.offset.checkpoint.interval.ms. The chance of this happening is low.
        return maybeHandleIOException(
                () -> "Exception while increasing log start offset for " + topicPartition() + " to " + newLogStartOffset + " in dir " + dir().getParent(),
                () -> {
                    synchronized (lock)  {
                        if (newLogStartOffset > highWatermark()) {
                            throw new OffsetOutOfRangeException("Cannot increment the log start offset to " + newLogStartOffset + " of partition " + topicPartition() +
                                    " since it is larger than the high watermark " + highWatermark());
                        }

                        if (remoteLogEnabled()) {
                            // This should be set log-start-offset is set more than the current local-log-start-offset
                            localLogStartOffset = Math.max(newLogStartOffset, localLogStartOffset());
                        }

                        localLog.checkIfMemoryMappedBufferClosed();
                        if (newLogStartOffset > logStartOffset) {
                            updateLogStartOffset(newLogStartOffset);
                            logger.info("Incremented log start offset to {} due to {}", newLogStartOffset, reason);
                            leaderEpochCache.truncateFromStartAsyncFlush(logStartOffset);
                            producerStateManager.onLogStartOffsetIncremented(newLogStartOffset);
                            maybeIncrementFirstUnstableOffset();
                            return true;
                        }
                    }
                    return false;
            });
    }

    private record AnalyzeAndValidateProducerStateResult(
            Map<Long, ProducerAppendInfo> updatedProducers,
            List<CompletedTxn> completedTxns,
            Optional<BatchMetadata> maybeDuplicate) {
    }

    private AnalyzeAndValidateProducerStateResult analyzeAndValidateProducerState(LogOffsetMetadata appendOffsetMetadata,
                                                                                  MemoryRecords records,
                                                                                  AppendOrigin origin,
                                                                                  VerificationGuard requestVerificationGuard,
                                                                                  short transactionVersion) {
        Map<Long, ProducerAppendInfo> updatedProducers = new HashMap<>();
        List<CompletedTxn> completedTxns = new ArrayList<>();
        int relativePositionInSegment = appendOffsetMetadata.relativePositionInSegment;

        for (MutableRecordBatch batch : records.batches()) {
            if (batch.hasProducerId()) {
                // if this is a client produce request, there will be up to 5 batches which could have been duplicated.
                // If we find a duplicate, we return the metadata of the appended batch to the client.
                if (origin == AppendOrigin.CLIENT) {
                    Optional<ProducerStateEntry> maybeLastEntry = producerStateManager.lastEntry(batch.producerId());

                    Optional<BatchMetadata> duplicateBatch = maybeLastEntry.flatMap(e -> e.findDuplicateBatch(batch));
                    if (duplicateBatch.isPresent()) {
                        return new AnalyzeAndValidateProducerStateResult(updatedProducers, completedTxns, duplicateBatch);
                    }
                }

                if (origin == AppendOrigin.CLIENT || origin == AppendOrigin.COORDINATOR) {
                    // Verify that if the record is transactional & the append origin is client/coordinator, that we either have an ongoing transaction or verified transaction state.
                    // This guarantees that transactional records are never written to the log outside of the transaction coordinator's knowledge of an open transaction on
                    // the partition. If we do not have an ongoing transaction or correct guard, return an error and do not append.
                    // There are two phases -- the first append to the log and subsequent appends.
                    //
                    // 1. First append: Verification starts with creating a VerificationGuard, sending a verification request to the transaction coordinator, and
                    // given a "verified" response, continuing the append path. (A non-verified response throws an error.) We create the unique VerificationGuard for the transaction
                    // to ensure there is no race between the transaction coordinator response and an abort marker getting written to the log. We need a unique guard because we could
                    // have a sequence of events where we start a transaction verification, have the transaction coordinator send a verified response, write an abort marker,
                    // start a new transaction not aware of the partition, and receive the stale verification (ABA problem). With a unique VerificationGuard, this sequence would not
                    // result in appending to the log and would return an error. The guard is removed after the first append to the transaction and from then, we can rely on phase 2.
                    //
                    // 2. Subsequent appends: Once we write to the transaction, the in-memory state currentTxnFirstOffset is populated. This field remains until the
                    // transaction is completed or aborted. We can guarantee the transaction coordinator knows about the transaction given step 1 and that the transaction is still
                    // ongoing. If the transaction is expected to be ongoing, we will not set a VerificationGuard. If the transaction is aborted, hasOngoingTransaction is false and
                    // requestVerificationGuard is the sentinel, so we will throw an error. A subsequent produce request (retry) should create verification state and return to phase 1.
                    if (batch.isTransactional() && !hasOngoingTransaction(batch.producerId(), batch.producerEpoch())) {
                        // Check epoch first: if producer epoch is stale, throw recoverable InvalidProducerEpochException.
                        ProducerStateEntry entry = producerStateManager.activeProducers().get(batch.producerId());
                        if (entry != null && batch.producerEpoch() < entry.producerEpoch()) {
                            String message = "Epoch of producer " + batch.producerId() + " is " + batch.producerEpoch() + 
                                ", which is smaller than the last seen epoch " + entry.producerEpoch();
                            throw new InvalidProducerEpochException(message);
                        }
                        
                        // Only check verification if epoch is current
                        if (batchMissingRequiredVerification(batch, requestVerificationGuard)) {
                            throw new InvalidTxnStateException("Record was not part of an ongoing transaction");
                        }
                    }
                }

                // We cache offset metadata for the start of each transaction. This allows us to
                // compute the last stable offset without relying on additional index lookups.
                Optional<LogOffsetMetadata> firstOffsetMetadata = batch.isTransactional()
                    ? Optional.of(new LogOffsetMetadata(batch.baseOffset(), appendOffsetMetadata.segmentBaseOffset, relativePositionInSegment))
                    : Optional.empty();

                Optional<CompletedTxn> maybeCompletedTxn = UnifiedLog.updateProducers(
                    producerStateManager,
                    batch, updatedProducers,
                    firstOffsetMetadata,
                    origin,
                    transactionVersion
                );
                maybeCompletedTxn.ifPresent(completedTxns::add);
            }

            relativePositionInSegment += batch.sizeInBytes();
        }
        return new AnalyzeAndValidateProducerStateResult(updatedProducers, completedTxns, Optional.empty());
    }

    private boolean batchMissingRequiredVerification(MutableRecordBatch batch, VerificationGuard requestVerificationGuard) {
        return producerStateManager.producerStateManagerConfig().transactionVerificationEnabled()
                && !batch.isControlBatch()
                && !verificationGuard(batch.producerId()).verify(requestVerificationGuard);
    }

    /**
     * Validate the following:
     * <ol>
     * <li> each message matches its CRC
     * <li> each message size is valid (if ignoreRecordSize is false)
     * <li> that the sequence numbers of the incoming record batches are consistent with the existing state and with each other
     * <li> that the offsets are monotonically increasing (if requireOffsetsMonotonic is true)
     * </ol>
     *
     * Also compute the following quantities:
     * <ol>
     * <li> First offset in the message set
     * <li> Last offset in the message set
     * <li> Number of messages
     * <li> Number of valid bytes
     * <li> Whether the offsets are monotonically increasing
     * <li> Whether any compression codec is used (if many are used, then the last one is given)
     * </ol>
     */
    private LogAppendInfo analyzeAndValidateRecords(MemoryRecords records,
                                                    AppendOrigin origin,
                                                    boolean ignoreRecordSize,
                                                    boolean requireOffsetsMonotonic,
                                                    int leaderEpoch) {
        int validBytesCount = 0;
        long firstOffset = UnifiedLog.UNKNOWN_OFFSET;
        long lastOffset = -1L;
        int lastLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH;
        CompressionType sourceCompression = CompressionType.NONE;
        boolean monotonic = true;
        long maxTimestamp = RecordBatch.NO_TIMESTAMP;
        boolean readFirstMessage = false;
        long lastOffsetOfFirstBatch = -1L;
        boolean skipRemainingBatches = false;

        for (MutableRecordBatch batch : records.batches()) {
            if (origin == AppendOrigin.RAFT_LEADER && batch.partitionLeaderEpoch() != leaderEpoch) {
                throw new InvalidRecordException("Append from Raft leader did not set the batch epoch correctly, expected " + leaderEpoch +
                        " but the batch has " + batch.partitionLeaderEpoch());
            }
            // we only validate V2 and higher to avoid potential compatibility issues with older clients
            if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2 && origin == AppendOrigin.CLIENT && batch.baseOffset() != 0) {
                throw new InvalidRecordException("The baseOffset of the record batch in the append to " + topicPartition() + " should " +
                        "be 0, but it is " + batch.baseOffset());
            }

            /* During replication of uncommitted data it is possible for the remote replica to send record batches after it lost
             * leadership. This can happen if sending FETCH responses is slow. There is a race between sending the FETCH
             * response and the replica truncating and appending to the log. The replicating replica resolves this issue by only
             * persisting up to the current leader epoch used in the fetch request. See KAFKA-18723 for more details.
             */
            skipRemainingBatches = skipRemainingBatches || hasHigherPartitionLeaderEpoch(batch, origin, leaderEpoch);
            if (skipRemainingBatches) {
                logger.info("Skipping batch {} from an origin of {} because its partition leader epoch {} is higher than the replica's current leader epoch {}",
                        batch, origin, batch.partitionLeaderEpoch(), leaderEpoch);
            } else {
                // update the first offset if on the first message. For magic versions older than 2, we use the last offset
                // to avoid the need to decompress the data (the last offset can be obtained directly from the wrapper message).
                // For magic version 2, we can get the first offset directly from the batch header.
                // When appending to the leader, we will update LogAppendInfo.baseOffset with the correct value. In the follower
                // case, validation will be more lenient.
                // Also indicate whether we have the accurate first offset or not
                if (!readFirstMessage) {
                    if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
                        firstOffset = batch.baseOffset();
                    }
                    lastOffsetOfFirstBatch = batch.lastOffset();
                    readFirstMessage = true;
                }

                // check that offsets are monotonically increasing
                if (lastOffset >= batch.lastOffset()) {
                    monotonic = false;
                }

                // update the last offset seen
                lastOffset = batch.lastOffset();
                lastLeaderEpoch = batch.partitionLeaderEpoch();

                // Check if the message sizes are valid.
                int batchSize = batch.sizeInBytes();
                if (!ignoreRecordSize && batchSize > config().maxMessageSize()) {
                    brokerTopicStats.topicStats(topicPartition().topic()).bytesRejectedRate().mark(records.sizeInBytes());
                    brokerTopicStats.allTopicsStats().bytesRejectedRate().mark(records.sizeInBytes());
                    throw new RecordTooLargeException("The record batch size in the append to " + topicPartition() + " is " + batchSize + " bytes " +
                            "which exceeds the maximum configured value of " + config().maxMessageSize() + ").");
                }

                // check the validity of the message by checking CRC
                if (!batch.isValid()) {
                    brokerTopicStats.allTopicsStats().invalidMessageCrcRecordsPerSec().mark();
                    throw new CorruptRecordException("Record is corrupt (stored crc = " + batch.checksum() + ") in topic partition " + topicPartition() + ".");
                }

                if (batch.maxTimestamp() > maxTimestamp) {
                    maxTimestamp = batch.maxTimestamp();
                }

                validBytesCount += batchSize;

                CompressionType batchCompression = CompressionType.forId(batch.compressionType().id);
                // sourceCompression is only used on the leader path, which only contains one batch if version is v2 or messages are compressed
                if (batchCompression != CompressionType.NONE) {
                    sourceCompression = batchCompression;
                }
            }

            if (requireOffsetsMonotonic && !monotonic) {
                throw new OffsetsOutOfOrderException("Out of order offsets found in append to " + topicPartition() + ": " +
                        StreamSupport.stream(records.records().spliterator(), false)
                            .map(Record::offset)
                            .map(String::valueOf)
                            .collect(Collectors.joining(",")));
            }
        }
        Optional<Integer> lastLeaderEpochOpt = (lastLeaderEpoch != RecordBatch.NO_PARTITION_LEADER_EPOCH)
                ? Optional.of(lastLeaderEpoch)
                : Optional.empty();

        return new LogAppendInfo(firstOffset, lastOffset, lastLeaderEpochOpt, maxTimestamp,
                RecordBatch.NO_TIMESTAMP, logStartOffset, RecordValidationStats.EMPTY, sourceCompression,
                validBytesCount, lastOffsetOfFirstBatch, Collections.emptyList(), LeaderHwChange.NONE);
    }

    /**
     * Return true if the record batch has a higher leader epoch than the specified leader epoch
     *
     * @param batch the batch to validate
     * @param origin the reason for appending the record batch
     * @param leaderEpoch the epoch to compare
     * @return true if the append reason is replication and the batch's partition leader epoch is
     *         greater than the specified leaderEpoch, otherwise false
     */
    private boolean hasHigherPartitionLeaderEpoch(RecordBatch batch, AppendOrigin origin, int leaderEpoch) {
        return origin == AppendOrigin.REPLICATION
                && batch.partitionLeaderEpoch() != RecordBatch.NO_PARTITION_LEADER_EPOCH
                && batch.partitionLeaderEpoch() > leaderEpoch;
    }

    /**
     * Trim any invalid bytes from the end of this message set (if there are any)
     *
     * @param records The records to trim
     * @param info The general information of the message set
     * @return A trimmed message set. This may be the same as what was passed in, or it may not.
     */
    private MemoryRecords trimInvalidBytes(MemoryRecords records, LogAppendInfo info) {
        int validBytes = info.validBytes();
        if (validBytes < 0) {
            throw new CorruptRecordException("Cannot append record batch with illegal length " + validBytes + " to " +
                    "log for " + topicPartition() + ". A possible cause is a corrupted produce request.");
        }
        if (validBytes == records.sizeInBytes()) {
            return records;
        } else {
            // trim invalid bytes
            ByteBuffer validByteBuffer = records.buffer().duplicate();
            validByteBuffer.limit(validBytes);
            return MemoryRecords.readableRecords(validByteBuffer);
        }
    }

    private void checkLogStartOffset(long offset) {
        if (offset < logStartOffset) {
            throw new OffsetOutOfRangeException("Received request for offset " + offset + " for partition " + topicPartition() + ", " +
                    "but we only have log segments starting from offset: " + logStartOffset + ".");
        }
    }

    /**
     * Read messages from the log.
     *
     * @param startOffset The offset to begin reading at
     * @param maxLength The maximum number of bytes to read
     * @param isolation The fetch isolation, which controls the maximum offset we are allowed to read
     * @param minOneMessage If this is true, the first message will be returned even if it exceeds `maxLength` (if one exists)
     * @throws OffsetOutOfRangeException If startOffset is beyond the log end offset or before the log start offset
     * @return The fetch data information including fetch starting offset metadata and messages read.
     */
    public FetchDataInfo read(long startOffset,
                              int maxLength,
                              FetchIsolation isolation,
                              boolean minOneMessage) throws IOException {
        checkLogStartOffset(startOffset);
        LogOffsetMetadata maxOffsetMetadata = switch (isolation) {
            case LOG_END -> localLog.logEndOffsetMetadata();
            case HIGH_WATERMARK -> fetchHighWatermarkMetadata();
            case TXN_COMMITTED -> fetchLastStableOffsetMetadata();
        };
        return localLog.read(startOffset, maxLength, minOneMessage, maxOffsetMetadata, isolation == FetchIsolation.TXN_COMMITTED);
    }

    public List<AbortedTxn> collectAbortedTransactions(long startOffset, long upperBoundOffset) {
        return localLog.collectAbortedTransactions(logStartOffset, startOffset, upperBoundOffset);
    }

    /**
     * Get an offset based on the given timestamp
     * The offset returned is the offset of the first message whose timestamp is greater than or equals to the
     * given timestamp. If no such message is found, the log end offset is returned.
     *
     * @param targetTimestamp The given timestamp for offset fetching.
     * @param remoteOffsetReader Optional AsyncOffsetReader instance if it exists.
     * @return the offset-result holder
     *         <ul>
     *           <li>When the partition is not enabled with remote storage, then it contains offset of the first message
     *           whose timestamp is greater than or equals to the given timestamp; None if no such message is found.
     *           <li>When the partition is enabled with remote storage, then it contains the job/task future and gets
     *           completed in the async fashion.
     *           <li>All special timestamp offset results are returned immediately irrespective of the remote storage.
     *         </ul>
     */
    public OffsetResultHolder fetchOffsetByTimestamp(long targetTimestamp, Optional<AsyncOffsetReader> remoteOffsetReader) {
        return maybeHandleIOException(
                () -> "Error while fetching offset by timestamp for " + topicPartition() + " in dir " + dir().getParent(),
                () -> {
                    logger.debug("Searching offset for timestamp {}.", targetTimestamp);

                    // For the earliest and latest, we do not need to return the timestamp.
                    if (targetTimestamp == ListOffsetsRequest.EARLIEST_TIMESTAMP ||
                            (!remoteLogEnabled() && targetTimestamp == ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP)) {
                        // The first cached epoch usually corresponds to the log start offset, but we have to verify this since
                        // it may not be true following a message format version bump as the epoch will not be available for
                        // log entries written in the older format.
                        Optional<EpochEntry> earliestEpochEntry = leaderEpochCache.earliestEntry();
                        Optional<Integer> epochOpt = (earliestEpochEntry.isPresent() && earliestEpochEntry.get().startOffset() <= logStartOffset)
                            ? Optional.of(earliestEpochEntry.get().epoch())
                            : Optional.empty();

                        return new OffsetResultHolder(new FileRecords.TimestampAndOffset(RecordBatch.NO_TIMESTAMP, logStartOffset, epochOpt));
                    } else if (targetTimestamp == ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP) {
                        long curLocalLogStartOffset = localLogStartOffset();

                        OptionalInt epochForOffset = leaderEpochCache.epochForOffset(curLocalLogStartOffset);
                        Optional<Integer> epochResult = epochForOffset.isPresent()
                            ? Optional.of(epochForOffset.getAsInt())
                            : Optional.empty();

                        return new OffsetResultHolder(new FileRecords.TimestampAndOffset(RecordBatch.NO_TIMESTAMP, curLocalLogStartOffset, epochResult));
                    } else if (targetTimestamp == ListOffsetsRequest.LATEST_TIMESTAMP) {
                        return new OffsetResultHolder(new FileRecords.TimestampAndOffset(RecordBatch.NO_TIMESTAMP, logEndOffset(), leaderEpochCache.latestEpoch()));
                    } else if (targetTimestamp == ListOffsetsRequest.LATEST_TIERED_TIMESTAMP) {
                        if (remoteLogEnabled()) {
                            long curHighestRemoteOffset = highestOffsetInRemoteStorage();
                            OptionalInt epochOpt = leaderEpochCache.epochForOffset(curHighestRemoteOffset);
                            Optional<Integer> epochResult = epochOpt.isPresent()
                                ? Optional.of(epochOpt.getAsInt())
                                : curHighestRemoteOffset == -1
                                    ? Optional.of(RecordBatch.NO_PARTITION_LEADER_EPOCH)
                                    : Optional.empty();
                            return new OffsetResultHolder(new FileRecords.TimestampAndOffset(RecordBatch.NO_TIMESTAMP, curHighestRemoteOffset, epochResult));
                        } else {
                            return new OffsetResultHolder(new FileRecords.TimestampAndOffset(RecordBatch.NO_TIMESTAMP, -1L, Optional.of(-1)));
                        }
                    } else if (targetTimestamp == ListOffsetsRequest.EARLIEST_PENDING_UPLOAD_TIMESTAMP) {
                        return fetchEarliestPendingUploadOffset(remoteOffsetReader);
                    } else if (targetTimestamp == ListOffsetsRequest.MAX_TIMESTAMP) {
                        // Cache to avoid race conditions.
                        List<LogSegment> segments = logSegments();
                        LogSegment latestTimestampSegment = null;
                        for (LogSegment segment : segments) {
                            if (latestTimestampSegment == null) {
                                latestTimestampSegment = segment;
                            } else if (segment.maxTimestampSoFar() > latestTimestampSegment.maxTimestampSoFar()) {
                                latestTimestampSegment = segment;
                            }
                        }
                        // cache the timestamp and offset
                        TimestampOffset maxTimestampSoFar = latestTimestampSegment.readMaxTimestampAndOffsetSoFar();
                        // lookup the position of batch to avoid extra I/O
                        OffsetPosition position = latestTimestampSegment.offsetIndex().lookup(maxTimestampSoFar.offset());
                        Optional<FileRecords.TimestampAndOffset> timestampAndOffsetOpt = findFirst(
                                latestTimestampSegment.log().batchesFrom(position.position()),
                                item -> item.maxTimestamp() == maxTimestampSoFar.timestamp())
                                    .flatMap(batch -> batch.offsetOfMaxTimestamp()
                                        .map(offset -> new FileRecords.TimestampAndOffset(
                                            batch.maxTimestamp(),
                                            offset,
                                            Optional.of(batch.partitionLeaderEpoch()).filter(epoch -> epoch >= 0))));
                        return new OffsetResultHolder(timestampAndOffsetOpt);
                    } else {
                        // We need to search the first segment whose largest timestamp is >= the target timestamp if there is one.
                        if (remoteLogEnabled() && !isEmpty()) {
                            if (remoteOffsetReader.isEmpty()) {
                                throw new KafkaException("RemoteLogManager is empty even though the remote log storage is enabled.");
                            }

                            AsyncOffsetReadFutureHolder<OffsetResultHolder.FileRecordsOrError> asyncOffsetReadFutureHolder =
                                    remoteOffsetReader.get().asyncOffsetRead(topicPartition(), targetTimestamp,
                                    logStartOffset, leaderEpochCache, () -> searchOffsetInLocalLog(targetTimestamp, localLogStartOffset()));
                            return new OffsetResultHolder(Optional.empty(), Optional.of(asyncOffsetReadFutureHolder));
                        } else {
                            return new OffsetResultHolder(searchOffsetInLocalLog(targetTimestamp, logStartOffset));
                        }
                    }
                });
    }

    private OffsetResultHolder fetchEarliestPendingUploadOffset(Optional<AsyncOffsetReader> remoteOffsetReader) {
        if (remoteLogEnabled()) {
            long curHighestRemoteOffset = highestOffsetInRemoteStorage();

            if (curHighestRemoteOffset == -1L) {
                if (localLogStartOffset() == logStartOffset()) {
                    // No segments have been uploaded yet
                    return fetchOffsetByTimestamp(ListOffsetsRequest.EARLIEST_TIMESTAMP, remoteOffsetReader);
                } else {
                    // Leader currently does not know about the already uploaded segments
                    return new OffsetResultHolder(Optional.of(new FileRecords.TimestampAndOffset(RecordBatch.NO_TIMESTAMP, -1L, Optional.of(-1))));
                }
            } else {
                long earliestPendingUploadOffset = Math.max(curHighestRemoteOffset + 1, logStartOffset());
                OptionalInt epochForOffset = leaderEpochCache.epochForOffset(earliestPendingUploadOffset);
                Optional<Integer> epochResult = epochForOffset.isPresent()
                    ? Optional.of(epochForOffset.getAsInt())
                    : Optional.empty();
                return new OffsetResultHolder(new FileRecords.TimestampAndOffset(RecordBatch.NO_TIMESTAMP, earliestPendingUploadOffset, epochResult));
            }
        } else {
            return new OffsetResultHolder(new FileRecords.TimestampAndOffset(RecordBatch.NO_TIMESTAMP, -1L, Optional.of(-1)));
        }
    }

    /**
     * Checks if the log is empty.
     * @return Returns True when the log is empty. Otherwise, false.
     */
    public boolean isEmpty() {
        return logStartOffset == logEndOffset();
    }

    private Optional<FileRecords.TimestampAndOffset> searchOffsetInLocalLog(long targetTimestamp, long startOffset) throws IOException {
        // Cache to avoid race conditions.
        List<LogSegment> segments = logSegments();
        for (LogSegment segment : segments) {
            if (segment.largestTimestamp() >= targetTimestamp) {
                return segment.findOffsetByTimestamp(targetTimestamp, startOffset);
            }
        }
        return Optional.empty();
    }

    /**
     * Given a message offset, find its corresponding offset metadata in the log.
     * 1. If the message offset is less than the log-start-offset (or) local-log-start-offset, then it returns the
     *     message-only metadata.
     * 2. If the message offset is beyond the log-end-offset, then it returns the message-only metadata.
     * 3. For all other cases, it returns the offset metadata from the log.
     */
    public LogOffsetMetadata maybeConvertToOffsetMetadata(long offset) throws IOException {
        try {
            return localLog.convertToOffsetMetadataOrThrow(offset);
        } catch (OffsetOutOfRangeException ignore) {
            return new LogOffsetMetadata(offset);
        }
    }

    /**
     * Delete any local log segments starting with the oldest segment and moving forward until
     * the user-supplied predicate is false or the segment containing the current high watermark is reached.
     * We do not delete segments with offsets at or beyond the high watermark to ensure that the log start
     * offset can never exceed it. If the high watermark has not yet been initialized, no segments are eligible
     * for deletion.
     *
     * @param predicate A function that takes in a candidate log segment and the next higher segment
     *                  (if there is one) and returns true iff it is deletable
     * @param reason The reason for the segment deletion
     * @return The number of segments deleted
     */
    private int deleteOldSegments(DeletionCondition predicate, SegmentDeletionReason reason) throws IOException {
        synchronized (lock)  {
            List<LogSegment> deletable = deletableSegments(predicate);
            if (!deletable.isEmpty()) {
                return deleteSegments(deletable, reason);
            } else {
                return 0;
            }
        }
    }

    /**
     * @return true if this topic enables tiered storage and remote log copy is enabled (i.e. remote.log.copy.disable=false)
     */
    private boolean remoteLogEnabledAndRemoteCopyEnabled() {
        return remoteLogEnabled() && !config().remoteLogCopyDisable();
    }

    private boolean isSegmentEligibleForDeletion(Optional<LogSegment> nextSegmentOpt, long upperBoundOffset) {
        boolean allowDeletionDueToLogStartOffsetIncremented = nextSegmentOpt.isPresent() && logStartOffset >= nextSegmentOpt.get().baseOffset();
        // Segments are eligible for deletion when:
        //    1. they are uploaded to the remote storage
        //    2. log-start-offset was incremented higher than the largest offset in the candidate segment
        // Note: when remote log copy is disabled, we will fall back to local log check using retention.ms/bytes
        if (remoteLogEnabledAndRemoteCopyEnabled()) {
            return (upperBoundOffset > 0 && upperBoundOffset - 1 <= highestOffsetInRemoteStorage()) ||
                    allowDeletionDueToLogStartOffsetIncremented;
        } else {
            return true;
        }
    }

    /**
     * Find segments starting from the oldest until the user-supplied predicate is false.
     * A final segment that is empty will never be returned.
     *
     * @param predicate A function that takes in a candidate log segment, the next higher segment
     *                  (if there is one). It returns true iff the segment is deletable.
     * @return the segments ready to be deleted
     */
    public List<LogSegment> deletableSegments(DeletionCondition predicate) throws IOException {
        if (localLog.segments().isEmpty()) {
            return List.of();
        } else {
            List<LogSegment> deletable = new ArrayList<>();
            Iterator<LogSegment> segmentsIterator = localLog.segments().values().iterator();
            Optional<LogSegment> segmentOpt = nextOption(segmentsIterator);
            boolean shouldRoll = false;
            while (segmentOpt.isPresent()) {
                LogSegment segment = segmentOpt.get();
                Optional<LogSegment> nextSegmentOpt = nextOption(segmentsIterator);
                boolean isLastSegmentAndEmpty = nextSegmentOpt.isEmpty() && segment.size() == 0;
                long upperBoundOffset = nextSegmentOpt.map(LogSegment::baseOffset).orElseGet(this::logEndOffset);
                // We don't delete segments with offsets at or beyond the high watermark to ensure that the log start
                // offset can never exceed it.
                boolean predicateResult = highWatermark() >= upperBoundOffset && predicate.execute(segment, nextSegmentOpt);

                // Roll the active segment when it breaches the configured retention policy. The rolled segment will be
                // eligible for deletion and gets removed in the next iteration.
                if (predicateResult && remoteLogEnabled() && nextSegmentOpt.isEmpty() && segment.size() > 0) {
                    shouldRoll = true;
                }
                if (predicateResult && !isLastSegmentAndEmpty && isSegmentEligibleForDeletion(nextSegmentOpt, upperBoundOffset)) {
                    deletable.add(segment);
                    segmentOpt = nextSegmentOpt;
                } else {
                    segmentOpt = Optional.empty();
                }
            }
            if (shouldRoll) {
                logger.info("Rolling the active segment to make it eligible for deletion");
                roll();
            }
            return deletable;
        }
    }

    /**
     * Wraps the value of iterator.next() in an optional.
     *
     * @param iterator the iterator
     * @return Optional of the next element if it exists otherwise Optional.empty()
     */
    private static <T> Optional<T> nextOption(Iterator<T> iterator) {
        return iterator.hasNext()
            ? Optional.of(iterator.next())
            : Optional.empty();
    }

    private int deleteSegments(List<LogSegment> deletable, SegmentDeletionReason reason) {
        return maybeHandleIOException(() -> "Error while deleting segments for " + topicPartition() + " in dir " + dir().getParent(),
                () -> {
                    int numToDelete = deletable.size();
                    if (numToDelete > 0) {
                        // we must always have at least one segment, so if we are going to delete all the segments, create a new one first
                        List<LogSegment> segmentsToDelete = deletable;
                        if (localLog.segments().numberOfSegments() == numToDelete) {
                            LogSegment newSegment = roll();
                            if (deletable.get(deletable.size() - 1).baseOffset() == newSegment.baseOffset()) {
                                logger.warn("Empty active segment at {} was deleted and recreated due to {}", deletable.get(deletable.size() - 1).baseOffset(), reason);
                                deletable.remove(deletable.size() - 1);
                                segmentsToDelete = List.copyOf(deletable);
                            }
                        }
                        localLog.checkIfMemoryMappedBufferClosed();
                        if (!segmentsToDelete.isEmpty()) {
                            // increment the local-log-start-offset or log-start-offset before removing the segment for lookups
                            long newLocalLogStartOffset = localLog.segments().higherSegment(segmentsToDelete.get(segmentsToDelete.size() - 1).baseOffset()).get().baseOffset();
                            if (remoteLogEnabledAndRemoteCopyEnabled()) {
                                maybeIncrementLocalLogStartOffset(newLocalLogStartOffset, LogStartOffsetIncrementReason.SegmentDeletion);
                            } else {
                                maybeIncrementLogStartOffset(newLocalLogStartOffset, LogStartOffsetIncrementReason.SegmentDeletion);
                            }
                            // remove the segments for lookups
                            localLog.removeAndDeleteSegments(segmentsToDelete, true, reason);
                        }
                        deleteProducerSnapshots(deletable, true);
                    }
                    return numToDelete;
                });
    }

    /**
     * If topic deletion is enabled, delete any local log segments that have either expired due to time based
     * retention or because the log size is > retentionSize. Empty cleanup.policy is the same as delete with 
     * infinite retention, so we only need to delete local segments if remote storage is enabled. Whether or 
     * not deletion is enabled, delete any local log segments that are before the log start offset
     */
    public int deleteOldSegments() throws IOException {
        if (config().delete) {
            return deleteLogStartOffsetBreachedSegments() +
                    deleteRetentionSizeBreachedSegments() +
                    deleteRetentionMsBreachedSegments();
        } else if (config().compact) {
            return deleteLogStartOffsetBreachedSegments();
        } else {
            // If cleanup.policy is empty and remote storage is enabled, the local log segments will 
            // be cleaned based on the values of log.local.retention.bytes and log.local.retention.ms
            if (remoteLogEnabledAndRemoteCopyEnabled()) {
                return deleteLogStartOffsetBreachedSegments() +
                        deleteRetentionSizeBreachedSegments() +
                        deleteRetentionMsBreachedSegments();
            } else {
                // If cleanup.policy is empty and remote storage is disabled, we should not delete any local log segments 
                // unless the log start offset advances through deleteRecords
                return deleteLogStartOffsetBreachedSegments();
            }
        }
    }

    public interface DeletionCondition {
        boolean execute(LogSegment segment, Optional<LogSegment> nextSegmentOpt) throws IOException;
    }

    private int deleteRetentionMsBreachedSegments() throws IOException {
        long retentionMs = UnifiedLog.localRetentionMs(config(), remoteLogEnabledAndRemoteCopyEnabled());
        if (retentionMs < 0) return 0;
        long startMs = time().milliseconds();

        DeletionCondition shouldDelete = (segment, nextSegmentOpt) -> {
            if (startMs < segment.largestTimestamp()) {
                futureTimestampLogger.warn("{} contains future timestamp(s), making it ineligible to be deleted", segment);
            }
            boolean delete = startMs - segment.largestTimestamp() > retentionMs;
            logger.debug("{} retentionMs breached: {}, startMs={}, retentionMs={}",
                    segment, delete, startMs, retentionMs);
            return delete;
        };
        return deleteOldSegments(shouldDelete, toDelete -> {
            long localRetentionMs = UnifiedLog.localRetentionMs(config(), remoteLogEnabledAndRemoteCopyEnabled());
            for (LogSegment segment : toDelete) {
                if (segment.largestRecordTimestamp().isPresent()) {
                    if (remoteLogEnabledAndRemoteCopyEnabled()) {
                        logger.info("Deleting segment {} due to local log retention time {}ms breach based on the largest " +
                                "record timestamp in the segment", segment, localRetentionMs);
                    } else {
                        logger.info("Deleting segment {} due to log retention time {}ms breach based on the largest " +
                                "record timestamp in the segment", segment, localRetentionMs);
                    }
                } else {
                    if (remoteLogEnabledAndRemoteCopyEnabled()) {
                        logger.info("Deleting segment {} due to local log retention time {}ms breach based on the " +
                                "last modified time of the segment", segment, localRetentionMs);
                    } else {
                        logger.info("Deleting segment {} due to log retention time {}ms breach based on the " +
                                "last modified time of the segment", segment, localRetentionMs);
                    }
                }
            }
        });
    }

    private int deleteRetentionSizeBreachedSegments() throws IOException {
        long retentionSize = UnifiedLog.localRetentionSize(config(), remoteLogEnabledAndRemoteCopyEnabled());
        long logSize = size();
        if (retentionSize < 0 || logSize < retentionSize) return 0;
        final AtomicLong diff = new AtomicLong(logSize - retentionSize);

        DeletionCondition shouldDelete = (segment, nextSegmentOpt) -> {
            int segmentSize = segment.size();
            boolean delete = diff.get() - segmentSize >= 0;
            logger.debug("{} retentionSize breached: {}, log size before delete segment={}, after delete segment={}",
                    segment, delete, diff.get(), diff.get() - segmentSize);
            if (delete) {
                diff.addAndGet(-segmentSize);
            }
            return delete;
        };
        return deleteOldSegments(shouldDelete, toDelete -> {
            long size = size();
            for (LogSegment segment : toDelete) {
                size -= segment.size();
                if (remoteLogEnabledAndRemoteCopyEnabled()) {
                    logger.info("Deleting segment {} due to local log retention size {} breach. Local log size after deletion will be {}.",
                            segment, UnifiedLog.localRetentionSize(config(), true), size);
                } else {
                    logger.info("Deleting segment {} due to log retention size {} breach. Log size after deletion will be {}.",
                            segment, config().retentionSize, size);
                }
            }
        });
    }

    private int deleteLogStartOffsetBreachedSegments() throws IOException {
        DeletionCondition shouldDelete = (segment, nextSegmentOpt) -> {
            boolean isRemoteLogEnabled = remoteLogEnabled();
            long localLSO = localLogStartOffset();
            long logStartOffsetValue = isRemoteLogEnabled ? localLSO : logStartOffset();
            boolean delete = nextSegmentOpt
                    .map(nextSegment -> nextSegment.baseOffset() <= logStartOffsetValue)
                    .orElse(false);
            logger.debug("{} logStartOffset breached: {}, nextSegmentOpt={}, {}",
                    segment, delete, nextSegmentOpt, isRemoteLogEnabled ? "localLogStartOffset=" + localLSO : "logStartOffset=" + logStartOffset);
            return delete;
        };
        return deleteOldSegments(shouldDelete, toDelete -> {
            if (remoteLogEnabledAndRemoteCopyEnabled()) {
                logger.info("Deleting segments due to local log start offset {} breach: {}",
                        localLogStartOffset(), toDelete.stream().map(LogSegment::toString).collect(Collectors.joining(",")));
            } else {
                logger.info("Deleting segments due to log start offset {} breach: {}",
                        logStartOffset, toDelete.stream().map(LogSegment::toString).collect(Collectors.joining(",")));
            }
        });
    }

    public boolean isFuture() {
        return localLog.isFuture();
    }

    /**
     * The size of the log in bytes
     */
    public long size() {
        return LogSegments.sizeInBytes(logSegments());
    }

    /**
     * The log size in bytes for all segments that are only in local log but not yet in remote log.
     */
    public long onlyLocalLogSegmentsSize() {
        return LogSegments.sizeInBytes(logSegments().stream().filter(s -> s.baseOffset() >= highestOffsetInRemoteStorage()).collect(Collectors.toList()));
    }

    /**
     * The number of segments that are only in local log but not yet in remote log.
     */
    public long onlyLocalLogSegmentsCount() {
        return logSegments().stream().filter(s -> s.baseOffset() >= highestOffsetInRemoteStorage()).count();
    }

    /**
     * The offset of the next message that will be appended to the log
     */
    public long logEndOffset() {
        return localLog.logEndOffset();
    }

    /**
     * The offset metadata of the next message that will be appended to the log
     */
    public LogOffsetMetadata logEndOffsetMetadata() {
        return localLog.logEndOffsetMetadata();
    }

    /**
     * Roll the log over to a new empty log segment if necessary.
     * The segment will be rolled if one of the following conditions met:
     * 1. The logSegment is full
     * 2. The maxTime has elapsed since the timestamp of first message in the segment (or since the
     *    create time if the first message does not have a timestamp)
     * 3. The index is full
     *
     * @param messagesSize The messages set size in bytes.
     * @param appendInfo log append information
     *
     * @return  The currently active segment after (perhaps) rolling to a new segment
     */
    private LogSegment maybeRoll(int messagesSize, LogAppendInfo appendInfo) throws IOException {
        synchronized (lock) {
            LogSegment segment = localLog.segments().activeSegment();
            long now = time().milliseconds();
            long maxTimestampInMessages = appendInfo.maxTimestamp();
            long maxOffsetInMessages = appendInfo.lastOffset();

            if (segment.shouldRoll(new RollParams(config().maxSegmentMs(), config().segmentSize(), appendInfo.maxTimestamp(), appendInfo.lastOffset(), messagesSize, now))) {
                logger.debug("Rolling new log segment (log_size = {}/{}}, " +
                          "offset_index_size = {}/{}, " +
                          "time_index_size = {}/{}, " +
                          "inactive_time_ms = {}/{}).",
                        segment.size(), config().segmentSize(),
                        segment.offsetIndex().entries(), segment.offsetIndex().maxEntries(),
                        segment.timeIndex().entries(), segment.timeIndex().maxEntries(),
                        segment.timeWaitedForRoll(now, maxTimestampInMessages), config().segmentMs - segment.rollJitterMs());
                /*
                    maxOffsetInMessages - Integer.MAX_VALUE is a heuristic value for the first offset in the set of messages.
                    Since the offset in messages will not differ by more than Integer.MAX_VALUE, this is guaranteed <= the real
                    first offset in the set. Determining the true first offset in the set requires decompression, which the follower
                    is trying to avoid during log append. Prior behavior assigned new baseOffset = logEndOffset from old segment.
                    This was problematic in the case that two consecutive messages differed in offset by
                    Integer.MAX_VALUE.toLong + 2 or more.  In this case, the prior behavior would roll a new log segment whose
                    base offset was too low to contain the next message.  This edge case is possible when a replica is recovering a
                    highly compacted topic from scratch.
                    Note that this is only required for pre-V2 message formats because these do not store the first message offset
                    in the header.
                */
                long rollOffset = appendInfo.firstOffset() == UnifiedLog.UNKNOWN_OFFSET
                    ? maxOffsetInMessages - Integer.MAX_VALUE
                    : appendInfo.firstOffset();
                return roll(Optional.of(rollOffset));
            } else {
                return segment;
            }
        }
    }

    /**
     * Roll the local log over to a new active segment starting with the localLog.logEndOffset.
     * This will trim the index to the exact size of the number of entries it currently contains.
     *
     * @return The newly rolled segment
     */
    public LogSegment roll() throws IOException {
        return roll(Optional.empty());
    }

    /**
     * Roll the local log over to a new active segment starting with the expectedNextOffset (when provided),
     * or localLog.logEndOffset otherwise. This will trim the index to the exact size of the number of entries
     * it currently contains.
     *
     * @return The newly rolled segment
     */
    public LogSegment roll(Optional<Long> expectedNextOffset) throws IOException {
        synchronized (lock) {
            long nextOffset = expectedNextOffset.orElse(0L);

            LogSegment newSegment = localLog.roll(nextOffset);
            // Take a snapshot of the producer state to facilitate recovery. It is useful to have the snapshot
            // offset align with the new segment offset since this ensures we can recover the segment by beginning
            // with the corresponding snapshot file and scanning the segment data. Because the segment base offset
            // may actually be ahead of the current producer state end offset (which corresponds to the log end offset),
            // we manually override the state offset here prior to taking the snapshot.
            producerStateManager.updateMapEndOffset(newSegment.baseOffset());
            // We avoid potentially-costly fsync call, since we acquire UnifiedLog#lock here
            // which could block subsequent produces in the meantime.
            // flush is done in the scheduler thread along with segment flushing below
            Optional<File> maybeSnapshot = producerStateManager.takeSnapshot(false);
            updateHighWatermarkWithLogEndOffset();
            // Schedule an asynchronous flush of the old segment
            scheduler().scheduleOnce("flush-log", () -> {
                maybeSnapshot.ifPresent(f -> flushProducerStateSnapshot(f.toPath()));
                flushUptoOffsetExclusive(newSegment.baseOffset());
            });
            return newSegment;
        }
    }

    /**
     * Flush all local log segments
     *
     * @param forceFlushActiveSegment should be true during a clean shutdown, and false otherwise. The reason is that
     * we have to pass logEndOffset + 1 to the `localLog.flush(offset: Long): Unit` function to flush empty
     * active segments, which is important to make sure we persist the active segment file during shutdown, particularly
     * when it's empty.
     */
    public void flush(boolean forceFlushActiveSegment) {
        flush(logEndOffset(), forceFlushActiveSegment);
    }

    /**
     * Flush local log segments for all offsets up to offset-1
     *
     * @param offset The offset to flush up to (non-inclusive); the new recovery point
     */
    public void flushUptoOffsetExclusive(long offset) {
        flush(offset, false);
    }

    /**
     * Flush local log segments for all offsets up to offset-1 if includingOffset=false; up to offset
     * if includingOffset=true. The recovery point is set to offset.
     *
     * @param offset The offset to flush up to; the new recovery point
     * @param includingOffset Whether the flush includes the provided offset.
     */
    private void flush(long offset, boolean includingOffset) {
        long flushOffset = includingOffset ? offset + 1 : offset;
        String includingOffsetStr = includingOffset ? "inclusive" : "exclusive";
        maybeHandleIOException(
                () -> "Error while flushing log for " + topicPartition() + " in dir " + dir().getParent() + " with offset " + offset +
                " (" + includingOffsetStr + ") and recovery point " + offset,
                () -> {
                    if (flushOffset > localLog.recoveryPoint()) {
                        logger.debug("Flushing log up to offset {} ({}) with recovery point {}, last flushed: {},  current time: {}, unflushed: {}",
                                offset, includingOffsetStr, offset, lastFlushTime(), time().milliseconds(), localLog.unflushedMessages());
                        localLog.flush(flushOffset);
                        synchronized (lock) {
                            localLog.markFlushed(offset);
                        }
                    }
                    return null;
                });
    }

    /**
     * Completely delete the local log directory and all contents from the file system with no delay
     */
    public void delete() {
        maybeHandleIOException(
            () -> "Error while deleting log for " + topicPartition() + " in dir " + dir().getParent(),
            () -> {
                synchronized (lock) {
                    localLog.checkIfMemoryMappedBufferClosed();
                    producerExpireCheck.cancel(true);
                    leaderEpochCache.clear();
                    List<LogSegment> deletedSegments = localLog.deleteAllSegments();
                    deleteProducerSnapshots(deletedSegments, false);
                    localLog.deleteEmptyDir();
                }
                return null;
            });
    }

    // visible for testing
    public void takeProducerSnapshot() throws IOException {
        synchronized (lock) {
            localLog.checkIfMemoryMappedBufferClosed();
            producerStateManager.takeSnapshot();
        }
    }

    // visible for testing
    public OptionalLong latestProducerSnapshotOffset() {
        synchronized (lock) {
            return producerStateManager.latestSnapshotOffset();
        }
    }

    // visible for testing
    public OptionalLong oldestProducerSnapshotOffset() {
        synchronized (lock) {
            return producerStateManager.oldestSnapshotOffset();
        }
    }

    // visible for testing
    public long latestProducerStateEndOffset() {
        synchronized (lock) {
            return producerStateManager.mapEndOffset();
        }
    }

    // visible for testing
    public void flushProducerStateSnapshot(Path snapshot) {
        maybeHandleIOException(
                () -> "Error while deleting producer state snapshot " + snapshot + " for " + topicPartition() + " in dir " + dir().getParent(),
                () -> {
                    Utils.flushFileIfExists(snapshot);
                    return null;
                });
    }

    /**
     * Truncate this log so that it ends with the greatest offset < targetOffset.
     *
     * @param targetOffset The offset to truncate to, an upper bound on all offsets in the log after truncation is complete.
     * @return True if targetOffset < logEndOffset
     */
    public boolean truncateTo(long targetOffset) {
        return maybeHandleIOException(
                () -> "Error while truncating log to offset " + targetOffset + " for " + topicPartition() + " in dir " + dir().getParent(),
                () -> {
                    if (targetOffset < 0) {
                        throw new IllegalArgumentException("Cannot truncate partition " + topicPartition() + " to a negative offset (" + targetOffset + ").");
                    }

                    long hwm = highWatermark();
                    if (targetOffset < hwm) {
                        logger.warn("Truncating {}{} to offset {} below high watermark {}", isFuture() ? "future " : "", topicPartition(), targetOffset, hwm);
                    }

                    if (targetOffset >= localLog.logEndOffset()) {
                        logger.info("Truncating to {} has no effect as the largest offset in the log is {}", targetOffset, localLog.logEndOffset() - 1);
                        // Always truncate epoch cache since we may have a conflicting epoch entry at the
                        // end of the log from the leader. This could happen if this broker was a leader
                        // and inserted the first start offset entry, but then failed to append any entries
                        // before another leader was elected.
                        synchronized (lock) {
                            leaderEpochCache.truncateFromEndAsyncFlush(logEndOffset());
                        }
                        return false;
                    } else {
                        logger.info("Truncating to offset {}", targetOffset);
                        synchronized (lock) {
                            localLog.checkIfMemoryMappedBufferClosed();
                            if (localLog.segments().firstSegmentBaseOffset().getAsLong() > targetOffset) {
                                truncateFullyAndStartAt(targetOffset, Optional.empty());
                            } else {
                                Collection<LogSegment> deletedSegments = localLog.truncateTo(targetOffset);
                                deleteProducerSnapshots(deletedSegments, true);
                                leaderEpochCache.truncateFromEndAsyncFlush(targetOffset);
                                logStartOffset = Math.min(targetOffset, logStartOffset);
                                rebuildProducerState(targetOffset, producerStateManager);
                                if (highWatermark() >= localLog.logEndOffset())
                                    updateHighWatermark(localLog.logEndOffsetMetadata());
                            }
                            return true;
                        }
                    }
                });
    }

    /**
     *  Delete all data in the log and start at the new offset
     *
     *  @param newOffset The new offset to start the log with
     *  @param logStartOffsetOpt The log start offset to set for the log. If None, the new offset will be used.
     */
    public void truncateFullyAndStartAt(long newOffset, Optional<Long> logStartOffsetOpt) {
        maybeHandleIOException(
                () -> "Error while truncating the entire log for " + topicPartition() + " in dir " + dir().getParent(),
                () -> {
                    logger.debug("Truncate and start at offset {}, logStartOffset: {}", newOffset, logStartOffsetOpt.orElse(newOffset));
                    synchronized (lock)  {
                        localLog.truncateFullyAndStartAt(newOffset);
                        leaderEpochCache.clearAndFlush();
                        producerStateManager.truncateFullyAndStartAt(newOffset);
                        logStartOffset = logStartOffsetOpt.orElse(newOffset);
                        if (remoteLogEnabled()) localLogStartOffset = newOffset;
                        rebuildProducerState(newOffset, producerStateManager);
                        return updateHighWatermark(localLog.logEndOffsetMetadata());
                    }
                });
    }

    /**
     * The time this log is last known to have been fully flushed to disk
     */
    public long lastFlushTime() {
        return localLog.lastFlushTime();
    }

    /**
     * The active segment that is currently taking appends
     */
    public LogSegment activeSegment() {
        return localLog.segments().activeSegment();
    }

    /**
     * All the log segments in this log ordered from oldest to newest
     */
    public List<LogSegment> logSegments() {
        synchronized (lock) {
            return List.copyOf(localLog.segments().values());
        }
    }

    /**
     * Get all segments beginning with the segment that includes "from" and ending with the segment
     * that includes up to "to-1" or the end of the log (if to > logEndOffset).
     */
    public List<LogSegment> logSegments(long from, long to) {
        synchronized (lock) {
            return List.copyOf(localLog.segments().values(from, to));
        }
    }

    public List<LogSegment> nonActiveLogSegmentsFrom(long from) {
        synchronized (lock) {
            return List.copyOf(localLog.segments().nonActiveLogSegmentsFrom(from));
        }
    }

    @Override
    public String toString() {
        StringBuilder logString = new StringBuilder();
        logString.append("Log(dir=");
        logString.append(dir());
        topicId.ifPresent(id -> {
            logString.append(", topicId=");
            logString.append(id);
        });
        logString.append(", topic=");
        logString.append(topicPartition().topic());
        logString.append(", partition=");
        logString.append(topicPartition().partition());
        logString.append(", highWatermark=");
        logString.append(highWatermark());
        logString.append(", lastStableOffset=");
        logString.append(lastStableOffset());
        logString.append(", logStartOffset=");
        logString.append(logStartOffset());
        logString.append(", logEndOffset=");
        logString.append(logEndOffset());
        logString.append(")");
        return logString.toString();
    }

    public void replaceSegments(List<LogSegment> newSegments, List<LogSegment> oldSegments) throws IOException {
        synchronized (lock) {
            localLog.checkIfMemoryMappedBufferClosed();
            List<LogSegment> deletedSegments = LocalLog.replaceSegments(localLog.segments(), newSegments, oldSegments, dir(), topicPartition(),
                    config(), scheduler(), logDirFailureChannel(), logIdent, false);
            deleteProducerSnapshots(deletedSegments, true);
        }
    }

    /**
     * This function does not acquire Log.lock. The caller has to make sure log segments don't get deleted during
     * this call, and also protects against calling this function on the same segment in parallel.
     *
     * <p>Currently, it is used by LogCleaner threads on log compact non-active segments only with LogCleanerManager's lock
     * to ensure no other LogCleaner threads and retention thread can work on the same segment.
     */
    public Collection<Long> getFirstBatchTimestampForSegments(Collection<LogSegment> segments) {
        return segments.stream().map(LogSegment::getFirstBatchTimestamp).toList();
    }

    /**
     * Remove deleted log metrics
     */
    public void removeLogMetrics() {
        metricNames.forEach(metricsGroup::removeMetric);
        metricNames.clear();
    }

    private <T> T maybeHandleIOException(Supplier<String> msg, StorageAction<T, IOException> fun) throws KafkaStorageException {
        return LocalLog.maybeHandleIOException(logDirFailureChannel(), parentDir(), msg, fun);
    }

    public List<LogSegment> splitOverflowedSegment(LogSegment segment) throws IOException {
        synchronized (lock) {
            LocalLog.SplitSegmentResult result = LocalLog.splitOverflowedSegment(segment, localLog.segments(), dir(), topicPartition(), config(), scheduler(), logDirFailureChannel(), logIdent);
            deleteProducerSnapshots(result.deletedSegments(), true);
            return result.newSegments();
        }
    }

    private void deleteProducerSnapshots(Collection<LogSegment> segments, boolean asyncDelete) throws IOException {
        UnifiedLog.deleteProducerSnapshots(segments, producerStateManager, asyncDelete, scheduler(), config(), logDirFailureChannel(), parentDir(), topicPartition());
    }

    private static <T> Optional<T> findFirst(Iterable<T> iterable, Predicate<T> predicate) {
        for (T item : iterable) {
            if (predicate.test(item)) {
                return Optional.of(item);
            }
        }
        return Optional.empty();
    }

    /**
     * Rebuilds producer state until the provided lastOffset. This function may be called from the
     * recovery code path, and thus must be free of all side effects, i.e. it must not update any
     * log-specific state.
     *
     * @param producerStateManager    The {@link ProducerStateManager} instance to be rebuilt.
     * @param segments                The segments of the log whose producer state is being rebuilt
     * @param logStartOffset          The log start offset
     * @param lastOffset              The last offset upto which the producer state needs to be rebuilt
     * @param time                    The time instance used for checking the clock
     * @param reloadFromCleanShutdown True if the producer state is being built after a clean shutdown, false otherwise.
     * @param logPrefix               The logging prefix
     */
    public static void rebuildProducerState(ProducerStateManager producerStateManager,
                                            LogSegments segments,
                                            long logStartOffset,
                                            long lastOffset,
                                            Time time,
                                            boolean reloadFromCleanShutdown,
                                            String logPrefix) throws IOException {
        List<Long> offsetsToSnapshot = new ArrayList<>();
        segments.lastSegment().ifPresent(lastSegment -> {
            long lastSegmentBaseOffset = lastSegment.baseOffset();
            segments.lowerSegment(lastSegmentBaseOffset).ifPresent(s -> offsetsToSnapshot.add(s.baseOffset()));
            offsetsToSnapshot.add(lastSegmentBaseOffset);
        });
        offsetsToSnapshot.add(lastOffset);

        LOG.info("{}Loading producer state till offset {}", logPrefix, lastOffset);

        // We want to avoid unnecessary scanning of the log to build the producer state when the broker is being
        // upgraded. The basic idea is to use the absence of producer snapshot files to detect the upgrade case,
        // but we have to be careful not to assume too much in the presence of broker failures. The most common
        // upgrade case in which we expect to find no snapshots is the following:
        //
        // * The broker has been upgraded, and we had a clean shutdown.
        //
        // If we hit this case, we skip producer state loading and write a new snapshot at the log end
        // offset (see below). The next time the log is reloaded, we will load producer state using this snapshot
        // (or later snapshots). Otherwise, if there is no snapshot file, then we have to rebuild producer state
        // from the first segment.
        if (producerStateManager.latestSnapshotOffset().isEmpty() && reloadFromCleanShutdown) {
            // To avoid an expensive scan through all the segments, we take empty snapshots from the start of the
            // last two segments and the last offset. This should avoid the full scan in the case that the log needs
            // truncation.
            for (long offset : offsetsToSnapshot) {
                producerStateManager.updateMapEndOffset(offset);
                producerStateManager.takeSnapshot();
            }
        } else {
            LOG.info("{}Reloading from producer snapshot and rebuilding producer state from offset {}", logPrefix, lastOffset);
            boolean isEmptyBeforeTruncation = producerStateManager.isEmpty() && producerStateManager.mapEndOffset() >= lastOffset;
            long producerStateLoadStart = time.milliseconds();
            producerStateManager.truncateAndReload(logStartOffset, lastOffset, time.milliseconds());
            long segmentRecoveryStart = time.milliseconds();

            // Only do the potentially expensive reloading if the last snapshot offset is lower than the log end
            // offset (which would be the case on first startup) and there were active producers prior to truncation
            // (which could be the case if truncating after initial loading). If there weren't, then truncating
            // shouldn't change that fact (although it could cause a producerId to expire earlier than expected),
            // and we can skip the loading. This is an optimization for users which are not yet using
            // idempotent/transactional features yet.
            if (lastOffset > producerStateManager.mapEndOffset() && !isEmptyBeforeTruncation) {
                Optional<LogSegment> segmentOfLastOffset = segments.floorSegment(lastOffset);

                for (LogSegment segment : segments.values(producerStateManager.mapEndOffset(), lastOffset)) {
                    long startOffset = Utils.max(segment.baseOffset(), producerStateManager.mapEndOffset(), logStartOffset);
                    producerStateManager.updateMapEndOffset(startOffset);

                    if (offsetsToSnapshot.contains(segment.baseOffset())) {
                        producerStateManager.takeSnapshot();
                    }
                    int maxPosition = segment.size();
                    if (segmentOfLastOffset.isPresent() && segmentOfLastOffset.get() == segment) {
                        FileRecords.LogOffsetPosition lop = segment.translateOffset(lastOffset);
                        maxPosition = lop != null ? lop.position : segment.size();
                    }

                    FetchDataInfo fetchDataInfo = segment.read(startOffset, Integer.MAX_VALUE, maxPosition);
                    if (fetchDataInfo != null) {
                        loadProducersFromRecords(producerStateManager, fetchDataInfo.records);
                    }
                }
            }
            producerStateManager.updateMapEndOffset(lastOffset);
            producerStateManager.takeSnapshot();
            LOG.info("{}Producer state recovery took {}ms for snapshot load and {}ms for segment recovery from offset {}",
                    logPrefix, segmentRecoveryStart - producerStateLoadStart, time.milliseconds() - segmentRecoveryStart, lastOffset);
        }
    }

    public static void deleteProducerSnapshots(Collection<LogSegment> segments,
                                               ProducerStateManager producerStateManager,
                                               boolean asyncDelete,
                                               Scheduler scheduler,
                                               LogConfig config,
                                               LogDirFailureChannel logDirFailureChannel,
                                               String parentDir,
                                               TopicPartition topicPartition) throws IOException {
        List<SnapshotFile> snapshotsToDelete = new ArrayList<>();
        for (LogSegment segment : segments) {
            Optional<SnapshotFile> snapshotFile = producerStateManager.removeAndMarkSnapshotForDeletion(segment.baseOffset());
            snapshotFile.ifPresent(snapshotsToDelete::add);
        }

        Runnable deleteProducerSnapshots = () -> deleteProducerSnapshots(snapshotsToDelete, logDirFailureChannel, parentDir, topicPartition);
        if (asyncDelete) {
            scheduler.scheduleOnce("delete-producer-snapshot", deleteProducerSnapshots, config.fileDeleteDelayMs);
        } else {
            deleteProducerSnapshots.run();
        }
    }

    private static void deleteProducerSnapshots(List<SnapshotFile> snapshotsToDelete, LogDirFailureChannel logDirFailureChannel, String parentDir, TopicPartition topicPartition) {
        LocalLog.maybeHandleIOException(
                logDirFailureChannel,
                parentDir,
                () -> "Error while deleting producer state snapshots for " + topicPartition + " in dir " + parentDir,
                () -> {
                    for (SnapshotFile snapshotFile : snapshotsToDelete) {
                        snapshotFile.deleteIfExists();
                    }
                    return null;
                });
    }

    private static void loadProducersFromRecords(ProducerStateManager producerStateManager, Records records) {
        Map<Long, ProducerAppendInfo> loadedProducers = new HashMap<>();
        final List<CompletedTxn> completedTxns = new ArrayList<>();
        records.batches().forEach(batch -> {
            if (batch.hasProducerId()) {
                Optional<CompletedTxn> maybeCompletedTxn = updateProducers(
                        producerStateManager,
                        batch,
                        loadedProducers,
                        Optional.empty(),
                        AppendOrigin.REPLICATION,
                        (short) 0);
                maybeCompletedTxn.ifPresent(completedTxns::add);
            }
        });
        loadedProducers.values().forEach(producerStateManager::update);
        completedTxns.forEach(producerStateManager::completeTxn);
    }

    public static Optional<CompletedTxn> updateProducers(ProducerStateManager producerStateManager,
                                                         RecordBatch batch,
                                                         Map<Long, ProducerAppendInfo> producers,
                                                         Optional<LogOffsetMetadata> firstOffsetMetadata,
                                                         AppendOrigin origin,
                                                         short transactionVersion) {
        long producerId = batch.producerId();
        ProducerAppendInfo appendInfo = producers.computeIfAbsent(producerId, __ -> producerStateManager.prepareUpdate(producerId, origin));
        Optional<CompletedTxn> completedTxn = appendInfo.append(batch, firstOffsetMetadata, transactionVersion);
        // Whether we wrote a control marker or a data batch, we may be able to remove VerificationGuard since either the transaction is complete or we have a first offset.
        if (batch.isTransactional()) {
            VerificationStateEntry entry = producerStateManager.verificationStateEntry(producerId);
            // The only case we should not remove the verification guard is if the marker was a control marker, we are using TV2 and the epochs match.
            // This is safe because we always bump epoch upon upgrading to TV2.
            boolean isV2NextTransactionStarted = entry != null && entry.supportsEpochBump() && batch.isControlBatch() && batch.producerEpoch() == entry.epoch();
            if (!isV2NextTransactionStarted)
                producerStateManager.clearVerificationStateEntry(producerId);
        }
        return completedTxn;
    }

    public static boolean isRemoteLogEnabled(boolean remoteStorageSystemEnable, LogConfig config, String topic) {
        // Remote log is enabled only for non-compact and non-internal topics
        return remoteStorageSystemEnable &&
                !(config.compact || Topic.isInternal(topic)
                        || TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_NAME.equals(topic)
                        || Topic.CLUSTER_METADATA_TOPIC_NAME.equals(topic)) &&
                config.remoteStorageEnable();
    }

    // Visible for benchmarking
    public static LogValidator.MetricsRecorder newValidatorMetricsRecorder(BrokerTopicMetrics allTopicsStats) {
        return new LogValidator.MetricsRecorder() {
            public void recordInvalidMagic() {
                allTopicsStats.invalidMagicNumberRecordsPerSec().mark();
            }

            public void recordInvalidOffset() {
                allTopicsStats.invalidOffsetOrSequenceRecordsPerSec().mark();
            }

            public void recordInvalidSequence() {
                allTopicsStats.invalidOffsetOrSequenceRecordsPerSec().mark();
            }

            public void recordInvalidChecksums() {
                allTopicsStats.invalidMessageCrcRecordsPerSec().mark();
            }

            public void recordNoKeyCompactedTopic() {
                allTopicsStats.noKeyCompactedTopicRecordsPerSec().mark();
            }
        };
    }

    /**
     * Create a new LeaderEpochFileCache instance and load the epoch entries from the backing checkpoint file or
     * the provided currentCache (if not empty).
     *
     * @param dir                  The directory in which the log will reside
     * @param topicPartition       The topic partition
     * @param logDirFailureChannel The LogDirFailureChannel to asynchronously handle log dir failure
     * @param currentCache         The current LeaderEpochFileCache instance (if any)
     * @param scheduler            The scheduler for executing asynchronous tasks
     * @return The new LeaderEpochFileCache instance
     */
    public static LeaderEpochFileCache createLeaderEpochCache(File dir,
                                                              TopicPartition topicPartition,
                                                              LogDirFailureChannel logDirFailureChannel,
                                                              Optional<LeaderEpochFileCache> currentCache,
                                                              Scheduler scheduler) throws IOException {
        File leaderEpochFile = LeaderEpochCheckpointFile.newFile(dir);
        LeaderEpochCheckpointFile checkpointFile = new LeaderEpochCheckpointFile(leaderEpochFile, logDirFailureChannel);
        return currentCache.map(cache -> cache.withCheckpoint(checkpointFile))
                .orElse(new LeaderEpochFileCache(topicPartition, checkpointFile, scheduler));

    }

    public static LogSegment createNewCleanedSegment(File dir, LogConfig logConfig, long baseOffset) throws IOException {
        return LocalLog.createNewCleanedSegment(dir, logConfig, baseOffset);
    }

    public static long localRetentionMs(LogConfig config, boolean remoteLogEnabledAndRemoteCopyEnabled) {
        return remoteLogEnabledAndRemoteCopyEnabled ? config.localRetentionMs() : config.retentionMs;
    }

    public static long localRetentionSize(LogConfig config, boolean remoteLogEnabledAndRemoteCopyEnabled) {
        return remoteLogEnabledAndRemoteCopyEnabled ? config.localRetentionBytes() : config.retentionSize;
    }

    public static String logDeleteDirName(TopicPartition topicPartition) {
        return LocalLog.logDeleteDirName(topicPartition);
    }

    public static String logFutureDirName(TopicPartition topicPartition) {
        return LocalLog.logFutureDirName(topicPartition);
    }

    public static String logStrayDirName(TopicPartition topicPartition) {
        return LocalLog.logStrayDirName(topicPartition);
    }

    public static String logDirName(TopicPartition topicPartition) {
        return LocalLog.logDirName(topicPartition);
    }

    public static File transactionIndexFile(File dir, long offset, String suffix) {
        return LogFileUtils.transactionIndexFile(dir, offset, suffix);
    }

    public static long offsetFromFile(File file) {
        return LogFileUtils.offsetFromFile(file);
    }

    public static long sizeInBytes(Collection<LogSegment> segments) {
        return LogSegments.sizeInBytes(segments);
    }

    public static TopicPartition parseTopicPartitionName(File dir) throws IOException {
        return LocalLog.parseTopicPartitionName(dir);
    }
}
