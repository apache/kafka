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
package kafka.log.remote;

import com.yammer.metrics.core.Gauge;
import kafka.cluster.EndPoint;
import kafka.cluster.Partition;
import kafka.log.LogSegment;
import kafka.log.UnifiedLog;
import kafka.server.BrokerTopicStats;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.RemoteLogInputStream;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.ChildFirstClassLoader;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.log.remote.metadata.storage.ClassLoaderAwareRemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.ClassLoaderAwareRemoteStorageManager;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.storage.internals.checkpoint.InMemoryLeaderEpochCheckpoint;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.internals.log.AbortedTxn;
import org.apache.kafka.storage.internals.log.EpochEntry;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.FetchIsolation;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.OffsetIndex;
import org.apache.kafka.storage.internals.log.OffsetPosition;
import org.apache.kafka.storage.internals.log.RemoteIndexCache;
import org.apache.kafka.storage.internals.log.RemoteLogReadResult;
import org.apache.kafka.storage.internals.log.RemoteStorageFetchInfo;
import org.apache.kafka.storage.internals.log.RemoteStorageThreadPool;
import org.apache.kafka.storage.internals.log.TransactionIndex;
import org.apache.kafka.storage.internals.log.TxnIndexSearchResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConverters;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX;

/**
 * This class is responsible for
 * - initializing `RemoteStorageManager` and `RemoteLogMetadataManager` instances
 * - receives any leader and follower replica events and partition stop events and act on them
 * - also provides APIs to fetch indexes, metadata about remote log segments
 * - copying log segments to remote storage
 */
public class RemoteLogManager implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteLogManager.class);
    private static final String REMOTE_LOG_READER_THREAD_NAME_PREFIX = "remote-log-reader";
    public static final String REMOTE_LOG_READER_METRICS_NAME_PREFIX = "RemoteLogReader";
    public static final String REMOTE_LOG_MANAGER_TASKS_AVG_IDLE_PERCENT = "RemoteLogManagerTasksAvgIdlePercent";
    private final RemoteLogManagerConfig rlmConfig;
    private final int brokerId;
    private final String logDir;
    private final Time time;
    private final Function<TopicPartition, Optional<UnifiedLog>> fetchLog;
    private final BrokerTopicStats brokerTopicStats;

    private final RemoteStorageManager remoteLogStorageManager;

    private final RemoteLogMetadataManager remoteLogMetadataManager;

    private final RemoteIndexCache indexCache;
    private final RemoteStorageThreadPool remoteStorageReaderThreadPool;
    private final RLMScheduledThreadPool rlmScheduledThreadPool;

    private final long delayInMs;

    private final ConcurrentHashMap<TopicIdPartition, RLMTaskWithFuture> leaderOrFollowerTasks = new ConcurrentHashMap<>();

    // topic ids that are received on leadership changes, this map is cleared on stop partitions
    private final ConcurrentMap<TopicPartition, Uuid> topicPartitionIds = new ConcurrentHashMap<>();
    private final String clusterId;

    // The endpoint for remote log metadata manager to connect to
    private Optional<EndPoint> endpoint = Optional.empty();
    private boolean closed = false;

    private KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(this.getClass());

    /**
     * Creates RemoteLogManager instance with the given arguments.
     *
     * @param rlmConfig Configuration required for remote logging subsystem(tiered storage) at the broker level.
     * @param brokerId  id of the current broker.
     * @param logDir    directory of Kafka log segments.
     * @param time      Time instance.
     * @param clusterId The cluster id.
     * @param fetchLog  function to get UnifiedLog instance for a given topic.
     */
    public RemoteLogManager(RemoteLogManagerConfig rlmConfig,
                            int brokerId,
                            String logDir,
                            String clusterId,
                            Time time,
                            Function<TopicPartition, Optional<UnifiedLog>> fetchLog,
                            BrokerTopicStats brokerTopicStats) throws IOException {
        this.rlmConfig = rlmConfig;
        this.brokerId = brokerId;
        this.logDir = logDir;
        this.clusterId = clusterId;
        this.time = time;
        this.fetchLog = fetchLog;
        this.brokerTopicStats = brokerTopicStats;

        remoteLogStorageManager = createRemoteStorageManager();
        remoteLogMetadataManager = createRemoteLogMetadataManager();
        indexCache = new RemoteIndexCache(1024, remoteLogStorageManager, logDir);
        delayInMs = rlmConfig.remoteLogManagerTaskIntervalMs();
        rlmScheduledThreadPool = new RLMScheduledThreadPool(rlmConfig.remoteLogManagerThreadPoolSize());

        metricsGroup.newGauge(REMOTE_LOG_MANAGER_TASKS_AVG_IDLE_PERCENT, new Gauge<Double>() {
            @Override
            public Double value() {
                return rlmScheduledThreadPool.getIdlePercent();
            }
        });

        remoteStorageReaderThreadPool = new RemoteStorageThreadPool(
                REMOTE_LOG_READER_THREAD_NAME_PREFIX,
                rlmConfig.remoteLogReaderThreads(),
                rlmConfig.remoteLogReaderMaxPendingTasks(),
                REMOTE_LOG_READER_METRICS_NAME_PREFIX
        );
    }

    private void removeMetrics() {
        metricsGroup.removeMetric(REMOTE_LOG_MANAGER_TASKS_AVG_IDLE_PERCENT);
        remoteStorageReaderThreadPool.removeMetrics();
    }

    private <T> T createDelegate(ClassLoader classLoader, String className) {
        try {
            return (T) classLoader.loadClass(className)
                    .getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException |
                 ClassNotFoundException e) {
            throw new KafkaException(e);
        }
    }

    RemoteStorageManager createRemoteStorageManager() {
        return AccessController.doPrivileged(new PrivilegedAction<RemoteStorageManager>() {
            private final String classPath = rlmConfig.remoteStorageManagerClassPath();

            public RemoteStorageManager run() {
                if (classPath != null && !classPath.trim().isEmpty()) {
                    ChildFirstClassLoader classLoader = new ChildFirstClassLoader(classPath, this.getClass().getClassLoader());
                    RemoteStorageManager delegate = createDelegate(classLoader, rlmConfig.remoteStorageManagerClassName());
                    return new ClassLoaderAwareRemoteStorageManager(delegate, classLoader);
                } else {
                    return createDelegate(this.getClass().getClassLoader(), rlmConfig.remoteStorageManagerClassName());
                }
            }
        });
    }

    private void configureRSM() {
        final Map<String, Object> rsmProps = new HashMap<>(rlmConfig.remoteStorageManagerProps());
        rsmProps.put(KafkaConfig.BrokerIdProp(), brokerId);
        remoteLogStorageManager.configure(rsmProps);
    }

    RemoteLogMetadataManager createRemoteLogMetadataManager() {
        return AccessController.doPrivileged(new PrivilegedAction<RemoteLogMetadataManager>() {
            private final String classPath = rlmConfig.remoteLogMetadataManagerClassPath();

            public RemoteLogMetadataManager run() {
                if (classPath != null && !classPath.trim().isEmpty()) {
                    ClassLoader classLoader = new ChildFirstClassLoader(classPath, this.getClass().getClassLoader());
                    RemoteLogMetadataManager delegate = createDelegate(classLoader, rlmConfig.remoteLogMetadataManagerClassName());
                    return new ClassLoaderAwareRemoteLogMetadataManager(delegate, classLoader);
                } else {
                    return createDelegate(this.getClass().getClassLoader(), rlmConfig.remoteLogMetadataManagerClassName());
                }
            }
        });
    }

    public void onEndPointCreated(EndPoint endpoint) {
        this.endpoint = Optional.of(endpoint);
    }

    private void configureRLMM() {
        final Map<String, Object> rlmmProps = new HashMap<>(rlmConfig.remoteLogMetadataManagerProps());

        rlmmProps.put(KafkaConfig.BrokerIdProp(), brokerId);
        rlmmProps.put(KafkaConfig.LogDirProp(), logDir);
        rlmmProps.put("cluster.id", clusterId);
        endpoint.ifPresent(e -> {
            rlmmProps.put(REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX + "bootstrap.servers", e.host() + ":" + e.port());
            rlmmProps.put(REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX + "security.protocol", e.securityProtocol().name);
        });

        remoteLogMetadataManager.configure(rlmmProps);
    }

    public void startup() {
        // Initialize and configure RSM and RLMM. This will start RSM, RLMM resources which may need to start resources
        // in connecting to the brokers or remote storages.
        configureRSM();
        configureRLMM();
    }

    public RemoteStorageManager storageManager() {
        return remoteLogStorageManager;
    }

    private Stream<Partition> filterPartitions(Set<Partition> partitions) {
        // We are not specifically checking for internal topics etc here as `log.remoteLogEnabled()` already handles that.
        return partitions.stream().filter(partition -> partition.log().exists(UnifiedLog::remoteLogEnabled));
    }

    private void cacheTopicPartitionIds(TopicIdPartition topicIdPartition) {
        Uuid previousTopicId = topicPartitionIds.put(topicIdPartition.topicPartition(), topicIdPartition.topicId());
        if (previousTopicId != null && previousTopicId != topicIdPartition.topicId()) {
            LOGGER.info("Previous cached topic id {} for {} does not match updated topic id {}",
                    previousTopicId, topicIdPartition.topicPartition(), topicIdPartition.topicId());
        }
    }

    // for testing
    public RLMScheduledThreadPool rlmScheduledThreadPool() {
        return rlmScheduledThreadPool;
    }

    /**
     * Callback to receive any leadership changes for the topic partitions assigned to this broker. If there are no
     * existing tasks for a given topic partition then it will assign new leader or follower task else it will convert the
     * task to respective target state(leader or follower).
     *
     * @param partitionsBecomeLeader   partitions that have become leaders on this broker.
     * @param partitionsBecomeFollower partitions that have become followers on this broker.
     * @param topicIds                 topic name to topic id mappings.
     */
    public void onLeadershipChange(Set<Partition> partitionsBecomeLeader,
                                   Set<Partition> partitionsBecomeFollower,
                                   Map<String, Uuid> topicIds) {
        LOGGER.debug("Received leadership changes for leaders: {} and followers: {}", partitionsBecomeLeader, partitionsBecomeFollower);

        Map<TopicIdPartition, Integer> leaderPartitionsWithLeaderEpoch = filterPartitions(partitionsBecomeLeader)
                .collect(Collectors.toMap(
                        partition -> new TopicIdPartition(topicIds.get(partition.topic()), partition.topicPartition()),
                        Partition::getLeaderEpoch));
        Set<TopicIdPartition> leaderPartitions = leaderPartitionsWithLeaderEpoch.keySet();

        Set<TopicIdPartition> followerPartitions = filterPartitions(partitionsBecomeFollower)
                .map(p -> new TopicIdPartition(topicIds.get(p.topic()), p.topicPartition())).collect(Collectors.toSet());

        if (!leaderPartitions.isEmpty() || !followerPartitions.isEmpty()) {
            LOGGER.debug("Effective topic partitions after filtering compact and internal topics, leaders: {} and followers: {}",
                    leaderPartitions, followerPartitions);

            leaderPartitions.forEach(this::cacheTopicPartitionIds);
            followerPartitions.forEach(this::cacheTopicPartitionIds);

            remoteLogMetadataManager.onPartitionLeadershipChanges(leaderPartitions, followerPartitions);
            followerPartitions.forEach(topicIdPartition ->
                    doHandleLeaderOrFollowerPartitions(topicIdPartition, RLMTask::convertToFollower));

            leaderPartitionsWithLeaderEpoch.forEach((topicIdPartition, leaderEpoch) ->
                    doHandleLeaderOrFollowerPartitions(topicIdPartition,
                            rlmTask -> rlmTask.convertToLeader(leaderEpoch)));
        }
    }

    /**
     * Deletes the internal topic partition info if delete flag is set as true.
     *
     * @param topicPartition topic partition to be stopped.
     * @param delete         flag to indicate whether the given topic partitions to be deleted or not.
     */
    public void stopPartitions(TopicPartition topicPartition, boolean delete) {
        if (delete) {
            // Delete from internal datastructures only if it is to be deleted.
            Uuid topicIdPartition = topicPartitionIds.remove(topicPartition);
            LOGGER.debug("Removed partition: {} from topicPartitionIds", topicIdPartition);
        }
    }

    public Optional<RemoteLogSegmentMetadata> fetchRemoteLogSegmentMetadata(TopicPartition topicPartition,
                                                                            int epochForOffset,
                                                                            long offset) throws RemoteStorageException {
        Uuid topicId = topicPartitionIds.get(topicPartition);

        if (topicId == null) {
            throw new KafkaException("No topic id registered for topic partition: " + topicPartition);
        }
        return remoteLogMetadataManager.remoteLogSegmentMetadata(new TopicIdPartition(topicId, topicPartition), epochForOffset, offset);
    }

    private Optional<FileRecords.TimestampAndOffset> lookupTimestamp(RemoteLogSegmentMetadata rlsMetadata, long timestamp, long startingOffset)
            throws RemoteStorageException, IOException {
        int startPos = indexCache.lookupTimestamp(rlsMetadata, timestamp, startingOffset);

        InputStream remoteSegInputStream = null;
        try {
            // Search forward for the position of the last offset that is greater than or equal to the startingOffset
            remoteSegInputStream = remoteLogStorageManager.fetchLogSegment(rlsMetadata, startPos);
            RemoteLogInputStream remoteLogInputStream = new RemoteLogInputStream(remoteSegInputStream);

            while (true) {
                RecordBatch batch = remoteLogInputStream.nextBatch();
                if (batch == null) break;
                if (batch.maxTimestamp() >= timestamp && batch.lastOffset() >= startingOffset) {
                    for (Record record : batch) {
                        if (record.timestamp() >= timestamp && record.offset() >= startingOffset)
                            return Optional.of(new FileRecords.TimestampAndOffset(record.timestamp(), record.offset(), maybeLeaderEpoch(batch.partitionLeaderEpoch())));
                    }
                }
            }

            return Optional.empty();
        } finally {
            Utils.closeQuietly(remoteSegInputStream, "RemoteLogSegmentInputStream");
        }
    }

    private Optional<Integer> maybeLeaderEpoch(int leaderEpoch) {
        return leaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH ? Optional.empty() : Optional.of(leaderEpoch);
    }

    /**
     * Search the message offset in the remote storage based on timestamp and offset.
     * <p>
     * This method returns an option of TimestampOffset. The returned value is determined using the following ordered list of rules:
     * <p>
     * - If there are no messages in the remote storage, return None
     * - If all the messages in the remote storage have smaller offsets, return None
     * - If all the messages in the remote storage have smaller timestamps, return None
     * - Otherwise, return an option of TimestampOffset. The offset is the offset of the first message whose timestamp
     * is greater than or equals to the target timestamp and whose offset is greater than or equals to the startingOffset.
     *
     * @param tp               topic partition in which the offset to be found.
     * @param timestamp        The timestamp to search for.
     * @param startingOffset   The starting offset to search.
     * @param leaderEpochCache LeaderEpochFileCache of the topic partition.
     * @return the timestamp and offset of the first message that meets the requirements. None will be returned if there
     * is no such message.
     */
    public Optional<FileRecords.TimestampAndOffset> findOffsetByTimestamp(TopicPartition tp,
                                                                          long timestamp,
                                                                          long startingOffset,
                                                                          LeaderEpochFileCache leaderEpochCache) throws RemoteStorageException, IOException {
        Uuid topicId = topicPartitionIds.get(tp);
        if (topicId == null) {
            throw new KafkaException("Topic id does not exist for topic partition: " + tp);
        }

        // Get the respective epoch in which the starting-offset exists.
        OptionalInt maybeEpoch = leaderEpochCache.epochForOffset(startingOffset);
        while (maybeEpoch.isPresent()) {
            int epoch = maybeEpoch.getAsInt();

            Iterator<RemoteLogSegmentMetadata> iterator = remoteLogMetadataManager.listRemoteLogSegments(new TopicIdPartition(topicId, tp), epoch);
            while (iterator.hasNext()) {
                RemoteLogSegmentMetadata rlsMetadata = iterator.next();
                if (rlsMetadata.maxTimestampMs() >= timestamp && rlsMetadata.endOffset() >= startingOffset) {
                    return lookupTimestamp(rlsMetadata, timestamp, startingOffset);
                }
            }

            // Move to the next epoch if not found with the current epoch.
            maybeEpoch = leaderEpochCache.nextEpoch(epoch);
        }

        return Optional.empty();
    }

    private static abstract class CancellableRunnable implements Runnable {
        private volatile boolean cancelled = false;

        public void cancel() {
            cancelled = true;
        }

        public boolean isCancelled() {
            return cancelled;
        }
    }

    /**
     * Returns the leader epoch checkpoint by truncating with the given start[exclusive] and end[inclusive] offset
     *
     * @param log         The actual log from where to take the leader-epoch checkpoint
     * @param startOffset The start offset of the checkpoint file (exclusive in the truncation).
     *                    If start offset is 6, then it will retain an entry at offset 6.
     * @param endOffset   The end offset of the checkpoint file (inclusive in the truncation)
     *                    If end offset is 100, then it will remove the entries greater than or equal to 100.
     * @return the truncated leader epoch checkpoint
     */
    InMemoryLeaderEpochCheckpoint getLeaderEpochCheckpoint(UnifiedLog log, long startOffset, long endOffset) {
        InMemoryLeaderEpochCheckpoint checkpoint = new InMemoryLeaderEpochCheckpoint();
        if (log.leaderEpochCache().isDefined()) {
            LeaderEpochFileCache cache = log.leaderEpochCache().get().writeTo(checkpoint);
            if (startOffset >= 0) {
                cache.truncateFromStart(startOffset);
            }
            cache.truncateFromEnd(endOffset);
        }

        return checkpoint;
    }

    class RLMTask extends CancellableRunnable {

        private final TopicIdPartition topicIdPartition;
        private final Logger logger;

        private volatile int leaderEpoch = -1;

        public RLMTask(TopicIdPartition topicIdPartition) {
            this.topicIdPartition = topicIdPartition;
            LogContext logContext = new LogContext("[RemoteLogManager=" + brokerId + " partition=" + topicIdPartition + "] ");
            logger = logContext.logger(RLMTask.class);
        }

        boolean isLeader() {
            return leaderEpoch >= 0;
        }

        // The copiedOffsetOption is OptionalLong.empty() initially for a new leader RLMTask, and needs to be fetched inside the task's run() method.
        private volatile OptionalLong copiedOffsetOption = OptionalLong.empty();

        public void convertToLeader(int leaderEpochVal) {
            if (leaderEpochVal < 0) {
                throw new KafkaException("leaderEpoch value for topic partition " + topicIdPartition + " can not be negative");
            }
            if (this.leaderEpoch != leaderEpochVal) {
                leaderEpoch = leaderEpochVal;
            }
            // Reset readOffset, so that it is set in next run of RLMTask
            copiedOffsetOption = OptionalLong.empty();
        }

        public void convertToFollower() {
            leaderEpoch = -1;
        }

        private void maybeUpdateReadOffset(UnifiedLog log) throws RemoteStorageException {
            if (!copiedOffsetOption.isPresent()) {
                logger.info("Find the highest remote offset for partition: {} after becoming leader, leaderEpoch: {}", topicIdPartition, leaderEpoch);

                // This is found by traversing from the latest leader epoch from leader epoch history and find the highest offset
                // of a segment with that epoch copied into remote storage. If it can not find an entry then it checks for the
                // previous leader epoch till it finds an entry, If there are no entries till the earliest leader epoch in leader
                // epoch cache then it starts copying the segments from the earliest epoch entry's offset.
                copiedOffsetOption = OptionalLong.of(findHighestRemoteOffset(topicIdPartition, log));
            }
        }

        /**
         *  Segments which match the following criteria are eligible for copying to remote storage:
         *  1) Segment is not the active segment and
         *  2) Segment end-offset is less than the last-stable-offset as remote storage should contain only
         *     committed/acked messages
         * @param log The log from which the segments are to be copied
         * @param fromOffset The offset from which the segments are to be copied
         * @param lastStableOffset The last stable offset of the log
         * @return candidate log segments to be copied to remote storage
         */
        List<EnrichedLogSegment> candidateLogSegments(UnifiedLog log, Long fromOffset, Long lastStableOffset) {
            List<EnrichedLogSegment> candidateLogSegments = new ArrayList<>();
            List<LogSegment> segments = JavaConverters.seqAsJavaList(log.logSegments(fromOffset, Long.MAX_VALUE).toSeq());
            if (!segments.isEmpty()) {
                for (int idx = 1; idx < segments.size(); idx++) {
                    LogSegment previousSeg = segments.get(idx - 1);
                    LogSegment currentSeg = segments.get(idx);
                    if (currentSeg.baseOffset() <= lastStableOffset) {
                        candidateLogSegments.add(new EnrichedLogSegment(previousSeg, currentSeg.baseOffset()));
                    }
                }
                // Discard the last active segment
            }
            return candidateLogSegments;
        }

        public void copyLogSegmentsToRemote(UnifiedLog log) throws InterruptedException {
            if (isCancelled())
                return;

            try {
                maybeUpdateReadOffset(log);
                long copiedOffset = copiedOffsetOption.getAsLong();

                // LSO indicates the offset below are ready to be consumed (high-watermark or committed)
                long lso = log.lastStableOffset();
                if (lso < 0) {
                    logger.warn("lastStableOffset for partition {} is {}, which should not be negative.", topicIdPartition, lso);
                } else if (lso > 0 && copiedOffset < lso) {
                    // log-start-offset can be ahead of the copied-offset, when:
                    // 1) log-start-offset gets incremented via delete-records API (or)
                    // 2) enabling the remote log for the first time
                    long fromOffset = Math.max(copiedOffset + 1, log.logStartOffset());
                    List<EnrichedLogSegment> candidateLogSegments = candidateLogSegments(log, fromOffset, lso);
                    logger.debug("Candidate log segments, logStartOffset: {}, copiedOffset: {}, fromOffset: {}, lso: {} " +
                            "and candidateLogSegments: {}", log.logStartOffset(), copiedOffset, fromOffset, lso, candidateLogSegments);
                    if (candidateLogSegments.isEmpty()) {
                        logger.debug("No segments found to be copied for partition {} with copiedOffset: {} and active segment's base-offset: {}",
                                topicIdPartition, copiedOffset, log.activeSegment().baseOffset());
                    } else {
                        for (EnrichedLogSegment candidateLogSegment : candidateLogSegments) {
                            if (isCancelled() || !isLeader()) {
                                logger.info("Skipping copying log segments as the current task state is changed, cancelled: {} leader:{}",
                                        isCancelled(), isLeader());
                                return;
                            }
                            copyLogSegment(log, candidateLogSegment.logSegment, candidateLogSegment.nextSegmentOffset);
                        }
                    }
                } else {
                    logger.debug("Skipping copying segments, current read-offset:{}, and LSO:{}", copiedOffset, lso);
                }
            } catch (InterruptedException ex) {
                throw ex;
            } catch (Exception ex) {
                if (!isCancelled()) {
                    brokerTopicStats.topicStats(log.topicPartition().topic()).failedRemoteCopyRequestRate().mark();
                    brokerTopicStats.allTopicsStats().failedRemoteCopyRequestRate().mark();
                    logger.error("Error occurred while copying log segments of partition: {}", topicIdPartition, ex);
                }
            }
        }

        private void copyLogSegment(UnifiedLog log, LogSegment segment, long nextSegmentBaseOffset) throws InterruptedException, ExecutionException, RemoteStorageException, IOException {
            File logFile = segment.log().file();
            String logFileName = logFile.getName();

            logger.info("Copying {} to remote storage.", logFileName);
            RemoteLogSegmentId id = RemoteLogSegmentId.generateNew(topicIdPartition);

            long endOffset = nextSegmentBaseOffset - 1;
            File producerStateSnapshotFile = log.producerStateManager().fetchSnapshot(nextSegmentBaseOffset).orElse(null);

            List<EpochEntry> epochEntries = getLeaderEpochCheckpoint(log, segment.baseOffset(), nextSegmentBaseOffset).read();
            Map<Integer, Long> segmentLeaderEpochs = new HashMap<>(epochEntries.size());
            epochEntries.forEach(entry -> segmentLeaderEpochs.put(entry.epoch, entry.startOffset));

            RemoteLogSegmentMetadata copySegmentStartedRlsm = new RemoteLogSegmentMetadata(id, segment.baseOffset(), endOffset,
                    segment.largestTimestamp(), brokerId, time.milliseconds(), segment.log().sizeInBytes(),
                    segmentLeaderEpochs);

            remoteLogMetadataManager.addRemoteLogSegmentMetadata(copySegmentStartedRlsm).get();

            ByteBuffer leaderEpochsIndex = getLeaderEpochCheckpoint(log, -1, nextSegmentBaseOffset).readAsByteBuffer();
            LogSegmentData segmentData = new LogSegmentData(logFile.toPath(), toPathIfExists(segment.lazyOffsetIndex().get().file()),
                    toPathIfExists(segment.lazyTimeIndex().get().file()), Optional.ofNullable(toPathIfExists(segment.txnIndex().file())),
                    producerStateSnapshotFile.toPath(), leaderEpochsIndex);
            brokerTopicStats.topicStats(log.topicPartition().topic()).remoteCopyRequestRate().mark();
            brokerTopicStats.allTopicsStats().remoteCopyRequestRate().mark();
            remoteLogStorageManager.copyLogSegmentData(copySegmentStartedRlsm, segmentData);

            RemoteLogSegmentMetadataUpdate copySegmentFinishedRlsm = new RemoteLogSegmentMetadataUpdate(id, time.milliseconds(),
                    RemoteLogSegmentState.COPY_SEGMENT_FINISHED, brokerId);

            remoteLogMetadataManager.updateRemoteLogSegmentMetadata(copySegmentFinishedRlsm).get();
            brokerTopicStats.topicStats(log.topicPartition().topic())
                .remoteCopyBytesRate().mark(copySegmentStartedRlsm.segmentSizeInBytes());
            brokerTopicStats.allTopicsStats().remoteCopyBytesRate().mark(copySegmentStartedRlsm.segmentSizeInBytes());
            copiedOffsetOption = OptionalLong.of(endOffset);
            log.updateHighestOffsetInRemoteStorage(endOffset);
            logger.info("Copied {} to remote storage with segment-id: {}", logFileName, copySegmentFinishedRlsm.remoteLogSegmentId());
        }

        private Path toPathIfExists(File file) {
            return file.exists() ? file.toPath() : null;
        }

        public void run() {
            if (isCancelled())
                return;

            try {
                Optional<UnifiedLog> unifiedLogOptional = fetchLog.apply(topicIdPartition.topicPartition());

                if (!unifiedLogOptional.isPresent()) {
                    return;
                }

                if (isLeader()) {
                    // Copy log segments to remote storage
                    copyLogSegmentsToRemote(unifiedLogOptional.get());
                }
            } catch (InterruptedException ex) {
                if (!isCancelled()) {
                    logger.warn("Current thread for topic-partition-id {} is interrupted, this task won't be rescheduled. " +
                            "Reason: {}", topicIdPartition, ex.getMessage());
                }
            } catch (Exception ex) {
                if (!isCancelled()) {
                    logger.warn("Current task for topic-partition {} received error but it will be scheduled. " +
                            "Reason: {}", topicIdPartition, ex.getMessage());
                }
            }
        }

        public String toString() {
            return this.getClass().toString() + "[" + topicIdPartition + "]";
        }
    }

    public FetchDataInfo read(RemoteStorageFetchInfo remoteStorageFetchInfo) throws RemoteStorageException, IOException {
        int fetchMaxBytes = remoteStorageFetchInfo.fetchMaxBytes;
        TopicPartition tp = remoteStorageFetchInfo.topicPartition;
        FetchRequest.PartitionData fetchInfo = remoteStorageFetchInfo.fetchInfo;

        boolean includeAbortedTxns = remoteStorageFetchInfo.fetchIsolation == FetchIsolation.TXN_COMMITTED;

        long offset = fetchInfo.fetchOffset;
        int maxBytes = Math.min(fetchMaxBytes, fetchInfo.maxBytes);

        Optional<UnifiedLog> logOptional = fetchLog.apply(tp);
        OptionalInt epoch = OptionalInt.empty();

        if (logOptional.isPresent()) {
            Option<LeaderEpochFileCache> leaderEpochCache = logOptional.get().leaderEpochCache();
            if (leaderEpochCache.isDefined()) {
                epoch = leaderEpochCache.get().epochForOffset(offset);
            }
        }

        Optional<RemoteLogSegmentMetadata> rlsMetadataOptional = epoch.isPresent()
                ? fetchRemoteLogSegmentMetadata(tp, epoch.getAsInt(), offset)
                : Optional.empty();

        if (!rlsMetadataOptional.isPresent()) {
            String epochStr = (epoch.isPresent()) ? Integer.toString(epoch.getAsInt()) : "NOT AVAILABLE";
            throw new OffsetOutOfRangeException("Received request for offset " + offset + " for leader epoch "
                    + epochStr + " and partition " + tp + " which does not exist in remote tier.");
        }

        RemoteLogSegmentMetadata remoteLogSegmentMetadata = rlsMetadataOptional.get();
        int startPos = lookupPositionForOffset(remoteLogSegmentMetadata, offset);
        InputStream remoteSegInputStream = null;
        try {
            // Search forward for the position of the last offset that is greater than or equal to the target offset
            remoteSegInputStream = remoteLogStorageManager.fetchLogSegment(remoteLogSegmentMetadata, startPos);
            RemoteLogInputStream remoteLogInputStream = new RemoteLogInputStream(remoteSegInputStream);

            RecordBatch firstBatch = findFirstBatch(remoteLogInputStream, offset);

            if (firstBatch == null)
                return new FetchDataInfo(new LogOffsetMetadata(offset), MemoryRecords.EMPTY, false,
                        includeAbortedTxns ? Optional.of(Collections.emptyList()) : Optional.empty());

            int firstBatchSize = firstBatch.sizeInBytes();
            // An empty record is sent instead of an incomplete batch when
            //  - there is no minimum-one-message constraint and
            //  - the first batch size is more than maximum bytes that can be sent and
            //  - for FetchRequest version 3 or above.
            if (!remoteStorageFetchInfo.minOneMessage &&
                    !remoteStorageFetchInfo.hardMaxBytesLimit &&
                    firstBatchSize > maxBytes) {
                return new FetchDataInfo(new LogOffsetMetadata(offset), MemoryRecords.EMPTY);
            }

            int updatedFetchSize =
                    remoteStorageFetchInfo.minOneMessage && firstBatchSize > maxBytes ? firstBatchSize : maxBytes;

            ByteBuffer buffer = ByteBuffer.allocate(updatedFetchSize);
            int remainingBytes = updatedFetchSize;

            firstBatch.writeTo(buffer);
            remainingBytes -= firstBatchSize;

            if (remainingBytes > 0) {
                // read the input stream until min of (EOF stream or buffer's remaining capacity).
                Utils.readFully(remoteSegInputStream, buffer);
            }
            buffer.flip();

            FetchDataInfo fetchDataInfo = new FetchDataInfo(
                    new LogOffsetMetadata(offset, remoteLogSegmentMetadata.startOffset(), startPos),
                    MemoryRecords.readableRecords(buffer));
            if (includeAbortedTxns) {
                fetchDataInfo = addAbortedTransactions(firstBatch.baseOffset(), remoteLogSegmentMetadata, fetchDataInfo, logOptional.get());
            }

            return fetchDataInfo;
        } finally {
            Utils.closeQuietly(remoteSegInputStream, "RemoteLogSegmentInputStream");
        }
    }

    private int lookupPositionForOffset(RemoteLogSegmentMetadata remoteLogSegmentMetadata, long offset) {
        return indexCache.lookupOffset(remoteLogSegmentMetadata, offset);
    }

    private FetchDataInfo addAbortedTransactions(long startOffset,
                                                 RemoteLogSegmentMetadata segmentMetadata,
                                                 FetchDataInfo fetchInfo,
                                                 UnifiedLog log) throws RemoteStorageException {
        int fetchSize = fetchInfo.records.sizeInBytes();
        OffsetPosition startOffsetPosition = new OffsetPosition(fetchInfo.fetchOffsetMetadata.messageOffset,
                fetchInfo.fetchOffsetMetadata.relativePositionInSegment);

        OffsetIndex offsetIndex = indexCache.getIndexEntry(segmentMetadata).offsetIndex();
        long upperBoundOffset = offsetIndex.fetchUpperBoundOffset(startOffsetPosition, fetchSize)
                .map(x -> x.offset).orElse(segmentMetadata.endOffset() + 1);

        final Set<FetchResponseData.AbortedTransaction> abortedTransactions = new HashSet<>();

        Consumer<List<AbortedTxn>> accumulator =
                abortedTxns -> abortedTransactions.addAll(abortedTxns.stream()
                        .map(AbortedTxn::asAbortedTransaction).collect(Collectors.toList()));

        collectAbortedTransactions(startOffset, upperBoundOffset, segmentMetadata, accumulator, log);

        return new FetchDataInfo(fetchInfo.fetchOffsetMetadata,
                fetchInfo.records,
                fetchInfo.firstEntryIncomplete,
                Optional.of(abortedTransactions.isEmpty() ? Collections.emptyList() : new ArrayList<>(abortedTransactions)));
    }

    private void collectAbortedTransactions(long startOffset,
                                            long upperBoundOffset,
                                            RemoteLogSegmentMetadata segmentMetadata,
                                            Consumer<List<AbortedTxn>> accumulator,
                                            UnifiedLog log) throws RemoteStorageException {
        // Search in remote segments first.
        Optional<RemoteLogSegmentMetadata> nextSegmentMetadataOpt = Optional.of(segmentMetadata);
        while (nextSegmentMetadataOpt.isPresent()) {
            Optional<TransactionIndex> txnIndexOpt = nextSegmentMetadataOpt.map(metadata -> indexCache.getIndexEntry(metadata).txnIndex());
            if (txnIndexOpt.isPresent()) {
                TxnIndexSearchResult searchResult = txnIndexOpt.get().collectAbortedTxns(startOffset, upperBoundOffset);
                accumulator.accept(searchResult.abortedTransactions);
                if (searchResult.isComplete) {
                    // Return immediately when the search result is complete, it does not need to go through local log segments.
                    return;
                }
            }

            nextSegmentMetadataOpt = findNextSegmentMetadata(nextSegmentMetadataOpt.get(), log.leaderEpochCache());
        }

        // Search in local segments
        collectAbortedTransactionInLocalSegments(startOffset, upperBoundOffset, accumulator, JavaConverters.asJavaIterator(log.logSegments().iterator()));
    }

    private void collectAbortedTransactionInLocalSegments(long startOffset,
                                                          long upperBoundOffset,
                                                          Consumer<List<AbortedTxn>> accumulator,
                                                          Iterator<LogSegment> localLogSegments) {
        while (localLogSegments.hasNext()) {
            TransactionIndex txnIndex = localLogSegments.next().txnIndex();
            if (txnIndex != null) {
                TxnIndexSearchResult searchResult = txnIndex.collectAbortedTxns(startOffset, upperBoundOffset);
                accumulator.accept(searchResult.abortedTransactions);
                if (searchResult.isComplete) {
                    return;
                }
            }
        }
    }

    private Optional<RemoteLogSegmentMetadata> findNextSegmentMetadata(RemoteLogSegmentMetadata segmentMetadata,
                                                                       Option<LeaderEpochFileCache> leaderEpochFileCacheOption) throws RemoteStorageException {
        if (leaderEpochFileCacheOption.isEmpty()) {
            return Optional.empty();
        }

        long nextSegmentBaseOffset = segmentMetadata.endOffset() + 1;
        OptionalInt epoch = leaderEpochFileCacheOption.get().epochForOffset(nextSegmentBaseOffset);
        return epoch.isPresent()
                ? fetchRemoteLogSegmentMetadata(segmentMetadata.topicIdPartition().topicPartition(), epoch.getAsInt(), nextSegmentBaseOffset)
                : Optional.empty();
    }

    private RecordBatch findFirstBatch(RemoteLogInputStream remoteLogInputStream, long offset) throws IOException {
        RecordBatch nextBatch;
        // Look for the batch which has the desired offset
        // We will always have a batch in that segment as it is a non-compacted topic.
        do {
            nextBatch = remoteLogInputStream.nextBatch();
        } while (nextBatch != null && nextBatch.lastOffset() < offset);

        return nextBatch;
    }

    long findHighestRemoteOffset(TopicIdPartition topicIdPartition, UnifiedLog log) throws RemoteStorageException {
        Optional<Long> offset = Optional.empty();

        Option<LeaderEpochFileCache> maybeLeaderEpochFileCache = log.leaderEpochCache();
        if (maybeLeaderEpochFileCache.isDefined()) {
            LeaderEpochFileCache cache = maybeLeaderEpochFileCache.get();
            OptionalInt epoch = cache.latestEpoch();
            while (!offset.isPresent() && epoch.isPresent()) {
                offset = remoteLogMetadataManager.highestOffsetForEpoch(topicIdPartition, epoch.getAsInt());
                epoch = cache.previousEpoch(epoch.getAsInt());
            }
        }

        return offset.orElse(-1L);
    }

    /**
     * Submit a remote log read task.
     * This method returns immediately. The read operation is executed in a thread pool.
     * The callback will be called when the task is done.
     *
     * @throws java.util.concurrent.RejectedExecutionException if the task cannot be accepted for execution (task queue is full)
     */
    public Future<Void> asyncRead(RemoteStorageFetchInfo fetchInfo, Consumer<RemoteLogReadResult> callback) {
        return remoteStorageReaderThreadPool.submit(new RemoteLogReader(fetchInfo, this, callback, brokerTopicStats));
    }

    void doHandleLeaderOrFollowerPartitions(TopicIdPartition topicPartition,
                                            Consumer<RLMTask> convertToLeaderOrFollower) {
        RLMTaskWithFuture rlmTaskWithFuture = leaderOrFollowerTasks.computeIfAbsent(topicPartition,
                topicIdPartition -> {
                    RLMTask task = new RLMTask(topicIdPartition);
                    // set this upfront when it is getting initialized instead of doing it after scheduling.
                    convertToLeaderOrFollower.accept(task);
                    LOGGER.info("Created a new task: {} and getting scheduled", task);
                    ScheduledFuture<?> future = rlmScheduledThreadPool.scheduleWithFixedDelay(task, 0, delayInMs, TimeUnit.MILLISECONDS);
                    return new RLMTaskWithFuture(task, future);
                }
        );
        convertToLeaderOrFollower.accept(rlmTaskWithFuture.rlmTask);
    }

    static class RLMTaskWithFuture {

        private final RLMTask rlmTask;
        private final Future<?> future;

        RLMTaskWithFuture(RLMTask rlmTask, Future<?> future) {
            this.rlmTask = rlmTask;
            this.future = future;
        }

        public void cancel() {
            rlmTask.cancel();
            try {
                future.cancel(true);
            } catch (Exception ex) {
                LOGGER.error("Error occurred while canceling the task: {}", rlmTask, ex);
            }
        }

    }

    /**
     * Closes and releases all the resources like RemoterStorageManager and RemoteLogMetadataManager.
     */
    public void close() {
        synchronized (this) {
            if (!closed) {
                leaderOrFollowerTasks.values().forEach(RLMTaskWithFuture::cancel);
                Utils.closeQuietly(remoteLogStorageManager, "RemoteLogStorageManager");
                Utils.closeQuietly(remoteLogMetadataManager, "RemoteLogMetadataManager");
                Utils.closeQuietly(indexCache, "RemoteIndexCache");

                rlmScheduledThreadPool.close();
                try {
                    shutdownAndAwaitTermination(remoteStorageReaderThreadPool, "RemoteStorageReaderThreadPool", 10, TimeUnit.SECONDS);
                } finally {
                    removeMetrics();
                }

                leaderOrFollowerTasks.clear();
                closed = true;
            }
        }
    }

    private static void shutdownAndAwaitTermination(ExecutorService pool, String poolName, long timeout, TimeUnit timeUnit) {
        // This pattern of shutting down thread pool is adopted from here: https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/ExecutorService.html
        LOGGER.info("Shutting down of thread pool {} is started", poolName);
        pool.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!pool.awaitTermination(timeout, timeUnit)) {
                LOGGER.info("Shutting down of thread pool {} could not be completed. It will retry cancelling the tasks using shutdownNow.", poolName);
                pool.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(timeout, timeUnit))
                    LOGGER.warn("Shutting down of thread pool {} could not be completed even after retrying cancellation of the tasks using shutdownNow.", poolName);
            }
        } catch (InterruptedException ex) {
            // (Re-)Cancel if current thread also interrupted
            LOGGER.warn("Encountered InterruptedException while shutting down thread pool {}. It will retry cancelling the tasks using shutdownNow.", poolName);
            pool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }

        LOGGER.info("Shutting down of thread pool {} is completed", poolName);
    }

    static class RLMScheduledThreadPool {

        private static final Logger LOGGER = LoggerFactory.getLogger(RLMScheduledThreadPool.class);
        private final int poolSize;
        private final ScheduledThreadPoolExecutor scheduledThreadPool;

        public RLMScheduledThreadPool(int poolSize) {
            this.poolSize = poolSize;
            scheduledThreadPool = createPool();
        }

        private ScheduledThreadPoolExecutor createPool() {
            ScheduledThreadPoolExecutor threadPool = new ScheduledThreadPoolExecutor(poolSize);
            threadPool.setRemoveOnCancelPolicy(true);
            threadPool.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            threadPool.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
            threadPool.setThreadFactory(new ThreadFactory() {
                private final AtomicInteger sequence = new AtomicInteger();

                public Thread newThread(Runnable r) {
                    return KafkaThread.daemon("kafka-rlm-thread-pool-" + sequence.incrementAndGet(), r);
                }
            });

            return threadPool;
        }

        public Double getIdlePercent() {
            return 1 - (double) scheduledThreadPool.getActiveCount() / (double) scheduledThreadPool.getCorePoolSize();
        }

        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable runnable, long initialDelay, long delay, TimeUnit timeUnit) {
            LOGGER.info("Scheduling runnable {} with initial delay: {}, fixed delay: {}", runnable, initialDelay, delay);
            return scheduledThreadPool.scheduleWithFixedDelay(runnable, initialDelay, delay, timeUnit);
        }

        public void close() {
            shutdownAndAwaitTermination(scheduledThreadPool, "RLMScheduledThreadPool", 10, TimeUnit.SECONDS);
        }
    }

    // Visible for testing
    static class EnrichedLogSegment {
        private final LogSegment logSegment;
        private final long nextSegmentOffset;

        public EnrichedLogSegment(LogSegment logSegment,
                                  long nextSegmentOffset) {
            this.logSegment = logSegment;
            this.nextSegmentOffset = nextSegmentOffset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EnrichedLogSegment that = (EnrichedLogSegment) o;
            return nextSegmentOffset == that.nextSegmentOffset && Objects.equals(logSegment, that.logSegment);
        }

        @Override
        public int hashCode() {
            return Objects.hash(logSegment, nextSegmentOffset);
        }

        @Override
        public String toString() {
            return "EnrichedLogSegment{" +
                    "logSegment=" + logSegment +
                    ", nextSegmentOffset=" + nextSegmentOffset +
                    '}';
        }
    }

}
