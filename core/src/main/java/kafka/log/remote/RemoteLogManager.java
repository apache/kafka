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

import kafka.cluster.Partition;
import kafka.log.LogSegment;
import kafka.log.UnifiedLog;
import kafka.server.KafkaConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.RemoteLogInputStream;
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
import org.apache.kafka.storage.internals.checkpoint.InMemoryLeaderEpochCheckpoint;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.internals.log.EpochEntry;
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
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

/**
 * This class is responsible for
 * - initializing `RemoteStorageManager` and `RemoteLogMetadataManager` instances
 * - receives any leader and follower replica events and partition stop events and act on them
 * - also provides APIs to fetch indexes, metadata about remote log segments
 * - copying log segments to remote storage
 */
public class RemoteLogManager implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteLogManager.class);

    private final RemoteLogManagerConfig rlmConfig;
    private final int brokerId;
    private final String logDir;
    private final Time time;
    private final Function<TopicPartition, Optional<UnifiedLog>> fetchLog;

    private final RemoteStorageManager remoteLogStorageManager;

    private final RemoteLogMetadataManager remoteLogMetadataManager;

    private final RemoteIndexCache indexCache;

    private final RLMScheduledThreadPool rlmScheduledThreadPool;

    private final long delayInMs;

    private final ConcurrentHashMap<TopicIdPartition, RLMTaskWithFuture> leaderOrFollowerTasks = new ConcurrentHashMap<>();

    // topic ids that are received on leadership changes, this map is cleared on stop partitions
    private final ConcurrentMap<TopicPartition, Uuid> topicPartitionIds = new ConcurrentHashMap<>();

    private boolean closed = false;

    /**
     * Creates RemoteLogManager instance with the given arguments.
     *
     * @param rlmConfig Configuration required for remote logging subsystem(tiered storage) at the broker level.
     * @param brokerId  id of the current broker.
     * @param logDir    directory of Kafka log segments.
     * @param time      Time instance.
     * @param fetchLog  function to get UnifiedLog instance for a given topic.
     */
    public RemoteLogManager(RemoteLogManagerConfig rlmConfig,
                            int brokerId,
                            String logDir,
                            Time time,
                            Function<TopicPartition, Optional<UnifiedLog>> fetchLog) {

        this.rlmConfig = rlmConfig;
        this.brokerId = brokerId;
        this.logDir = logDir;
        this.time = time;
        this.fetchLog = fetchLog;

        remoteLogStorageManager = createRemoteStorageManager();
        remoteLogMetadataManager = createRemoteLogMetadataManager();
        indexCache = new RemoteIndexCache(1024, remoteLogStorageManager, logDir);
        delayInMs = rlmConfig.remoteLogManagerTaskIntervalMs();
        rlmScheduledThreadPool = new RLMScheduledThreadPool(rlmConfig.remoteLogManagerThreadPoolSize());
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
            private String classPath = rlmConfig.remoteLogMetadataManagerClassPath();

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

    private void configureRLMM() {
        final Map<String, Object> rlmmProps = new HashMap<>(rlmConfig.remoteLogMetadataManagerProps());

        rlmmProps.put(KafkaConfig.BrokerIdProp(), brokerId);
        rlmmProps.put(KafkaConfig.LogDirProp(), logDir);
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
                        partition -> partition.getLeaderEpoch()));
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
                    doHandleLeaderOrFollowerPartitions(topicIdPartition, rlmTask -> rlmTask.convertToFollower()));

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

        private void maybeUpdateReadOffset() throws RemoteStorageException {
            if (!copiedOffsetOption.isPresent()) {
                logger.info("Find the highest remote offset for partition: {} after becoming leader, leaderEpoch: {}", topicIdPartition, leaderEpoch);

                // This is found by traversing from the latest leader epoch from leader epoch history and find the highest offset
                // of a segment with that epoch copied into remote storage. If it can not find an entry then it checks for the
                // previous leader epoch till it finds an entry, If there are no entries till the earliest leader epoch in leader
                // epoch cache then it starts copying the segments from the earliest epoch entry's offset.
                copiedOffsetOption = OptionalLong.of(findHighestRemoteOffset(topicIdPartition));
            }
        }

        public void copyLogSegmentsToRemote() throws InterruptedException {
            if (isCancelled())
                return;

            try {
                maybeUpdateReadOffset();
                long copiedOffset = copiedOffsetOption.getAsLong();
                Optional<UnifiedLog> maybeLog = fetchLog.apply(topicIdPartition.topicPartition());
                if (!maybeLog.isPresent()) {
                    return;
                }

                UnifiedLog log = maybeLog.get();

                // LSO indicates the offset below are ready to be consumed (high-watermark or committed)
                long lso = log.lastStableOffset();
                if (lso < 0) {
                    logger.warn("lastStableOffset for partition {} is {}, which should not be negative.", topicIdPartition, lso);
                } else if (lso > 0 && copiedOffset < lso) {
                    // Copy segments only till the last-stable-offset as remote storage should contain only committed/acked
                    // messages
                    long toOffset = lso;
                    logger.debug("Checking for segments to copy, copiedOffset: {} and toOffset: {}", copiedOffset, toOffset);
                    long activeSegBaseOffset = log.activeSegment().baseOffset();
                    // log-start-offset can be ahead of the read-offset, when:
                    // 1) log-start-offset gets incremented via delete-records API (or)
                    // 2) enabling the remote log for the first time
                    long fromOffset = Math.max(copiedOffset + 1, log.logStartOffset());
                    ArrayList<LogSegment> sortedSegments = new ArrayList<>(JavaConverters.asJavaCollection(log.logSegments(fromOffset, toOffset)));
                    sortedSegments.sort(Comparator.comparingLong(LogSegment::baseOffset));
                    List<Long> sortedBaseOffsets = sortedSegments.stream().map(x -> x.baseOffset()).collect(Collectors.toList());
                    int activeSegIndex = Collections.binarySearch(sortedBaseOffsets, activeSegBaseOffset);

                    // sortedSegments becomes empty list when fromOffset and toOffset are same, and activeSegIndex becomes -1
                    if (activeSegIndex < 0) {
                        logger.debug("No segments found to be copied for partition {} with copiedOffset: {} and active segment's base-offset: {}",
                                topicIdPartition, copiedOffset, activeSegBaseOffset);
                    } else {
                        ListIterator<LogSegment> logSegmentsIter = sortedSegments.subList(0, activeSegIndex).listIterator();
                        while (logSegmentsIter.hasNext()) {
                            LogSegment segment = logSegmentsIter.next();
                            if (isCancelled() || !isLeader()) {
                                logger.info("Skipping copying log segments as the current task state is changed, cancelled: {} leader:{}",
                                        isCancelled(), isLeader());
                                return;
                            }

                            copyLogSegment(log, segment, getNextSegmentBaseOffset(activeSegBaseOffset, logSegmentsIter));
                        }
                    }
                } else {
                    logger.debug("Skipping copying segments, current read-offset:{}, and LSO:{}", copiedOffset, lso);
                }
            } catch (InterruptedException ex) {
                throw ex;
            } catch (Exception ex) {
                if (!isCancelled()) {
                    logger.error("Error occurred while copying log segments of partition: {}", topicIdPartition, ex);
                }
            }
        }

        private long getNextSegmentBaseOffset(long activeSegBaseOffset, ListIterator<LogSegment> logSegmentsIter) {
            long nextSegmentBaseOffset;
            if (logSegmentsIter.hasNext()) {
                nextSegmentBaseOffset = logSegmentsIter.next().baseOffset();
                logSegmentsIter.previous();
            } else {
                nextSegmentBaseOffset = activeSegBaseOffset;
            }

            return nextSegmentBaseOffset;
        }

        private void copyLogSegment(UnifiedLog log, LogSegment segment, long nextSegmentBaseOffset) throws InterruptedException, ExecutionException, RemoteStorageException, IOException {
            File logFile = segment.log().file();
            String logFileName = logFile.getName();

            logger.info("Copying {} to remote storage.", logFileName);
            RemoteLogSegmentId id = new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid());

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
            remoteLogStorageManager.copyLogSegmentData(copySegmentStartedRlsm, segmentData);

            RemoteLogSegmentMetadataUpdate copySegmentFinishedRlsm = new RemoteLogSegmentMetadataUpdate(id, time.milliseconds(),
                    RemoteLogSegmentState.COPY_SEGMENT_FINISHED, brokerId);

            remoteLogMetadataManager.updateRemoteLogSegmentMetadata(copySegmentFinishedRlsm).get();

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
                if (isLeader()) {
                    // Copy log segments to remote storage
                    copyLogSegmentsToRemote();
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

    long findHighestRemoteOffset(TopicIdPartition topicIdPartition) throws RemoteStorageException {
        Optional<Long> offset = Optional.empty();
        Optional<UnifiedLog> maybeLog = fetchLog.apply(topicIdPartition.topicPartition());
        if (maybeLog.isPresent()) {
            UnifiedLog log = maybeLog.get();
            Option<LeaderEpochFileCache> maybeLeaderEpochFileCache = log.leaderEpochCache();
            if (maybeLeaderEpochFileCache.isDefined()) {
                LeaderEpochFileCache cache = maybeLeaderEpochFileCache.get();
                OptionalInt epoch = cache.latestEpoch();
                while (!offset.isPresent() && epoch.isPresent()) {
                    offset = remoteLogMetadataManager.highestOffsetForEpoch(topicIdPartition, epoch.getAsInt());
                    epoch = cache.previousEpoch(epoch.getAsInt());
                }
            }
        }

        return offset.orElse(-1L);
    }

    void doHandleLeaderOrFollowerPartitions(TopicIdPartition topicPartition,
                                            Consumer<RLMTask> convertToLeaderOrFollower) {
        RLMTaskWithFuture rlmTaskWithFuture = leaderOrFollowerTasks.computeIfAbsent(topicPartition,
                topicIdPartition -> {
                    RLMTask task = new RLMTask(topicIdPartition);
                    // set this upfront when it is getting initialized instead of doing it after scheduling.
                    convertToLeaderOrFollower.accept(task);
                    LOGGER.info("Created a new task: {} and getting scheduled", task);
                    ScheduledFuture future = rlmScheduledThreadPool.scheduleWithFixedDelay(task, 0, delayInMs, TimeUnit.MILLISECONDS);
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
                try {
                    rlmScheduledThreadPool.shutdown();
                } catch (InterruptedException e) {
                    // ignore
                }
                leaderOrFollowerTasks.clear();
                closed = true;
            }
        }
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

        public ScheduledFuture scheduleWithFixedDelay(Runnable runnable, long initialDelay, long delay, TimeUnit timeUnit) {
            LOGGER.info("Scheduling runnable {} with initial delay: {}, fixed delay: {}", runnable, initialDelay, delay);
            return scheduledThreadPool.scheduleWithFixedDelay(runnable, initialDelay, delay, timeUnit);
        }

        public boolean shutdown() throws InterruptedException {
            LOGGER.info("Shutting down scheduled thread pool");
            scheduledThreadPool.shutdownNow();
            //waits for 2 mins to terminate the current tasks
            return scheduledThreadPool.awaitTermination(2, TimeUnit.MINUTES);
        }
    }

}