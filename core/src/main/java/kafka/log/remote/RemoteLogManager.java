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
import kafka.log.UnifiedLog;
import kafka.server.BrokerTopicStats;
import kafka.server.KafkaConfig;
import kafka.server.StopPartition;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.errors.RetriableException;
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
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.server.log.remote.metadata.storage.ClassLoaderAwareRemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.ClassLoaderAwareRemoteStorageManager;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata.CustomMetadata;
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
import org.apache.kafka.storage.internals.log.LogSegment;
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
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
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
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageMetrics.REMOTE_LOG_MANAGER_TASKS_AVG_IDLE_PERCENT_METRIC;

/**
 * This class is responsible for
 * - initializing `RemoteStorageManager` and `RemoteLogMetadataManager` instances
 * - receives any leader and follower replica events and partition stop events and act on them
 * - also provides APIs to fetch indexes, metadata about remote log segments
 * - copying log segments to the remote storage
 * - cleaning up segments that are expired based on retention size or retention time
 */
public class RemoteLogManager implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteLogManager.class);
    private static final String REMOTE_LOG_READER_THREAD_NAME_PREFIX = "remote-log-reader";
    private final RemoteLogManagerConfig rlmConfig;
    private final int brokerId;
    private final String logDir;
    private final Time time;
    private final Function<TopicPartition, Optional<UnifiedLog>> fetchLog;
    private final BiConsumer<TopicPartition, Long> updateRemoteLogStartOffset;
    private final BrokerTopicStats brokerTopicStats;

    private final RemoteStorageManager remoteLogStorageManager;

    private final RemoteLogMetadataManager remoteLogMetadataManager;

    private final RemoteIndexCache indexCache;
    private final RemoteStorageThreadPool remoteStorageReaderThreadPool;
    private final RLMScheduledThreadPool rlmScheduledThreadPool;

    private final long delayInMs;

    private final ConcurrentHashMap<TopicIdPartition, RLMTaskWithFuture> leaderOrFollowerTasks = new ConcurrentHashMap<>();

    // topic ids that are received on leadership changes, this map is cleared on stop partitions
    private final ConcurrentMap<TopicPartition, Uuid> topicIdByPartitionMap = new ConcurrentHashMap<>();
    private final String clusterId;
    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(this.getClass());

    // The endpoint for remote log metadata manager to connect to
    private Optional<EndPoint> endpoint = Optional.empty();
    private boolean closed = false;

    /**
     * Creates RemoteLogManager instance with the given arguments.
     *
     * @param rlmConfig Configuration required for remote logging subsystem(tiered storage) at the broker level.
     * @param brokerId  id of the current broker.
     * @param logDir    directory of Kafka log segments.
     * @param time      Time instance.
     * @param clusterId The cluster id.
     * @param fetchLog  function to get UnifiedLog instance for a given topic.
     * @param updateRemoteLogStartOffset function to update the log-start-offset for a given topic partition.
     * @param brokerTopicStats BrokerTopicStats instance to update the respective metrics.
     */
    public RemoteLogManager(RemoteLogManagerConfig rlmConfig,
                            int brokerId,
                            String logDir,
                            String clusterId,
                            Time time,
                            Function<TopicPartition, Optional<UnifiedLog>> fetchLog,
                            BiConsumer<TopicPartition, Long> updateRemoteLogStartOffset,
                            BrokerTopicStats brokerTopicStats) throws IOException {
        this.rlmConfig = rlmConfig;
        this.brokerId = brokerId;
        this.logDir = logDir;
        this.clusterId = clusterId;
        this.time = time;
        this.fetchLog = fetchLog;
        this.updateRemoteLogStartOffset = updateRemoteLogStartOffset;
        this.brokerTopicStats = brokerTopicStats;

        remoteLogStorageManager = createRemoteStorageManager();
        remoteLogMetadataManager = createRemoteLogMetadataManager();
        indexCache = new RemoteIndexCache(rlmConfig.remoteLogIndexFileCacheTotalSizeBytes(), remoteLogStorageManager, logDir);
        delayInMs = rlmConfig.remoteLogManagerTaskIntervalMs();
        rlmScheduledThreadPool = new RLMScheduledThreadPool(rlmConfig.remoteLogManagerThreadPoolSize());

        metricsGroup.newGauge(REMOTE_LOG_MANAGER_TASKS_AVG_IDLE_PERCENT_METRIC.getName(), new Gauge<Double>() {
            @Override
            public Double value() {
                return rlmScheduledThreadPool.getIdlePercent();
            }
        });

        remoteStorageReaderThreadPool = new RemoteStorageThreadPool(
                REMOTE_LOG_READER_THREAD_NAME_PREFIX,
                rlmConfig.remoteLogReaderThreads(),
                rlmConfig.remoteLogReaderMaxPendingTasks()
        );
    }

    public void resizeCacheSize(long remoteLogIndexFileCacheSize) {
        indexCache.resizeCacheSize(remoteLogIndexFileCacheSize);
    }

    private void removeMetrics() {
        metricsGroup.removeMetric(REMOTE_LOG_MANAGER_TASKS_AVG_IDLE_PERCENT_METRIC.getName());
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
        return java.security.AccessController.doPrivileged(new PrivilegedAction<RemoteStorageManager>() {
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
        return java.security.AccessController.doPrivileged(new PrivilegedAction<RemoteLogMetadataManager>() {
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
        final Map<String, Object> rlmmProps = new HashMap<>();
        endpoint.ifPresent(e -> {
            rlmmProps.put(REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX + "bootstrap.servers", e.host() + ":" + e.port());
            rlmmProps.put(REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX + "security.protocol", e.securityProtocol().name);
        });
        // update the remoteLogMetadataProps here to override endpoint config if any
        rlmmProps.putAll(rlmConfig.remoteLogMetadataManagerProps());

        rlmmProps.put(KafkaConfig.BrokerIdProp(), brokerId);
        rlmmProps.put(KafkaConfig.LogDirProp(), logDir);
        rlmmProps.put("cluster.id", clusterId);

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
        Uuid previousTopicId = topicIdByPartitionMap.put(topicIdPartition.topicPartition(), topicIdPartition.topicId());
        if (previousTopicId != null && !previousTopicId.equals(topicIdPartition.topicId())) {
            LOGGER.info("Previous cached topic id {} for {} does not match updated topic id {}",
                    previousTopicId, topicIdPartition.topicPartition(), topicIdPartition.topicId());
        }
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
            followerPartitions.forEach(this::removeRemoteTopicPartitionMetrics);

            remoteLogMetadataManager.onPartitionLeadershipChanges(leaderPartitions, followerPartitions);
            followerPartitions.forEach(topicIdPartition ->
                    doHandleLeaderOrFollowerPartitions(topicIdPartition, RLMTask::convertToFollower));

            leaderPartitionsWithLeaderEpoch.forEach((topicIdPartition, leaderEpoch) ->
                    doHandleLeaderOrFollowerPartitions(topicIdPartition,
                            rlmTask -> rlmTask.convertToLeader(leaderEpoch)));
        }
    }

    /**
     * Stop the remote-log-manager task for the given partitions. And, calls the
     * {@link RemoteLogMetadataManager#onStopPartitions(Set)} when {@link StopPartition#deleteLocalLog()} is true.
     * Deletes the partitions from the remote storage when {@link StopPartition#deleteRemoteLog()} is true.
     *
     * @param stopPartitions topic partitions that needs to be stopped.
     * @param errorHandler   callback to handle any errors while stopping the partitions.
     */
    public void stopPartitions(Set<StopPartition> stopPartitions,
                               BiConsumer<TopicPartition, Throwable> errorHandler) {
        LOGGER.debug("Stop partitions: {}", stopPartitions);
        for (StopPartition stopPartition: stopPartitions) {
            TopicPartition tp = stopPartition.topicPartition();
            try {
                if (topicIdByPartitionMap.containsKey(tp)) {
                    TopicIdPartition tpId = new TopicIdPartition(topicIdByPartitionMap.get(tp), tp);
                    RLMTaskWithFuture task = leaderOrFollowerTasks.remove(tpId);
                    if (task != null) {
                        LOGGER.info("Cancelling the RLM task for tpId: {}", tpId);
                        task.cancel();
                    }

                    removeRemoteTopicPartitionMetrics(tpId);

                    if (stopPartition.deleteRemoteLog()) {
                        LOGGER.info("Deleting the remote log segments task for partition: {}", tpId);
                        deleteRemoteLogPartition(tpId);
                    }
                } else {
                    LOGGER.warn("StopPartition call is not expected for partition: {}", tp);
                }
            } catch (Exception ex) {
                errorHandler.accept(tp, ex);
                LOGGER.error("Error while stopping the partition: {}", stopPartition, ex);
            }
        }
        // Note `deleteLocalLog` will always be true when `deleteRemoteLog` is true but not the other way around.
        Set<TopicIdPartition> deleteLocalPartitions = stopPartitions.stream()
                .filter(sp -> sp.deleteLocalLog() && topicIdByPartitionMap.containsKey(sp.topicPartition()))
                .map(sp -> new TopicIdPartition(topicIdByPartitionMap.get(sp.topicPartition()), sp.topicPartition()))
                .collect(Collectors.toSet());
        if (!deleteLocalPartitions.isEmpty()) {
            // NOTE: In ZK mode, this#stopPartitions method is called when Replica state changes to Offline and
            // ReplicaDeletionStarted
            remoteLogMetadataManager.onStopPartitions(deleteLocalPartitions);
            deleteLocalPartitions.forEach(tpId -> topicIdByPartitionMap.remove(tpId.topicPartition()));
        }
    }

    private void deleteRemoteLogPartition(TopicIdPartition partition) throws RemoteStorageException, ExecutionException, InterruptedException {
        List<RemoteLogSegmentMetadata> metadataList = new ArrayList<>();
        remoteLogMetadataManager.listRemoteLogSegments(partition).forEachRemaining(metadataList::add);

        List<RemoteLogSegmentMetadataUpdate> deleteSegmentStartedEvents = metadataList.stream()
                .map(metadata ->
                        new RemoteLogSegmentMetadataUpdate(metadata.remoteLogSegmentId(), time.milliseconds(),
                                metadata.customMetadata(), RemoteLogSegmentState.DELETE_SEGMENT_STARTED, brokerId))
                .collect(Collectors.toList());
        publishEvents(deleteSegmentStartedEvents).get();

        // KAFKA-15313: Delete remote log segments partition asynchronously when a partition is deleted.
        Collection<Uuid> deletedSegmentIds = new ArrayList<>();
        for (RemoteLogSegmentMetadata metadata: metadataList) {
            deletedSegmentIds.add(metadata.remoteLogSegmentId().id());
            remoteLogStorageManager.deleteLogSegmentData(metadata);
        }
        indexCache.removeAll(deletedSegmentIds);

        List<RemoteLogSegmentMetadataUpdate> deleteSegmentFinishedEvents = metadataList.stream()
                .map(metadata ->
                        new RemoteLogSegmentMetadataUpdate(metadata.remoteLogSegmentId(), time.milliseconds(),
                                metadata.customMetadata(), RemoteLogSegmentState.DELETE_SEGMENT_FINISHED, brokerId))
                .collect(Collectors.toList());
        publishEvents(deleteSegmentFinishedEvents).get();
    }

    private CompletableFuture<Void> publishEvents(List<RemoteLogSegmentMetadataUpdate> events) throws RemoteStorageException {
        List<CompletableFuture<Void>> result = new ArrayList<>();
        for (RemoteLogSegmentMetadataUpdate event : events) {
            result.add(remoteLogMetadataManager.updateRemoteLogSegmentMetadata(event));
        }
        return CompletableFuture.allOf(result.toArray(new CompletableFuture[0]));
    }

    public Optional<RemoteLogSegmentMetadata> fetchRemoteLogSegmentMetadata(TopicPartition topicPartition,
                                                                            int epochForOffset,
                                                                            long offset) throws RemoteStorageException {
        Uuid topicId = topicIdByPartitionMap.get(topicPartition);

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
        Uuid topicId = topicIdByPartitionMap.get(tp);
        if (topicId == null) {
            throw new KafkaException("Topic id does not exist for topic partition: " + tp);
        }

        Optional<UnifiedLog> unifiedLogOptional = fetchLog.apply(tp);
        if (!unifiedLogOptional.isPresent()) {
            throw new KafkaException("UnifiedLog does not exist for topic partition: " + tp);
        }

        UnifiedLog unifiedLog = unifiedLogOptional.get();

        // Get the respective epoch in which the starting-offset exists.
        OptionalInt maybeEpoch = leaderEpochCache.epochForOffset(startingOffset);
        TopicIdPartition topicIdPartition = new TopicIdPartition(topicId, tp);
        NavigableMap<Integer, Long> epochWithOffsets = buildFilteredLeaderEpochMap(leaderEpochCache.epochWithOffsets());
        while (maybeEpoch.isPresent()) {
            int epoch = maybeEpoch.getAsInt();

            // KAFKA-15802: Add a new API for RLMM to choose how to implement the predicate.
            // currently, all segments are returned and then iterated, and filtered
            Iterator<RemoteLogSegmentMetadata> iterator = remoteLogMetadataManager.listRemoteLogSegments(topicIdPartition, epoch);
            while (iterator.hasNext()) {
                RemoteLogSegmentMetadata rlsMetadata = iterator.next();
                if (rlsMetadata.maxTimestampMs() >= timestamp
                    && rlsMetadata.endOffset() >= startingOffset
                    && isRemoteSegmentWithinLeaderEpochs(rlsMetadata, unifiedLog.logEndOffset(), epochWithOffsets)
                    && rlsMetadata.state().equals(RemoteLogSegmentState.COPY_SEGMENT_FINISHED)) {
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
        private final int customMetadataSizeLimit;
        private final Logger logger;

        private volatile int leaderEpoch = -1;

        public RLMTask(TopicIdPartition topicIdPartition, int customMetadataSizeLimit) {
            this.topicIdPartition = topicIdPartition;
            this.customMetadataSizeLimit = customMetadataSizeLimit;
            LogContext logContext = new LogContext("[RemoteLogManager=" + brokerId + " partition=" + topicIdPartition + "] ");
            logger = logContext.logger(RLMTask.class);
        }

        boolean isLeader() {
            return leaderEpoch >= 0;
        }

        // The copied and log-start offset is empty initially for a new leader RLMTask, and needs to be fetched inside
        // the task's run() method.
        private volatile Optional<OffsetAndEpoch> copiedOffsetOption = Optional.empty();
        private volatile boolean isLogStartOffsetUpdatedOnBecomingLeader = false;

        public void convertToLeader(int leaderEpochVal) {
            if (leaderEpochVal < 0) {
                throw new KafkaException("leaderEpoch value for topic partition " + topicIdPartition + " can not be negative");
            }
            if (this.leaderEpoch != leaderEpochVal) {
                leaderEpoch = leaderEpochVal;
            }
            // Reset copied and log-start offset, so that it is set in next run of RLMTask
            copiedOffsetOption = Optional.empty();
            isLogStartOffsetUpdatedOnBecomingLeader = false;
        }

        public void convertToFollower() {
            leaderEpoch = -1;
        }

        private void maybeUpdateLogStartOffsetOnBecomingLeader(UnifiedLog log) throws RemoteStorageException {
            if (!isLogStartOffsetUpdatedOnBecomingLeader) {
                long logStartOffset = findLogStartOffset(topicIdPartition, log);
                updateRemoteLogStartOffset.accept(topicIdPartition.topicPartition(), logStartOffset);
                isLogStartOffsetUpdatedOnBecomingLeader = true;
                logger.info("Found the logStartOffset: {} for partition: {} after becoming leader, leaderEpoch: {}",
                        logStartOffset, topicIdPartition, leaderEpoch);
            }
        }

        private void maybeUpdateCopiedOffset(UnifiedLog log) throws RemoteStorageException {
            if (!copiedOffsetOption.isPresent()) {
                // This is found by traversing from the latest leader epoch from leader epoch history and find the highest offset
                // of a segment with that epoch copied into remote storage. If it can not find an entry then it checks for the
                // previous leader epoch till it finds an entry, If there are no entries till the earliest leader epoch in leader
                // epoch cache then it starts copying the segments from the earliest epoch entry's offset.
                copiedOffsetOption = Optional.of(findHighestRemoteOffset(topicIdPartition, log));
                logger.info("Found the highest copiedRemoteOffset: {} for partition: {} after becoming leader, " +
                                "leaderEpoch: {}", copiedOffsetOption, topicIdPartition, leaderEpoch);
                copiedOffsetOption.ifPresent(offsetAndEpoch ->  log.updateHighestOffsetInRemoteStorage(offsetAndEpoch.offset()));
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
                maybeUpdateLogStartOffsetOnBecomingLeader(log);
                maybeUpdateCopiedOffset(log);
                long copiedOffset = copiedOffsetOption.get().offset();

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
            } catch (CustomMetadataSizeLimitExceededException e) {
                // Only stop this task. Logging is done where the exception is thrown.
                brokerTopicStats.topicStats(log.topicPartition().topic()).failedRemoteCopyRequestRate().mark();
                brokerTopicStats.allTopicsStats().failedRemoteCopyRequestRate().mark();
                this.cancel();
            } catch (InterruptedException | RetriableException ex) {
                throw ex;
            } catch (Exception ex) {
                if (!isCancelled()) {
                    brokerTopicStats.topicStats(log.topicPartition().topic()).failedRemoteCopyRequestRate().mark();
                    brokerTopicStats.allTopicsStats().failedRemoteCopyRequestRate().mark();
                    logger.error("Error occurred while copying log segments of partition: {}", topicIdPartition, ex);
                }
            }
        }

        private void copyLogSegment(UnifiedLog log, LogSegment segment, long nextSegmentBaseOffset)
                throws InterruptedException, ExecutionException, RemoteStorageException, IOException,
                CustomMetadataSizeLimitExceededException {
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
            LogSegmentData segmentData = new LogSegmentData(logFile.toPath(), toPathIfExists(segment.offsetIndex().file()),
                    toPathIfExists(segment.timeIndex().file()), Optional.ofNullable(toPathIfExists(segment.txnIndex().file())),
                    producerStateSnapshotFile.toPath(), leaderEpochsIndex);
            brokerTopicStats.topicStats(log.topicPartition().topic()).remoteCopyRequestRate().mark();
            brokerTopicStats.allTopicsStats().remoteCopyRequestRate().mark();
            Optional<CustomMetadata> customMetadata = remoteLogStorageManager.copyLogSegmentData(copySegmentStartedRlsm, segmentData);

            RemoteLogSegmentMetadataUpdate copySegmentFinishedRlsm = new RemoteLogSegmentMetadataUpdate(id, time.milliseconds(),
                    customMetadata, RemoteLogSegmentState.COPY_SEGMENT_FINISHED, brokerId);

            if (customMetadata.isPresent()) {
                long customMetadataSize = customMetadata.get().value().length;
                if (customMetadataSize > this.customMetadataSizeLimit) {
                    CustomMetadataSizeLimitExceededException e = new CustomMetadataSizeLimitExceededException();
                    logger.error("Custom metadata size {} exceeds configured limit {}." +
                                    " Copying will be stopped and copied segment will be attempted to clean." +
                                    " Original metadata: {}",
                            customMetadataSize, this.customMetadataSizeLimit, copySegmentStartedRlsm, e);
                    try {
                        // For deletion, we provide back the custom metadata by creating a new metadata object from the update.
                        // However, the update itself will not be stored in this case.
                        remoteLogStorageManager.deleteLogSegmentData(copySegmentStartedRlsm.createWithUpdates(copySegmentFinishedRlsm));
                        logger.info("Successfully cleaned segment after custom metadata size exceeded");
                    } catch (RemoteStorageException e1) {
                        logger.error("Error while cleaning segment after custom metadata size exceeded, consider cleaning manually", e1);
                    }
                    throw e;
                }
            }

            remoteLogMetadataManager.updateRemoteLogSegmentMetadata(copySegmentFinishedRlsm).get();
            brokerTopicStats.topicStats(log.topicPartition().topic())
                .remoteCopyBytesRate().mark(copySegmentStartedRlsm.segmentSizeInBytes());
            brokerTopicStats.allTopicsStats().remoteCopyBytesRate().mark(copySegmentStartedRlsm.segmentSizeInBytes());

            // `epochEntries` cannot be empty, there is a pre-condition validation in RemoteLogSegmentMetadata
            // constructor
            int lastEpochInSegment = epochEntries.get(epochEntries.size() - 1).epoch;
            copiedOffsetOption = Optional.of(new OffsetAndEpoch(endOffset, lastEpochInSegment));
            // Update the highest offset in remote storage for this partition's log so that the local log segments
            // are not deleted before they are copied to remote storage.
            log.updateHighestOffsetInRemoteStorage(endOffset);
            logger.info("Copied {} to remote storage with segment-id: {}", logFileName, copySegmentFinishedRlsm.remoteLogSegmentId());

            long bytesLag = log.onlyLocalLogSegmentsSize() - log.activeSegment().size();
            String topic = topicIdPartition.topic();
            int partition = topicIdPartition.partition();
            long segmentsLag = log.onlyLocalLogSegmentsCount();
            brokerTopicStats.recordRemoteCopyLagBytes(topic, partition, bytesLag);
            brokerTopicStats.recordRemoteCopyLagSegments(topic, partition, segmentsLag);
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

                UnifiedLog log = unifiedLogOptional.get();
                if (isLeader()) {
                    // Copy log segments to remote storage
                    copyLogSegmentsToRemote(log);
                    // Cleanup/delete expired remote log segments
                    cleanupExpiredRemoteLogSegments();
                } else {
                    OffsetAndEpoch offsetAndEpoch = findHighestRemoteOffset(topicIdPartition, log);
                    // Update the highest offset in remote storage for this partition's log so that the local log segments
                    // are not deleted before they are copied to remote storage.
                    log.updateHighestOffsetInRemoteStorage(offsetAndEpoch.offset());
                }
            } catch (InterruptedException ex) {
                if (!isCancelled()) {
                    logger.warn("Current thread for topic-partition-id {} is interrupted. Reason: {}", topicIdPartition, ex.getMessage());
                }
            } catch (RetriableException ex) {
                logger.debug("Encountered a retryable error while executing current task for topic-partition {}", topicIdPartition, ex);
            } catch (Exception ex) {
                if (!isCancelled()) {
                    logger.warn("Current task for topic-partition {} received error but it will be scheduled. " +
                            "Reason: {}", topicIdPartition, ex.getMessage());
                }
            }
        }

        public void handleLogStartOffsetUpdate(TopicPartition topicPartition, long remoteLogStartOffset) {
            if (isLeader()) {
                logger.debug("Updating {} with remoteLogStartOffset: {}", topicPartition, remoteLogStartOffset);
                updateRemoteLogStartOffset.accept(topicPartition, remoteLogStartOffset);
            }
        }

        class RemoteLogRetentionHandler {

            private final Optional<RetentionSizeData> retentionSizeData;
            private final Optional<RetentionTimeData> retentionTimeData;

            private long remainingBreachedSize;

            private OptionalLong logStartOffset = OptionalLong.empty();

            public RemoteLogRetentionHandler(Optional<RetentionSizeData> retentionSizeData, Optional<RetentionTimeData> retentionTimeData) {
                this.retentionSizeData = retentionSizeData;
                this.retentionTimeData = retentionTimeData;
                remainingBreachedSize = retentionSizeData.map(sizeData -> sizeData.remainingBreachedSize).orElse(0L);
            }

            private boolean isSegmentBreachedByRetentionSize(RemoteLogSegmentMetadata metadata) {
                boolean shouldDeleteSegment = false;
                if (!retentionSizeData.isPresent()) {
                    return shouldDeleteSegment;
                }
                // Assumption that segments contain size >= 0
                if (remainingBreachedSize > 0) {
                    long remainingBytes = remainingBreachedSize - metadata.segmentSizeInBytes();
                    if (remainingBytes >= 0) {
                        remainingBreachedSize = remainingBytes;
                        shouldDeleteSegment = true;
                    }
                }
                if (shouldDeleteSegment) {
                    logStartOffset = OptionalLong.of(metadata.endOffset() + 1);
                    logger.info("About to delete remote log segment {} due to retention size {} breach. Log size after deletion will be {}.",
                            metadata.remoteLogSegmentId(), retentionSizeData.get().retentionSize, remainingBreachedSize + retentionSizeData.get().retentionSize);
                }
                return shouldDeleteSegment;
            }

            public boolean isSegmentBreachedByRetentionTime(RemoteLogSegmentMetadata metadata) {
                boolean shouldDeleteSegment = false;
                if (!retentionTimeData.isPresent()) {
                    return shouldDeleteSegment;
                }
                shouldDeleteSegment = metadata.maxTimestampMs() <= retentionTimeData.get().cleanupUntilMs;
                if (shouldDeleteSegment) {
                    remainingBreachedSize = Math.max(0, remainingBreachedSize - metadata.segmentSizeInBytes());
                    // It is fine to have logStartOffset as `metadata.endOffset() + 1` as the segment offset intervals
                    // are ascending with in an epoch.
                    logStartOffset = OptionalLong.of(metadata.endOffset() + 1);
                    logger.info("About to delete remote log segment {} due to retention time {}ms breach based on the largest record timestamp in the segment",
                            metadata.remoteLogSegmentId(), retentionTimeData.get().retentionMs);
                }
                return shouldDeleteSegment;
            }

            private boolean isSegmentBreachByLogStartOffset(RemoteLogSegmentMetadata metadata,
                                                            long logStartOffset,
                                                            NavigableMap<Integer, Long> leaderEpochEntries) {
                boolean shouldDeleteSegment = false;
                if (!leaderEpochEntries.isEmpty()) {
                    // Note that `logStartOffset` and `leaderEpochEntries.firstEntry().getValue()` should be same
                    Integer firstEpoch = leaderEpochEntries.firstKey();
                    shouldDeleteSegment = metadata.segmentLeaderEpochs().keySet().stream().allMatch(epoch -> epoch <= firstEpoch)
                            && metadata.endOffset() < logStartOffset;
                }
                if (shouldDeleteSegment) {
                    logger.info("About to delete remote log segment {} due to log-start-offset {} breach. " +
                            "Current earliest-epoch-entry: {}, segment-end-offset: {} and segment-epochs: {}",
                            metadata.remoteLogSegmentId(), logStartOffset, leaderEpochEntries.firstEntry(),
                            metadata.endOffset(), metadata.segmentLeaderEpochs());
                }
                return shouldDeleteSegment;
            }

            // It removes the segments beyond the current leader's earliest epoch. Those segments are considered as
            // unreferenced because they are not part of the current leader epoch lineage.
            private boolean deleteLogSegmentsDueToLeaderEpochCacheTruncation(EpochEntry earliestEpochEntry,
                                                                             RemoteLogSegmentMetadata metadata)
                    throws RemoteStorageException, ExecutionException, InterruptedException {
                boolean isSegmentDeleted = deleteRemoteLogSegment(metadata, ignored ->
                        metadata.segmentLeaderEpochs().keySet().stream().allMatch(epoch -> epoch < earliestEpochEntry.epoch));
                if (isSegmentDeleted) {
                    logger.info("Deleted remote log segment {} due to leader-epoch-cache truncation. " +
                                    "Current earliest-epoch-entry: {}, segment-end-offset: {} and segment-epochs: {}",
                            metadata.remoteLogSegmentId(), earliestEpochEntry, metadata.endOffset(), metadata.segmentLeaderEpochs().keySet());
                }
                // No need to update the log-start-offset as these epochs/offsets are earlier to that value.
                return isSegmentDeleted;
            }

            private boolean deleteRemoteLogSegment(RemoteLogSegmentMetadata segmentMetadata, Predicate<RemoteLogSegmentMetadata> predicate)
                    throws RemoteStorageException, ExecutionException, InterruptedException {
                if (predicate.test(segmentMetadata)) {
                    logger.debug("Deleting remote log segment {}", segmentMetadata.remoteLogSegmentId());

                    String topic = segmentMetadata.topicIdPartition().topic();

                    // Publish delete segment started event.
                    remoteLogMetadataManager.updateRemoteLogSegmentMetadata(
                            new RemoteLogSegmentMetadataUpdate(segmentMetadata.remoteLogSegmentId(), time.milliseconds(),
                                    segmentMetadata.customMetadata(), RemoteLogSegmentState.DELETE_SEGMENT_STARTED, brokerId)).get();

                    brokerTopicStats.topicStats(topic).remoteDeleteRequestRate().mark();
                    brokerTopicStats.allTopicsStats().remoteDeleteRequestRate().mark();

                    // Delete the segment in remote storage.
                    try {
                        remoteLogStorageManager.deleteLogSegmentData(segmentMetadata);
                    } catch (RemoteStorageException e) {
                        brokerTopicStats.topicStats(topic).failedRemoteDeleteRequestRate().mark();
                        brokerTopicStats.allTopicsStats().failedRemoteDeleteRequestRate().mark();
                        throw e;
                    }

                    // Publish delete segment finished event.
                    remoteLogMetadataManager.updateRemoteLogSegmentMetadata(
                            new RemoteLogSegmentMetadataUpdate(segmentMetadata.remoteLogSegmentId(), time.milliseconds(),
                                    segmentMetadata.customMetadata(), RemoteLogSegmentState.DELETE_SEGMENT_FINISHED, brokerId)).get();
                    logger.debug("Deleted remote log segment {}", segmentMetadata.remoteLogSegmentId());
                    return true;
                }
                return false;
            }

        }

        private void updateMetadataCountAndLogSizeWith(int metadataCount, long remoteLogSizeBytes) {
            int partition = topicIdPartition.partition();
            String topic = topicIdPartition.topic();
            brokerTopicStats.recordRemoteLogMetadataCount(topic, partition, metadataCount);
            brokerTopicStats.recordRemoteLogSizeBytes(topic, partition, remoteLogSizeBytes);
        }

        private void updateRemoteDeleteLagWith(int segmentsLeftToDelete, long sizeOfDeletableSegmentsBytes) {
            String topic = topicIdPartition.topic();
            int partition = topicIdPartition.partition();
            brokerTopicStats.recordRemoteDeleteLagSegments(topic, partition, segmentsLeftToDelete);
            brokerTopicStats.recordRemoteDeleteLagBytes(topic, partition, sizeOfDeletableSegmentsBytes);
        }

        void cleanupExpiredRemoteLogSegments() throws RemoteStorageException, ExecutionException, InterruptedException {
            if (isCancelled() || !isLeader()) {
                logger.info("Returning from remote log segments cleanup as the task state is changed");
                return;
            }

            final Optional<UnifiedLog> logOptional = fetchLog.apply(topicIdPartition.topicPartition());
            if (!logOptional.isPresent()) {
                logger.debug("No UnifiedLog instance available for partition: {}", topicIdPartition);
                return;
            }

            final UnifiedLog log = logOptional.get();
            final Option<LeaderEpochFileCache> leaderEpochCacheOption = log.leaderEpochCache();
            if (leaderEpochCacheOption.isEmpty()) {
                logger.debug("No leader epoch cache available for partition: {}", topicIdPartition);
                return;
            }

            // Cleanup remote log segments and update the log start offset if applicable.
            final Iterator<RemoteLogSegmentMetadata> segmentMetadataIter = remoteLogMetadataManager.listRemoteLogSegments(topicIdPartition);
            if (!segmentMetadataIter.hasNext()) {
                updateMetadataCountAndLogSizeWith(0, 0);
                logger.debug("No remote log segments available on remote storage for partition: {}", topicIdPartition);
                return;
            }

            final Set<Integer> epochsSet = new HashSet<>();
            int metadataCount = 0;
            long remoteLogSizeBytes = 0;
            // Good to have an API from RLMM to get all the remote leader epochs of all the segments of a partition
            // instead of going through all the segments and building it here.
            while (segmentMetadataIter.hasNext()) {
                RemoteLogSegmentMetadata segmentMetadata = segmentMetadataIter.next();
                epochsSet.addAll(segmentMetadata.segmentLeaderEpochs().keySet());
                metadataCount++;
                remoteLogSizeBytes += segmentMetadata.segmentSizeInBytes();
            }

            updateMetadataCountAndLogSizeWith(metadataCount, remoteLogSizeBytes);

            // All the leader epochs in sorted order that exists in remote storage
            final List<Integer> remoteLeaderEpochs = new ArrayList<>(epochsSet);
            Collections.sort(remoteLeaderEpochs);

            LeaderEpochFileCache leaderEpochCache = leaderEpochCacheOption.get();
            // Build the leader epoch map by filtering the epochs that do not have any records.
            NavigableMap<Integer, Long> epochWithOffsets = buildFilteredLeaderEpochMap(leaderEpochCache.epochWithOffsets());

            long logStartOffset = log.logStartOffset();
            long logEndOffset = log.logEndOffset();
            Optional<RetentionSizeData> retentionSizeData = buildRetentionSizeData(log.config().retentionSize,
                    log.onlyLocalLogSegmentsSize(), logEndOffset, epochWithOffsets);
            Optional<RetentionTimeData> retentionTimeData = buildRetentionTimeData(log.config().retentionMs);

            RemoteLogRetentionHandler remoteLogRetentionHandler = new RemoteLogRetentionHandler(retentionSizeData, retentionTimeData);
            Iterator<Integer> epochIterator = epochWithOffsets.navigableKeySet().iterator();
            boolean canProcess = true;
            List<RemoteLogSegmentMetadata> segmentsToDelete = new ArrayList<>();
            long sizeOfDeletableSegmentsBytes = 0L;
            while (canProcess && epochIterator.hasNext()) {
                Integer epoch = epochIterator.next();
                Iterator<RemoteLogSegmentMetadata> segmentsIterator = remoteLogMetadataManager.listRemoteLogSegments(topicIdPartition, epoch);
                while (canProcess && segmentsIterator.hasNext()) {
                    if (isCancelled() || !isLeader()) {
                        logger.info("Returning from remote log segments cleanup for the remaining segments as the task state is changed.");
                        return;
                    }
                    RemoteLogSegmentMetadata metadata = segmentsIterator.next();

                    if (RemoteLogSegmentState.DELETE_SEGMENT_FINISHED.equals(metadata.state())) {
                        continue;
                    }
                    if (segmentsToDelete.contains(metadata)) {
                        continue;
                    }
                    // When the log-start-offset is moved by the user, the leader-epoch-checkpoint file gets truncated
                    // as per the log-start-offset. Until the rlm-cleaner-thread runs in the next iteration, those
                    // remote log segments won't be removed. The `isRemoteSegmentWithinLeaderEpoch` validates whether
                    // the epochs present in the segment lies in the checkpoint file. It will always return false
                    // since the checkpoint file was already truncated.
                    boolean shouldDeleteSegment = remoteLogRetentionHandler.isSegmentBreachByLogStartOffset(
                            metadata, logStartOffset, epochWithOffsets);
                    boolean isValidSegment = false;
                    if (!shouldDeleteSegment) {
                        // check whether the segment contains the required epoch range with in the current leader epoch lineage.
                        isValidSegment = isRemoteSegmentWithinLeaderEpochs(metadata, logEndOffset, epochWithOffsets);
                        if (isValidSegment) {
                            shouldDeleteSegment =
                                    remoteLogRetentionHandler.isSegmentBreachedByRetentionTime(metadata) ||
                                            remoteLogRetentionHandler.isSegmentBreachedByRetentionSize(metadata);
                        }
                    }
                    if (shouldDeleteSegment) {
                        segmentsToDelete.add(metadata);
                        sizeOfDeletableSegmentsBytes += metadata.segmentSizeInBytes();
                    }
                    canProcess = shouldDeleteSegment || !isValidSegment;
                }
            }

            // Update log start offset with the computed value after retention cleanup is done
            remoteLogRetentionHandler.logStartOffset.ifPresent(offset -> handleLogStartOffsetUpdate(topicIdPartition.topicPartition(), offset));

            // At this point in time we have updated the log start offsets, but not initiated a deletion.
            // Either a follower has picked up the changes to the log start offset, or they have not.
            // If the follower HAS picked up the changes, and they become the leader this replica won't successfully complete
            // the deletion.
            // However, the new leader will correctly pick up all breaching segments as log start offset breaching ones
            // and delete them accordingly.
            // If the follower HAS NOT picked up the changes, and they become the leader then they will go through this process
            // again and delete them with the original deletion reason i.e. size, time or log start offset breach.
            int segmentsLeftToDelete = segmentsToDelete.size();
            updateRemoteDeleteLagWith(segmentsLeftToDelete, sizeOfDeletableSegmentsBytes);
            List<String> undeletedSegments = new ArrayList<>();
            for (RemoteLogSegmentMetadata segmentMetadata : segmentsToDelete) {
                if (!remoteLogRetentionHandler.deleteRemoteLogSegment(segmentMetadata, x -> !isCancelled() && isLeader())) {
                    undeletedSegments.add(segmentMetadata.remoteLogSegmentId().toString());
                } else {
                    sizeOfDeletableSegmentsBytes -= segmentMetadata.segmentSizeInBytes();
                    segmentsLeftToDelete--;
                    updateRemoteDeleteLagWith(segmentsLeftToDelete, sizeOfDeletableSegmentsBytes);
                }
            }
            if (!undeletedSegments.isEmpty()) {
                logger.info("The following remote segments could not be deleted: {}", String.join(",", undeletedSegments));
            }

            // Remove the remote log segments whose segment-leader-epochs are less than the earliest-epoch known
            // to the leader. This will remove the unreferenced segments in the remote storage. This is needed for
            // unclean leader election scenarios as the remote storage can have epochs earlier to the current leader's
            // earliest leader epoch.
            Optional<EpochEntry> earliestEpochEntryOptional = leaderEpochCache.earliestEntry();
            if (earliestEpochEntryOptional.isPresent()) {
                EpochEntry earliestEpochEntry = earliestEpochEntryOptional.get();
                Iterator<Integer> epochsToClean = remoteLeaderEpochs.stream()
                        .filter(remoteEpoch -> remoteEpoch < earliestEpochEntry.epoch)
                        .iterator();

                List<RemoteLogSegmentMetadata> listOfSegmentsToBeCleaned = new ArrayList<>();

                while (epochsToClean.hasNext()) {
                    int epoch = epochsToClean.next();
                    Iterator<RemoteLogSegmentMetadata> segmentsToBeCleaned = remoteLogMetadataManager.listRemoteLogSegments(topicIdPartition, epoch);
                    while (segmentsToBeCleaned.hasNext()) {
                        if (!isCancelled() && isLeader()) {
                            RemoteLogSegmentMetadata nextSegmentMetadata = segmentsToBeCleaned.next();
                            sizeOfDeletableSegmentsBytes += nextSegmentMetadata.segmentSizeInBytes();
                            listOfSegmentsToBeCleaned.add(nextSegmentMetadata);
                        }
                    }
                }

                segmentsLeftToDelete += listOfSegmentsToBeCleaned.size();
                updateRemoteDeleteLagWith(segmentsLeftToDelete, sizeOfDeletableSegmentsBytes);
                for (RemoteLogSegmentMetadata segmentMetadata : listOfSegmentsToBeCleaned) {
                    if (!isCancelled() && isLeader()) {
                        // No need to update the log-start-offset even though the segment is deleted as these epochs/offsets are earlier to that value.
                        if (remoteLogRetentionHandler.deleteLogSegmentsDueToLeaderEpochCacheTruncation(earliestEpochEntry, segmentMetadata)) {
                            sizeOfDeletableSegmentsBytes -= segmentMetadata.segmentSizeInBytes();
                            segmentsLeftToDelete--;
                            updateRemoteDeleteLagWith(segmentsLeftToDelete, sizeOfDeletableSegmentsBytes);
                        }
                    }
                }
            }
        }

        private Optional<RetentionTimeData> buildRetentionTimeData(long retentionMs) {
            return retentionMs > -1
                    ? Optional.of(new RetentionTimeData(retentionMs, time.milliseconds() - retentionMs))
                    : Optional.empty();
        }

        private Optional<RetentionSizeData> buildRetentionSizeData(long retentionSize,
                                                                   long onlyLocalLogSegmentsSize,
                                                                   long logEndOffset,
                                                                   NavigableMap<Integer, Long> epochEntries) throws RemoteStorageException {
            if (retentionSize > -1) {
                long startTimeMs = time.milliseconds();
                long remoteLogSizeBytes = 0L;
                Set<RemoteLogSegmentId> visitedSegmentIds = new HashSet<>();
                for (Integer epoch : epochEntries.navigableKeySet()) {
                    // remoteLogSize(topicIdPartition, epochEntry.epoch) may not be completely accurate as the remote
                    // log size may be computed for all the segments but not for segments with in the current
                    // partition's leader epoch lineage. Better to revisit this API.
                    // remoteLogSizeBytes += remoteLogMetadataManager.remoteLogSize(topicIdPartition, epochEntry.epoch);
                    Iterator<RemoteLogSegmentMetadata> segmentsIterator = remoteLogMetadataManager.listRemoteLogSegments(topicIdPartition, epoch);
                    while (segmentsIterator.hasNext()) {
                        RemoteLogSegmentMetadata segmentMetadata = segmentsIterator.next();
                        RemoteLogSegmentId segmentId = segmentMetadata.remoteLogSegmentId();
                        if (!visitedSegmentIds.contains(segmentId) && isRemoteSegmentWithinLeaderEpochs(segmentMetadata, logEndOffset, epochEntries)) {
                            remoteLogSizeBytes += segmentMetadata.segmentSizeInBytes();
                            visitedSegmentIds.add(segmentId);
                        }
                    }
                }

                brokerTopicStats.recordRemoteLogSizeComputationTime(topicIdPartition.topic(), topicIdPartition.partition(), time.milliseconds() - startTimeMs);

                // This is the total size of segments in local log that have their base-offset > local-log-start-offset
                // and size of the segments in remote storage which have their end-offset < local-log-start-offset.
                long totalSize = onlyLocalLogSegmentsSize + remoteLogSizeBytes;
                if (totalSize > retentionSize) {
                    long remainingBreachedSize = totalSize - retentionSize;
                    RetentionSizeData retentionSizeData = new RetentionSizeData(retentionSize, remainingBreachedSize);
                    return Optional.of(retentionSizeData);
                }
            }

            return Optional.empty();
        }

        public String toString() {
            return this.getClass().toString() + "[" + topicIdPartition + "]";
        }
    }

    /**
     * Returns true if the remote segment's epoch/offsets are within the leader epoch lineage of the partition.
     * The constraints here are as follows:
     * - The segment's first epoch's offset should be more than or equal to the respective leader epoch's offset in the partition leader epoch lineage.
     * - The segment's end offset should be less than or equal to the respective leader epoch's offset in the partition leader epoch lineage.
     * - The segment's epoch lineage(epoch and offset) should be same as leader epoch lineage((epoch and offset)) except
     * for the first and the last epochs in the segment.
     *
     * @param segmentMetadata The remote segment metadata to be validated.
     * @param logEndOffset    The log end offset of the partition.
     * @param leaderEpochs    The leader epoch lineage of the partition by filtering the epochs containing no data.
     * @return true if the remote segment's epoch/offsets are within the leader epoch lineage of the partition.
     */
    // Visible for testing
    public static boolean isRemoteSegmentWithinLeaderEpochs(RemoteLogSegmentMetadata segmentMetadata,
                                                            long logEndOffset,
                                                            NavigableMap<Integer, Long> leaderEpochs) {
        long segmentEndOffset = segmentMetadata.endOffset();
        // Filter epochs that does not have any messages/records associated with them.
        NavigableMap<Integer, Long> segmentLeaderEpochs = buildFilteredLeaderEpochMap(segmentMetadata.segmentLeaderEpochs());
        // Check for out of bound epochs between segment epochs and current leader epochs.
        Integer segmentFirstEpoch = segmentLeaderEpochs.firstKey();
        Integer segmentLastEpoch = segmentLeaderEpochs.lastKey();
        if (segmentFirstEpoch < leaderEpochs.firstKey() || segmentLastEpoch > leaderEpochs.lastKey()) {
            LOGGER.debug("Segment {} is not within the partition leader epoch lineage. " +
                            "Remote segment epochs: {} and partition leader epochs: {}",
                    segmentMetadata.remoteLogSegmentId(), segmentLeaderEpochs, leaderEpochs);
            return false;
        }

        for (Map.Entry<Integer, Long> entry : segmentLeaderEpochs.entrySet()) {
            int epoch = entry.getKey();
            long offset = entry.getValue();

            // If segment's epoch does not exist in the leader epoch lineage then it is not a valid segment.
            if (!leaderEpochs.containsKey(epoch)) {
                LOGGER.debug("Segment {} epoch {} is not within the leader epoch lineage. " +
                                "Remote segment epochs: {} and partition leader epochs: {}",
                        segmentMetadata.remoteLogSegmentId(), epoch, segmentLeaderEpochs, leaderEpochs);
                return false;
            }

            // Segment's first epoch's offset should be more than or equal to the respective leader epoch's offset.
            if (epoch == segmentFirstEpoch && offset < leaderEpochs.get(epoch)) {
                LOGGER.debug("Segment {} first epoch {} offset is less than leader epoch offset {}.",
                        segmentMetadata.remoteLogSegmentId(), epoch, leaderEpochs.get(epoch));
                return false;
            }

            // Segment's end offset should be less than or equal to the respective leader epoch's offset.
            if (epoch == segmentLastEpoch) {
                Map.Entry<Integer, Long> nextEntry = leaderEpochs.higherEntry(epoch);
                if (nextEntry != null && segmentEndOffset > nextEntry.getValue() - 1) {
                    LOGGER.debug("Segment {} end offset {} is more than leader epoch offset {}.",
                            segmentMetadata.remoteLogSegmentId(), segmentEndOffset, nextEntry.getValue() - 1);
                    return false;
                }
            }

            // Next segment epoch entry and next leader epoch entry should be same to ensure that the segment's epoch
            // is within the leader epoch lineage.
            if (epoch != segmentLastEpoch && !leaderEpochs.higherEntry(epoch).equals(segmentLeaderEpochs.higherEntry(epoch))) {
                LOGGER.debug("Segment {} epoch {} is not within the leader epoch lineage. " +
                                "Remote segment epochs: {} and partition leader epochs: {}",
                        segmentMetadata.remoteLogSegmentId(), epoch, segmentLeaderEpochs, leaderEpochs);
                return false;
            }
        }
        // segment end offset should be with in the log end offset.
        if (segmentEndOffset >= logEndOffset) {
            LOGGER.debug("Segment {} end offset {} is more than log end offset {}.",
                    segmentMetadata.remoteLogSegmentId(), segmentEndOffset, logEndOffset);
            return false;
        }
        return true;
    }

    /**
     * Returns a map containing the epoch vs start-offset for the given leader epoch map by filtering the epochs that
     * does not contain any messages/records associated with them.
     *
     * For ex:
     *  <epoch - start offset>
     *  0 - 0
     *  1 - 10
     *  2 - 20
     *  3 - 30
     *  4 - 40
     *  5 - 60  // epoch 5 does not have records or messages associated with it
     *  6 - 60
     *  7 - 70
     *
     *  When the above leaderEpochMap is passed to this method, it returns the following map:
     *  <epoch - start offset>
     *  0 - 0
     *  1 - 10
     *  2 - 20
     *  3 - 30
     *  4 - 40
     *  6 - 60
     *  7 - 70
     *
     * @param leaderEpochs The leader epoch map to be refined.
     */
    // Visible for testing
    public static NavigableMap<Integer, Long> buildFilteredLeaderEpochMap(NavigableMap<Integer, Long> leaderEpochs) {
        List<Integer> duplicatedEpochs = new ArrayList<>();
        Map.Entry<Integer, Long> previousEntry = null;
        for (Map.Entry<Integer, Long> entry : leaderEpochs.entrySet()) {
            if (previousEntry != null && previousEntry.getValue().equals(entry.getValue())) {
                duplicatedEpochs.add(previousEntry.getKey());
            }
            previousEntry = entry;
        }

        if (duplicatedEpochs.isEmpty()) {
            return leaderEpochs;
        }

        TreeMap<Integer, Long> filteredLeaderEpochs = new TreeMap<>(leaderEpochs);
        for (Integer duplicatedEpoch : duplicatedEpochs) {
            filteredLeaderEpochs.remove(duplicatedEpoch);
        }
        return filteredLeaderEpochs;
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
        InputStream remoteSegInputStream = null;
        try {
            int startPos = 0;
            RecordBatch firstBatch = null;

            //  Iteration over multiple RemoteSegmentMetadata is required in case of log compaction.
            //  It may be possible the offset is log compacted in the current RemoteLogSegmentMetadata
            //  And we need to iterate over the next segment metadata to fetch messages higher than the given offset.
            while (firstBatch == null && rlsMetadataOptional.isPresent()) {
                remoteLogSegmentMetadata = rlsMetadataOptional.get();
                // Search forward for the position of the last offset that is greater than or equal to the target offset
                startPos = lookupPositionForOffset(remoteLogSegmentMetadata, offset);
                remoteSegInputStream = remoteLogStorageManager.fetchLogSegment(remoteLogSegmentMetadata, startPos);
                RemoteLogInputStream remoteLogInputStream = getRemoteLogInputStream(remoteSegInputStream);
                firstBatch = findFirstBatch(remoteLogInputStream, offset);
                if (firstBatch == null) {
                    rlsMetadataOptional = findNextSegmentMetadata(rlsMetadataOptional.get(), logOptional.get().leaderEpochCache());
                }
            }
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
    // for testing
    RemoteLogInputStream getRemoteLogInputStream(InputStream in) {
        return new RemoteLogInputStream(in);
    }

    // Visible for testing
    int lookupPositionForOffset(RemoteLogSegmentMetadata remoteLogSegmentMetadata, long offset) {
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
                .map(position -> position.offset).orElse(segmentMetadata.endOffset() + 1);

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
        collectAbortedTransactionInLocalSegments(startOffset, upperBoundOffset, accumulator, log.logSegments().iterator());
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

    // visible for testing.
    Optional<RemoteLogSegmentMetadata> findNextSegmentMetadata(RemoteLogSegmentMetadata segmentMetadata,
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

    // Visible for testing
    RecordBatch findFirstBatch(RemoteLogInputStream remoteLogInputStream, long offset) throws IOException {
        RecordBatch nextBatch;
        // Look for the batch which has the desired offset
        // We will always have a batch in that segment as it is a non-compacted topic.
        do {
            nextBatch = remoteLogInputStream.nextBatch();
        } while (nextBatch != null && nextBatch.lastOffset() < offset);

        return nextBatch;
    }

    OffsetAndEpoch findHighestRemoteOffset(TopicIdPartition topicIdPartition, UnifiedLog log) throws RemoteStorageException {
        OffsetAndEpoch offsetAndEpoch = null;
        Option<LeaderEpochFileCache> leaderEpochCacheOpt = log.leaderEpochCache();
        if (leaderEpochCacheOpt.isDefined()) {
            LeaderEpochFileCache cache = leaderEpochCacheOpt.get();
            Optional<EpochEntry> maybeEpochEntry = cache.latestEntry();
            while (offsetAndEpoch == null && maybeEpochEntry.isPresent()) {
                int epoch = maybeEpochEntry.get().epoch;
                Optional<Long> highestRemoteOffsetOpt =
                        remoteLogMetadataManager.highestOffsetForEpoch(topicIdPartition, epoch);
                if (highestRemoteOffsetOpt.isPresent()) {
                    Map.Entry<Integer, Long> entry = cache.endOffsetFor(epoch, log.logEndOffset());
                    int requestedEpoch = entry.getKey();
                    long endOffset = entry.getValue();
                    long highestRemoteOffset = highestRemoteOffsetOpt.get();
                    if (endOffset <= highestRemoteOffset) {
                        LOGGER.info("The end-offset for epoch {}: ({}, {}) is less than or equal to the " +
                                "highest-remote-offset: {} for partition: {}", epoch, requestedEpoch, endOffset,
                                highestRemoteOffset, topicIdPartition);
                        offsetAndEpoch = new OffsetAndEpoch(endOffset - 1, requestedEpoch);
                    } else {
                        offsetAndEpoch = new OffsetAndEpoch(highestRemoteOffset, epoch);
                    }
                }
                maybeEpochEntry = cache.previousEntry(epoch);
            }
        }
        if (offsetAndEpoch == null) {
            offsetAndEpoch = new OffsetAndEpoch(-1L, RecordBatch.NO_PARTITION_LEADER_EPOCH);
        }
        return offsetAndEpoch;
    }

    long findLogStartOffset(TopicIdPartition topicIdPartition, UnifiedLog log) throws RemoteStorageException {
        Optional<Long> logStartOffset = Optional.empty();
        Option<LeaderEpochFileCache> maybeLeaderEpochFileCache = log.leaderEpochCache();
        if (maybeLeaderEpochFileCache.isDefined()) {
            LeaderEpochFileCache cache = maybeLeaderEpochFileCache.get();
            OptionalInt earliestEpochOpt = cache.earliestEntry()
                    .map(epochEntry -> OptionalInt.of(epochEntry.epoch))
                    .orElseGet(OptionalInt::empty);
            while (!logStartOffset.isPresent() && earliestEpochOpt.isPresent()) {
                Iterator<RemoteLogSegmentMetadata> iterator =
                        remoteLogMetadataManager.listRemoteLogSegments(topicIdPartition, earliestEpochOpt.getAsInt());
                if (iterator.hasNext()) {
                    logStartOffset = Optional.of(iterator.next().startOffset());
                }
                earliestEpochOpt = cache.nextEpoch(earliestEpochOpt.getAsInt());
            }
        }
        return logStartOffset.orElseGet(log::localLogStartOffset);
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
                    RLMTask task = new RLMTask(topicIdPartition, this.rlmConfig.remoteLogMetadataCustomMetadataMaxBytes());
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

    private void removeRemoteTopicPartitionMetrics(TopicIdPartition topicIdPartition) {
        String topic = topicIdPartition.topic();
        if (!brokerTopicStats.isTopicStatsExisted(topicIdPartition.topic())) {
            // The topic metrics are already removed, removing this topic key from broker-level metrics
            brokerTopicStats.removeBrokerLevelRemoteCopyLagBytes(topic);
            brokerTopicStats.removeBrokerLevelRemoteCopyLagSegments(topic);
            brokerTopicStats.removeBrokerLevelRemoteDeleteLagBytes(topic);
            brokerTopicStats.removeBrokerLevelRemoteDeleteLagSegments(topic);
            brokerTopicStats.removeBrokerLevelRemoteLogMetadataCount(topic);
            brokerTopicStats.removeBrokerLevelRemoteLogSizeComputationTime(topic);
            brokerTopicStats.removeBrokerLevelRemoteLogSizeBytes(topic);
        } else {
            int partition = topicIdPartition.partition();
            // remove the partition metric values and update the broker-level metrics
            brokerTopicStats.removeRemoteCopyLagBytes(topic, partition);
            brokerTopicStats.removeRemoteCopyLagSegments(topic, partition);
            brokerTopicStats.removeRemoteDeleteLagBytes(topic, partition);
            brokerTopicStats.removeRemoteDeleteLagSegments(topic, partition);
            brokerTopicStats.removeRemoteLogMetadataCount(topic, partition);
            brokerTopicStats.removeRemoteLogSizeComputationTime(topic, partition);
            brokerTopicStats.removeRemoteLogSizeBytes(topic, partition);
        }
    }

    //Visible for testing
    RLMTaskWithFuture task(TopicIdPartition partition) {
        return leaderOrFollowerTasks.get(partition);
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
    public static class RetentionSizeData {
        private final long retentionSize;
        private final long remainingBreachedSize;

        public RetentionSizeData(long retentionSize, long remainingBreachedSize) {
            if (retentionSize < 0)
                throw new IllegalArgumentException("retentionSize should be non negative, but it is " + retentionSize);

            if (remainingBreachedSize <= 0) {
                throw new IllegalArgumentException("remainingBreachedSize should be more than zero, but it is " + remainingBreachedSize);
            }

            this.retentionSize = retentionSize;
            this.remainingBreachedSize = remainingBreachedSize;
        }
    }

    // Visible for testing
    public static class RetentionTimeData {

        private final long retentionMs;
        private final long cleanupUntilMs;

        public RetentionTimeData(long retentionMs, long cleanupUntilMs) {
            if (retentionMs < 0)
                throw new IllegalArgumentException("retentionMs should be non negative, but it is " + retentionMs);

            if (cleanupUntilMs < 0)
                throw new IllegalArgumentException("cleanupUntilMs should be non negative, but it is " + cleanupUntilMs);

            this.retentionMs = retentionMs;
            this.cleanupUntilMs = cleanupUntilMs;
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
