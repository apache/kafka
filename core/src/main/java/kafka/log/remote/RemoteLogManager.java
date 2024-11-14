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
import kafka.log.AsyncOffsetReadFutureHolder;
import kafka.log.UnifiedLog;
import kafka.server.DelayedRemoteListOffsets;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.internals.SecurityManagerCompatibility;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Quota;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.RemoteLogInputStream;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ChildFirstClassLoader;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.common.CheckpointFile;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.server.common.StopPartition;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.log.remote.metadata.storage.ClassLoaderAwareRemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.quota.RLMQuotaManager;
import org.apache.kafka.server.log.remote.quota.RLMQuotaManagerConfig;
import org.apache.kafka.server.log.remote.quota.RLMQuotaMetrics;
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
import org.apache.kafka.server.purgatory.DelayedOperationPurgatory;
import org.apache.kafka.server.purgatory.TopicPartitionOperationKey;
import org.apache.kafka.server.quota.QuotaType;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.storage.internals.checkpoint.LeaderEpochCheckpointFile;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.internals.log.AbortedTxn;
import org.apache.kafka.storage.internals.log.EpochEntry;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
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
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import com.yammer.metrics.core.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Option;
import scala.jdk.javaapi.CollectionConverters;
import scala.util.Either;

import static org.apache.kafka.server.config.ServerLogConfigs.LOG_DIR_CONFIG;
import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_COMMON_CLIENT_PREFIX;
import static org.apache.kafka.server.log.remote.quota.RLMQuotaManagerConfig.INACTIVE_SENSOR_EXPIRATION_TIME_SECONDS;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageMetrics.REMOTE_LOG_MANAGER_TASKS_AVG_IDLE_PERCENT_METRIC;
import static org.apache.kafka.server.log.remote.storage.RemoteStorageMetrics.REMOTE_LOG_READER_FETCH_RATE_AND_TIME_METRIC;

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
    private final Metrics metrics;

    private final RemoteStorageManager remoteLogStorageManager;

    private final RemoteLogMetadataManager remoteLogMetadataManager;

    private final ReentrantLock copyQuotaManagerLock = new ReentrantLock(true);
    private final Condition copyQuotaManagerLockCondition = copyQuotaManagerLock.newCondition();
    private final RLMQuotaManager rlmCopyQuotaManager;
    private final RLMQuotaManager rlmFetchQuotaManager;
    private final Sensor fetchThrottleTimeSensor;
    private final Sensor copyThrottleTimeSensor;

    private final RemoteIndexCache indexCache;
    private final RemoteStorageThreadPool remoteStorageReaderThreadPool;
    private final RLMScheduledThreadPool rlmCopyThreadPool;
    private final RLMScheduledThreadPool rlmExpirationThreadPool;
    private final RLMScheduledThreadPool followerThreadPool;

    private final long delayInMs;

    private final ConcurrentHashMap<TopicIdPartition, RLMTaskWithFuture> leaderCopyRLMTasks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<TopicIdPartition, RLMTaskWithFuture> leaderExpirationRLMTasks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<TopicIdPartition, RLMTaskWithFuture> followerRLMTasks = new ConcurrentHashMap<>();
    private final Set<RemoteLogSegmentId> segmentIdsBeingCopied = ConcurrentHashMap.newKeySet();

    // topic ids that are received on leadership changes, this map is cleared on stop partitions
    private final ConcurrentMap<TopicPartition, Uuid> topicIdByPartitionMap = new ConcurrentHashMap<>();
    private final String clusterId;
    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(this.getClass());

    // The endpoint for remote log metadata manager to connect to
    private Optional<Endpoint> endpoint = Optional.empty();
    private boolean closed = false;

    private volatile boolean remoteLogManagerConfigured = false;
    private final Timer remoteReadTimer;
    private volatile DelayedOperationPurgatory<DelayedRemoteListOffsets> delayedRemoteListOffsetsPurgatory;

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
     * @param metrics  Metrics instance
     */
    @SuppressWarnings({"this-escape"})
    public RemoteLogManager(RemoteLogManagerConfig rlmConfig,
                            int brokerId,
                            String logDir,
                            String clusterId,
                            Time time,
                            Function<TopicPartition, Optional<UnifiedLog>> fetchLog,
                            BiConsumer<TopicPartition, Long> updateRemoteLogStartOffset,
                            BrokerTopicStats brokerTopicStats,
                            Metrics metrics) throws IOException {
        this.rlmConfig = rlmConfig;
        this.brokerId = brokerId;
        this.logDir = logDir;
        this.clusterId = clusterId;
        this.time = time;
        this.fetchLog = fetchLog;
        this.updateRemoteLogStartOffset = updateRemoteLogStartOffset;
        this.brokerTopicStats = brokerTopicStats;
        this.metrics = metrics;

        remoteLogStorageManager = createRemoteStorageManager();
        remoteLogMetadataManager = createRemoteLogMetadataManager();
        rlmCopyQuotaManager = createRLMCopyQuotaManager();
        rlmFetchQuotaManager = createRLMFetchQuotaManager();

        fetchThrottleTimeSensor = new RLMQuotaMetrics(metrics, "remote-fetch-throttle-time", RemoteLogManager.class.getSimpleName(),
            "The %s time in millis remote fetches was throttled by a broker", INACTIVE_SENSOR_EXPIRATION_TIME_SECONDS).sensor();
        copyThrottleTimeSensor = new RLMQuotaMetrics(metrics, "remote-copy-throttle-time", RemoteLogManager.class.getSimpleName(),
            "The %s time in millis remote copies was throttled by a broker", INACTIVE_SENSOR_EXPIRATION_TIME_SECONDS).sensor();

        indexCache = new RemoteIndexCache(rlmConfig.remoteLogIndexFileCacheTotalSizeBytes(), remoteLogStorageManager, logDir);
        delayInMs = rlmConfig.remoteLogManagerTaskIntervalMs();
        rlmCopyThreadPool = new RLMScheduledThreadPool(rlmConfig.remoteLogManagerCopierThreadPoolSize(),
            "RLMCopyThreadPool", "kafka-rlm-copy-thread-pool-");
        rlmExpirationThreadPool = new RLMScheduledThreadPool(rlmConfig.remoteLogManagerExpirationThreadPoolSize(),
            "RLMExpirationThreadPool", "kafka-rlm-expiration-thread-pool-");
        followerThreadPool = new RLMScheduledThreadPool(rlmConfig.remoteLogManagerThreadPoolSize(),
            "RLMFollowerScheduledThreadPool", "kafka-rlm-follower-thread-pool-");

        metricsGroup.newGauge(REMOTE_LOG_MANAGER_TASKS_AVG_IDLE_PERCENT_METRIC, rlmCopyThreadPool::getIdlePercent);
        remoteReadTimer = metricsGroup.newTimer(REMOTE_LOG_READER_FETCH_RATE_AND_TIME_METRIC,
                TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

        remoteStorageReaderThreadPool = new RemoteStorageThreadPool(
                REMOTE_LOG_READER_THREAD_NAME_PREFIX,
                rlmConfig.remoteLogReaderThreads(),
                rlmConfig.remoteLogReaderMaxPendingTasks()
        );
    }

    public void setDelayedOperationPurgatory(DelayedOperationPurgatory<DelayedRemoteListOffsets> delayedRemoteListOffsetsPurgatory) {
        this.delayedRemoteListOffsetsPurgatory = delayedRemoteListOffsetsPurgatory;
    }

    public void resizeCacheSize(long remoteLogIndexFileCacheSize) {
        indexCache.resizeCacheSize(remoteLogIndexFileCacheSize);
    }

    public void updateCopyQuota(long quota) {
        LOGGER.info("Updating remote copy quota to {} bytes per second", quota);
        rlmCopyQuotaManager.updateQuota(new Quota(quota, true));
    }

    public void updateFetchQuota(long quota) {
        LOGGER.info("Updating remote fetch quota to {} bytes per second", quota);
        rlmFetchQuotaManager.updateQuota(new Quota(quota, true));
    }

    private void removeMetrics() {
        metricsGroup.removeMetric(REMOTE_LOG_MANAGER_TASKS_AVG_IDLE_PERCENT_METRIC);
        metricsGroup.removeMetric(REMOTE_LOG_READER_FETCH_RATE_AND_TIME_METRIC);
        remoteStorageReaderThreadPool.removeMetrics();
    }

    /**
     * Returns the timeout for the RLM Tasks to wait for the quota to be available
     */
    Duration quotaTimeout() {
        return Duration.ofSeconds(1);
    }

    RLMQuotaManager createRLMCopyQuotaManager() {
        return new RLMQuotaManager(copyQuotaManagerConfig(rlmConfig), metrics, QuotaType.RLM_COPY,
          "Tracking copy byte-rate for Remote Log Manager", time);
    }

    RLMQuotaManager createRLMFetchQuotaManager() {
        return new RLMQuotaManager(fetchQuotaManagerConfig(rlmConfig), metrics, QuotaType.RLM_FETCH,
          "Tracking fetch byte-rate for Remote Log Manager", time);
    }

    public long getFetchThrottleTimeMs() {
        return rlmFetchQuotaManager.getThrottleTimeMs();
    }

    public Sensor fetchThrottleTimeSensor() {
        return fetchThrottleTimeSensor;
    }

    static RLMQuotaManagerConfig copyQuotaManagerConfig(RemoteLogManagerConfig rlmConfig) {
        return new RLMQuotaManagerConfig(rlmConfig.remoteLogManagerCopyMaxBytesPerSecond(),
          rlmConfig.remoteLogManagerCopyNumQuotaSamples(),
          rlmConfig.remoteLogManagerCopyQuotaWindowSizeSeconds());
    }

    static RLMQuotaManagerConfig fetchQuotaManagerConfig(RemoteLogManagerConfig rlmConfig) {
        return new RLMQuotaManagerConfig(rlmConfig.remoteLogManagerFetchMaxBytesPerSecond(),
          rlmConfig.remoteLogManagerFetchNumQuotaSamples(),
          rlmConfig.remoteLogManagerFetchQuotaWindowSizeSeconds());
    }

    @SuppressWarnings("unchecked")
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
        return SecurityManagerCompatibility.get().doPrivileged(() -> {
            final String classPath = rlmConfig.remoteStorageManagerClassPath();
            if (classPath != null && !classPath.trim().isEmpty()) {
                ChildFirstClassLoader classLoader = new ChildFirstClassLoader(classPath, this.getClass().getClassLoader());
                RemoteStorageManager delegate = createDelegate(classLoader, rlmConfig.remoteStorageManagerClassName());
                return (RemoteStorageManager) new ClassLoaderAwareRemoteStorageManager(delegate, classLoader);
            } else {
                return createDelegate(this.getClass().getClassLoader(), rlmConfig.remoteStorageManagerClassName());
            }
        });
    }

    private void configureRSM() {
        final Map<String, Object> rsmProps = new HashMap<>(rlmConfig.remoteStorageManagerProps());
        rsmProps.put(ServerConfigs.BROKER_ID_CONFIG, brokerId);
        remoteLogStorageManager.configure(rsmProps);
    }

    RemoteLogMetadataManager createRemoteLogMetadataManager() {
        return SecurityManagerCompatibility.get().doPrivileged(() -> {
            final String classPath = rlmConfig.remoteLogMetadataManagerClassPath();
            if (classPath != null && !classPath.trim().isEmpty()) {
                ClassLoader classLoader = new ChildFirstClassLoader(classPath, this.getClass().getClassLoader());
                RemoteLogMetadataManager delegate = createDelegate(classLoader, rlmConfig.remoteLogMetadataManagerClassName());
                return (RemoteLogMetadataManager) new ClassLoaderAwareRemoteLogMetadataManager(delegate, classLoader);
            } else {
                return createDelegate(this.getClass().getClassLoader(), rlmConfig.remoteLogMetadataManagerClassName());
            }
        });
    }

    public void onEndPointCreated(Endpoint endpoint) {
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

        rlmmProps.put(ServerConfigs.BROKER_ID_CONFIG, brokerId);
        rlmmProps.put(LOG_DIR_CONFIG, logDir);
        rlmmProps.put("cluster.id", clusterId);

        remoteLogMetadataManager.configure(rlmmProps);
    }

    public void startup() {
        // Initialize and configure RSM and RLMM. This will start RSM, RLMM resources which may need to start resources
        // in connecting to the brokers or remote storages.
        configureRSM();
        configureRLMM();
        remoteLogManagerConfigured = true;
    }

    private boolean isRemoteLogManagerConfigured() {
        return this.remoteLogManagerConfigured;
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

        if (rlmConfig.isRemoteStorageSystemEnabled() && !isRemoteLogManagerConfigured()) {
            throw new KafkaException("RemoteLogManager is not configured when remote storage system is enabled");
        }

        Map<TopicIdPartition, Boolean> leaderPartitions = filterPartitions(partitionsBecomeLeader)
                .collect(Collectors.toMap(p -> new TopicIdPartition(topicIds.get(p.topic()), p.topicPartition()),
                        p -> p.log().exists(log -> log.config().remoteLogCopyDisable())));

        Map<TopicIdPartition, Boolean> followerPartitions = filterPartitions(partitionsBecomeFollower)
                .collect(Collectors.toMap(p -> new TopicIdPartition(topicIds.get(p.topic()), p.topicPartition()),
                        p -> p.log().exists(log -> log.config().remoteLogCopyDisable())));

        if (!leaderPartitions.isEmpty() || !followerPartitions.isEmpty()) {
            LOGGER.debug("Effective topic partitions after filtering compact and internal topics, leaders: {} and followers: {}",
                    leaderPartitions, followerPartitions);

            leaderPartitions.forEach((tp, __) -> cacheTopicPartitionIds(tp));
            followerPartitions.forEach((tp, __) -> cacheTopicPartitionIds(tp));

            remoteLogMetadataManager.onPartitionLeadershipChanges(leaderPartitions.keySet(), followerPartitions.keySet());
            followerPartitions.forEach((tp, __) -> doHandleFollowerPartition(tp));

            // If this node was the previous leader for the partition, then the RLMTask might be running in the
            // background thread and might emit metrics. So, removing the metrics after marking this node as follower.
            followerPartitions.forEach((tp, __) -> removeRemoteTopicPartitionMetrics(tp));

            leaderPartitions.forEach(this::doHandleLeaderPartition);
        }
    }

    public void stopLeaderCopyRLMTasks(Set<Partition> partitions) {
        for (Partition partition : partitions) {
            TopicPartition tp = partition.topicPartition();
            if (topicIdByPartitionMap.containsKey(tp)) {
                TopicIdPartition tpId = new TopicIdPartition(topicIdByPartitionMap.get(tp), tp);
                leaderCopyRLMTasks.computeIfPresent(tpId, (topicIdPartition, task) -> {
                    LOGGER.info("Cancelling the copy RLM task for partition: {}", tpId);
                    task.cancel();
                    LOGGER.info("Resetting remote copy lag metrics for partition: {}", tpId);
                    ((RLMCopyTask) task.rlmTask).resetLagStats();
                    return null;
                });
            }
        }
    }

    /**
     * Stop the remote-log-manager task for the given partitions. And, calls the
     * {@link RemoteLogMetadataManager#onStopPartitions(Set)} when {@link StopPartition#deleteLocalLog} is true.
     * Deletes the partitions from the remote storage when {@link StopPartition#deleteRemoteLog} is true.
     *
     * @param stopPartitions topic partitions that needs to be stopped.
     * @param errorHandler   callback to handle any errors while stopping the partitions.
     */
    public void stopPartitions(Set<StopPartition> stopPartitions,
                               BiConsumer<TopicPartition, Throwable> errorHandler) {
        LOGGER.debug("Stop partitions: {}", stopPartitions);
        for (StopPartition stopPartition: stopPartitions) {
            TopicPartition tp = stopPartition.topicPartition;
            try {
                if (topicIdByPartitionMap.containsKey(tp)) {
                    TopicIdPartition tpId = new TopicIdPartition(topicIdByPartitionMap.get(tp), tp);
                    leaderCopyRLMTasks.computeIfPresent(tpId, (topicIdPartition, task) -> {
                        LOGGER.info("Cancelling the copy RLM task for partition: {}", tpId);
                        task.cancel();
                        return null;
                    });
                    leaderExpirationRLMTasks.computeIfPresent(tpId, (topicIdPartition, task) -> {
                        LOGGER.info("Cancelling the expiration RLM task for partition: {}", tpId);
                        task.cancel();
                        return null;
                    });
                    followerRLMTasks.computeIfPresent(tpId, (topicIdPartition, task) -> {
                        LOGGER.info("Cancelling the follower RLM task for partition: {}", tpId);
                        task.cancel();
                        return null;
                    });

                    removeRemoteTopicPartitionMetrics(tpId);

                    if (stopPartition.deleteRemoteLog) {
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

        // We want to remote topicId map and stopPartition on RLMM for deleteLocalLog or stopRLMM partitions because
        // in both case, they all mean the topic will not be held in this broker anymore.
        // NOTE: In ZK mode, this#stopPartitions method is called when Replica state changes to Offline and ReplicaDeletionStarted
        Set<TopicIdPartition> pendingActionsPartitions = stopPartitions.stream()
                .filter(sp -> (sp.stopRemoteLogMetadataManager || sp.deleteLocalLog) && topicIdByPartitionMap.containsKey(sp.topicPartition))
                .map(sp -> new TopicIdPartition(topicIdByPartitionMap.get(sp.topicPartition), sp.topicPartition))
                .collect(Collectors.toSet());

        if (!pendingActionsPartitions.isEmpty()) {
            pendingActionsPartitions.forEach(tpId -> topicIdByPartitionMap.remove(tpId.topicPartition()));
            remoteLogMetadataManager.onStopPartitions(pendingActionsPartitions);
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

    /**
     * Returns the next segment that may contain the aborted transaction entries. The search ensures that the returned
     * segment offsets are greater than or equal to the given offset and in the same epoch.
     * @param topicPartition topic partition to search
     * @param epochForOffset the epoch
     * @param offset the offset
     * @return The next segment that contains the transaction index in the same epoch.
     * @throws RemoteStorageException If an error occurs while fetching the remote log segment metadata.
     */
    public Optional<RemoteLogSegmentMetadata> fetchNextSegmentWithTxnIndex(TopicPartition topicPartition,
                                                                           int epochForOffset,
                                                                           long offset) throws RemoteStorageException {
        Uuid topicId = topicIdByPartitionMap.get(topicPartition);
        if (topicId == null) {
            throw new KafkaException("No topic id registered for topic partition: " + topicPartition);
        }
        TopicIdPartition tpId = new TopicIdPartition(topicId, topicPartition);
        return remoteLogMetadataManager.nextSegmentWithTxnIndex(tpId, epochForOffset, offset);
    }

    Optional<FileRecords.TimestampAndOffset> lookupTimestamp(RemoteLogSegmentMetadata rlsMetadata, long timestamp, long startingOffset)
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
                    try (CloseableIterator<Record> recordStreamingIterator = batch.streamingIterator(BufferSupplier.NO_CACHING)) {
                        while (recordStreamingIterator.hasNext()) {
                            Record record = recordStreamingIterator.next();
                            if (record.timestamp() >= timestamp && record.offset() >= startingOffset)
                                return Optional.of(new FileRecords.TimestampAndOffset(record.timestamp(), record.offset(), maybeLeaderEpoch(batch.partitionLeaderEpoch())));
                        }
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

    public AsyncOffsetReadFutureHolder<Either<Exception, Option<FileRecords.TimestampAndOffset>>> asyncOffsetRead(
            TopicPartition topicPartition,
            Long timestamp,
            Long startingOffset,
            LeaderEpochFileCache leaderEpochCache,
            Supplier<Option<FileRecords.TimestampAndOffset>> searchLocalLog) {
        CompletableFuture<Either<Exception, Option<FileRecords.TimestampAndOffset>>> taskFuture = new CompletableFuture<>();
        Future<Void> jobFuture = remoteStorageReaderThreadPool.submit(
                new RemoteLogOffsetReader(this, topicPartition, timestamp, startingOffset, leaderEpochCache, searchLocalLog, result -> {
                    TopicPartitionOperationKey key = new TopicPartitionOperationKey(topicPartition.topic(), topicPartition.partition());
                    taskFuture.complete(result);
                    delayedRemoteListOffsetsPurgatory.checkAndComplete(key);
                })
        );
        return new AsyncOffsetReadFutureHolder<>(jobFuture, taskFuture);
    }

    /**
     * Search the message offset in the remote storage for the given timestamp and starting-offset.
     * Once the target segment where the search to be performed is found:
     * 1. If the target segment lies in the local storage (common segments that lies in both remote and local storage),
     *    then the search will be performed in the local storage.
     * 2. If the target segment is found only in the remote storage, then the search will be performed in the remote storage.
     *
     *  <p>
     * This method returns an option of TimestampOffset. The returned value is determined using the following ordered list of rules:
     * <p>
     * - If there are no messages in the remote storage, return Empty
     * - If all the messages in the remote storage have smaller offsets, return Empty
     * - If all the messages in the remote storage have smaller timestamps, return Empty
     * - Otherwise, return an option of TimestampOffset. The offset is the offset of the first message whose timestamp
     * is greater than or equals to the target timestamp and whose offset is greater than or equals to the startingOffset.
     *
     * @param tp               topic partition in which the offset to be found.
     * @param timestamp        The timestamp to search for.
     * @param startingOffset   The starting offset to search.
     * @param leaderEpochCache LeaderEpochFileCache of the topic partition.
     * @return the timestamp and offset of the first message that meets the requirements. Empty will be returned if there
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
        if (unifiedLogOptional.isEmpty()) {
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
                    // cache to avoid race conditions
                    List<LogSegment> segmentsCopy = new ArrayList<>(unifiedLog.logSegments());
                    if (segmentsCopy.isEmpty() || rlsMetadata.startOffset() < segmentsCopy.get(0).baseOffset()) {
                        // search in remote-log
                        return lookupTimestamp(rlsMetadata, timestamp, startingOffset);
                    } else {
                        // search in local-log
                        for (LogSegment segment : segmentsCopy) {
                            if (segment.largestTimestamp() >= timestamp) {
                                return segment.findOffsetByTimestamp(timestamp, startingOffset);
                            }
                        }
                    }
                }
            }
            // Move to the next epoch if not found with the current epoch.
            maybeEpoch = leaderEpochCache.nextEpoch(epoch);
        }
        return Optional.empty();
    }

    private abstract static class CancellableRunnable implements Runnable {
        private volatile boolean cancelled = false;

        public void cancel() {
            cancelled = true;
        }

        public boolean isCancelled() {
            return cancelled;
        }
    }

    /**
     * Returns the leader epoch entries within the range of the given start[exclusive] and end[inclusive] offset.
     * <p>
     * Visible for testing.
     *
     * @param log         The actual log from where to take the leader-epoch checkpoint
     * @param startOffset The start offset of the epoch entries (inclusive).
     *                    If start offset is 6, then it will retain an entry at offset 6.
     * @param endOffset   The end offset of the epoch entries (exclusive)
     *                    If end offset is 100, then it will remove the entries greater than or equal to 100.
     * @return the leader epoch entries
     */
    List<EpochEntry> getLeaderEpochEntries(UnifiedLog log, long startOffset, long endOffset) {
        if (log.leaderEpochCache().isDefined()) {
            return log.leaderEpochCache().get().epochEntriesInRange(startOffset, endOffset);
        } else {
            return Collections.emptyList();
        }
    }

    // VisibleForTesting
    RLMTask rlmCopyTask(TopicIdPartition topicIdPartition) {
        RLMTaskWithFuture task = leaderCopyRLMTasks.get(topicIdPartition);
        if (task != null) {
            return task.rlmTask;
        }
        return null;
    }

    abstract class RLMTask extends CancellableRunnable {

        protected final TopicIdPartition topicIdPartition;
        private final Logger logger;

        public RLMTask(TopicIdPartition topicIdPartition) {
            this.topicIdPartition = topicIdPartition;
            this.logger = getLogContext().logger(RLMTask.class);
        }

        protected LogContext getLogContext() {
            return new LogContext("[RemoteLogManager=" + brokerId + " partition=" + topicIdPartition + "] ");
        }

        public void run() {
            if (isCancelled()) {
                logger.debug("Skipping the current run for partition {} as it is cancelled", topicIdPartition);
                return;
            }
            if (!remoteLogMetadataManager.isReady(topicIdPartition)) {
                logger.debug("Skipping the current run for partition {} as the remote-log metadata is not ready", topicIdPartition);
                return;
            }

            try {
                Optional<UnifiedLog> unifiedLogOptional = fetchLog.apply(topicIdPartition.topicPartition());

                if (unifiedLogOptional.isEmpty()) {
                    return;
                }

                execute(unifiedLogOptional.get());
            } catch (InterruptedException ex) {
                if (!isCancelled()) {
                    logger.warn("Current thread for partition {} is interrupted", topicIdPartition, ex);
                }
            } catch (RetriableException ex) {
                logger.debug("Encountered a retryable error while executing current task for partition {}", topicIdPartition, ex);
            } catch (Exception ex) {
                if (!isCancelled()) {
                    logger.warn("Current task for partition {} received error but it will be scheduled", topicIdPartition, ex);
                }
            }
        }

        protected abstract void execute(UnifiedLog log) throws InterruptedException, RemoteStorageException, ExecutionException;

        public String toString() {
            return this.getClass() + "[" + topicIdPartition + "]";
        }
    }

    class RLMCopyTask extends RLMTask {
        private final int customMetadataSizeLimit;
        private final Logger logger;

        // The copied and log-start offset is empty initially for a new RLMCopyTask, and needs to be fetched inside
        // the task's run() method.
        private volatile Optional<OffsetAndEpoch> copiedOffsetOption = Optional.empty();
        private volatile boolean isLogStartOffsetUpdated = false;
        private volatile Optional<String> logDirectory = Optional.empty();

        public RLMCopyTask(TopicIdPartition topicIdPartition, int customMetadataSizeLimit) {
            super(topicIdPartition);
            this.customMetadataSizeLimit = customMetadataSizeLimit;
            this.logger = getLogContext().logger(RLMCopyTask.class);
        }

        @Override
        protected void execute(UnifiedLog log) throws InterruptedException {
            // In the first run after completing altering logDir within broker, we should make sure the state is reset. (KAFKA-16711)
            if (!log.parentDir().equals(logDirectory.orElse(null))) {
                copiedOffsetOption = Optional.empty();
                isLogStartOffsetUpdated = false;
                logDirectory = Optional.of(log.parentDir());
            }

            copyLogSegmentsToRemote(log);
        }

        private void maybeUpdateLogStartOffsetOnBecomingLeader(UnifiedLog log) throws RemoteStorageException {
            if (!isLogStartOffsetUpdated) {
                long logStartOffset = findLogStartOffset(topicIdPartition, log);
                updateRemoteLogStartOffset.accept(topicIdPartition.topicPartition(), logStartOffset);
                isLogStartOffsetUpdated = true;
                logger.info("Found the logStartOffset: {} for partition: {} after becoming leader",
                        logStartOffset, topicIdPartition);
            }
        }

        private void maybeUpdateCopiedOffset(UnifiedLog log) throws RemoteStorageException {
            if (copiedOffsetOption.isEmpty()) {
                // This is found by traversing from the latest leader epoch from leader epoch history and find the highest offset
                // of a segment with that epoch copied into remote storage. If it can not find an entry then it checks for the
                // previous leader epoch till it finds an entry, If there are no entries till the earliest leader epoch in leader
                // epoch cache then it starts copying the segments from the earliest epoch entry's offset.
                copiedOffsetOption = Optional.of(findHighestRemoteOffset(topicIdPartition, log));
                logger.info("Found the highest copiedRemoteOffset: {} for partition: {} after becoming leader", copiedOffsetOption, topicIdPartition);
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
            List<LogSegment> segments = CollectionConverters.asJava(log.logSegments(fromOffset, Long.MAX_VALUE).toSeq());
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
                            if (isCancelled()) {
                                logger.info("Skipping copying log segments as the current task state is changed, cancelled: {}",
                                        isCancelled());
                                return;
                            }

                            copyQuotaManagerLock.lock();
                            try {
                                long throttleTimeMs = rlmCopyQuotaManager.getThrottleTimeMs();
                                while (throttleTimeMs > 0) {
                                    copyThrottleTimeSensor.record(throttleTimeMs, time.milliseconds());
                                    logger.debug("Quota exceeded for copying log segments, waiting for the quota to be available.");
                                    // If the thread gets interrupted while waiting, the InterruptedException is thrown
                                    // back to the caller. It's important to note that the task being executed is already
                                    // cancelled before the executing thread is interrupted. The caller is responsible
                                    // for handling the exception gracefully by checking if the task is already cancelled.
                                    boolean ignored = copyQuotaManagerLockCondition.await(quotaTimeout().toMillis(), TimeUnit.MILLISECONDS);
                                    throttleTimeMs = rlmCopyQuotaManager.getThrottleTimeMs();
                                }
                                rlmCopyQuotaManager.record(candidateLogSegment.logSegment.log().sizeInBytes());
                                // Signal waiting threads to check the quota again
                                copyQuotaManagerLockCondition.signalAll();
                            } finally {
                                copyQuotaManagerLock.unlock();
                            }

                            RemoteLogSegmentId segmentId = RemoteLogSegmentId.generateNew(topicIdPartition);
                            segmentIdsBeingCopied.add(segmentId);
                            try {
                                copyLogSegment(log, candidateLogSegment.logSegment, segmentId, candidateLogSegment.nextSegmentOffset);
                            } finally {
                                segmentIdsBeingCopied.remove(segmentId);
                            }
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

        private void copyLogSegment(UnifiedLog log, LogSegment segment, RemoteLogSegmentId segmentId, long nextSegmentBaseOffset)
                throws InterruptedException, ExecutionException, RemoteStorageException, IOException,
                CustomMetadataSizeLimitExceededException {
            File logFile = segment.log().file();
            String logFileName = logFile.getName();

            logger.info("Copying {} to remote storage.", logFileName);

            long endOffset = nextSegmentBaseOffset - 1;
            File producerStateSnapshotFile = log.producerStateManager().fetchSnapshot(nextSegmentBaseOffset).orElse(null);

            List<EpochEntry> epochEntries = getLeaderEpochEntries(log, segment.baseOffset(), nextSegmentBaseOffset);
            Map<Integer, Long> segmentLeaderEpochs = new HashMap<>(epochEntries.size());
            epochEntries.forEach(entry -> segmentLeaderEpochs.put(entry.epoch, entry.startOffset));

            boolean isTxnIdxEmpty = segment.txnIndex().isEmpty();
            RemoteLogSegmentMetadata copySegmentStartedRlsm = new RemoteLogSegmentMetadata(segmentId, segment.baseOffset(), endOffset,
                    segment.largestTimestamp(), brokerId, time.milliseconds(), segment.log().sizeInBytes(),
                    segmentLeaderEpochs, isTxnIdxEmpty);

            remoteLogMetadataManager.addRemoteLogSegmentMetadata(copySegmentStartedRlsm).get();

            ByteBuffer leaderEpochsIndex = epochEntriesAsByteBuffer(getLeaderEpochEntries(log, -1, nextSegmentBaseOffset));
            LogSegmentData segmentData = new LogSegmentData(logFile.toPath(), toPathIfExists(segment.offsetIndex().file()),
                    toPathIfExists(segment.timeIndex().file()), Optional.ofNullable(toPathIfExists(segment.txnIndex().file())),
                    producerStateSnapshotFile.toPath(), leaderEpochsIndex);
            brokerTopicStats.topicStats(log.topicPartition().topic()).remoteCopyRequestRate().mark();
            brokerTopicStats.allTopicsStats().remoteCopyRequestRate().mark();
            Optional<CustomMetadata> customMetadata;
            
            try {
                customMetadata = remoteLogStorageManager.copyLogSegmentData(copySegmentStartedRlsm, segmentData);
            } catch (RemoteStorageException e) {
                logger.info("Copy failed, cleaning segment {}", copySegmentStartedRlsm.remoteLogSegmentId());
                try {
                    deleteRemoteLogSegment(copySegmentStartedRlsm, ignored -> !isCancelled());
                    LOGGER.info("Cleanup completed for segment {}", copySegmentStartedRlsm.remoteLogSegmentId());
                } catch (RemoteStorageException e1) {
                    LOGGER.info("Cleanup failed, will retry later with segment {}: {}", copySegmentStartedRlsm.remoteLogSegmentId(), e1.getMessage());
                }
                throw e;
            }

            RemoteLogSegmentMetadataUpdate copySegmentFinishedRlsm = new RemoteLogSegmentMetadataUpdate(segmentId, time.milliseconds(),
                    customMetadata, RemoteLogSegmentState.COPY_SEGMENT_FINISHED, brokerId);

            if (customMetadata.isPresent()) {
                long customMetadataSize = customMetadata.get().value().length;
                if (customMetadataSize > this.customMetadataSizeLimit) {
                    CustomMetadataSizeLimitExceededException e = new CustomMetadataSizeLimitExceededException();
                    logger.info("Custom metadata size {} exceeds configured limit {}." +
                                    " Copying will be stopped and copied segment will be attempted to clean." +
                                    " Original metadata: {}",
                            customMetadataSize, this.customMetadataSizeLimit, copySegmentStartedRlsm, e);
                    // For deletion, we provide back the custom metadata by creating a new metadata object from the update.
                    // However, the update itself will not be stored in this case.
                    RemoteLogSegmentMetadata newMetadata = copySegmentStartedRlsm.createWithUpdates(copySegmentFinishedRlsm);
                    try {
                        deleteRemoteLogSegment(newMetadata, ignored -> !isCancelled());
                        LOGGER.info("Cleanup completed for segment {}", newMetadata.remoteLogSegmentId());
                    } catch (RemoteStorageException e1) {
                        LOGGER.info("Cleanup failed, will retry later with segment {}: {}", newMetadata.remoteLogSegmentId(), e1.getMessage());
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
            logger.info("Copied {} to remote storage with segment-id: {}",
                    logFileName, copySegmentFinishedRlsm.remoteLogSegmentId());

            long bytesLag = log.onlyLocalLogSegmentsSize() - log.activeSegment().size();
            long segmentsLag = log.onlyLocalLogSegmentsCount() - 1;
            recordLagStats(bytesLag, segmentsLag);
        }

        // VisibleForTesting
        void recordLagStats(long bytesLag, long segmentsLag) {
            if (!isCancelled()) {
                String topic = topicIdPartition.topic();
                int partition = topicIdPartition.partition();
                brokerTopicStats.recordRemoteCopyLagBytes(topic, partition, bytesLag);
                brokerTopicStats.recordRemoteCopyLagSegments(topic, partition, segmentsLag);
            }
        }

        void resetLagStats() {
            String topic = topicIdPartition.topic();
            int partition = topicIdPartition.partition();
            brokerTopicStats.recordRemoteCopyLagBytes(topic, partition, 0);
            brokerTopicStats.recordRemoteCopyLagSegments(topic, partition, 0);
        }

        private Path toPathIfExists(File file) {
            return file.exists() ? file.toPath() : null;
        }
    }

    class RLMExpirationTask extends RLMTask {
        private final Logger logger;

        public RLMExpirationTask(TopicIdPartition topicIdPartition) {
            super(topicIdPartition);
            this.logger = getLogContext().logger(RLMExpirationTask.class);
        }

        @Override
        protected void execute(UnifiedLog log) throws InterruptedException, RemoteStorageException, ExecutionException {
            cleanupExpiredRemoteLogSegments();
        }

        public void handleLogStartOffsetUpdate(TopicPartition topicPartition, long remoteLogStartOffset) {
            logger.debug("Updating {} with remoteLogStartOffset: {}", topicPartition, remoteLogStartOffset);
            updateRemoteLogStartOffset.accept(topicPartition, remoteLogStartOffset);
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
                if (retentionSizeData.isEmpty()) {
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
                    if (logStartOffset.isEmpty() || logStartOffset.getAsLong() < metadata.endOffset() + 1) {
                        logStartOffset = OptionalLong.of(metadata.endOffset() + 1);
                    }
                    logger.info("About to delete remote log segment {} due to retention size {} breach. Log size after deletion will be {}.",
                            metadata.remoteLogSegmentId(), retentionSizeData.get().retentionSize, remainingBreachedSize + retentionSizeData.get().retentionSize);
                }
                return shouldDeleteSegment;
            }

            public boolean isSegmentBreachedByRetentionTime(RemoteLogSegmentMetadata metadata) {
                boolean shouldDeleteSegment = false;
                if (retentionTimeData.isEmpty()) {
                    return shouldDeleteSegment;
                }
                shouldDeleteSegment = metadata.maxTimestampMs() <= retentionTimeData.get().cleanupUntilMs;
                if (shouldDeleteSegment) {
                    remainingBreachedSize = Math.max(0, remainingBreachedSize - metadata.segmentSizeInBytes());
                    // It is fine to have logStartOffset as `metadata.endOffset() + 1` as the segment offset intervals
                    // are ascending with in an epoch.
                    if (logStartOffset.isEmpty() || logStartOffset.getAsLong() < metadata.endOffset() + 1) {
                        logStartOffset = OptionalLong.of(metadata.endOffset() + 1);
                    }
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
                boolean isSegmentDeleted = deleteRemoteLogSegment(metadata, 
                    ignored -> metadata.segmentLeaderEpochs().keySet().stream().allMatch(epoch -> epoch < earliestEpochEntry.epoch));
                if (isSegmentDeleted) {
                    logger.info("Deleted remote log segment {} due to leader-epoch-cache truncation. " +
                                    "Current earliest-epoch-entry: {}, segment-end-offset: {} and segment-epochs: {}",
                            metadata.remoteLogSegmentId(), earliestEpochEntry, metadata.endOffset(), metadata.segmentLeaderEpochs().keySet());
                }
                // No need to update the log-start-offset as these epochs/offsets are earlier to that value.
                return isSegmentDeleted;
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

        /** Cleanup expired and dangling remote log segments. */
        void cleanupExpiredRemoteLogSegments() throws RemoteStorageException, ExecutionException, InterruptedException {
            if (isCancelled()) {
                logger.info("Returning from remote log segments cleanup as the task state is changed");
                return;
            }

            final Optional<UnifiedLog> logOptional = fetchLog.apply(topicIdPartition.topicPartition());
            if (logOptional.isEmpty()) {
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
                    if (isCancelled()) {
                        logger.info("Returning from remote log segments cleanup for the remaining segments as the task state is changed.");
                        return;
                    }
                    RemoteLogSegmentMetadata metadata = segmentsIterator.next();

                    if (segmentIdsBeingCopied.contains(metadata.remoteLogSegmentId())) {
                        logger.debug("Copy for the segment {} is currently in process. Skipping cleanup for it and the remaining segments",
                                metadata.remoteLogSegmentId());
                        canProcess = false;
                        continue;
                    }
                    // This works as retry mechanism for dangling remote segments that failed the deletion in previous attempts.
                    // Rather than waiting for the retention to kick in, we cleanup early to avoid polluting the cache and possibly waste remote storage.
                    if (RemoteLogSegmentState.DELETE_SEGMENT_STARTED.equals(metadata.state())) {
                        segmentsToDelete.add(metadata);
                        continue;
                    }
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
                if (!deleteRemoteLogSegment(segmentMetadata, ignored -> !isCancelled())) {
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
                        if (!isCancelled()) {
                            RemoteLogSegmentMetadata nextSegmentMetadata = segmentsToBeCleaned.next();
                            sizeOfDeletableSegmentsBytes += nextSegmentMetadata.segmentSizeInBytes();
                            listOfSegmentsToBeCleaned.add(nextSegmentMetadata);
                        }
                    }
                }

                segmentsLeftToDelete += listOfSegmentsToBeCleaned.size();
                updateRemoteDeleteLagWith(segmentsLeftToDelete, sizeOfDeletableSegmentsBytes);
                for (RemoteLogSegmentMetadata segmentMetadata : listOfSegmentsToBeCleaned) {
                    if (!isCancelled()) {
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
            long cleanupUntilMs = time.milliseconds() - retentionMs;
            return retentionMs > -1 && cleanupUntilMs >= 0
                    ? Optional.of(new RetentionTimeData(retentionMs, cleanupUntilMs))
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
                        // Count only the size of segments in "COPY_SEGMENT_FINISHED" state because 
                        // "COPY_SEGMENT_STARTED" means copy didn't complete and we will count them later,
                        // "DELETE_SEGMENT_STARTED" means deletion failed in the previous attempt and we will retry later,
                        // "DELETE_SEGMENT_FINISHED" means deletion completed, so there is nothing to count.
                        if (segmentMetadata.state().equals(RemoteLogSegmentState.COPY_SEGMENT_FINISHED)) {
                            RemoteLogSegmentId segmentId = segmentMetadata.remoteLogSegmentId();
                            if (!visitedSegmentIds.contains(segmentId) && isRemoteSegmentWithinLeaderEpochs(segmentMetadata, logEndOffset, epochEntries)) {
                                remoteLogSizeBytes += segmentMetadata.segmentSizeInBytes();
                                visitedSegmentIds.add(segmentId);
                            }
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
    }

    class RLMFollowerTask extends RLMTask {

        public RLMFollowerTask(TopicIdPartition topicIdPartition) {
            super(topicIdPartition);
        }

        @Override
        protected void execute(UnifiedLog log) throws InterruptedException, RemoteStorageException, ExecutionException {
            OffsetAndEpoch offsetAndEpoch = findHighestRemoteOffset(topicIdPartition, log);
            // Update the highest offset in remote storage for this partition's log so that the local log segments
            // are not deleted before they are copied to remote storage.
            log.updateHighestOffsetInRemoteStorage(offsetAndEpoch.offset());
        }
    }
    
    private boolean deleteRemoteLogSegment(
        RemoteLogSegmentMetadata segmentMetadata,
        Predicate<RemoteLogSegmentMetadata> predicate
    ) throws RemoteStorageException, ExecutionException, InterruptedException {
        if (predicate.test(segmentMetadata)) {
            LOGGER.debug("Deleting remote log segment {}", segmentMetadata.remoteLogSegmentId());
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
            LOGGER.debug("Deleted remote log segment {}", segmentMetadata.remoteLogSegmentId());
            return true;
        }
        return false;
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
    static boolean isRemoteSegmentWithinLeaderEpochs(RemoteLogSegmentMetadata segmentMetadata,
                                                     long logEndOffset,
                                                     NavigableMap<Integer, Long> leaderEpochs) {
        long segmentEndOffset = segmentMetadata.endOffset();
        // Filter epochs that does not have any messages/records associated with them.
        NavigableMap<Integer, Long> segmentLeaderEpochs = buildFilteredLeaderEpochMap(segmentMetadata.segmentLeaderEpochs());
        // Check for out of bound epochs between segment epochs and current leader epochs.
        Integer segmentLastEpoch = segmentLeaderEpochs.lastKey();
        if (segmentLastEpoch < leaderEpochs.firstKey() || segmentLastEpoch > leaderEpochs.lastKey()) {
            LOGGER.debug("Segment {} is not within the partition leader epoch lineage. " +
                            "Remote segment epochs: {} and partition leader epochs: {}",
                    segmentMetadata.remoteLogSegmentId(), segmentLeaderEpochs, leaderEpochs);
            return false;
        }
        // There can be overlapping remote log segments in the remote storage. (eg)
        // leader-epoch-file-cache: {(5, 10), (7, 15), (9, 100)}
        // segment1: offset-range = 5-50, Broker = 0, epochs = {(5, 10), (7, 15)}
        // segment2: offset-range = 14-150, Broker = 1, epochs = {(5, 14), (7, 15), (9, 100)}, after leader-election.
        // When the segment1 gets deleted, then the log-start-offset = 51 and leader-epoch-file-cache gets updated to: {(7, 51), (9, 100)}.
        // While validating the segment2, we should ensure the overlapping remote log segments case.
        Integer segmentFirstEpoch = segmentLeaderEpochs.ceilingKey(leaderEpochs.firstKey());
        if (segmentFirstEpoch == null) {
            LOGGER.debug("Segment {} is not within the partition leader epoch lineage. " +
                            "Remote segment epochs: {} and partition leader epochs: {}",
                    segmentMetadata.remoteLogSegmentId(), segmentLeaderEpochs, leaderEpochs);
            return false;
        }
        for (Map.Entry<Integer, Long> entry : segmentLeaderEpochs.entrySet()) {
            int epoch = entry.getKey();
            long offset = entry.getValue();
            if (epoch < segmentFirstEpoch) {
                continue;
            }
            // If segment's epoch does not exist in the leader epoch lineage then it is not a valid segment.
            if (!leaderEpochs.containsKey(epoch)) {
                LOGGER.debug("Segment {} epoch {} is not within the leader epoch lineage. " +
                                "Remote segment epochs: {} and partition leader epochs: {}",
                        segmentMetadata.remoteLogSegmentId(), epoch, segmentLeaderEpochs, leaderEpochs);
                return false;
            }
            // Two cases:
            // case-1: When the segment-first-epoch equals to the first-epoch in the leader-epoch-lineage, then the
            // offset value can lie anywhere between 0 to (next-epoch-start-offset - 1) is valid.
            // case-2: When the segment-first-epoch is not equal to the first-epoch in the leader-epoch-lineage, then
            // the offset value should be between (current-epoch-start-offset) to (next-epoch-start-offset - 1).
            if (epoch == segmentFirstEpoch && leaderEpochs.lowerKey(epoch) != null && offset < leaderEpochs.get(epoch)) {
                LOGGER.debug("Segment {} first-valid epoch {} offset is less than first leader epoch offset {}." +
                                "Remote segment epochs: {} and partition leader epochs: {}",
                        segmentMetadata.remoteLogSegmentId(), epoch, leaderEpochs.get(epoch),
                        segmentLeaderEpochs, leaderEpochs);
                return false;
            }
            // Segment's end offset should be less than or equal to the respective leader epoch's offset.
            if (epoch == segmentLastEpoch) {
                Map.Entry<Integer, Long> nextEntry = leaderEpochs.higherEntry(epoch);
                if (nextEntry != null && segmentEndOffset > nextEntry.getValue() - 1) {
                    LOGGER.debug("Segment {} end offset {} is more than leader epoch offset {}." +
                                    "Remote segment epochs: {} and partition leader epochs: {}",
                            segmentMetadata.remoteLogSegmentId(), segmentEndOffset, nextEntry.getValue() - 1,
                            segmentLeaderEpochs, leaderEpochs);
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
     * For ex:
     * <pre>
     * {@code
     *  <epoch - start offset>
     *  0 - 0
     *  1 - 10
     *  2 - 20
     *  3 - 30
     *  4 - 40
     *  5 - 60  // epoch 5 does not have records or messages associated with it
     *  6 - 60
     *  7 - 70
     * }
     * </pre>
     * When the above leaderEpochMap is passed to this method, it returns the following map:
     * <pre>
     * {@code
     *  <epoch - start offset>
     *  0 - 0
     *  1 - 10
     *  2 - 20
     *  3 - 30
     *  4 - 40
     *  6 - 60
     *  7 - 70
     * }
     * </pre>
     * @param leaderEpochs The leader epoch map to be refined.
     */
    // Visible for testing
    static NavigableMap<Integer, Long> buildFilteredLeaderEpochMap(NavigableMap<Integer, Long> leaderEpochs) {
        List<Integer> epochsWithNoMessages = new ArrayList<>();
        Map.Entry<Integer, Long> previousEpochAndOffset = null;
        for (Map.Entry<Integer, Long> currentEpochAndOffset : leaderEpochs.entrySet()) {
            if (previousEpochAndOffset != null && previousEpochAndOffset.getValue().equals(currentEpochAndOffset.getValue())) {
                epochsWithNoMessages.add(previousEpochAndOffset.getKey());
            }
            previousEpochAndOffset = currentEpochAndOffset;
        }
        if (epochsWithNoMessages.isEmpty()) {
            return leaderEpochs;
        }
        TreeMap<Integer, Long> filteredLeaderEpochs = new TreeMap<>(leaderEpochs);
        for (Integer epochWithNoMessage : epochsWithNoMessages) {
            filteredLeaderEpochs.remove(epochWithNoMessage);
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
            if (leaderEpochCache != null && leaderEpochCache.isDefined()) {
                epoch = leaderEpochCache.get().epochForOffset(offset);
            }
        }

        Optional<RemoteLogSegmentMetadata> rlsMetadataOptional = epoch.isPresent()
                ? fetchRemoteLogSegmentMetadata(tp, epoch.getAsInt(), offset)
                : Optional.empty();

        if (rlsMetadataOptional.isEmpty()) {
            String epochStr = (epoch.isPresent()) ? Integer.toString(epoch.getAsInt()) : "NOT AVAILABLE";
            throw new OffsetOutOfRangeException("Received request for offset " + offset + " for leader epoch "
                    + epochStr + " and partition " + tp + " which does not exist in remote tier.");
        }

        RemoteLogSegmentMetadata remoteLogSegmentMetadata = rlsMetadataOptional.get();
        EnrichedRecordBatch enrichedRecordBatch = new EnrichedRecordBatch(null, 0);
        InputStream remoteSegInputStream = null;
        try {
            int startPos = 0;
            //  Iteration over multiple RemoteSegmentMetadata is required in case of log compaction.
            //  It may be possible the offset is log compacted in the current RemoteLogSegmentMetadata
            //  And we need to iterate over the next segment metadata to fetch messages higher than the given offset.
            while (enrichedRecordBatch.batch == null && rlsMetadataOptional.isPresent()) {
                remoteLogSegmentMetadata = rlsMetadataOptional.get();
                // Search forward for the position of the last offset that is greater than or equal to the target offset
                startPos = lookupPositionForOffset(remoteLogSegmentMetadata, offset);
                remoteSegInputStream = remoteLogStorageManager.fetchLogSegment(remoteLogSegmentMetadata, startPos);
                RemoteLogInputStream remoteLogInputStream = getRemoteLogInputStream(remoteSegInputStream);
                enrichedRecordBatch = findFirstBatch(remoteLogInputStream, offset);
                if (enrichedRecordBatch.batch == null) {
                    Utils.closeQuietly(remoteSegInputStream, "RemoteLogSegmentInputStream");
                    rlsMetadataOptional = findNextSegmentMetadata(rlsMetadataOptional.get(), logOptional.get().leaderEpochCache());
                }
            }
            RecordBatch firstBatch = enrichedRecordBatch.batch;
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

            startPos = startPos + enrichedRecordBatch.skippedBytes;
            FetchDataInfo fetchDataInfo = new FetchDataInfo(
                    new LogOffsetMetadata(firstBatch.baseOffset(), remoteLogSegmentMetadata.startOffset(), startPos),
                    MemoryRecords.readableRecords(buffer));
            if (includeAbortedTxns) {
                fetchDataInfo = addAbortedTransactions(firstBatch.baseOffset(), remoteLogSegmentMetadata, fetchDataInfo, logOptional.get());
            }

            return fetchDataInfo;
        } finally {
            if (enrichedRecordBatch.batch != null) {
                Utils.closeQuietly(remoteSegInputStream, "RemoteLogSegmentInputStream");
            }
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

        long startTimeNs = time.nanoseconds();
        collectAbortedTransactions(startOffset, upperBoundOffset, segmentMetadata, accumulator, log);
        LOGGER.debug("Time taken to collect: {} aborted transactions for {} in {} ns", abortedTransactions.size(),
                segmentMetadata, time.nanoseconds() - startTimeNs);

        return new FetchDataInfo(fetchInfo.fetchOffsetMetadata,
                fetchInfo.records,
                fetchInfo.firstEntryIncomplete,
                Optional.of(abortedTransactions.isEmpty() ? Collections.emptyList() : new ArrayList<>(abortedTransactions)));
    }

    /**
     * Collects the aborted transaction entries from the current and subsequent segments until the upper bound offset.
     * Note that the accumulated aborted transaction entries might contain duplicates as it collects the entries across
     * segments. We are relying on the client to discard the duplicates.
     * @param startOffset The start offset of the fetch request.
     * @param upperBoundOffset The upper bound offset of the fetch request.
     * @param segmentMetadata The current segment metadata.
     * @param accumulator The accumulator to collect the aborted transactions.
     * @param log The unified log instance.
     * @throws RemoteStorageException If an error occurs while fetching the remote log segment metadata.
     */
    private void collectAbortedTransactions(long startOffset,
                                            long upperBoundOffset,
                                            RemoteLogSegmentMetadata segmentMetadata,
                                            Consumer<List<AbortedTxn>> accumulator,
                                            UnifiedLog log) throws RemoteStorageException {
        TopicPartition tp = segmentMetadata.topicIdPartition().topicPartition();
        boolean isSearchComplete = false;
        LeaderEpochFileCache leaderEpochCache = log.leaderEpochCache().getOrElse(null);
        Optional<RemoteLogSegmentMetadata> currentMetadataOpt = Optional.of(segmentMetadata);
        while (!isSearchComplete && currentMetadataOpt.isPresent()) {
            RemoteLogSegmentMetadata currentMetadata = currentMetadataOpt.get();
            Optional<TransactionIndex> txnIndexOpt = getTransactionIndex(currentMetadata);
            if (txnIndexOpt.isPresent()) {
                TransactionIndex txnIndex = txnIndexOpt.get();
                TxnIndexSearchResult searchResult = txnIndex.collectAbortedTxns(startOffset, upperBoundOffset);
                accumulator.accept(searchResult.abortedTransactions);
                isSearchComplete = searchResult.isComplete;
            }
            if (!isSearchComplete) {
                currentMetadataOpt = findNextSegmentWithTxnIndex(tp, currentMetadata.endOffset() + 1, leaderEpochCache);
            }
        }
        // Search in local segments
        if (!isSearchComplete) {
            collectAbortedTransactionInLocalSegments(startOffset, upperBoundOffset, accumulator, log.logSegments().iterator());
        }
    }

    private Optional<TransactionIndex> getTransactionIndex(RemoteLogSegmentMetadata currentMetadata) {
        return !currentMetadata.isTxnIdxEmpty() ?
                // `ofNullable` is needed for backward compatibility for old events that were stored in the
                // `__remote_log_metadata` topic. The old events will return the `txnIdxEmpty` as false, but the
                // transaction index may not exist in the remote storage.
                Optional.ofNullable(indexCache.getIndexEntry(currentMetadata).txnIndex()) : Optional.empty();
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

    /**
     * Returns the next segment metadata that contains the aborted transaction entries from the given offset.
     * Note that the search starts from the given (offset-for-epoch, offset) pair, when there are no segments contains
     * the transaction index in that epoch, then it proceeds to the next epoch (next-epoch, epoch-start-offset)
     * and the search ends when the segment metadata is found or the leader epoch cache is exhausted.
     * Note that the returned segment metadata may or may not contain the transaction index.
     * Visible for testing
     * @param tp The topic partition.
     * @param offset The offset to start the search.
     * @param leaderEpochCache The leader epoch file cache, this could be null.
     * @return The next segment metadata that contains the transaction index. The transaction index may or may not exist
     * in that segment metadata which depends on the RLMM plugin implementation. The caller of this method should handle
     * for both the cases.
     * @throws RemoteStorageException If an error occurs while fetching the remote log segment metadata.
     */
    Optional<RemoteLogSegmentMetadata> findNextSegmentWithTxnIndex(TopicPartition tp,
                                                                   long offset,
                                                                   LeaderEpochFileCache leaderEpochCache) throws RemoteStorageException {
        if (leaderEpochCache == null) {
            return Optional.empty();
        }
        OptionalInt initialEpochOpt = leaderEpochCache.epochForOffset(offset);
        if (initialEpochOpt.isEmpty()) {
            return Optional.empty();
        }
        int initialEpoch = initialEpochOpt.getAsInt();
        for (EpochEntry epochEntry : leaderEpochCache.epochEntries()) {
            if (epochEntry.epoch >= initialEpoch) {
                long startOffset = Math.max(epochEntry.startOffset, offset);
                Optional<RemoteLogSegmentMetadata> metadataOpt = fetchNextSegmentWithTxnIndex(tp, epochEntry.epoch, startOffset);
                if (metadataOpt.isPresent()) {
                    return metadataOpt;
                }
            }
        }
        return Optional.empty();
    }

    // Visible for testing
    EnrichedRecordBatch findFirstBatch(RemoteLogInputStream remoteLogInputStream, long offset) throws IOException {
        int skippedBytes = 0;
        RecordBatch nextBatch = null;
        // Look for the batch which has the desired offset
        // We will always have a batch in that segment as it is a non-compacted topic.
        do {
            if (nextBatch != null) {
                skippedBytes += nextBatch.sizeInBytes();
            }
            nextBatch = remoteLogInputStream.nextBatch();
        } while (nextBatch != null && nextBatch.lastOffset() < offset);
        return new EnrichedRecordBatch(nextBatch, skippedBytes);
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
            while (logStartOffset.isEmpty() && earliestEpochOpt.isPresent()) {
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
        return remoteStorageReaderThreadPool.submit(
                new RemoteLogReader(fetchInfo, this, callback, brokerTopicStats, rlmFetchQuotaManager, remoteReadTimer));
    }

    void doHandleLeaderPartition(TopicIdPartition topicPartition, Boolean remoteLogCopyDisable) {
        RLMTaskWithFuture followerRLMTaskWithFuture = followerRLMTasks.remove(topicPartition);
        if (followerRLMTaskWithFuture != null) {
            LOGGER.info("Cancelling the follower task: {}", followerRLMTaskWithFuture.rlmTask);
            followerRLMTaskWithFuture.cancel();
        }

        // Only create copy task when remoteLogCopyDisable is disabled
        if (!remoteLogCopyDisable) {
            leaderCopyRLMTasks.computeIfAbsent(topicPartition, topicIdPartition -> {
                RLMCopyTask task = new RLMCopyTask(topicIdPartition, this.rlmConfig.remoteLogMetadataCustomMetadataMaxBytes());
                // set this upfront when it is getting initialized instead of doing it after scheduling.
                LOGGER.info("Created a new copy task: {} and getting scheduled", task);
                ScheduledFuture<?> future = rlmCopyThreadPool.scheduleWithFixedDelay(task, 0, delayInMs, TimeUnit.MILLISECONDS);
                return new RLMTaskWithFuture(task, future);
            });
        }

        leaderExpirationRLMTasks.computeIfAbsent(topicPartition, topicIdPartition -> {
            RLMExpirationTask task = new RLMExpirationTask(topicIdPartition);
            LOGGER.info("Created a new expiration task: {} and getting scheduled", task);
            ScheduledFuture<?> future = rlmExpirationThreadPool.scheduleWithFixedDelay(task, 0, delayInMs, TimeUnit.MILLISECONDS);
            return new RLMTaskWithFuture(task, future);
        });
    }

    void doHandleFollowerPartition(TopicIdPartition topicPartition) {
        RLMTaskWithFuture copyRLMTaskWithFuture = leaderCopyRLMTasks.remove(topicPartition);
        if (copyRLMTaskWithFuture != null) {
            LOGGER.info("Cancelling the copy task: {}", copyRLMTaskWithFuture.rlmTask);
            copyRLMTaskWithFuture.cancel();
        }

        RLMTaskWithFuture expirationRLMTaskWithFuture = leaderExpirationRLMTasks.remove(topicPartition);
        if (expirationRLMTaskWithFuture != null) {
            LOGGER.info("Cancelling the expiration task: {}", expirationRLMTaskWithFuture.rlmTask);
            expirationRLMTaskWithFuture.cancel();
        }

        followerRLMTasks.computeIfAbsent(topicPartition, topicIdPartition -> {
            RLMFollowerTask task = new RLMFollowerTask(topicIdPartition);
            LOGGER.info("Created a new follower task: {} and getting scheduled", task);
            ScheduledFuture<?> future = followerThreadPool.scheduleWithFixedDelay(task, 0, delayInMs, TimeUnit.MILLISECONDS);
            return new RLMTaskWithFuture(task, future);
        });
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
                leaderCopyRLMTasks.values().forEach(RLMTaskWithFuture::cancel);
                leaderExpirationRLMTasks.values().forEach(RLMTaskWithFuture::cancel);
                followerRLMTasks.values().forEach(RLMTaskWithFuture::cancel);
                Utils.closeQuietly(remoteLogStorageManager, "RemoteLogStorageManager");
                Utils.closeQuietly(remoteLogMetadataManager, "RemoteLogMetadataManager");
                Utils.closeQuietly(indexCache, "RemoteIndexCache");

                rlmCopyThreadPool.close();
                rlmExpirationThreadPool.close();
                followerThreadPool.close();
                try {
                    shutdownAndAwaitTermination(remoteStorageReaderThreadPool, "RemoteStorageReaderThreadPool", 10, TimeUnit.SECONDS);
                } finally {
                    removeMetrics();
                }

                leaderCopyRLMTasks.clear();
                leaderExpirationRLMTasks.clear();
                followerRLMTasks.clear();
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

    //Visible for testing
    static ByteBuffer epochEntriesAsByteBuffer(List<EpochEntry> epochEntries) throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(stream, StandardCharsets.UTF_8))) {
            CheckpointFile.CheckpointWriteBuffer<EpochEntry> writeBuffer =
                    new CheckpointFile.CheckpointWriteBuffer<>(writer, 0, LeaderEpochCheckpointFile.FORMATTER);
            writeBuffer.write(epochEntries);
            writer.flush();
        }

        return ByteBuffer.wrap(stream.toByteArray());
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
    RLMTaskWithFuture leaderCopyTask(TopicIdPartition partition) {
        return leaderCopyRLMTasks.get(partition);
    }
    RLMTaskWithFuture leaderExpirationTask(TopicIdPartition partition) {
        return leaderExpirationRLMTasks.get(partition);
    }
    RLMTaskWithFuture followerTask(TopicIdPartition partition) {
        return followerRLMTasks.get(partition);
    }

    static class RLMScheduledThreadPool {

        private static final Logger LOGGER = LoggerFactory.getLogger(RLMScheduledThreadPool.class);
        private final int poolSize;
        private final String threadPoolName;
        private final String threadNamePrefix;
        private final ScheduledThreadPoolExecutor scheduledThreadPool;

        public RLMScheduledThreadPool(int poolSize, String threadPoolName, String threadNamePrefix) {
            this.poolSize = poolSize;
            this.threadPoolName = threadPoolName;
            this.threadNamePrefix = threadNamePrefix;
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
                    return KafkaThread.daemon(threadNamePrefix + sequence.incrementAndGet(), r);
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
            shutdownAndAwaitTermination(scheduledThreadPool, threadPoolName, 10, TimeUnit.SECONDS);
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

    static class EnrichedRecordBatch {
        private final RecordBatch batch;
        private final int skippedBytes;

        public EnrichedRecordBatch(RecordBatch batch, int skippedBytes) {
            this.batch = batch;
            this.skippedBytes = skippedBytes;
        }
    }
}
