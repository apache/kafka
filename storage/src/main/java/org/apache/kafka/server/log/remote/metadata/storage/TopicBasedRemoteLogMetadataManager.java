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
package org.apache.kafka.server.log.remote.metadata.storage;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.Exit;
import org.apache.kafka.common.utils.internals.KafkaThread;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_UPDATE_KEY_SUFFIX;

/**
 * This is the {@link RemoteLogMetadataManager} implementation with storage as an internal topic with name
 * {@link TopicBasedRemoteLogMetadataManagerConfig#REMOTE_LOG_METADATA_TOPIC_NAME}.
 * This is used to publish and fetch {@link RemoteLogMetadata} for the registered user topic partitions with
 * {@link #onPartitionLeadershipChanges(Set, Set)}. Each broker will have an instance of this class, and it subscribes
 * to metadata updates for the registered user topic partitions.
 */
public class TopicBasedRemoteLogMetadataManager implements BrokerReadyCallback, RemoteLogMetadataManager {
    private static final Logger log = LoggerFactory.getLogger(TopicBasedRemoteLogMetadataManager.class);
    private final Time time = Time.SYSTEM;

    private final AtomicBoolean configured = new AtomicBoolean(false);
    private final AtomicBoolean closing = new AtomicBoolean(false);
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    private Thread initializationThread;
    private volatile ProducerManager producerManager;
    private volatile ConsumerManager consumerManager;

    // This allows to gracefully close this instance using {@link #close()} method while there are some pending or new
    // requests calling different methods which use the resources like producer/consumer managers.
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final RemotePartitionMetadataStore remotePartitionMetadataStore;
    private final Set<TopicIdPartition> pendingAssignPartitions = Collections.synchronizedSet(new HashSet<>());
    private final Function<Integer, RemoteLogMetadataTopicPartitioner> partitionerFunction;

    public TopicBasedRemoteLogMetadataManager() {
        this(RemoteLogMetadataTopicPartitioner::new, RemotePartitionMetadataStore::new);
    }

    TopicBasedRemoteLogMetadataManager(Function<Integer, RemoteLogMetadataTopicPartitioner> partitionerFunction,
                                       Supplier<RemotePartitionMetadataStore> metadataStoreSupplier) {
        this.partitionerFunction = partitionerFunction;
        this.remotePartitionMetadataStore = metadataStoreSupplier.get();
    }

    /**
     * Adds metadata for a remote log segment to the metadata store.
     * The provided metadata must have the state {@code COPY_SEGMENT_STARTED}.
     *
     * @param remoteLogSegmentMetadata the metadata of the remote log segment to be added; must not be null
     * @return a {@link CompletableFuture} that completes once the metadata has been published to the topic
     * @throws RemoteStorageException   if an error occurs while storing the metadata
     * @throws IllegalArgumentException if the state of the provided metadata is not {@code COPY_SEGMENT_STARTED}
     */
    @Override
    public CompletableFuture<Void> addRemoteLogSegmentMetadata(RemoteLogSegmentMetadata remoteLogSegmentMetadata)
            throws RemoteStorageException {
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata can not be null");
        return withReadLockAndEnsureInitialized(() -> {
            if (remoteLogSegmentMetadata.state() != RemoteLogSegmentState.COPY_SEGMENT_STARTED) {
                throw new IllegalArgumentException(
                        "Given remoteLogSegmentMetadata should have state as " + RemoteLogSegmentState.COPY_SEGMENT_STARTED
                                + " but it contains state as: " + remoteLogSegmentMetadata.state());
            }
            return storeRemoteLogMetadata(remoteLogSegmentMetadata);
        });
    }

    @Override
    public CompletableFuture<Void> updateRemoteLogSegmentMetadata(RemoteLogSegmentMetadataUpdate metadataUpdate)
            throws RemoteStorageException {
        Objects.requireNonNull(metadataUpdate, "metadataUpdate can not be null");
        return withReadLockAndEnsureInitialized(() -> {
            if (metadataUpdate.state() == RemoteLogSegmentState.COPY_SEGMENT_STARTED) {
                throw new IllegalArgumentException("Given remoteLogSegmentMetadataUpdate should not have the state as: "
                        + RemoteLogSegmentState.COPY_SEGMENT_STARTED);
            }

            if (metadataUpdate.state() == RemoteLogSegmentState.DELETE_SEGMENT_FINISHED) {
                return tombstoneSegmentMetadata(metadataUpdate.remoteLogSegmentId().topicIdPartition(), metadataUpdate);
            }
            return storeRemoteLogMetadata(metadataUpdate);
        });
    }

    /**
     * Tombstones segment metadata by publishing DELETE_SEGMENT_FINISHED and sending tombstones for historical records.
     *
     * The DELETE_SEGMENT_FINISHED event is critical and must be delivered successfully. However, tombstone messages
     * are sent on a best-effort basis (fire-and-forget) as they are optimization hints for compaction, not critical
     * state updates. The segment is considered deleted once DELETE_SEGMENT_FINISHED is published, regardless of
     * whether tombstones succeed.
     *
     * @param topicIdPartition the topic partition
     * @param segmentMetadataUpdate the DELETE_SEGMENT_FINISHED update
     * @return CompletableFuture that completes when DELETE_SEGMENT_FINISHED is successfully published
     */
    private CompletableFuture<Void> tombstoneSegmentMetadata(TopicIdPartition topicIdPartition, RemoteLogSegmentMetadataUpdate segmentMetadataUpdate)
            throws RemoteStorageException {
        Objects.requireNonNull(segmentMetadataUpdate, "segmentMetadataUpdate can not be null");

        lock.readLock().lock();
        try {
            ensureInitializedAndNotClosed();

            // 1. Publish DELETE_SEGMENT_FINISHED - this is the critical operation
            CompletableFuture<Void> deleteFinishedFuture = storeRemoteLogMetadata(segmentMetadataUpdate);

            // 2. Send tombstones on a best-effort basis (fire-and-forget)
            // These are optimization hints for compaction and don't affect correctness.
            // Index cleanup will happen when the tombstones are consumed by ConsumerTask.
            try {
                Iterator<String> toBeTombstonedKeys = remotePartitionMetadataStore.listRemoteLogSegmentKeysByEndOffset(
                        topicIdPartition, segmentMetadataUpdate.endOffset(), segmentMetadataUpdate.brokerLeaderEpoch());

                while (toBeTombstonedKeys.hasNext()) {
                    String metadataKey = toBeTombstonedKeys.next();

                    // Fire-and-forget: send tombstones but don't wait for completion
                    // First tombstone the update key, then tombstone the metadata key
                    producerManager.publishTombstone(topicIdPartition, metadataKey + REMOTE_LOG_METADATA_UPDATE_KEY_SUFFIX)
                            .whenComplete((metadata, exception) -> {
                                if (exception != null) {
                                    log.warn("Failed to publish tombstone for key: {}{}. This is non-critical and " +
                                            "will be retried in the future. Error: {}", metadataKey, REMOTE_LOG_METADATA_UPDATE_KEY_SUFFIX, exception.getMessage());
                                } else {
                                    log.debug("Successfully published tombstone for key: {}{}", metadataKey, REMOTE_LOG_METADATA_UPDATE_KEY_SUFFIX);

                                    // After update key tombstone succeeds, tombstone the metadata key
                                    producerManager.publishTombstone(topicIdPartition, metadataKey)
                                            .whenComplete((metadata2, exception2) -> {
                                                if (exception2 != null) {
                                                    log.warn("Failed to publish tombstone for key: {}. This is non-critical and " +
                                                            "will be retried in the future. Error: {}", metadataKey, exception2.getMessage());
                                                } else {
                                                    log.debug("Successfully published tombstone for key: {}", metadataKey);
                                                }
                                            });
                                }
                            });
                }
            } catch (Exception e) {
                // Log but don't fail the operation - tombstones are best-effort
                log.warn("Failed to query or publish tombstones for segment with endOffset: {}. " +
                        "This is non-critical. Error: {}", segmentMetadataUpdate.endOffset(), e.getMessage());
            }

            // 3. Return the DELETE_SEGMENT_FINISHED future - operation succeeds when this completes
            return deleteFinishedFuture;

        } catch (KafkaException e) {
            if (e instanceof RetriableException) {
                throw e;
            } else {
                throw new RemoteStorageException(e);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public CompletableFuture<Void> putRemotePartitionDeleteMetadata(RemotePartitionDeleteMetadata deleteMetadata)
            throws RemoteStorageException {
        Objects.requireNonNull(deleteMetadata, "deleteMetadata can not be null");
        return withReadLockAndEnsureInitialized(
                () -> storeRemoteLogMetadata(deleteMetadata));
    }

    /**
     * Returns {@link CompletableFuture} which will complete only after publishing of the given {@code remoteLogMetadata} into
     * the remote log metadata topic and the internal consumer is caught up until the produced record's offset.
     *
     *  @param remoteLogMetadata RemoteLogMetadata to be stored.
     * @return a future with acknowledge and potentially waiting also for consumer to catch up.
     * This ensures cache is synchronized with backing topic.
     * @throws RemoteStorageException if there are any storage errors occur.
     */
    private CompletableFuture<Void> storeRemoteLogMetadata(RemoteLogMetadata remoteLogMetadata) throws RemoteStorageException {
        log.debug("Storing the partition: {} metadata: {}", remoteLogMetadata.topicIdPartition(), remoteLogMetadata);
        try {
            // Publish the message to the metadata topic.
            CompletableFuture<RecordMetadata> produceFuture = producerManager.publishMessage(remoteLogMetadata);
            // Create and return a `CompletableFuture` instance which completes when the consumer is caught up with the produced record's offset.
            return produceFuture.thenAcceptAsync(recordMetadata -> {
                try {
                    consumerManager.waitTillConsumptionCatchesUp(recordMetadata);
                } catch (TimeoutException e) {
                    throw new KafkaException(e);
                }
            });
        } catch (KafkaException e) {
            if (e instanceof RetriableException) {
                throw e;
            } else {
                throw new RemoteStorageException(e);
            }
        }
    }

    @Override
    public Optional<RemoteLogSegmentMetadata> remoteLogSegmentMetadata(TopicIdPartition topicIdPartition,
                                                                       int epochForOffset,
                                                                       long offset) throws RemoteStorageException {
        return withReadLockAndEnsureInitialized(
                () -> remotePartitionMetadataStore.remoteLogSegmentMetadata(topicIdPartition, offset, epochForOffset));
    }

    @Override
    public Optional<Long> highestOffsetForEpoch(TopicIdPartition topicIdPartition,
                                                int leaderEpoch)
            throws RemoteStorageException {
        return withReadLockAndEnsureInitialized(
                () -> remotePartitionMetadataStore.highestLogOffset(topicIdPartition, leaderEpoch));
    }

    @Override
    public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicIdPartition topicIdPartition)
            throws RemoteStorageException {
        Objects.requireNonNull(topicIdPartition, "topicIdPartition can not be null");
        return withReadLockAndEnsureInitialized(
                () -> remotePartitionMetadataStore.listRemoteLogSegments(topicIdPartition));
    }

    @Override
    public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicIdPartition topicIdPartition, int leaderEpoch)
            throws RemoteStorageException {
        Objects.requireNonNull(topicIdPartition, "topicIdPartition can not be null");
        return withReadLockAndEnsureInitialized(
                () -> remotePartitionMetadataStore.listRemoteLogSegments(topicIdPartition, leaderEpoch));
    }

    @Override
    public void onPartitionLeadershipChanges(Set<TopicIdPartition> leaderPartitions,
                                             Set<TopicIdPartition> followerPartitions) {
        Objects.requireNonNull(leaderPartitions, "leaderPartitions can not be null");
        Objects.requireNonNull(followerPartitions, "followerPartitions can not be null");
        log.info("Received leadership notifications with leader partitions {} and follower partitions {}",
                 leaderPartitions, followerPartitions);
        lock.readLock().lock();
        try {
            if (closing.get()) {
                throw new IllegalStateException("This instance is in closing state");
            }
            Set<TopicIdPartition> allPartitions = new HashSet<>(leaderPartitions);
            allPartitions.addAll(followerPartitions);
            if (!initialized.get()) {
                // If it is not yet initialized, then keep them as pending partitions and assign them
                // when it is initialized successfully in initializeResources().
                this.pendingAssignPartitions.addAll(allPartitions);
            } else {
                assignPartitions(allPartitions);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    private void assignPartitions(Set<TopicIdPartition> allPartitions) {
        for (TopicIdPartition partition : allPartitions) {
            remotePartitionMetadataStore.maybeLoadPartition(partition);
        }
        consumerManager.addAssignmentsForPartitions(allPartitions);
    }

    @Override
    public void onStopPartitions(Set<TopicIdPartition> partitions) {
        lock.readLock().lock();
        try {
            if (closing.get()) {
                throw new IllegalStateException("This instance is in closing state");
            }
            if (!initialized.get()) {
                // If it is not yet initialized, then remove them from the pending partitions if any.
                if (!pendingAssignPartitions.isEmpty()) {
                    pendingAssignPartitions.removeAll(partitions);
                }
            } else {
                consumerManager.removeAssignmentsForPartitions(partitions);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public long remoteLogSize(TopicIdPartition topicIdPartition, int leaderEpoch) throws RemoteStorageException {
        long remoteLogSize = 0L;
        // This is a simple-to-understand but not the most optimal solution.
        // The TopicBasedRemoteLogMetadataManager's remote metadata store is file-based. During design discussions
        // at https://lists.apache.org/thread/kxd6fffq02thbpd0p5y4mfbs062g7jr6
        // we reached a consensus that sequential iteration over files on the local file system is performant enough.
        // Should this stop being the case, the remote log size could be calculated by incrementing/decrementing
        // counters during API calls for a more performant implementation.
        Iterator<RemoteLogSegmentMetadata> remoteLogSegmentMetadataIterator =
                remotePartitionMetadataStore.listRemoteLogSegments(topicIdPartition, leaderEpoch);
        while (remoteLogSegmentMetadataIterator.hasNext()) {
            RemoteLogSegmentMetadata remoteLogSegmentMetadata = remoteLogSegmentMetadataIterator.next();
            remoteLogSize += remoteLogSegmentMetadata.segmentSizeInBytes();
        }
        return remoteLogSize;
    }

    @Override
    public Optional<RemoteLogSegmentMetadata> nextSegmentWithTxnIndex(TopicIdPartition topicIdPartition,
                                                                      int epoch,
                                                                      long offset) throws RemoteStorageException {
        return withReadLockAndEnsureInitialized(
                () -> remotePartitionMetadataStore.nextSegmentWithTxnIndex(topicIdPartition, epoch, offset));
    }

    @Override
    public void configure(Map<String, ?> configs) {
        Objects.requireNonNull(configs, "configs can not be null.");
        lock.writeLock().lock();
        try {
            if (configured.compareAndSet(false, true)) {
                TopicBasedRemoteLogMetadataManagerConfig rlmmConfig = new TopicBasedRemoteLogMetadataManagerConfig(configs);
                // Creates initialization thread for producer/consumer managers. It will be started when
                // the broker is ready via onBrokerReady(). The thread retries until resources are available.
                initializationThread = KafkaThread.nonDaemon(
                        "RLMMInitializationThread", () -> initializeResources(rlmmConfig));
                log.info("Successfully configured topic-based RLMM with config: {}", rlmmConfig);
            } else {
                log.info("Skipping configure as it is already configured.");
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public boolean isReady(TopicIdPartition topicIdPartition) {
        return remotePartitionMetadataStore.isInitialized(topicIdPartition);
    }

    private void handleRetry(long retryIntervalMs) {
        log.info("Sleep for {} ms before retrying.", retryIntervalMs);
        Utils.sleep(retryIntervalMs);
    }

    private void initializeResources(TopicBasedRemoteLogMetadataManagerConfig rlmmConfig) {
        log.info("Initializing topic-based RLMM resources");
        int metadataTopicPartitionCount = rlmmConfig.metadataTopicPartitionsCount();
        long retryIntervalMs = rlmmConfig.initializationRetryIntervalMs();
        long retryMaxTimeoutMs = rlmmConfig.initializationRetryMaxTimeoutMs();
        RemoteLogMetadataTopicPartitioner partitioner = partitionerFunction.apply(metadataTopicPartitionCount);
        NewTopic newTopic = newRemoteLogMetadataTopic(rlmmConfig);
        boolean isTopicCreated = false;
        long startTimeMs = time.milliseconds();
        boolean initializationFailed = false;
        try (Admin admin = Admin.create(rlmmConfig.adminProperties())) {
            while (!(initialized.get() || closing.get() || initializationFailed)) {
                if (time.milliseconds() - startTimeMs > retryMaxTimeoutMs) {
                    log.error("Timed out to initialize the resources within {} ms.", retryMaxTimeoutMs);
                    initializationFailed = true;
                    break;
                }
                isTopicCreated = isTopicCreated || createTopic(admin, newTopic);
                if (!isTopicCreated) {
                    handleRetry(retryIntervalMs);
                    continue;
                }
                try {
                    if (!isPartitionsCountSameAsConfigured(admin, newTopic.name(), metadataTopicPartitionCount)) {
                        initializationFailed = true;
                        break;
                    }
                } catch (Exception e) {
                    handleRetry(retryIntervalMs);
                    continue;
                }
                // Create producer and consumer managers.
                lock.writeLock().lock();
                try {
                    producerManager = new ProducerManager(rlmmConfig, partitioner);
                    consumerManager = new ConsumerManager(rlmmConfig, remotePartitionMetadataStore, partitioner, time);
                    consumerManager.startConsumerThread();
                    if (!pendingAssignPartitions.isEmpty()) {
                        assignPartitions(pendingAssignPartitions);
                        pendingAssignPartitions.clear();
                    }
                    initialized.set(true);
                    log.info("Initialized topic-based RLMM resources successfully");
                } catch (Exception e) {
                    log.error("Encountered error while initializing producer/consumer", e);
                    initializationFailed = true;
                } finally {
                    lock.writeLock().unlock();
                }
            }
        } catch (KafkaException e) {
            log.error("Encountered error while initializing topic-based RLMM resources", e);
            initializationFailed = true;
        } finally {
            if (initializationFailed) {
                log.error("Stopping the server as it failed to initialize topic-based RLMM resources");
                Exit.exit(1);
            }
        }
    }

    /**
     * Invoked when the broker is ready to handle requests. This triggers the initialization of
     * resources including Kafka clients that operate on the remote log metadata topic.
     * <p>
     * The target cluster for the topic is determined by configuration and can be either:
     * <ol>
     *   <li>The local cluster (most common) - the initialization is deferred until the broker is ready
     *       to handle requests. Early initialization would lead to connection failures.</li>
     *   <li>A remote cluster - the delay is not necessary but causes no harm.</li>
     * </ol>
     * <p>
     * By using the broker ready state as the initialization trigger, the implementation optimally handles
     * the typical case while remaining correct for alternative configurations.
     */
    @Override
    public void onBrokerReady() {
        initializationThread.start();
    }

    boolean doesTopicExist(Admin admin, String topic) throws ExecutionException, InterruptedException {
        try {
            TopicDescription description = admin.describeTopics(Set.of(topic))
                    .topicNameValues()
                    .get(topic)
                    .get();
            log.info("Topic {} exists. TopicId: {}, numPartitions: {}", topic, description.topicId(),
                    description.partitions().size());
            return true;
        } catch (ExecutionException | InterruptedException ex) {
            if (ex.getCause() instanceof UnknownTopicOrPartitionException) {
                log.info("Topic {} does not exist", topic);
                return false;
            }
            throw ex;
        }
    }

    private boolean isPartitionsCountSameAsConfigured(Admin admin,
                                                      String topicName,
                                                      int metadataTopicPartitionCount) throws InterruptedException, ExecutionException {
        log.debug("Getting topic details to check for partition count and replication factor.");
        TopicDescription topicDescription = admin
                .describeTopics(Set.of(topicName))
                .topicNameValues()
                .get(topicName)
                .get();
        int topicPartitionsSize = topicDescription.partitions().size();
        if (topicPartitionsSize != metadataTopicPartitionCount) {
            log.error("Existing topic partition count {} is not same as the expected partition count {}",
                      topicPartitionsSize, metadataTopicPartitionCount);
            return false;
        }
        return true;
    }

    private NewTopic newRemoteLogMetadataTopic(TopicBasedRemoteLogMetadataManagerConfig rlmmConfig) {
        Map<String, String> topicConfigs = new HashMap<>();
        topicConfigs.put(TopicConfig.RETENTION_MS_CONFIG, Long.toString(rlmmConfig.metadataTopicRetentionMs()));
        topicConfigs.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false");
        topicConfigs.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Short.toString(rlmmConfig.metadataTopicMinIsr()));

        // Always use compaction for new topic creation.
        // For new clusters, remote.log.metadata.version will be at the latest (V1) by default.
        // For existing clusters, the topic already exists so this code path is not used.
        // When existing clusters upgrade the feature to V1, the controller will update the
        // existing topic's cleanup policy to compact.
        topicConfigs.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);

        return new NewTopic(rlmmConfig.remoteLogMetadataTopicName(),
                            rlmmConfig.metadataTopicPartitionsCount(),
                            rlmmConfig.metadataTopicReplicationFactor()).configs(topicConfigs);
    }

    /**
     * @param newTopic topic to be created.
     * @return Returns true if the topic already exists, or it is created successfully.
     */
    private boolean createTopic(Admin admin, NewTopic newTopic) {
        boolean doesTopicExist = false;
        String topic = newTopic.name();
        try {
            doesTopicExist = doesTopicExist(admin, topic);
            if (!doesTopicExist) {
                CreateTopicsResult result = admin.createTopics(Set.of(newTopic));
                result.all().get();
                List<String> overriddenConfigs = result.config(topic).get()
                        .entries()
                        .stream()
                        .filter(entry -> !entry.isDefault())
                        .map(entry -> entry.name() + "=" + entry.value())
                        .toList();
                log.info("Topic {} created. TopicId: {}, numPartitions: {}, replicationFactor: {}, config: {}",
                        topic, result.topicId(topic).get(), result.numPartitions(topic).get(),
                        result.replicationFactor(topic).get(), overriddenConfigs);
                doesTopicExist = true;
            }
        } catch (Exception e) {
            // This exception can still occur as multiple brokers may call create topics and one of them may become
            // successful and other would throw TopicExistsException
            if (e.getCause() instanceof TopicExistsException) {
                log.info("Topic: {} already exists", topic);
                doesTopicExist = true;
            } else {
                log.error("Encountered error while querying or creating {} topic.", topic, e);
            }
        }
        return doesTopicExist;
    }

    boolean isInitialized() {
        return initialized.get();
    }


    private void ensureInitializedAndNotClosed() {
        if (closing.get() || !initialized.get()) {
            throw new IllegalStateException("This instance is in invalid state, initialized: " + initialized +
                                                    " close: " + closing);
        }
    }

    @Override
    public void close() throws IOException {
        // Close all the resources.
        log.info("Closing topic-based RLMM resources");
        if (closing.compareAndSet(false, true)) {
            if (initializationThread != null) {
                try {
                    initializationThread.join();
                } catch (InterruptedException e) {
                    log.error("Initialization thread was interrupted while waiting to join on close.", e);
                }
            }
            Utils.closeQuietly(producerManager, "ProducerTask");
            Utils.closeQuietly(consumerManager, "RLMMConsumerManager");
            Utils.closeQuietly(remotePartitionMetadataStore, "RemotePartitionMetadataStore");
            log.info("Closed topic-based RLMM resources");
        }
    }

    private <T> T withReadLockAndEnsureInitialized(ThrowingSupplier<T, RemoteStorageException> action) throws RemoteStorageException {
        lock.readLock().lock();
        try {
            ensureInitializedAndNotClosed();
            return action.get();
        } finally {
            lock.readLock().unlock();
        }
    }

    @FunctionalInterface
    public interface ThrowingSupplier<T, E extends Exception> {
        /**
         * Supplies a result, potentially throwing an exception.
         *
         * @return the supplied result.
         * @throws E an exception that may be thrown during execution.
         */
        T get() throws E;
    }
}
