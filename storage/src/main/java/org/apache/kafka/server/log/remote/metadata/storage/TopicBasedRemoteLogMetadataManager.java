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
import org.apache.kafka.common.internals.FatalExitError;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
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
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This is the {@link RemoteLogMetadataManager} implementation with storage as an internal topic with name {@link TopicBasedRemoteLogMetadataManagerConfig#REMOTE_LOG_METADATA_TOPIC_NAME}.
 * This is used to publish and fetch {@link RemoteLogMetadata} for the registered user topic partitions with
 * {@link #onPartitionLeadershipChanges(Set, Set)}. Each broker will have an instance of this class, and it subscribes
 * to metadata updates for the registered user topic partitions.
 */
public class TopicBasedRemoteLogMetadataManager implements RemoteLogMetadataManager {
    private static final Logger log = LoggerFactory.getLogger(TopicBasedRemoteLogMetadataManager.class);

    private volatile boolean configured = false;

    // It indicates whether the close process of this instance is started or not via #close() method.
    // Using AtomicBoolean instead of volatile as it may encounter http://findbugs.sourceforge.net/bugDescriptions.html#SP_SPIN_ON_FIELD
    // if the field is read but not updated in a spin loop like in #initializeResources() method.
    private final AtomicBoolean closing = new AtomicBoolean(false);
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final Time time = Time.SYSTEM;
    private final boolean startConsumerThread;

    private Thread initializationThread;
    private volatile ProducerManager producerManager;
    private volatile ConsumerManager consumerManager;

    // This allows to gracefully close this instance using {@link #close()} method while there are some pending or new
    // requests calling different methods which use the resources like producer/consumer managers.
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private RemotePartitionMetadataStore remotePartitionMetadataStore;
    private volatile TopicBasedRemoteLogMetadataManagerConfig rlmmConfig;
    private volatile RemoteLogMetadataTopicPartitioner rlmTopicPartitioner;
    private final Set<TopicIdPartition> pendingAssignPartitions = Collections.synchronizedSet(new HashSet<>());
    private volatile boolean initializationFailed;
    private final Supplier<RemotePartitionMetadataStore> remoteLogMetadataManagerSupplier;
    private final Function<Integer, RemoteLogMetadataTopicPartitioner> remoteLogMetadataTopicPartitionerFunction;

    /**
     * The default constructor delegates to the internal one, starting the consumer thread and
     * supplying an instance of RemoteLogMetadataTopicPartitioner and RemotePartitionMetadataStore by default.
     */
    public TopicBasedRemoteLogMetadataManager() {
        this(true, RemoteLogMetadataTopicPartitioner::new, RemotePartitionMetadataStore::new);
    }

    /**
     * Used in tests to dynamically configure the instance.
     */
    TopicBasedRemoteLogMetadataManager(boolean startConsumerThread, Function<Integer, RemoteLogMetadataTopicPartitioner> remoteLogMetadataTopicPartitionerFunction, Supplier<RemotePartitionMetadataStore> remoteLogMetadataManagerSupplier) {
        this.startConsumerThread = startConsumerThread;
        this.remoteLogMetadataManagerSupplier = remoteLogMetadataManagerSupplier;
        this.remoteLogMetadataTopicPartitionerFunction = remoteLogMetadataTopicPartitionerFunction;
    }

    @Override
    public CompletableFuture<Void> addRemoteLogSegmentMetadata(RemoteLogSegmentMetadata remoteLogSegmentMetadata)
            throws RemoteStorageException {
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata can not be null");

        // This allows gracefully rejecting the requests while closing of this instance is in progress, which triggers
        // closing the producer/consumer manager instances.
        lock.readLock().lock();
        try {
            ensureInitializedAndNotClosed();

            // This method is allowed only to add remote log segment with the initial state(which is RemoteLogSegmentState.COPY_SEGMENT_STARTED)
            // but not to update the existing remote log segment metadata.
            if (remoteLogSegmentMetadata.state() != RemoteLogSegmentState.COPY_SEGMENT_STARTED) {
                throw new IllegalArgumentException(
                        "Given remoteLogSegmentMetadata should have state as " + RemoteLogSegmentState.COPY_SEGMENT_STARTED
                                + " but it contains state as: " + remoteLogSegmentMetadata.state());
            }

            // Publish the message to the topic.
            return storeRemoteLogMetadata(remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition(),
                                          remoteLogSegmentMetadata);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public CompletableFuture<Void> updateRemoteLogSegmentMetadata(RemoteLogSegmentMetadataUpdate segmentMetadataUpdate)
            throws RemoteStorageException {
        Objects.requireNonNull(segmentMetadataUpdate, "segmentMetadataUpdate can not be null");

        lock.readLock().lock();
        try {
            ensureInitializedAndNotClosed();

            // Callers should use addRemoteLogSegmentMetadata to add RemoteLogSegmentMetadata with state as
            // RemoteLogSegmentState.COPY_SEGMENT_STARTED.
            if (segmentMetadataUpdate.state() == RemoteLogSegmentState.COPY_SEGMENT_STARTED) {
                throw new IllegalArgumentException("Given remoteLogSegmentMetadata should not have the state as: "
                                                           + RemoteLogSegmentState.COPY_SEGMENT_STARTED);
            }

            // Publish the message to the topic.
            return storeRemoteLogMetadata(segmentMetadataUpdate.remoteLogSegmentId().topicIdPartition(), segmentMetadataUpdate);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public CompletableFuture<Void> putRemotePartitionDeleteMetadata(RemotePartitionDeleteMetadata remotePartitionDeleteMetadata)
            throws RemoteStorageException {
        Objects.requireNonNull(remotePartitionDeleteMetadata, "remotePartitionDeleteMetadata can not be null");

        lock.readLock().lock();
        try {
            ensureInitializedAndNotClosed();

            return storeRemoteLogMetadata(remotePartitionDeleteMetadata.topicIdPartition(), remotePartitionDeleteMetadata);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns {@link CompletableFuture} which will complete only after publishing of the given {@code remoteLogMetadata} into
     * the remote log metadata topic and the internal consumer is caught up until the produced record's offset.
     *
     * @param topicIdPartition partition of the given remoteLogMetadata.
     * @param remoteLogMetadata RemoteLogMetadata to be stored.
     * @return a future with acknowledge and potentially waiting also for consumer to catch up.
     * This ensures cache is synchronized with backing topic.
     * @throws RemoteStorageException if there are any storage errors occur.
     */
    private CompletableFuture<Void> storeRemoteLogMetadata(TopicIdPartition topicIdPartition,
                                                           RemoteLogMetadata remoteLogMetadata)
            throws RemoteStorageException {
        log.debug("Storing the partition: {} metadata: {}", topicIdPartition, remoteLogMetadata);
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
                                                                       long offset)
            throws RemoteStorageException {
        lock.readLock().lock();
        try {
            ensureInitializedAndNotClosed();

            return remotePartitionMetadataStore.remoteLogSegmentMetadata(topicIdPartition, offset, epochForOffset);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Optional<Long> highestOffsetForEpoch(TopicIdPartition topicIdPartition,
                                                int leaderEpoch)
            throws RemoteStorageException {
        lock.readLock().lock();
        try {

            ensureInitializedAndNotClosed();

            return remotePartitionMetadataStore.highestLogOffset(topicIdPartition, leaderEpoch);
        } finally {
            lock.readLock().unlock();
        }

    }

    @Override
    public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicIdPartition topicIdPartition)
            throws RemoteStorageException {
        Objects.requireNonNull(topicIdPartition, "topicIdPartition can not be null");

        lock.readLock().lock();
        try {
            ensureInitializedAndNotClosed();

            return remotePartitionMetadataStore.listRemoteLogSegments(topicIdPartition);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicIdPartition topicIdPartition, int leaderEpoch)
            throws RemoteStorageException {
        Objects.requireNonNull(topicIdPartition, "topicIdPartition can not be null");

        lock.readLock().lock();
        try {
            ensureInitializedAndNotClosed();

            return remotePartitionMetadataStore.listRemoteLogSegments(topicIdPartition, leaderEpoch);
        } finally {
            lock.readLock().unlock();
        }
    }

    public int metadataPartition(TopicIdPartition topicIdPartition) {
        return rlmTopicPartitioner.metadataPartition(topicIdPartition);
    }

    // Visible For Testing
    public Optional<Long> readOffsetForPartition(int metadataPartition) {
        return consumerManager.readOffsetForPartition(metadataPartition);
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

            HashSet<TopicIdPartition> allPartitions = new HashSet<>(leaderPartitions);
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
        Iterator<RemoteLogSegmentMetadata> remoteLogSegmentMetadataIterator = remotePartitionMetadataStore.listRemoteLogSegments(topicIdPartition, leaderEpoch);
        while (remoteLogSegmentMetadataIterator.hasNext()) {
            RemoteLogSegmentMetadata remoteLogSegmentMetadata = remoteLogSegmentMetadataIterator.next();
            remoteLogSize += remoteLogSegmentMetadata.segmentSizeInBytes();
        }
        return remoteLogSize;
    }

    @Override
    public Optional<RemoteLogSegmentMetadata> nextSegmentWithTxnIndex(TopicIdPartition topicIdPartition, int epoch, long offset) throws RemoteStorageException {
        lock.readLock().lock();
        try {
            ensureInitializedAndNotClosed();
            return remotePartitionMetadataStore.nextSegmentWithTxnIndex(topicIdPartition, epoch, offset);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        Objects.requireNonNull(configs, "configs can not be null.");

        lock.writeLock().lock();
        try {
            if (configured) {
                log.info("Skipping configure as it is already configured.");
                return;
            }

            log.info("Started configuring topic-based RLMM with configs: {}", configs);

            rlmmConfig = new TopicBasedRemoteLogMetadataManagerConfig(configs);
            rlmTopicPartitioner = remoteLogMetadataTopicPartitionerFunction.apply(rlmmConfig.metadataTopicPartitionsCount());
            remotePartitionMetadataStore = remoteLogMetadataManagerSupplier.get();
            configured = true;
            log.info("Successfully configured topic-based RLMM with config: {}", rlmmConfig);

            // Scheduling the initialization producer/consumer managers in a separate thread. Required resources may
            // not yet be available now. This thread makes sure that it is retried at regular intervals until it is
            // successful.
            initializationThread = KafkaThread.nonDaemon("RLMMInitializationThread", this::initializeResources);
            initializationThread.start();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public boolean isReady(TopicIdPartition topicIdPartition) {
        return remotePartitionMetadataStore.isInitialized(topicIdPartition);
    }

    private void initializeResources() {
        log.info("Initializing topic-based RLMM resources");
        final NewTopic remoteLogMetadataTopicRequest = createRemoteLogMetadataTopicRequest();
        boolean topicCreated = false;
        long startTimeMs = time.milliseconds();
        Admin adminClient = null;
        try {
            adminClient = Admin.create(rlmmConfig.commonProperties());
            // Stop if it is already initialized or closing.
            while (!(initialized.get() || closing.get())) {

                // If it is timed out then raise an error to exit.
                if (time.milliseconds() - startTimeMs > rlmmConfig.initializationRetryMaxTimeoutMs()) {
                    log.error("Timed out in initializing the resources, retried to initialize the resource for {} ms.",
                            rlmmConfig.initializationRetryMaxTimeoutMs());
                    initializationFailed = true;
                    return;
                }

                if (!topicCreated) {
                    topicCreated = createTopic(adminClient, remoteLogMetadataTopicRequest);
                }

                if (!topicCreated) {
                    // Sleep for INITIALIZATION_RETRY_INTERVAL_MS before trying to create the topic again.
                    log.info("Sleep for {} ms before it is retried again.", rlmmConfig.initializationRetryIntervalMs());
                    Utils.sleep(rlmmConfig.initializationRetryIntervalMs());
                    continue;
                } else {
                    // If topic is already created, validate the existing topic partitions.
                    try {
                        String topicName = remoteLogMetadataTopicRequest.name();
                        // If the existing topic partition size is not same as configured, mark initialization as failed and exit.
                        if (!isPartitionsCountSameAsConfigured(adminClient, topicName)) {
                            initializationFailed = true;
                        }
                    } catch (Exception e) {
                        log.info("Sleep for {} ms before it is retried again.", rlmmConfig.initializationRetryIntervalMs());
                        Utils.sleep(rlmmConfig.initializationRetryIntervalMs());
                        continue;
                    }
                }

                // Create producer and consumer managers.
                lock.writeLock().lock();
                try {
                    producerManager = new ProducerManager(rlmmConfig, rlmTopicPartitioner);
                    consumerManager = new ConsumerManager(rlmmConfig, remotePartitionMetadataStore, rlmTopicPartitioner, time);
                    if (startConsumerThread) {
                        consumerManager.startConsumerThread();
                    } else {
                        log.info("RLMM Consumer task thread is not configured to be started.");
                    }

                    if (!pendingAssignPartitions.isEmpty()) {
                        assignPartitions(pendingAssignPartitions);
                        pendingAssignPartitions.clear();
                    }

                    initialized.set(true);
                    log.info("Initialized topic-based RLMM resources successfully");
                } catch (Exception e) {
                    log.error("Encountered error while initializing producer/consumer", e);
                    initializationFailed = true;
                    return;
                } finally {
                    lock.writeLock().unlock();
                }
            }
        } catch (Exception e) {
            log.error("Encountered error while initializing topic-based RLMM resources", e);
            initializationFailed = true;
        } finally {
            Utils.closeQuietly(adminClient, "AdminClient");
        }
    }

    boolean doesTopicExist(Admin adminClient, String topic) {
        try {
            TopicDescription description = adminClient.describeTopics(Collections.singleton(topic))
                    .topicNameValues()
                    .get(topic)
                    .get();
            if (description != null) {
                log.info("Topic {} exists. TopicId: {}, numPartitions: {}, ", topic,
                        description.topicId(), description.partitions().size());
            } else {
                log.info("Topic {} does not exist.", topic);
            }
            return description != null;
        } catch (ExecutionException | InterruptedException ex) {
            log.info("Topic {} does not exist. Error: {}", topic, ex.getCause().getMessage());
            return false;
        }
    }

    private boolean isPartitionsCountSameAsConfigured(Admin adminClient,
                                                      String topicName) throws InterruptedException, ExecutionException {
        log.debug("Getting topic details to check for partition count and replication factor.");
        TopicDescription topicDescription = adminClient.describeTopics(Collections.singleton(topicName))
                                                       .topicNameValues().get(topicName).get();
        int expectedPartitions = rlmmConfig.metadataTopicPartitionsCount();
        int topicPartitionsSize = topicDescription.partitions().size();

        if (topicPartitionsSize != expectedPartitions) {
            log.error("Existing topic partition count [{}] is not same as the expected partition count [{}]",
                      topicPartitionsSize, expectedPartitions);
            return false;
        }

        return true;
    }

    private NewTopic createRemoteLogMetadataTopicRequest() {
        Map<String, String> topicConfigs = new HashMap<>();
        topicConfigs.put(TopicConfig.RETENTION_MS_CONFIG, Long.toString(rlmmConfig.metadataTopicRetentionMs()));
        topicConfigs.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
        topicConfigs.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false");
        return new NewTopic(rlmmConfig.remoteLogMetadataTopicName(),
                            rlmmConfig.metadataTopicPartitionsCount(),
                            rlmmConfig.metadataTopicReplicationFactor()).configs(topicConfigs);
    }

    /**
     * @param newTopic topic to be created.
     * @return Returns true if the topic already exists, or it is created successfully.
     */
    private boolean createTopic(Admin adminClient, NewTopic newTopic) {
        boolean doesTopicExist = false;
        String topic = newTopic.name();
        try {
            doesTopicExist = doesTopicExist(adminClient, topic);
            if (!doesTopicExist) {
                CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));
                result.all().get();
                List<String> overriddenConfigs = result.config(topic).get()
                        .entries()
                        .stream()
                        .filter(entry -> !entry.isDefault())
                        .map(entry -> entry.name() + "=" + entry.value())
                        .collect(Collectors.toList());
                log.info("Topic {} created. TopicId: {}, numPartitions: {}, replicationFactor: {}, config: {}",
                        topic, result.topicId(topic).get(), result.numPartitions(topic).get(),
                        result.replicationFactor(topic).get(), overriddenConfigs);
                doesTopicExist = true;
            }
        } catch (Exception e) {
            // This exception can still occur as multiple brokers may call create topics and one of them may become
            // successful and other would throw TopicExistsException
            if (e.getCause() instanceof TopicExistsException) {
                log.info("Topic [{}] already exists", topic);
                doesTopicExist = true;
            } else {
                log.error("Encountered error while creating {} topic.", topic, e);
            }
        }
        return doesTopicExist;
    }

    public boolean isInitialized() {
        return initialized.get();
    }

    private void ensureInitializedAndNotClosed() {
        if (initializationFailed) {
            // If initialization is failed, shutdown the broker.
            throw new FatalExitError();
        }
        if (closing.get() || !initialized.get()) {
            throw new IllegalStateException("This instance is in invalid state, initialized: " + initialized +
                                                    " close: " + closing);
        }
    }

    // Visible for testing.
    public TopicBasedRemoteLogMetadataManagerConfig config() {
        return rlmmConfig;
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
}
