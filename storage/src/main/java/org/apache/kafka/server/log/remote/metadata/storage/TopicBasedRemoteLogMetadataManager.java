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

import org.apache.kafka.clients.admin.AdminClient;
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
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This is the {@link RemoteLogMetadataManager} implementation with storage as an internal topic with name {@link TopicBasedRemoteLogMetadataManagerConfig#REMOTE_LOG_METADATA_TOPIC_NAME}.
 * This is used to publish and fetch {@link RemoteLogMetadata} for the registered user topic partitions with
 * {@link #onPartitionLeadershipChanges(Set, Set)}. Each broker will have an instance of this class and it subscribes
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

    private Thread initializationThread;
    private volatile ProducerManager producerManager;
    private volatile ConsumerManager consumerManager;

    // This allows to gracefully close this instance using {@link #close()} method while there are some pending or new
    // requests calling different methods which use the resources like producer/consumer managers.
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final RemotePartitionMetadataStore remotePartitionMetadataStore = new RemotePartitionMetadataStore();
    private volatile TopicBasedRemoteLogMetadataManagerConfig rlmmConfig;
    private volatile RemoteLogMetadataTopicPartitioner rlmmTopicPartitioner;
    private final Set<TopicIdPartition> pendingAssignPartitions = Collections.synchronizedSet(new HashSet<>());
    private volatile boolean initializationFailed;

    @Override
    public void addRemoteLogSegmentMetadata(RemoteLogSegmentMetadata remoteLogSegmentMetadata)
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
            doPublishMetadata(remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition(),
                              remoteLogSegmentMetadata);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void updateRemoteLogSegmentMetadata(RemoteLogSegmentMetadataUpdate segmentMetadataUpdate)
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
            doPublishMetadata(segmentMetadataUpdate.remoteLogSegmentId().topicIdPartition(), segmentMetadataUpdate);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void putRemotePartitionDeleteMetadata(RemotePartitionDeleteMetadata remotePartitionDeleteMetadata)
            throws RemoteStorageException {
        Objects.requireNonNull(remotePartitionDeleteMetadata, "remotePartitionDeleteMetadata can not be null");

        lock.readLock().lock();
        try {
            ensureInitializedAndNotClosed();

            doPublishMetadata(remotePartitionDeleteMetadata.topicIdPartition(), remotePartitionDeleteMetadata);
        } finally {
            lock.readLock().unlock();
        }
    }

    private void doPublishMetadata(TopicIdPartition topicIdPartition, RemoteLogMetadata remoteLogMetadata)
            throws RemoteStorageException {
        log.debug("Publishing metadata for partition: [{}] with context: [{}]", topicIdPartition, remoteLogMetadata);

        try {
            // Publish the message to the topic.
            RecordMetadata recordMetadata = producerManager.publishMessage(remoteLogMetadata);
            // Wait until the consumer catches up with this offset. This will ensure read-after-write consistency
            // semantics.
            consumerManager.waitTillConsumptionCatchesUp(recordMetadata);
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
        return rlmmTopicPartitioner.metadataPartition(topicIdPartition);
    }

    // Visible For Testing
    public Optional<Long> receivedOffsetForPartition(int metadataPartition) {
        return consumerManager.receivedOffsetForPartition(metadataPartition);
    }

    @Override
    public void onPartitionLeadershipChanges(Set<TopicIdPartition> leaderPartitions,
                                             Set<TopicIdPartition> followerPartitions) {
        Objects.requireNonNull(leaderPartitions, "leaderPartitions can not be null");
        Objects.requireNonNull(followerPartitions, "followerPartitions can not be null");

        log.info("Received leadership notifications with leader partitions {} and follower partitions {}",
                 leaderPartitions, followerPartitions);

        HashSet<TopicIdPartition> allPartitions = new HashSet<>(leaderPartitions);
        allPartitions.addAll(followerPartitions);
        lock.readLock().lock();
        try {
            if (closing.get()) {
                throw new IllegalStateException("This instance is in closing state");
            }

            if (!initialized.get()) {
                // If it is not yet initialized, then keep them as pending partitions and assign them
                // when it is initialized successfully in initializeResources().
                this.pendingAssignPartitions.addAll(allPartitions);
            } else {
                consumerManager.addAssignmentsForPartitions(allPartitions);
            }
        } finally {
            lock.readLock().unlock();
        }
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
    public void configure(Map<String, ?> configs) {
        Objects.requireNonNull(configs, "configs can not be null.");

        lock.writeLock().lock();
        try {
            if (configured) {
                log.info("Skipping configure as it is already configured.");
                return;
            }

            log.info("Started initializing with configs: {}", configs);

            rlmmConfig = new TopicBasedRemoteLogMetadataManagerConfig(configs);
            rlmmTopicPartitioner = new RemoteLogMetadataTopicPartitioner(rlmmConfig.metadataTopicPartitionsCount());
            configured = true;
            log.info("Successfully initialized with rlmmConfig: {}", rlmmConfig);

            // Scheduling the initialization producer/consumer managers in a separate thread. Required resources may
            // not yet be available now. This thread makes sure that it is retried at regular intervals until it is
            // successful.
            initializationThread = KafkaThread.nonDaemon("RLMMInitializationThread", () -> initializeResources());
            initializationThread.start();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void initializeResources() {
        log.info("Initializing the resources.");
        final NewTopic remoteLogMetadataTopicRequest = createRemoteLogMetadataTopicRequest();
        boolean topicCreated = false;
        long startTimeMs = time.milliseconds();
        AdminClient adminClient = null;
        try {
            adminClient = AdminClient.create(rlmmConfig.producerProperties());

            // Stop if it is already initialized or closing.
            while (!(initialized.get() || closing.get())) {

                // If it is timed out then raise an error to exit.
                if (time.milliseconds() - startTimeMs > rlmmConfig.initializationRetryMaxTimeoutMs()) {
                    log.error("Timed out in initializing the resources, retried to initialize the resource for [{}] ms.",
                              rlmmConfig.initializationRetryMaxTimeoutMs());
                    initializationFailed = true;
                    return;
                }

                if (!topicCreated) {
                    topicCreated = createTopic(adminClient, remoteLogMetadataTopicRequest);
                }

                if (!topicCreated) {
                    // Sleep for INITIALIZATION_RETRY_INTERVAL_MS before trying to create the topic again.
                    log.info("Sleep for : {} ms before it is retried again.", rlmmConfig.initializationRetryIntervalMs());
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
                        log.info("Sleep for : {} ms before it is retried again.", rlmmConfig.initializationRetryIntervalMs());
                        Utils.sleep(rlmmConfig.initializationRetryIntervalMs());
                        continue;
                    }
                }

                // Create producer and consumer managers.
                lock.writeLock().lock();
                try {
                    producerManager = new ProducerManager(rlmmConfig, rlmmTopicPartitioner);
                    consumerManager = new ConsumerManager(rlmmConfig, remotePartitionMetadataStore, rlmmTopicPartitioner, time);
                    consumerManager.startConsumerThread();

                    if (!pendingAssignPartitions.isEmpty()) {
                        consumerManager.addAssignmentsForPartitions(pendingAssignPartitions);
                        pendingAssignPartitions.clear();
                    }

                    initialized.set(true);
                    log.info("Initialized resources successfully.");
                } catch (Exception e) {
                    log.error("Encountered error while initializing producer/consumer", e);
                    return;
                } finally {
                    lock.writeLock().unlock();
                }
            }

        } finally {
            if (adminClient != null) {
                try {
                    adminClient.close(Duration.ofSeconds(10));
                } catch (Exception e) {
                    // Ignore the error.
                    log.debug("Error occurred while closing the admin client", e);
                }
            }
        }
    }

    private boolean isPartitionsCountSameAsConfigured(AdminClient adminClient,
                                                      String topicName) throws InterruptedException, ExecutionException {
        log.debug("Getting topic details to check for partition count and replication factor.");
        TopicDescription topicDescription = adminClient.describeTopics(Collections.singleton(topicName))
                                                       .values().get(topicName).get();
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
        return new NewTopic(rlmmConfig.remoteLogMetadataTopicName(),
                            rlmmConfig.metadataTopicPartitionsCount(),
                            rlmmConfig.metadataTopicReplicationFactor()).configs(topicConfigs);
    }

    /**
     * @param topic topic to be created.
     * @return Returns true if the topic already exists or it is created successfully.
     */
    private boolean createTopic(AdminClient adminClient, NewTopic topic) {
        boolean topicCreated = false;
        try {
            adminClient.createTopics(Collections.singleton(topic)).all().get();
            topicCreated = true;
        } catch (Exception e) {
            if (e.getCause() instanceof TopicExistsException) {
                log.info("Topic [{}] already exists", topic.name());
                topicCreated = true;
            } else {
                log.error("Encountered error while creating remote log metadata topic.", e);
            }
        }

        return topicCreated;
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

    @Override
    public void close() throws IOException {
        // Close all the resources.
        log.info("Closing the resources.");
        if (closing.compareAndSet(false, true)) {
            lock.writeLock().lock();
            try {
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
            } finally {
                lock.writeLock().unlock();
                log.info("Closed the resources.");
            }
        }
    }
}
