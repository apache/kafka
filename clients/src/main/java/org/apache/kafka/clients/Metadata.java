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
package org.apache.kafka.clients;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidMetadataException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * A class encapsulating some of the logic around metadata.
 * <p>
 * This class is shared by the client thread (for partitioning) and the background sender thread.
 *
 * Metadata is maintained for only a subset of topics, which can be added to over time. When we request metadata for a
 * topic we don't have any metadata for it will trigger a metadata update.
 * <p>
 * If topic expiry is enabled for the metadata, any topic that has not been used within the expiry interval
 * is removed from the metadata refresh set after an update. Consumers disable topic expiry since they explicitly
 * manage topics while producers rely on topic expiry to limit the refresh set.
 */
public class Metadata implements Closeable {
    private final Logger log;
    private final long refreshBackoffMs;
    private final long metadataExpireMs;
    private int updateVersion;  // bumped on every metadata response
    private int requestVersion; // bumped on every new topic addition
    private long lastRefreshMs;
    private long lastSuccessfulRefreshMs;
    private KafkaException fatalException;
    private KafkaException recoverableException;
    private MetadataCache cache = MetadataCache.empty();
    private boolean needUpdate;
    private final ClusterResourceListeners clusterResourceListeners;
    private boolean isClosed;
    private final Map<TopicPartition, Integer> lastSeenLeaderEpochs;

    /**
     * Create a new Metadata instance
     *
     * @param refreshBackoffMs         The minimum amount of time that must expire between metadata refreshes to avoid busy
     *                                 polling
     * @param metadataExpireMs         The maximum amount of time that metadata can be retained without refresh
     * @param logContext               Log context corresponding to the containing client
     * @param clusterResourceListeners List of ClusterResourceListeners which will receive metadata updates.
     */
    public Metadata(long refreshBackoffMs,
                    long metadataExpireMs,
                    LogContext logContext,
                    ClusterResourceListeners clusterResourceListeners) {
        this.log = logContext.logger(Metadata.class);
        this.refreshBackoffMs = refreshBackoffMs;
        this.metadataExpireMs = metadataExpireMs;
        this.lastRefreshMs = 0L;
        this.lastSuccessfulRefreshMs = 0L;
        this.requestVersion = 0;
        this.updateVersion = 0;
        this.needUpdate = false;
        this.clusterResourceListeners = clusterResourceListeners;
        this.isClosed = false;
        this.lastSeenLeaderEpochs = new HashMap<>();
    }

    /**
     * Get the current cluster info without blocking
     */
    public synchronized Cluster fetch() {
        return cache.cluster();
    }

    /**
     * Return the next time when the current cluster info can be updated (i.e., backoff time has elapsed).
     *
     * @param nowMs current time in ms
     * @return remaining time in ms till the cluster info can be updated again
     */
    public synchronized long timeToAllowUpdate(long nowMs) {
        return Math.max(this.lastRefreshMs + this.refreshBackoffMs - nowMs, 0);
    }

    /**
     * The next time to update the cluster info is the maximum of the time the current info will expire and the time the
     * current info can be updated (i.e. backoff time has elapsed); If an update has been request then the expiry time
     * is now
     *
     * @param nowMs current time in ms
     * @return remaining time in ms till updating the cluster info
     */
    public synchronized long timeToNextUpdate(long nowMs) {
        long timeToExpire = needUpdate ? 0 : Math.max(this.lastSuccessfulRefreshMs + this.metadataExpireMs - nowMs, 0);
        return Math.max(timeToExpire, timeToAllowUpdate(nowMs));
    }

    public long metadataExpireMs() {
        return this.metadataExpireMs;
    }

    /**
     * Request an update of the current cluster metadata info, return the current updateVersion before the update
     */
    public synchronized int requestUpdate() {
        this.needUpdate = true;
        return this.updateVersion;
    }

    /**
     * Request an update for the partition metadata iff the given leader epoch is at newer than the last seen leader epoch
     */
    public synchronized boolean updateLastSeenEpochIfNewer(TopicPartition topicPartition, int leaderEpoch) {
        Objects.requireNonNull(topicPartition, "TopicPartition cannot be null");
        return updateLastSeenEpoch(topicPartition, leaderEpoch, oldEpoch -> leaderEpoch > oldEpoch, true);
    }


    public Optional<Integer> lastSeenLeaderEpoch(TopicPartition topicPartition) {
        return Optional.ofNullable(lastSeenLeaderEpochs.get(topicPartition));
    }

    /**
     * Conditionally update the leader epoch for a partition
     *
     * @param topicPartition       topic+partition to update the epoch for
     * @param epoch                the new epoch
     * @param epochTest            a predicate to determine if the old epoch should be replaced
     * @param setRequestUpdateFlag sets the "needUpdate" flag to true if the epoch is updated
     * @return true if the epoch was updated, false otherwise
     */
    private synchronized boolean updateLastSeenEpoch(TopicPartition topicPartition,
                                                     int epoch,
                                                     Predicate<Integer> epochTest,
                                                     boolean setRequestUpdateFlag) {
        Integer oldEpoch = lastSeenLeaderEpochs.get(topicPartition);
        log.trace("Determining if we should replace existing epoch {} with new epoch {}", oldEpoch, epoch);
        if (oldEpoch == null || epochTest.test(oldEpoch)) {
            log.debug("Updating last seen epoch from {} to {} for partition {}", oldEpoch, epoch, topicPartition);
            lastSeenLeaderEpochs.put(topicPartition, epoch);
            if (setRequestUpdateFlag) {
                this.needUpdate = true;
            }
            return true;
        } else {
            log.debug("Not replacing existing epoch {} with new epoch {}", oldEpoch, epoch);
            return false;
        }
    }

    /**
     * Check whether an update has been explicitly requested.
     *
     * @return true if an update was requested, false otherwise
     */
    public synchronized boolean updateRequested() {
        return this.needUpdate;
    }

    /**
     * Return the cached partition info if it exists and a newer leader epoch isn't known about.
     */
    public synchronized Optional<MetadataCache.PartitionInfoAndEpoch> partitionInfoIfCurrent(TopicPartition topicPartition) {
        Integer epoch = lastSeenLeaderEpochs.get(topicPartition);
        if (epoch == null) {
            // old cluster format (no epochs)
            return cache.getPartitionInfo(topicPartition);
        } else {
            return cache.getPartitionInfoHavingEpoch(topicPartition, epoch);
        }
    }

    /**
     * If any non-retriable exceptions were encountered during metadata update, clear and return the exception.
     */
    public synchronized KafkaException getAndClearMetadataException() {
        KafkaException metadataException = Optional.ofNullable(fatalException).orElse(recoverableException);
        fatalException = null;
        recoverableException = null;
        return metadataException;
    }

    public synchronized void bootstrap(List<InetSocketAddress> addresses, long now) {
        this.needUpdate = true;
        this.lastRefreshMs = now;
        this.lastSuccessfulRefreshMs = now;
        this.updateVersion += 1;
        this.cache = MetadataCache.bootstrap(addresses);
    }

    /**
     * Update metadata assuming the current request version. This is mainly for convenience in testing.
     */
    public synchronized void update(MetadataResponse response, long now) {
        this.update(this.requestVersion, response, now);
    }

    /**
     * Updates the cluster metadata. If topic expiry is enabled, expiry time
     * is set for topics if required and expired topics are removed from the metadata.
     *
     * @param requestVersion The request version corresponding to the update response, as provided by
     *     {@link #newMetadataRequestAndVersion()}.
     * @param response metadata response received from the broker
     * @param now current time in milliseconds
     */
    public synchronized void update(int requestVersion, MetadataResponse response, long now) {
        Objects.requireNonNull(response, "Metadata response cannot be null");
        if (isClosed())
            throw new IllegalStateException("Update requested after metadata close");

        if (requestVersion == this.requestVersion)
            this.needUpdate = false;
        else
            requestUpdate();

        this.lastRefreshMs = now;
        this.lastSuccessfulRefreshMs = now;
        this.updateVersion += 1;

        String previousClusterId = cache.cluster().clusterResource().clusterId();

        this.cache = handleMetadataResponse(response, topic -> retainTopic(topic.topic(), topic.isInternal(), now));

        Cluster cluster = cache.cluster();
        maybeSetMetadataError(cluster);

        this.lastSeenLeaderEpochs.keySet().removeIf(tp -> !retainTopic(tp.topic(), false, now));

        String newClusterId = cache.cluster().clusterResource().clusterId();
        if (!Objects.equals(previousClusterId, newClusterId)) {
            log.info("Cluster ID: {}", newClusterId);
        }
        clusterResourceListeners.onUpdate(cache.cluster().clusterResource());

        log.debug("Updated cluster metadata updateVersion {} to {}", this.updateVersion, this.cache);
    }

    private void maybeSetMetadataError(Cluster cluster) {
        // if we encounter any invalid topics, cache the exception to later throw to the user
        recoverableException = null;
        checkInvalidTopics(cluster);
        checkUnauthorizedTopics(cluster);
    }

    private void checkInvalidTopics(Cluster cluster) {
        if (!cluster.invalidTopics().isEmpty()) {
            log.error("Metadata response reported invalid topics {}", cluster.invalidTopics());
            // We may be able to recover from this exception if metadata for this topic is no longer needed
            recoverableException = new InvalidTopicException(cluster.invalidTopics());
        }
    }

    private void checkUnauthorizedTopics(Cluster cluster) {
        if (!cluster.unauthorizedTopics().isEmpty()) {
            log.error("Topic authorization failed for topics {}", cluster.unauthorizedTopics());
            // We may be able to recover from this exception if metadata for this topic is no longer needed
            recoverableException = new TopicAuthorizationException(new HashSet<>(cluster.unauthorizedTopics()));
        }
    }

    /**
     * Transform a MetadataResponse into a new MetadataCache instance.
     */
    private MetadataCache handleMetadataResponse(MetadataResponse metadataResponse,
                                                 Predicate<MetadataResponse.TopicMetadata> topicsToRetain) {
        Set<String> internalTopics = new HashSet<>();
        List<MetadataCache.PartitionInfoAndEpoch> partitions = new ArrayList<>();
        for (MetadataResponse.TopicMetadata metadata : metadataResponse.topicMetadata()) {
            if (!topicsToRetain.test(metadata))
                continue;

            if (metadata.error() == Errors.NONE) {
                if (metadata.isInternal())
                    internalTopics.add(metadata.topic());
                for (MetadataResponse.PartitionMetadata partitionMetadata : metadata.partitionMetadata()) {

                    // Even if the partition's metadata includes an error, we need to handle the update to catch new epochs
                    updatePartitionInfo(metadata.topic(), partitionMetadata, partitionInfo -> {
                        int epoch = partitionMetadata.leaderEpoch().orElse(RecordBatch.NO_PARTITION_LEADER_EPOCH);
                        partitions.add(new MetadataCache.PartitionInfoAndEpoch(partitionInfo, epoch));
                    });

                    if (partitionMetadata.error().exception() instanceof InvalidMetadataException) {
                        log.debug("Requesting metadata update for partition {} due to error {}",
                                new TopicPartition(metadata.topic(), partitionMetadata.partition()), partitionMetadata.error());
                        requestUpdate();
                    }
                }
            } else if (metadata.error().exception() instanceof InvalidMetadataException) {
                log.debug("Requesting metadata update for topic {} due to error {}", metadata.topic(), metadata.error());
                requestUpdate();
            }
        }

        return new MetadataCache(metadataResponse.clusterId(), new ArrayList<>(metadataResponse.brokers()), partitions,
                metadataResponse.topicsByError(Errors.TOPIC_AUTHORIZATION_FAILED),
                metadataResponse.topicsByError(Errors.INVALID_TOPIC_EXCEPTION),
                internalTopics, metadataResponse.controller());
    }

    /**
     * Compute the correct PartitionInfo to cache for a topic+partition and pass to the given consumer.
     */
    private void updatePartitionInfo(String topic,
                                     MetadataResponse.PartitionMetadata partitionMetadata,
                                     Consumer<PartitionInfo> partitionInfoConsumer) {

        TopicPartition tp = new TopicPartition(topic, partitionMetadata.partition());
        if (partitionMetadata.leaderEpoch().isPresent()) {
            int newEpoch = partitionMetadata.leaderEpoch().get();
            // If the received leader epoch is at least the same as the previous one, update the metadata
            if (updateLastSeenEpoch(tp, newEpoch, oldEpoch -> newEpoch >= oldEpoch, false)) {
                partitionInfoConsumer.accept(MetadataResponse.partitionMetaToInfo(topic, partitionMetadata));
            } else {
                // Otherwise ignore the new metadata and use the previously cached info
                PartitionInfo previousInfo = cache.cluster().partition(tp);
                if (previousInfo != null) {
                    partitionInfoConsumer.accept(previousInfo);
                }
            }
        } else {
            // Handle old cluster formats as well as error responses where leader and epoch are missing
            lastSeenLeaderEpochs.remove(tp);
            partitionInfoConsumer.accept(MetadataResponse.partitionMetaToInfo(topic, partitionMetadata));
        }
    }

    public synchronized void maybeThrowException() {
        KafkaException metadataException = getAndClearMetadataException();
        if (metadataException != null)
            throw metadataException;
    }

    /**
     * Record an attempt to update the metadata that failed. We need to keep track of this
     * to avoid retrying immediately.
     */
    public synchronized void failedUpdate(long now, KafkaException fatalException) {
        this.lastRefreshMs = now;
        this.fatalException = fatalException;
    }

    /**
     * @return The current metadata updateVersion
     */
    public synchronized int updateVersion() {
        return this.updateVersion;
    }

    /**
     * The last time metadata was successfully updated.
     */
    public synchronized long lastSuccessfulUpdate() {
        return this.lastSuccessfulRefreshMs;
    }

    /**
     * Close this metadata instance to indicate that metadata updates are no longer possible.
     */
    @Override
    public synchronized void close() {
        this.isClosed = true;
    }

    /**
     * Check if this metadata instance has been closed. See {@link #close()} for more information.
     *
     * @return True if this instance has been closed; false otherwise
     */
    public synchronized boolean isClosed() {
        return this.isClosed;
    }

    public synchronized void requestUpdateForNewTopics() {
        // Override the timestamp of last refresh to let immediate update.
        this.lastRefreshMs = 0;
        this.requestVersion++;
        requestUpdate();
    }

    public synchronized MetadataRequestAndVersion newMetadataRequestAndVersion() {
        return new MetadataRequestAndVersion(newMetadataRequestBuilder(), requestVersion);
    }

    protected MetadataRequest.Builder newMetadataRequestBuilder() {
        return MetadataRequest.Builder.allTopics();
    }

    protected boolean retainTopic(String topic, boolean isInternal, long nowMs) {
        return true;
    }

    public static class MetadataRequestAndVersion {
        public final MetadataRequest.Builder requestBuilder;
        public final int requestVersion;

        private MetadataRequestAndVersion(MetadataRequest.Builder requestBuilder,
                                          int requestVersion) {
            this.requestBuilder = requestBuilder;
            this.requestVersion = requestVersion;
        }
    }

    public synchronized LeaderAndEpoch leaderAndEpoch(TopicPartition tp) {
        return partitionInfoIfCurrent(tp)
                .map(infoAndEpoch -> {
                    Node leader = infoAndEpoch.partitionInfo().leader();
                    return new LeaderAndEpoch(leader == null ? Node.noNode() : leader, Optional.of(infoAndEpoch.epoch()));
                })
                .orElse(new LeaderAndEpoch(Node.noNode(), lastSeenLeaderEpoch(tp)));
    }

    public static class LeaderAndEpoch {

        public static final LeaderAndEpoch NO_LEADER_OR_EPOCH = new LeaderAndEpoch(Node.noNode(), Optional.empty());

        public final Node leader;
        public final Optional<Integer> epoch;

        public LeaderAndEpoch(Node leader, Optional<Integer> epoch) {
            this.leader = Objects.requireNonNull(leader);
            this.epoch = Objects.requireNonNull(epoch);
        }

        public static LeaderAndEpoch noLeaderOrEpoch() {
            return NO_LEADER_OR_EPOCH;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            LeaderAndEpoch that = (LeaderAndEpoch) o;

            if (!leader.equals(that.leader)) return false;
            return epoch.equals(that.epoch);
        }

        @Override
        public int hashCode() {
            int result = leader.hashCode();
            result = 31 * result + epoch.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "LeaderAndEpoch{" +
                    "leader=" + leader +
                    ", epoch=" + epoch.map(Number::toString).orElse("absent") +
                    '}';
        }
    }
}
