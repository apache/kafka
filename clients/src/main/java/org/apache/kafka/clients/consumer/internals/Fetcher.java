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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.message.ApiVersionsResponseData.ApiVersion;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.utils.Timer;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * This class manages the fetching process with the brokers.
 */
public interface Fetcher<K, V> extends Closeable {

    /**
     * Represents data about an offset returned by a broker.
     */
    class ListOffsetData {
        final long offset;
        final Long timestamp; //  null if the broker does not support returning timestamps
        final Optional<Integer> leaderEpoch; // empty if the leader epoch is not known

        ListOffsetData(long offset, Long timestamp, Optional<Integer> leaderEpoch) {
            this.offset = offset;
            this.timestamp = timestamp;
            this.leaderEpoch = leaderEpoch;
        }
    }

    /**
     * Return whether we have any completed fetches pending return to the user. This method is thread-safe. Has
     * visibility for testing.
     * @return true if there are completed fetches, false otherwise
     */
    boolean hasCompletedFetches();

    /**
     * Return whether we have any completed fetches that are fetchable. This method is thread-safe.
     * @return true if there are completed fetches that can be returned, false otherwise
     */
    boolean hasAvailableFetches();

    /**
     * Set-up a fetch request for any node that we have assigned partitions for which doesn't already have
     * an in-flight fetch or pending fetch data.
     * @return number of fetches sent
     */
    int sendFetches();

    /**
     * Get topic metadata for all topics in the cluster
     * @param timer Timer bounding how long this method can block
     * @return The map of topics with their partition information
     */
    Map<String, List<PartitionInfo>> getAllTopicMetadata(Timer timer);

    /**
     * Get metadata for all topics present in Kafka cluster
     *
     * @param request The MetadataRequest to send
     * @param timer Timer bounding how long this method can block
     * @return The map of topics with their partition information
     */
    Map<String, List<PartitionInfo>> getTopicMetadata(MetadataRequest.Builder request, Timer timer);

    /**
     * Reset offsets for all assigned partitions that require it.
     *
     * @throws org.apache.kafka.clients.consumer.NoOffsetForPartitionException If no offset reset strategy is defined
     *   and one or more partitions aren't awaiting a seekToBeginning() or seekToEnd().
     */
    void resetOffsetsIfNeeded();

    /**
     * Validate offsets for all assigned partitions for which a leader change has been detected.
     */
    void validateOffsetsIfNeeded();

    Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Timer timer);

    Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Timer timer);

    Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Timer timer);

    /**
     * Return the fetched records, empty the record buffer and update the consumed position.
     * <p/>
     * NOTE: returning an {@link Fetch#isEmpty empty} fetch guarantees the consumed position is not updated.
     *
     * @return A {@link Fetch} for the requested partitions
     * @throws OffsetOutOfRangeException If there is OffsetOutOfRange error in fetchResponse and
     *         the defaultResetPolicy is NONE
     * @throws TopicAuthorizationException If there is TopicAuthorization error in fetchResponse.
     */
    Fetch<K, V> collectFetch();

    // Visible for testing
    void resetOffsetIfNeeded(TopicPartition partition, OffsetResetStrategy requestedResetStrategy, ListOffsetData offsetData);

    static boolean hasUsableOffsetForLeaderEpochVersion(NodeApiVersions nodeApiVersions) {
        ApiVersion apiVersion = nodeApiVersions.apiVersion(ApiKeys.OFFSET_FOR_LEADER_EPOCH);
        if (apiVersion == null)
            return false;

        return OffsetsForLeaderEpochRequest.supportsTopicPermission(apiVersion.maxVersion());
    }

    /**
     * Determine which replica to read from.
     */
    Node selectReadReplica(TopicPartition partition, Node leaderReplica, long currentTimeMs);

    /**
     * Clear the buffered data which are not a part of newly assigned partitions
     *
     * @param assignedPartitions  newly assigned {@link TopicPartition}
     */
    void clearBufferedDataForUnassignedPartitions(Collection<TopicPartition> assignedPartitions);

    /**
     * Clear the buffered data which are not a part of newly assigned topics
     *
     * @param assignedTopics  newly assigned topics
     */
    void clearBufferedDataForUnassignedTopics(Collection<String> assignedTopics);

    static Sensor throttleTimeSensor(Metrics metrics, FetcherMetricsRegistry metricsRegistry) {
        Sensor fetchThrottleTimeSensor = metrics.sensor("fetch-throttle-time");
        fetchThrottleTimeSensor.add(metrics.metricInstance(metricsRegistry.fetchThrottleTimeAvg), new Avg());

        fetchThrottleTimeSensor.add(metrics.metricInstance(metricsRegistry.fetchThrottleTimeMax), new Max());

        return fetchThrottleTimeSensor;
    }

    void close();
}