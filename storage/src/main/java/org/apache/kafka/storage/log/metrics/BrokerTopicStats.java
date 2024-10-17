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
package org.apache.kafka.storage.log.metrics;

import org.apache.kafka.server.log.remote.storage.RemoteStorageMetrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class BrokerTopicStats implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(BrokerTopicStats.class);

    private final boolean remoteStorageEnabled;
    private final BrokerTopicMetrics allTopicsStats;
    private final ConcurrentMap<String, BrokerTopicMetrics> stats;

    public BrokerTopicStats() {
        this(false);
    }

    public BrokerTopicStats(boolean remoteStorageEnabled) {
        this.remoteStorageEnabled = remoteStorageEnabled;
        this.allTopicsStats = new BrokerTopicMetrics(remoteStorageEnabled);
        this.stats = new ConcurrentHashMap<>();
    }

    public boolean isTopicStatsExisted(String topic) {
        return stats.containsKey(topic);
    }

    public BrokerTopicMetrics topicStats(String topic) {
        return stats.computeIfAbsent(topic, k -> new BrokerTopicMetrics(k, remoteStorageEnabled));
    }

    public void updateReplicationBytesIn(long value) {
        allTopicsStats.replicationBytesInRate().ifPresent(metric -> metric.mark(value));
    }

    private void updateReplicationBytesOut(long value) {
        allTopicsStats.replicationBytesOutRate().ifPresent(metric -> metric.mark(value));
    }

    public void updateReassignmentBytesIn(long value) {
        allTopicsStats.reassignmentBytesInPerSec().ifPresent(metric -> metric.mark(value));
    }

    private void updateReassignmentBytesOut(long value) {
        allTopicsStats.reassignmentBytesOutPerSec().ifPresent(metric -> metric.mark(value));
    }

    // This method only removes metrics only used for leader
    public void removeOldLeaderMetrics(String topic) {
        BrokerTopicMetrics topicMetrics = topicStats(topic);
        if (topicMetrics != null) {
            topicMetrics.closeMetric(BrokerTopicMetrics.MESSAGE_IN_PER_SEC);
            topicMetrics.closeMetric(BrokerTopicMetrics.BYTES_IN_PER_SEC);
            topicMetrics.closeMetric(BrokerTopicMetrics.BYTES_REJECTED_PER_SEC);
            topicMetrics.closeMetric(BrokerTopicMetrics.FAILED_PRODUCE_REQUESTS_PER_SEC);
            topicMetrics.closeMetric(BrokerTopicMetrics.TOTAL_PRODUCE_REQUESTS_PER_SEC);
            topicMetrics.closeMetric(BrokerTopicMetrics.PRODUCE_MESSAGE_CONVERSIONS_PER_SEC);
            topicMetrics.closeMetric(BrokerTopicMetrics.REPLICATION_BYTES_OUT_PER_SEC);
            topicMetrics.closeMetric(BrokerTopicMetrics.REASSIGNMENT_BYTES_OUT_PER_SEC);
            topicMetrics.closeMetric(RemoteStorageMetrics.REMOTE_COPY_BYTES_PER_SEC_METRIC.getName());
            topicMetrics.closeMetric(RemoteStorageMetrics.REMOTE_FETCH_BYTES_PER_SEC_METRIC.getName());
            topicMetrics.closeMetric(RemoteStorageMetrics.REMOTE_FETCH_REQUESTS_PER_SEC_METRIC.getName());
            topicMetrics.closeMetric(RemoteStorageMetrics.REMOTE_COPY_REQUESTS_PER_SEC_METRIC.getName());
            topicMetrics.closeMetric(RemoteStorageMetrics.REMOTE_DELETE_REQUESTS_PER_SEC_METRIC.getName());
            topicMetrics.closeMetric(RemoteStorageMetrics.BUILD_REMOTE_LOG_AUX_STATE_REQUESTS_PER_SEC_METRIC.getName());
            topicMetrics.closeMetric(RemoteStorageMetrics.FAILED_REMOTE_FETCH_PER_SEC_METRIC.getName());
            topicMetrics.closeMetric(RemoteStorageMetrics.FAILED_REMOTE_COPY_PER_SEC_METRIC.getName());
            topicMetrics.closeMetric(RemoteStorageMetrics.REMOTE_COPY_LAG_BYTES_METRIC.getName());
            topicMetrics.closeMetric(RemoteStorageMetrics.REMOTE_LOG_METADATA_COUNT_METRIC.getName());
            topicMetrics.closeMetric(RemoteStorageMetrics.REMOTE_LOG_SIZE_COMPUTATION_TIME_METRIC.getName());
            topicMetrics.closeMetric(RemoteStorageMetrics.REMOTE_LOG_SIZE_BYTES_METRIC.getName());
            topicMetrics.closeMetric(RemoteStorageMetrics.FAILED_REMOTE_DELETE_PER_SEC_METRIC.getName());
            topicMetrics.closeMetric(RemoteStorageMetrics.FAILED_BUILD_REMOTE_LOG_AUX_STATE_PER_SEC_METRIC.getName());
            topicMetrics.closeMetric(RemoteStorageMetrics.REMOTE_COPY_LAG_BYTES_METRIC.getName());
            topicMetrics.closeMetric(RemoteStorageMetrics.REMOTE_COPY_LAG_SEGMENTS_METRIC.getName());
            topicMetrics.closeMetric(RemoteStorageMetrics.REMOTE_DELETE_LAG_BYTES_METRIC.getName());
            topicMetrics.closeMetric(RemoteStorageMetrics.REMOTE_DELETE_LAG_SEGMENTS_METRIC.getName());
        }
    }

    // This method only removes metrics only used for follower
    public void removeOldFollowerMetrics(String topic) {
        BrokerTopicMetrics topicMetrics = topicStats(topic);
        if (topicMetrics != null) {
            topicMetrics.closeMetric(BrokerTopicMetrics.REPLICATION_BYTES_IN_PER_SEC);
            topicMetrics.closeMetric(BrokerTopicMetrics.REASSIGNMENT_BYTES_IN_PER_SEC);
        }
    }

    public void removeMetrics(String topic) {
        BrokerTopicMetrics metrics = stats.remove(topic);
        if (metrics != null) {
            metrics.close();
        }
    }

    public void updateBytesOut(String topic, boolean isFollower, boolean isReassignment, long value) {
        if (isFollower) {
            if (isReassignment) {
                updateReassignmentBytesOut(value);
            }
            updateReplicationBytesOut(value);
        } else {
            topicStats(topic).bytesOutRate().mark(value);
            allTopicsStats.bytesOutRate().mark(value);
        }
    }

    // Update the broker-level all topic metric values so that we have a sample right for all topics metric after update of partition.
    public void recordRemoteCopyLagBytes(String topic, int partition, long value) {
        BrokerTopicMetrics topicMetric = topicStats(topic);
        topicMetric.remoteCopyLagBytesAggrMetric().setValue(String.valueOf(partition), value);
        allTopicsStats.remoteCopyLagBytesAggrMetric().setValue(topic, topicMetric.remoteCopyLagBytes());
    }

    // Update the broker-level all topic metric values so that we have a sample right for all topics metric after removal of partition.
    public void removeRemoteCopyLagBytes(String topic, int partition) {
        BrokerTopicMetrics topicMetric = topicStats(topic);
        topicMetric.remoteCopyLagBytesAggrMetric().removeKey(String.valueOf(partition));
        allTopicsStats.remoteCopyLagBytesAggrMetric().setValue(topic, topicMetric.remoteCopyLagBytes());
    }

    public void removeBrokerLevelRemoteCopyLagBytes(String topic) {
        allTopicsStats.remoteCopyLagBytesAggrMetric().removeKey(topic);
    }

    // Update the broker-level all topic metric values so that we have a sample right for all topics metric after update of partition.
    public void recordRemoteCopyLagSegments(String topic, int partition, long value) {
        BrokerTopicMetrics topicMetric = topicStats(topic);
        topicMetric.remoteCopyLagSegmentsAggrMetric().setValue(String.valueOf(partition), value);
        allTopicsStats.remoteCopyLagSegmentsAggrMetric().setValue(topic, topicMetric.remoteCopyLagSegments());
    }

    // Update the broker-level all topic metric values so that we have a sample right for all topics metric after removal of partition.
    public void removeRemoteCopyLagSegments(String topic, int partition) {
        BrokerTopicMetrics topicMetric = topicStats(topic);
        topicMetric.remoteCopyLagSegmentsAggrMetric().removeKey(String.valueOf(partition));
        allTopicsStats.remoteCopyLagSegmentsAggrMetric().setValue(topic, topicMetric.remoteCopyLagSegments());
    }

    public void removeBrokerLevelRemoteCopyLagSegments(String topic) {
        allTopicsStats.remoteCopyLagSegmentsAggrMetric().removeKey(topic);
    }

    // Update the broker-level all topic metric values so that we have a sample right for all topics metric after update of partition.
    public void recordRemoteDeleteLagBytes(String topic, int partition, long value) {
        BrokerTopicMetrics topicMetric = topicStats(topic);
        topicMetric.remoteDeleteLagBytesAggrMetric().setValue(String.valueOf(partition), value);
        allTopicsStats.remoteDeleteLagBytesAggrMetric().setValue(topic, topicMetric.remoteDeleteLagBytes());
    }

    // Update the broker-level all topic metric values so that we have a sample right for all topics metric after removal of partition.
    public void removeRemoteDeleteLagBytes(String topic, int partition) {
        BrokerTopicMetrics topicMetric = topicStats(topic);
        topicMetric.remoteDeleteLagBytesAggrMetric().removeKey(String.valueOf(partition));
        allTopicsStats.remoteDeleteLagBytesAggrMetric().setValue(topic, topicMetric.remoteDeleteLagBytes());
    }

    public void removeBrokerLevelRemoteDeleteLagBytes(String topic) {
        allTopicsStats.remoteDeleteLagBytesAggrMetric().removeKey(topic);
    }

    // Update the broker-level all topic metric values so that we have a sample right for all topics metric after update of partition.
    public void recordRemoteDeleteLagSegments(String topic, int partition, long value) {
        BrokerTopicMetrics topicMetric = topicStats(topic);
        topicMetric.remoteDeleteLagSegmentsAggrMetric().setValue(String.valueOf(partition), value);
        allTopicsStats.remoteDeleteLagSegmentsAggrMetric().setValue(topic, topicMetric.remoteDeleteLagSegments());
    }

    // Update the broker-level all topic metric values so that we have a sample right for all topics metric after removal of partition.
    public void removeRemoteDeleteLagSegments(String topic, int partition) {
        BrokerTopicMetrics topicMetric = topicStats(topic);
        topicMetric.remoteDeleteLagSegmentsAggrMetric().removeKey(String.valueOf(partition));
        allTopicsStats.remoteDeleteLagSegmentsAggrMetric().setValue(topic, topicMetric.remoteDeleteLagSegments());
    }

    public void removeBrokerLevelRemoteDeleteLagSegments(String topic) {
        allTopicsStats.remoteDeleteLagSegmentsAggrMetric().removeKey(topic);
    }

    // Update the broker-level all topic metric values so that we have a sample right for all topics metric after update of partition.
    public void recordRemoteLogMetadataCount(String topic, int partition, long value) {
        BrokerTopicMetrics topicMetric = topicStats(topic);
        topicMetric.remoteLogMetadataCountAggrMetric().setValue(String.valueOf(partition), value);
        allTopicsStats.remoteLogMetadataCountAggrMetric().setValue(topic, topicMetric.remoteLogMetadataCount());
    }

    // Update the broker-level all topic metric values so that we have a sample right for all topics metric after removal of partition.
    public void removeRemoteLogMetadataCount(String topic, int partition) {
        BrokerTopicMetrics topicMetric = topicStats(topic);
        topicMetric.remoteLogMetadataCountAggrMetric().removeKey(String.valueOf(partition));
        allTopicsStats.remoteLogMetadataCountAggrMetric().setValue(topic, topicMetric.remoteLogMetadataCount());
    }

    public void removeBrokerLevelRemoteLogMetadataCount(String topic) {
        allTopicsStats.remoteLogMetadataCountAggrMetric().removeKey(topic);
    }

    // Update the broker-level all topic metric values so that we have a sample right for all topics metric after update of partition.
    public void recordRemoteLogSizeComputationTime(String topic, int partition, long value) {
        BrokerTopicMetrics topicMetric = topicStats(topic);
        topicMetric.remoteLogSizeComputationTimeAggrMetric().setValue(String.valueOf(partition), value);
        allTopicsStats.remoteLogSizeComputationTimeAggrMetric().setValue(topic, topicMetric.remoteLogSizeComputationTime());
    }

    // Update the broker-level all topic metric values so that we have a sample right for all topics metric after removal of partition.
    public void removeRemoteLogSizeComputationTime(String topic, int partition) {
        BrokerTopicMetrics topicMetric = topicStats(topic);
        topicMetric.remoteLogSizeComputationTimeAggrMetric().removeKey(String.valueOf(partition));
        allTopicsStats.remoteLogSizeComputationTimeAggrMetric().setValue(topic, topicMetric.remoteLogSizeComputationTime());
    }

    public void removeBrokerLevelRemoteLogSizeComputationTime(String topic) {
        allTopicsStats.remoteLogSizeComputationTimeAggrMetric().removeKey(topic);
    }

    // Update the broker-level all topic metric values so that we have a sample right for all topics metric after update of partition.
    public void recordRemoteLogSizeBytes(String topic, int partition, long value) {
        BrokerTopicMetrics topicMetric = topicStats(topic);
        topicMetric.remoteLogSizeBytesAggrMetric().setValue(String.valueOf(partition), value);
        allTopicsStats.remoteLogSizeBytesAggrMetric().setValue(topic, topicMetric.remoteLogSizeBytes());
    }

    // Update the broker-level all topic metric values so that we have a sample right for all topics metric after removal of partition.
    public void removeRemoteLogSizeBytes(String topic, int partition) {
        BrokerTopicMetrics topicMetric = topicStats(topic);
        topicMetric.remoteLogSizeBytesAggrMetric().removeKey(String.valueOf(partition));
        allTopicsStats.remoteLogSizeBytesAggrMetric().setValue(topic, topicMetric.remoteLogSizeBytes());
    }

    public void removeBrokerLevelRemoteLogSizeBytes(String topic) {
        allTopicsStats.remoteLogSizeBytesAggrMetric().removeKey(topic);
    }

    @Override
    public void close() {
        allTopicsStats.close();
        stats.values().forEach(BrokerTopicMetrics::close);
        LOG.info("Broker and topic stats closed");
    }

    public BrokerTopicMetrics allTopicsStats() {
        return allTopicsStats;
    }
}
