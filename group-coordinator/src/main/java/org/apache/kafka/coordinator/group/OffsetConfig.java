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

package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.record.CompressionType;

public class OffsetConfig {
    public static final int DEFAULT_MAX_METADATA_SIZE = 4096;
    public static final int DEFAULT_LOAD_BUFFER_SIZE = 5 * 1024 * 1024;
    public static final long DEFAULT_OFFSET_RETENTION_MS = 24 * 60 * 60 * 1000L;
    public static final long DEFAULT_OFFSETS_RETENTION_CHECK_INTERVAL_MS = 600000L;
    public static final int DEFAULT_OFFSETS_TOPIC_NUM_PARTITIONS = 50;
    public static final int DEFAULT_OFFSETS_TOPIC_SEGMENT_BYTES = 100 * 1024 * 1024;
    public static final short DEFAULT_OFFSETS_TOPIC_REPLICATION_FACTOR = 3;
    public static final CompressionType DEFAULT_OFFSETS_TOPIC_COMPRESSION_TYPE = CompressionType.NONE;
    public static final int DEFAULT_OFFSET_COMMIT_TIMEOUT_MS = 5000;
    public static final short DEFAULT_OFFSET_COMMIT_REQUIRED_ACKS = -1;

    public final int maxMetadataSize;
    public final int loadBufferSize;
    public final long offsetsRetentionMs;
    public final long offsetsRetentionCheckIntervalMs;
    public final int offsetsTopicNumPartitions;
    public final int offsetsTopicSegmentBytes;
    public final short offsetsTopicReplicationFactor;
    public final CompressionType offsetsTopicCompressionType;
    public final int offsetCommitTimeoutMs;
    public final short offsetCommitRequiredAcks;

    /**
     * Configuration settings for in-built offset management
     * @param maxMetadataSize The maximum allowed metadata for any offset commit.
     * @param loadBufferSize Batch size for reading from the offsets segments when loading offsets into the cache.
     * @param offsetsRetentionMs For subscribed consumers, committed offset of a specific partition will be expired and discarded when
     *                             1) this retention period has elapsed after the consumer group loses all its consumers (i.e. becomes empty);
     *                             2) this retention period has elapsed since the last time an offset is committed for the partition AND the group is no longer subscribed to the corresponding topic.
     *                           For standalone consumers (using manual assignment), offsets will be expired after this retention period has elapsed since the time of last commit.
     *                           Note that when a group is deleted via the delete-group request, its committed offsets will also be deleted immediately;
     *                           Also when a topic is deleted via the delete-topic request, upon propagated metadata update any group's committed offsets for that topic will also be deleted without extra retention period
     * @param offsetsRetentionCheckIntervalMs Frequency at which to check for expired offsets.
     * @param offsetsTopicNumPartitions The number of partitions for the offset commit topic (should not change after deployment).
     * @param offsetsTopicSegmentBytes The offsets topic segment bytes should be kept relatively small to facilitate faster
     *                                 log compaction and faster offset loads
     * @param offsetsTopicReplicationFactor The replication factor for the offset commit topic (set higher to ensure availability).
     * @param offsetsTopicCompressionType Compression type for the offsets topic - compression should be turned on in
     *                                     order to achieve "atomic" commits.
     * @param offsetCommitTimeoutMs The offset commit will be delayed until all replicas for the offsets topic receive the
     *                              commit or this timeout is reached. (Similar to the producer request timeout.)
     * @param offsetCommitRequiredAcks The required acks before the commit can be accepted. In general, the default (-1)
     *                                 should not be overridden.
     */
    public OffsetConfig(int maxMetadataSize,
                        int loadBufferSize,
                        long offsetsRetentionMs,
                        long offsetsRetentionCheckIntervalMs,
                        int offsetsTopicNumPartitions,
                        int offsetsTopicSegmentBytes,
                        short offsetsTopicReplicationFactor,
                        CompressionType offsetsTopicCompressionType,
                        int offsetCommitTimeoutMs,
                        short offsetCommitRequiredAcks
    ) {
        this.maxMetadataSize = maxMetadataSize;
        this.loadBufferSize = loadBufferSize;
        this.offsetsRetentionMs = offsetsRetentionMs;
        this.offsetsRetentionCheckIntervalMs = offsetsRetentionCheckIntervalMs;
        this.offsetsTopicNumPartitions = offsetsTopicNumPartitions;
        this.offsetsTopicSegmentBytes = offsetsTopicSegmentBytes;
        this.offsetsTopicReplicationFactor = offsetsTopicReplicationFactor;
        this.offsetsTopicCompressionType = offsetsTopicCompressionType;
        this.offsetCommitTimeoutMs = offsetCommitTimeoutMs;
        this.offsetCommitRequiredAcks = offsetCommitRequiredAcks;
    }

    public OffsetConfig() {
        this(DEFAULT_MAX_METADATA_SIZE, DEFAULT_LOAD_BUFFER_SIZE, DEFAULT_OFFSET_RETENTION_MS,
                DEFAULT_OFFSETS_RETENTION_CHECK_INTERVAL_MS, DEFAULT_OFFSETS_TOPIC_NUM_PARTITIONS,
                DEFAULT_OFFSETS_TOPIC_SEGMENT_BYTES, DEFAULT_OFFSETS_TOPIC_REPLICATION_FACTOR,
                DEFAULT_OFFSETS_TOPIC_COMPRESSION_TYPE, DEFAULT_OFFSET_COMMIT_TIMEOUT_MS,
                DEFAULT_OFFSET_COMMIT_REQUIRED_ACKS);
    }
}
