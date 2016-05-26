/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.coordinator

import kafka.message.{NoCompressionCodec, CompressionCodec}

/**
 * Configuration settings for in-built offset management
 * @param maxMetadataSize The maximum allowed metadata for any offset commit.
 * @param loadBufferSize Batch size for reading from the offsets segments when loading offsets into the cache.
 * @param offsetsRetentionMs Offsets older than this retention period will be discarded.
 * @param offsetsRetentionCheckIntervalMs Frequency at which to check for expired offsets.
 * @param offsetsTopicNumPartitions The number of partitions for the offset commit topic (should not change after deployment).
 * @param offsetsTopicSegmentBytes The offsets topic segment bytes should be kept relatively small to facilitate faster
 *                                 log compaction and faster offset loads
 * @param offsetsTopicReplicationFactor The replication factor for the offset commit topic (set higher to ensure availability).
 * @param offsetsTopicCompressionCodec Compression codec for the offsets topic - compression should be turned on in
 *                                     order to achieve "atomic" commits.
 * @param offsetCommitTimeoutMs The offset commit will be delayed until all replicas for the offsets topic receive the
 *                              commit or this timeout is reached. (Similar to the producer request timeout.)
 * @param offsetCommitRequiredAcks The required acks before the commit can be accepted. In general, the default (-1)
 *                                 should not be overridden.
 */
case class OffsetConfig(maxMetadataSize: Int = OffsetConfig.DefaultMaxMetadataSize,
                        loadBufferSize: Int = OffsetConfig.DefaultLoadBufferSize,
                        offsetsRetentionMs: Long = OffsetConfig.DefaultOffsetRetentionMs,
                        offsetsRetentionCheckIntervalMs: Long = OffsetConfig.DefaultOffsetsRetentionCheckIntervalMs,
                        offsetsTopicNumPartitions: Int = OffsetConfig.DefaultOffsetsTopicNumPartitions,
                        offsetsTopicSegmentBytes: Int = OffsetConfig.DefaultOffsetsTopicSegmentBytes,
                        offsetsTopicReplicationFactor: Short = OffsetConfig.DefaultOffsetsTopicReplicationFactor,
                        offsetsTopicCompressionCodec: CompressionCodec = OffsetConfig.DefaultOffsetsTopicCompressionCodec,
                        offsetCommitTimeoutMs: Int = OffsetConfig.DefaultOffsetCommitTimeoutMs,
                        offsetCommitRequiredAcks: Short = OffsetConfig.DefaultOffsetCommitRequiredAcks)

object OffsetConfig {
  val DefaultMaxMetadataSize = 4096
  val DefaultLoadBufferSize = 5*1024*1024
  val DefaultOffsetRetentionMs = 24*60*60*1000L
  val DefaultOffsetsRetentionCheckIntervalMs = 600000L
  val DefaultOffsetsTopicNumPartitions = 50
  val DefaultOffsetsTopicSegmentBytes = 100*1024*1024
  val DefaultOffsetsTopicReplicationFactor = 3.toShort
  val DefaultOffsetsTopicCompressionCodec = NoCompressionCodec
  val DefaultOffsetCommitTimeoutMs = 5000
  val DefaultOffsetCommitRequiredAcks = (-1).toShort
}