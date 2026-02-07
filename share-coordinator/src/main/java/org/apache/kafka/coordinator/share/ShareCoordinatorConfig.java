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
package org.apache.kafka.coordinator.share;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.Records;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Range.between;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.SHORT;

public class ShareCoordinatorConfig {
    public static final String STATE_TOPIC_NUM_PARTITIONS_CONFIG = "share.coordinator.state.topic.num.partitions";
    public static final int STATE_TOPIC_NUM_PARTITIONS_DEFAULT = 50;
    public static final String STATE_TOPIC_NUM_PARTITIONS_DOC = "The number of partitions for the share-group state topic (should not change after deployment).";

    public static final String STATE_TOPIC_REPLICATION_FACTOR_CONFIG = "share.coordinator.state.topic.replication.factor";
    public static final short STATE_TOPIC_REPLICATION_FACTOR_DEFAULT = 3;
    public static final String STATE_TOPIC_REPLICATION_FACTOR_DOC = "Replication factor for the share-group state topic. " +
        "Topic creation will fail until the cluster size meets this replication factor requirement.";

    public static final String STATE_TOPIC_MIN_ISR_CONFIG = "share.coordinator.state.topic.min.isr";
    public static final short STATE_TOPIC_MIN_ISR_DEFAULT = 2;
    public static final String STATE_TOPIC_MIN_ISR_DOC = "Overridden min.insync.replicas for the share-group state topic.";

    public static final String STATE_TOPIC_SEGMENT_BYTES_CONFIG = "share.coordinator.state.topic.segment.bytes";
    public static final int STATE_TOPIC_SEGMENT_BYTES_DEFAULT = 100 * 1024 * 1024;
    public static final String STATE_TOPIC_SEGMENT_BYTES_DOC = "The log segment size for the share-group state topic.";

    public static final String NUM_THREADS_CONFIG = "share.coordinator.threads";
    public static final int NUM_THREADS_DEFAULT = 1;
    public static final String NUM_THREADS_DOC = "The number of threads used by the share coordinator.";

    public static final String SNAPSHOT_UPDATE_RECORDS_PER_SNAPSHOT_CONFIG = "share.coordinator.snapshot.update.records.per.snapshot";
    public static final int SNAPSHOT_UPDATE_RECORDS_PER_SNAPSHOT_DEFAULT = 500;
    public static final String SNAPSHOT_UPDATE_RECORDS_PER_SNAPSHOT_DOC = "The number of update records the share coordinator writes between snapshot records.";

    public static final String WRITE_TIMEOUT_MS_CONFIG = "share.coordinator.write.timeout.ms";
    public static final int WRITE_TIMEOUT_MS_DEFAULT = 5000;
    public static final String WRITE_TIMEOUT_MS_DOC = "The duration in milliseconds that the share coordinator will wait for all replicas of the share-group state topic to receive a write.";

    public static final String LOAD_BUFFER_SIZE_CONFIG = "share.coordinator.load.buffer.size";
    public static final int LOAD_BUFFER_SIZE_DEFAULT = 5 * 1024 * 1024;
    public static final String LOAD_BUFFER_SIZE_DOC = "Batch size for reading from the share-group state topic when loading state information into the cache (soft-limit, overridden if records are too large).";

    public static final String STATE_TOPIC_COMPRESSION_CODEC_CONFIG = "share.coordinator.state.topic.compression.codec";
    public static final CompressionType STATE_TOPIC_COMPRESSION_CODEC_DEFAULT = CompressionType.NONE;
    public static final String STATE_TOPIC_COMPRESSION_CODEC_DOC = "Compression codec for the share-group state topic.";

    public static final String APPEND_LINGER_MS_CONFIG = "share.coordinator.append.linger.ms";
    public static final int APPEND_LINGER_MS_DEFAULT = -1;
    public static final String APPEND_LINGER_MS_DOC = "The duration in milliseconds that the share coordinator will wait for writes to accumulate before flushing them to disk. " +
        "Set to -1 for an adaptive linger time that minimizes latency based on the workload.";

    public static final String STATE_TOPIC_PRUNE_INTERVAL_MS_CONFIG = "share.coordinator.state.topic.prune.interval.ms";
    public static final int STATE_TOPIC_PRUNE_INTERVAL_MS_DEFAULT = 5 * 60 * 1000; // 5 minutes
    public static final String STATE_TOPIC_PRUNE_INTERVAL_MS_DOC = "The duration in milliseconds that the share coordinator will wait between pruning eligible records in share-group state topic.";

    public static final String COLD_PARTITION_SNAPSHOT_INTERVAL_MS_CONFIG = "share.coordinator.cold.partition.snapshot.interval.ms";
    public static final int COLD_PARTITION_SNAPSHOT_INTERVAL_MS_DEFAULT = 5 * 60 * 1000; // 5 minutes
    public static final String COLD_PARTITION_SNAPSHOT_INTERVAL_MS_DOC = "The duration in milliseconds that the share coordinator will wait between force snapshotting share partitions which are not being updated.";

    public static final String CACHED_BUFFER_MAX_BYTES_CONFIG = "share.coordinator.cached.buffer.max.bytes";
    public static final int CACHED_BUFFER_MAX_BYTES_DEFAULT = 1024 * 1024 + Records.LOG_OVERHEAD;
    public static final String CACHED_BUFFER_MAX_BYTES_DOC = "The maximum buffer size that the ShareCoordinator will retain for reuse. " +
        "Note: Setting this larger than the maximum message size is not recommended. In this case, every write buffer will be eligible " +
        "for recycling, which renders this configuration ineffective as a size limit.";

    public static final Set<String> RECONFIGURABLE_CONFIGS = Set.of(
        CACHED_BUFFER_MAX_BYTES_CONFIG
    );

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(STATE_TOPIC_NUM_PARTITIONS_CONFIG, INT, STATE_TOPIC_NUM_PARTITIONS_DEFAULT, atLeast(1), HIGH, STATE_TOPIC_NUM_PARTITIONS_DOC)
        .define(STATE_TOPIC_REPLICATION_FACTOR_CONFIG, SHORT, STATE_TOPIC_REPLICATION_FACTOR_DEFAULT, atLeast(1), HIGH, STATE_TOPIC_REPLICATION_FACTOR_DOC)
        .define(STATE_TOPIC_MIN_ISR_CONFIG, SHORT, STATE_TOPIC_MIN_ISR_DEFAULT, atLeast(1), HIGH, STATE_TOPIC_MIN_ISR_DOC)
        .define(STATE_TOPIC_SEGMENT_BYTES_CONFIG, INT, STATE_TOPIC_SEGMENT_BYTES_DEFAULT, atLeast(1), HIGH, STATE_TOPIC_SEGMENT_BYTES_DOC)
        .define(NUM_THREADS_CONFIG, INT, NUM_THREADS_DEFAULT, atLeast(1), MEDIUM, NUM_THREADS_DOC)
        .define(SNAPSHOT_UPDATE_RECORDS_PER_SNAPSHOT_CONFIG, INT, SNAPSHOT_UPDATE_RECORDS_PER_SNAPSHOT_DEFAULT, between(0, 500), MEDIUM, SNAPSHOT_UPDATE_RECORDS_PER_SNAPSHOT_DOC)
        .define(LOAD_BUFFER_SIZE_CONFIG, INT, LOAD_BUFFER_SIZE_DEFAULT, atLeast(1), HIGH, LOAD_BUFFER_SIZE_DOC)
        .define(STATE_TOPIC_COMPRESSION_CODEC_CONFIG, INT, (int) STATE_TOPIC_COMPRESSION_CODEC_DEFAULT.id, HIGH, STATE_TOPIC_COMPRESSION_CODEC_DOC)
        .define(APPEND_LINGER_MS_CONFIG, INT, APPEND_LINGER_MS_DEFAULT, atLeast(-1), MEDIUM, APPEND_LINGER_MS_DOC)
        .define(WRITE_TIMEOUT_MS_CONFIG, INT, WRITE_TIMEOUT_MS_DEFAULT, atLeast(1), HIGH, WRITE_TIMEOUT_MS_DOC)
        .defineInternal(STATE_TOPIC_PRUNE_INTERVAL_MS_CONFIG, INT, STATE_TOPIC_PRUNE_INTERVAL_MS_DEFAULT, atLeast(1), LOW, STATE_TOPIC_PRUNE_INTERVAL_MS_DOC)
        .defineInternal(COLD_PARTITION_SNAPSHOT_INTERVAL_MS_CONFIG, INT, COLD_PARTITION_SNAPSHOT_INTERVAL_MS_DEFAULT, atLeast(1), LOW, COLD_PARTITION_SNAPSHOT_INTERVAL_MS_DOC)
        // The minimum size is set equal to `INITIAL_BUFFER_SIZE` to prevent CACHED_BUFFER_MAX_BYTES from being configured too small,
        // which could otherwise negatively impact performance.
        .define(CACHED_BUFFER_MAX_BYTES_CONFIG, INT, CACHED_BUFFER_MAX_BYTES_DEFAULT, atLeast(512 * 1024), MEDIUM, CACHED_BUFFER_MAX_BYTES_DOC);

    private final int stateTopicNumPartitions;
    private final short stateTopicReplicationFactor;
    private final int stateTopicMinIsr;
    private final int stateTopicSegmentBytes;
    private final int numThreads;
    private final int snapshotUpdateRecordsPerSnapshot;
    private final int writeTimeoutMs;
    private final int loadBufferSize;
    private final CompressionType compressionType;
    private final int appendLingerMs;
    private final int pruneIntervalMs;
    private final int coldPartitionSnapshotIntervalMs;

    private final AbstractConfig config;

    public ShareCoordinatorConfig(AbstractConfig config) {
        stateTopicNumPartitions = config.getInt(STATE_TOPIC_NUM_PARTITIONS_CONFIG);
        stateTopicReplicationFactor = config.getShort(STATE_TOPIC_REPLICATION_FACTOR_CONFIG);
        stateTopicMinIsr = config.getShort(STATE_TOPIC_MIN_ISR_CONFIG);
        stateTopicSegmentBytes = config.getInt(STATE_TOPIC_SEGMENT_BYTES_CONFIG);
        numThreads = config.getInt(NUM_THREADS_CONFIG);
        snapshotUpdateRecordsPerSnapshot = config.getInt(SNAPSHOT_UPDATE_RECORDS_PER_SNAPSHOT_CONFIG);
        writeTimeoutMs = config.getInt(WRITE_TIMEOUT_MS_CONFIG);
        loadBufferSize = config.getInt(LOAD_BUFFER_SIZE_CONFIG);
        compressionType = Optional.ofNullable(config.getInt(STATE_TOPIC_COMPRESSION_CODEC_CONFIG))
            .map(CompressionType::forId)
            .orElse(null);
        appendLingerMs = config.getInt(APPEND_LINGER_MS_CONFIG);
        pruneIntervalMs = config.getInt(STATE_TOPIC_PRUNE_INTERVAL_MS_CONFIG);
        coldPartitionSnapshotIntervalMs = config.getInt(COLD_PARTITION_SNAPSHOT_INTERVAL_MS_CONFIG);
        this.config = config;
    }

    public int shareCoordinatorStateTopicNumPartitions() {
        return stateTopicNumPartitions;
    }

    public short shareCoordinatorStateTopicReplicationFactor() {
        return stateTopicReplicationFactor;
    }

    public int shareCoordinatorStateTopicMinIsr() {
        return stateTopicMinIsr;
    }

    public int shareCoordinatorStateTopicSegmentBytes() {
        return stateTopicSegmentBytes;
    }

    public int shareCoordinatorNumThreads() {
        return numThreads;
    }

    public int shareCoordinatorSnapshotUpdateRecordsPerSnapshot() {
        return snapshotUpdateRecordsPerSnapshot;
    }

    public int shareCoordinatorWriteTimeoutMs() {
        return writeTimeoutMs;
    }

    public int shareCoordinatorLoadBufferSize() {
        return loadBufferSize;
    }

    /**
     * The duration in milliseconds that the coordinator will wait for writes to
     * accumulate before flushing them to disk. {@code OptionalInt.empty()} indicates
     * an adaptive linger time based on the workload.
     */
    public OptionalInt shareCoordinatorAppendLingerMs() {
        if (appendLingerMs == -1) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(appendLingerMs);
    }

    public CompressionType shareCoordinatorStateTopicCompressionType() {
        return compressionType;
    }

    public int shareCoordinatorTopicPruneIntervalMs() {
        return pruneIntervalMs;
    }

    public int shareCoordinatorColdPartitionSnapshotIntervalMs() {
        return coldPartitionSnapshotIntervalMs;
    }

    /**
     * The maximum buffer size that the share coordinator can cache.
     *
     * Note: On hot paths, frequent calls to this method may cause performance bottlenecks due to synchronization overhead.
     */
    public int shareCoordinatorCachedBufferMaxBytes() {
        return config.getInt(CACHED_BUFFER_MAX_BYTES_CONFIG);
    }
}
