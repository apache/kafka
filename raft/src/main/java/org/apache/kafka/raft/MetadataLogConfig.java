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
package org.apache.kafka.raft;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.server.config.ServerLogConfigs;

import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

public class MetadataLogConfig {

    public static final String METADATA_LOG_DIR_CONFIG = "metadata.log.dir";
    public static final String METADATA_LOG_DIR_DOC = "This configuration determines where we put the metadata log. " +
            "If it is not set, the metadata log is placed in the first log directory from log.dirs.";

    public static final String METADATA_SNAPSHOT_MAX_INTERVAL_MS_CONFIG = "metadata.log.max.snapshot.interval.ms";
    public static final long METADATA_SNAPSHOT_MAX_INTERVAL_MS_DEFAULT = TimeUnit.HOURS.toMillis(1);
    public static final String METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_CONFIG = "metadata.log.max.record.bytes.between.snapshots";
    public static final int METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES = 20 * 1024 * 1024;
    public static final String METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_DOC = "This is the maximum number of bytes in the log between the latest " +
            "snapshot and the high-watermark needed before generating a new snapshot. The default value is " +
            METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES + ". To generate snapshots based on the time elapsed, see the <code>" +
            METADATA_SNAPSHOT_MAX_INTERVAL_MS_CONFIG + "</code> configuration. The Kafka node will generate a snapshot when " +
            "either the maximum time interval is reached or the maximum bytes limit is reached.";
    public static final String METADATA_SNAPSHOT_MAX_INTERVAL_MS_DOC = "This is the maximum number of milliseconds to wait to generate a snapshot " +
            "if there are committed records in the log that are not included in the latest snapshot. A value of zero disables " +
            "time based snapshot generation. The default value is " + METADATA_SNAPSHOT_MAX_INTERVAL_MS_DEFAULT + ". To generate " +
            "snapshots based on the number of metadata bytes, see the <code>" + METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_CONFIG + "</code> " +
            "configuration. The Kafka node will generate a snapshot when either the maximum time interval is reached or the " +
            "maximum bytes limit is reached.";

    public static final String METADATA_LOG_SEGMENT_MIN_BYTES_CONFIG = "metadata.log.segment.min.bytes";
    public static final String METADATA_LOG_SEGMENT_MIN_BYTES_DOC = "Override the minimum size for a single metadata log file. This should be used for testing only.";
    public static final int METADATA_LOG_SEGMENT_MIN_BYTES_DEFAULT = 8 * 1024 * 1024;

    public static final String METADATA_LOG_SEGMENT_BYTES_CONFIG = "metadata.log.segment.bytes";
    public static final String METADATA_LOG_SEGMENT_BYTES_DOC = "The maximum size of a single metadata log file.";
    public static final int METADATA_LOG_SEGMENT_BYTES_DEFAULT = 1024 * 1024 * 1024;

    public static final String METADATA_LOG_SEGMENT_MILLIS_CONFIG = "metadata.log.segment.ms";
    public static final String METADATA_LOG_SEGMENT_MILLIS_DOC = "The maximum time before a new metadata log file is rolled out (in milliseconds).";
    public static final long METADATA_LOG_SEGMENT_MILLIS_DEFAULT = 24 * 7 * 60 * 60 * 1000L;

    public static final String METADATA_MAX_RETENTION_BYTES_CONFIG = "metadata.max.retention.bytes";
    public static final int METADATA_MAX_RETENTION_BYTES_DEFAULT = 100 * 1024 * 1024;
    public static final String METADATA_MAX_RETENTION_BYTES_DOC = "The maximum combined size of the metadata log and snapshots before deleting old " +
            "snapshots and log files. Since at least one snapshot must exist before any logs can be deleted, this is a soft limit.";

    public static final String METADATA_MAX_RETENTION_MILLIS_CONFIG = "metadata.max.retention.ms";
    public static final String METADATA_MAX_RETENTION_MILLIS_DOC = "The number of milliseconds to keep a metadata log file or snapshot before " +
            "deleting it. Since at least one snapshot must exist before any logs can be deleted, this is a soft limit.";
    public static final long METADATA_MAX_RETENTION_MILLIS_DEFAULT = 24 * 7 * 60 * 60 * 1000L;

    public static final String METADATA_MAX_IDLE_INTERVAL_MS_CONFIG = "metadata.max.idle.interval.ms";
    public static final int METADATA_MAX_IDLE_INTERVAL_MS_DEFAULT = 500;
    public static final String METADATA_MAX_IDLE_INTERVAL_MS_DOC = "This configuration controls how often the active " +
            "controller should write no-op records to the metadata partition. If the value is 0, no-op records " +
            "are not appended to the metadata partition. The default value is " + METADATA_MAX_IDLE_INTERVAL_MS_DEFAULT;

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_CONFIG, LONG, METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES, atLeast(1), HIGH, METADATA_SNAPSHOT_MAX_NEW_RECORD_BYTES_DOC)
            .define(METADATA_SNAPSHOT_MAX_INTERVAL_MS_CONFIG, LONG, METADATA_SNAPSHOT_MAX_INTERVAL_MS_DEFAULT, atLeast(0), HIGH, METADATA_SNAPSHOT_MAX_INTERVAL_MS_DOC)
            .define(METADATA_LOG_DIR_CONFIG, STRING, null, null, HIGH, METADATA_LOG_DIR_DOC)
            .define(METADATA_LOG_SEGMENT_BYTES_CONFIG, INT, METADATA_LOG_SEGMENT_BYTES_DEFAULT, atLeast(Records.LOG_OVERHEAD), HIGH, METADATA_LOG_SEGMENT_BYTES_DOC)
            .defineInternal(METADATA_LOG_SEGMENT_MIN_BYTES_CONFIG, INT, METADATA_LOG_SEGMENT_MIN_BYTES_DEFAULT, atLeast(Records.LOG_OVERHEAD), HIGH, METADATA_LOG_SEGMENT_MIN_BYTES_DOC)
            .define(METADATA_LOG_SEGMENT_MILLIS_CONFIG, LONG, METADATA_LOG_SEGMENT_MILLIS_DEFAULT, null, HIGH, METADATA_LOG_SEGMENT_MILLIS_DOC)
            .define(METADATA_MAX_RETENTION_BYTES_CONFIG, LONG, METADATA_MAX_RETENTION_BYTES_DEFAULT, null, HIGH, METADATA_MAX_RETENTION_BYTES_DOC)
            .define(METADATA_MAX_RETENTION_MILLIS_CONFIG, LONG, METADATA_MAX_RETENTION_MILLIS_DEFAULT, null, HIGH, METADATA_MAX_RETENTION_MILLIS_DOC)
            .define(METADATA_MAX_IDLE_INTERVAL_MS_CONFIG, INT, METADATA_MAX_IDLE_INTERVAL_MS_DEFAULT, atLeast(0), LOW, METADATA_MAX_IDLE_INTERVAL_MS_DOC);

    private final int logSegmentBytes;
    private final int logSegmentMinBytes;
    private final long logSegmentMillis;
    private final long retentionMaxBytes;
    private final long retentionMillis;
    private final int maxBatchSizeInBytes;
    private final int maxFetchSizeInBytes;
    private final long deleteDelayMillis;

    /**
     * Configuration for the metadata log
     * @param logSegmentBytes The maximum size of a single metadata log file
     * @param logSegmentMinBytes The minimum size of a single metadata log file
     * @param logSegmentMillis The maximum time before a new metadata log file is rolled out
     * @param retentionMaxBytes The size of the metadata log and snapshots before deleting old snapshots and log files
     * @param retentionMillis The time to keep a metadata log file or snapshot before deleting it
     * @param maxBatchSizeInBytes The largest record batch size allowed in the metadata log
     * @param maxFetchSizeInBytes The maximum number of bytes to read when fetching from the metadata log
     * @param deleteDelayMillis The amount of time to wait before deleting a file from the filesystem
     */
    public MetadataLogConfig(int logSegmentBytes,
                             int logSegmentMinBytes,
                             long logSegmentMillis,
                             long retentionMaxBytes,
                             long retentionMillis,
                             int maxBatchSizeInBytes,
                             int maxFetchSizeInBytes,
                             long deleteDelayMillis) {
        this.logSegmentBytes = logSegmentBytes;
        this.logSegmentMinBytes = logSegmentMinBytes;
        this.logSegmentMillis = logSegmentMillis;
        this.retentionMaxBytes = retentionMaxBytes;
        this.retentionMillis = retentionMillis;
        this.maxBatchSizeInBytes = maxBatchSizeInBytes;
        this.maxFetchSizeInBytes = maxFetchSizeInBytes;
        this.deleteDelayMillis = deleteDelayMillis;
    }

    public MetadataLogConfig(AbstractConfig config) {
        this.logSegmentBytes = config.getInt(METADATA_LOG_SEGMENT_BYTES_CONFIG);
        this.logSegmentMinBytes = config.getInt(METADATA_LOG_SEGMENT_MIN_BYTES_CONFIG);
        this.logSegmentMillis = config.getLong(METADATA_LOG_SEGMENT_MILLIS_CONFIG);
        this.retentionMaxBytes = config.getLong(METADATA_MAX_RETENTION_BYTES_CONFIG);
        this.retentionMillis = config.getLong(METADATA_MAX_RETENTION_MILLIS_CONFIG);
        this.maxBatchSizeInBytes = KafkaRaftClient.MAX_BATCH_SIZE_BYTES;
        this.maxFetchSizeInBytes = KafkaRaftClient.MAX_FETCH_SIZE_BYTES;
        this.deleteDelayMillis = ServerLogConfigs.LOG_DELETE_DELAY_MS_DEFAULT;
    }

    public int logSegmentBytes() {
        return logSegmentBytes;
    }

    public int logSegmentMinBytes() {
        return logSegmentMinBytes;
    }

    public long logSegmentMillis() {
        return logSegmentMillis;
    }

    public long retentionMaxBytes() {
        return retentionMaxBytes;
    }

    public long retentionMillis() {
        return retentionMillis;
    }

    public int maxBatchSizeInBytes() {
        return maxBatchSizeInBytes;
    }

    public int maxFetchSizeInBytes() {
        return maxFetchSizeInBytes;
    }

    public long deleteDelayMillis() {
        return deleteDelayMillis;
    }
}
