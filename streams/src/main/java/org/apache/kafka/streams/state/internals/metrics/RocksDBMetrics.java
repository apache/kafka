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
package org.apache.kafka.streams.state.internals.metrics;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.AVG_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.MAX_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.MIN_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.RATIO_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.STATE_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addAmountRateMetricToSensor;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addAmountRateAndTotalMetricsToSensor;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addAvgAndTotalMetricsToSensor;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addTotalMetricToSensor;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addValueMetricToSensor;

public class RocksDBMetrics {
    private RocksDBMetrics() {}

    private static final String BYTES_WRITTEN_TO_DB = "bytes-written";
    private static final String BYTES_READ_FROM_DB = "bytes-read";
    private static final String MEMTABLE_BYTES_FLUSHED = "memtable-bytes-flushed";
    private static final String MEMTABLE_HIT_RATIO = "memtable-hit" + RATIO_SUFFIX;
    private static final String MEMTABLE_FLUSH_TIME = "memtable-flush-time";
    private static final String WRITE_STALL_DURATION = "write-stall-duration";
    private static final String BLOCK_CACHE_DATA_HIT_RATIO = "block-cache-data-hit" + RATIO_SUFFIX;
    private static final String BLOCK_CACHE_INDEX_HIT_RATIO = "block-cache-index-hit" + RATIO_SUFFIX;
    private static final String BLOCK_CACHE_FILTER_HIT_RATIO = "block-cache-filter-hit" + RATIO_SUFFIX;
    private static final String BYTES_WRITTEN_DURING_COMPACTION = "bytes-written-compaction";
    private static final String BYTES_READ_DURING_COMPACTION = "bytes-read-compaction";
    private static final String COMPACTION_TIME = "compaction-time";
    private static final String NUMBER_OF_OPEN_FILES = "number-open-files";
    private static final String NUMBER_OF_FILE_ERRORS = "number-file-errors";

    private static final String STORE_TYPE_PREFIX = "rocksdb-";

    public static Sensor bytesWrittenToDatabaseSensor(final StreamsMetricsImpl streamsMetrics,
                                                      final String taskName,
                                                      final String storeType,
                                                      final String storeName) {
        final Sensor sensor = streamsMetrics
            .storeLevelSensor(taskName, storeName, BYTES_WRITTEN_TO_DB, RecordingLevel.DEBUG);
        addAmountRateAndTotalMetricsToSensor(sensor,
                                             STATE_LEVEL_GROUP,
                                             streamsMetrics.storeLevelTagMap(taskName,
                                                                             STORE_TYPE_PREFIX + storeType, storeName),
                                             BYTES_WRITTEN_TO_DB,
                                             "Average per-second number of bytes written to the RocksDB state store",
                                             "Total number of bytes written to the RocksDB state store");
        return sensor;
    }

    public static Sensor bytesReadFromDatabaseSensor(final StreamsMetricsImpl streamsMetrics,
                                                     final String taskName,
                                                     final String storeType,
                                                     final String storeName) {
        final Sensor sensor = streamsMetrics
            .storeLevelSensor(taskName, storeName, BYTES_READ_FROM_DB, RecordingLevel.DEBUG);
        addAmountRateAndTotalMetricsToSensor(sensor,
                                             STATE_LEVEL_GROUP,
                                             streamsMetrics.storeLevelTagMap(taskName,
                                                                             STORE_TYPE_PREFIX + storeType, storeName),
                                             BYTES_READ_FROM_DB,
                                             "Average per-second number of bytes read from the RocksDB state store",
                                             "Total number of bytes read from the RocksDB state store");
        return sensor;
    }

    public static Sensor memtableBytesFlushedSensor(final StreamsMetricsImpl streamsMetrics,
                                                    final String taskName,
                                                    final String storeType,
                                                    final String storeName) {
        final Sensor sensor = streamsMetrics
            .storeLevelSensor(taskName, storeName, MEMTABLE_BYTES_FLUSHED, RecordingLevel.DEBUG);
        addAmountRateAndTotalMetricsToSensor(sensor,
                                             STATE_LEVEL_GROUP,
                                             streamsMetrics.storeLevelTagMap(taskName,
                                                                             STORE_TYPE_PREFIX + storeType, storeName),
                                             MEMTABLE_BYTES_FLUSHED,
                                             "Average per-second number of bytes flushed from the memtable to disk",
                                             "Total number of bytes flushed from the memtable to disk");
        return sensor;
    }

    public static Sensor memtableHitRatioSensor(final StreamsMetricsImpl streamsMetrics,
                                                final String taskName,
                                                final String storeType,
                                                final String storeName) {
        final Sensor sensor = streamsMetrics
            .storeLevelSensor(taskName, storeName, MEMTABLE_HIT_RATIO, RecordingLevel.DEBUG);
        addValueMetricToSensor(sensor,
                               STATE_LEVEL_GROUP,
                               streamsMetrics.storeLevelTagMap(taskName, STORE_TYPE_PREFIX + storeType, storeName),
                               MEMTABLE_HIT_RATIO,
                               "Ratio of memtable hits relative to all lookups to the memtable");
        return sensor;
    }

    public static Sensor memtableAvgFlushTimeSensor(final StreamsMetricsImpl streamsMetrics,
                                                    final String taskName,
                                                    final String storeType,
                                                    final String storeName) {
        final Sensor sensor = streamsMetrics
            .storeLevelSensor(taskName, storeName, MEMTABLE_FLUSH_TIME + AVG_SUFFIX, RecordingLevel.DEBUG);
        addValueMetricToSensor(sensor,
                               STATE_LEVEL_GROUP,
                               streamsMetrics.storeLevelTagMap(taskName, STORE_TYPE_PREFIX + storeType, storeName),
                               MEMTABLE_FLUSH_TIME + AVG_SUFFIX,
                               "Average time spent on flushing the memtable to disk in ms");
        return sensor;
    }

    public static Sensor memtableMinFlushTimeSensor(final StreamsMetricsImpl streamsMetrics,
                                                    final String taskName,
                                                    final String storeType,
                                                    final String storeName) {
        final Sensor sensor = streamsMetrics
            .storeLevelSensor(taskName, storeName, MEMTABLE_FLUSH_TIME + MIN_SUFFIX, RecordingLevel.DEBUG);
        addValueMetricToSensor(sensor,
                               STATE_LEVEL_GROUP,
                               streamsMetrics.storeLevelTagMap(taskName, STORE_TYPE_PREFIX + storeType, storeName),
                               MEMTABLE_FLUSH_TIME + MIN_SUFFIX,
                               "Minimum time spent on flushing the memtable to disk in ms");
        return sensor;
    }

    public static Sensor memtableMaxFlushTimeSensor(final StreamsMetricsImpl streamsMetrics,
                                                    final String taskName,
                                                    final String storeType,
                                                    final String storeName) {
        final Sensor sensor = streamsMetrics
            .storeLevelSensor(taskName, storeName, MEMTABLE_FLUSH_TIME + MAX_SUFFIX, RecordingLevel.DEBUG);
        addValueMetricToSensor(sensor,
                               STATE_LEVEL_GROUP,
                               streamsMetrics.storeLevelTagMap(taskName, STORE_TYPE_PREFIX + storeType, storeName),
                               MEMTABLE_FLUSH_TIME + MAX_SUFFIX,
                               "Maximum time spent on flushing the memtable to disk in ms");
        return sensor;
    }

    public static Sensor writeStallDurationSensor(final StreamsMetricsImpl streamsMetrics,
                                                  final String taskName,
                                                  final String storeType,
                                                  final String storeName) {
        final Sensor sensor = streamsMetrics
            .storeLevelSensor(taskName, storeName, WRITE_STALL_DURATION, RecordingLevel.DEBUG);
        addAvgAndTotalMetricsToSensor(sensor,
                                      STATE_LEVEL_GROUP,
                                      streamsMetrics
                                          .storeLevelTagMap(taskName, STORE_TYPE_PREFIX + storeType, storeName),
                                      WRITE_STALL_DURATION,
                                      "Moving average duration of write stalls in ms",
                                      "Total duration of write stalls in ms");
        return sensor;
    }

    public static Sensor blockCacheDataHitRatioSensor(final StreamsMetricsImpl streamsMetrics,
                                                      final String taskName,
                                                      final String storeType,
                                                      final String storeName) {
        final Sensor sensor = streamsMetrics
            .storeLevelSensor(taskName, storeName, BLOCK_CACHE_DATA_HIT_RATIO, RecordingLevel.DEBUG);
        addValueMetricToSensor(sensor,
                               STATE_LEVEL_GROUP,
                               streamsMetrics.storeLevelTagMap(taskName, STORE_TYPE_PREFIX + storeType, storeName),
                               BLOCK_CACHE_DATA_HIT_RATIO,
                               "Ratio of block cache hits for data relative to all lookups for data to the block cache");
        return sensor;
    }

    public static Sensor blockCacheIndexHitRatioSensor(final StreamsMetricsImpl streamsMetrics,
                                                       final String taskName,
                                                       final String storeType,
                                                       final String storeName) {
        final Sensor sensor = streamsMetrics
            .storeLevelSensor(taskName, storeName, BLOCK_CACHE_INDEX_HIT_RATIO, RecordingLevel.DEBUG);
        addValueMetricToSensor(sensor,
                               STATE_LEVEL_GROUP,
                               streamsMetrics.storeLevelTagMap(taskName, STORE_TYPE_PREFIX + storeType, storeName),
                               BLOCK_CACHE_INDEX_HIT_RATIO,
                               "Ratio of block cache hits for indexes relative to all lookups for indexes to"
                                   + " the block cache");
        return sensor;
    }

    public static Sensor blockCacheFilterHitRatioSensor(final StreamsMetricsImpl streamsMetrics,
                                                        final String taskName,
                                                        final String storeType,
                                                        final String storeName) {
        final Sensor sensor = streamsMetrics
            .storeLevelSensor(taskName, storeName, BLOCK_CACHE_FILTER_HIT_RATIO, RecordingLevel.DEBUG);
        addValueMetricToSensor(sensor,
                               STATE_LEVEL_GROUP,
                               streamsMetrics.storeLevelTagMap(taskName, STORE_TYPE_PREFIX + storeType, storeName),
                               BLOCK_CACHE_FILTER_HIT_RATIO,
                               "Ratio of block cache hits for filters relative to all lookups for filters to"
                                   + " the block cache");
        return sensor;
    }

    public static Sensor bytesReadDuringCompactionSensor(final StreamsMetricsImpl streamsMetrics,
                                                         final String taskName,
                                                         final String storeType,
                                                         final String storeName) {
        final Sensor sensor = streamsMetrics
            .storeLevelSensor(taskName, storeName, BYTES_READ_DURING_COMPACTION, RecordingLevel.DEBUG);
        addAmountRateMetricToSensor(sensor,
                                    STATE_LEVEL_GROUP,
                                    streamsMetrics.storeLevelTagMap(taskName, STORE_TYPE_PREFIX + storeType, storeName),
                                    BYTES_READ_DURING_COMPACTION,
                                    "Average per-second number of bytes read during compaction");
        return sensor;
    }

    public static Sensor bytesWrittenDuringCompactionSensor(final StreamsMetricsImpl streamsMetrics,
                                                            final String taskName,
                                                            final String storeType,
                                                            final String storeName) {
        final Sensor sensor = streamsMetrics
            .storeLevelSensor(taskName, storeName, BYTES_WRITTEN_DURING_COMPACTION, RecordingLevel.DEBUG);
        addAmountRateMetricToSensor(sensor,
                                    STATE_LEVEL_GROUP,
                                    streamsMetrics.storeLevelTagMap(taskName, STORE_TYPE_PREFIX + storeType, storeName),
                                    BYTES_WRITTEN_DURING_COMPACTION,
                                    "Average per-second number of bytes written during compaction");
        return sensor;
    }

    public static Sensor compactionTimeAvgSensor(final StreamsMetricsImpl streamsMetrics,
                                                 final String taskName,
                                                 final String storeType,
                                                 final String storeName) {
        final Sensor sensor = streamsMetrics
            .storeLevelSensor(taskName, storeName, COMPACTION_TIME + AVG_SUFFIX, RecordingLevel.DEBUG);
        addValueMetricToSensor(sensor,
                               STATE_LEVEL_GROUP,
                               streamsMetrics.storeLevelTagMap(taskName, STORE_TYPE_PREFIX + storeType, storeName),
                               COMPACTION_TIME + AVG_SUFFIX,
                               "Average time spent on compaction in ms");
        return sensor;
    }

    public static Sensor compactionTimeMinSensor(final StreamsMetricsImpl streamsMetrics,
                                                 final String taskName,
                                                 final String storeType,
                                                 final String storeName) {
        final Sensor sensor = streamsMetrics
            .storeLevelSensor(taskName, storeName, COMPACTION_TIME + MIN_SUFFIX, RecordingLevel.DEBUG);
        addValueMetricToSensor(sensor,
                               STATE_LEVEL_GROUP,
                               streamsMetrics.storeLevelTagMap(taskName, STORE_TYPE_PREFIX + storeType, storeName),
                               COMPACTION_TIME + MIN_SUFFIX,
                               "Minimum time spent on compaction in ms");
        return sensor;
    }

    public static Sensor compactionTimeMaxSensor(final StreamsMetricsImpl streamsMetrics,
                                                 final String taskName,
                                                 final String storeType,
                                                 final String storeName) {
        final Sensor sensor = streamsMetrics
            .storeLevelSensor(taskName, storeName, COMPACTION_TIME + MAX_SUFFIX, RecordingLevel.DEBUG);
        addValueMetricToSensor(sensor,
                               STATE_LEVEL_GROUP,
                               streamsMetrics.storeLevelTagMap(taskName, STORE_TYPE_PREFIX + storeType, storeName),
                               COMPACTION_TIME + MAX_SUFFIX,
                               "Maximum time spent on compaction in ms");
        return sensor;
    }

    public static Sensor numberOfOpenFilesSensor(final StreamsMetricsImpl streamsMetrics,
                                                 final String taskName,
                                                 final String storeType,
                                                 final String storeName) {
        final Sensor sensor = streamsMetrics
            .storeLevelSensor(taskName, storeName, NUMBER_OF_OPEN_FILES, RecordingLevel.DEBUG);
        addValueMetricToSensor(sensor,
                               STATE_LEVEL_GROUP,
                               streamsMetrics.storeLevelTagMap(taskName, STORE_TYPE_PREFIX + storeType, storeName),
                               NUMBER_OF_OPEN_FILES,
                               "Number of currently open files");
        return sensor;
    }

    public static Sensor numberOfFileErrorsSensor(final StreamsMetricsImpl streamsMetrics,
                                                  final String taskName,
                                                  final String storeType,
                                                  final String storeName) {
        final Sensor sensor = streamsMetrics
            .storeLevelSensor(taskName, storeName, NUMBER_OF_FILE_ERRORS, RecordingLevel.DEBUG);
        addTotalMetricToSensor(sensor,
                               STATE_LEVEL_GROUP,
                               streamsMetrics.storeLevelTagMap(taskName, STORE_TYPE_PREFIX + storeType, storeName),
                               NUMBER_OF_FILE_ERRORS,
                               "Total number of file errors occurred");
        return sensor;
    }
}
