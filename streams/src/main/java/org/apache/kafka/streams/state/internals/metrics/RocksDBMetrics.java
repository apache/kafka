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

    public static class RocksDBMetricContext {
        private final String taskName;
        private final String storeType;
        private final String storeName;

        public RocksDBMetricContext(final String taskName, final String storeType, final String storeName) {
            this.taskName = taskName;
            this.storeType = "rocksdb-" + storeType;
            this.storeName = storeName;
        }

        public String taskName() {
            return taskName;
        }
        public String storeType() {
            return storeType;
        }
        public String storeName() {
            return storeName;
        }
    }

    public static Sensor bytesWrittenToDatabaseSensor(final StreamsMetricsImpl streamsMetrics,
                                                      final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, BYTES_WRITTEN_TO_DB);
        addAmountRateAndTotalMetricsToSensor(
            sensor,
            STATE_LEVEL_GROUP,
            streamsMetrics
                .storeLevelTagMap(metricContext.taskName(), metricContext.storeType(), metricContext.storeName()),
            BYTES_WRITTEN_TO_DB,
            "Average number of bytes written per second to the RocksDB state store",
            "Total number of bytes written to the RocksDB state store");
        return sensor;
    }

    public static Sensor bytesReadFromDatabaseSensor(final StreamsMetricsImpl streamsMetrics,
                                                     final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, BYTES_READ_FROM_DB);
        addAmountRateAndTotalMetricsToSensor(sensor,
            STATE_LEVEL_GROUP,
            streamsMetrics
                .storeLevelTagMap(metricContext.taskName(), metricContext.storeType(), metricContext.storeName()),
            BYTES_READ_FROM_DB,
            "Average number of bytes read per second from the RocksDB state store",
            "Total number of bytes read from the RocksDB state store"
        );
        return sensor;
    }

    public static Sensor memtableBytesFlushedSensor(final StreamsMetricsImpl streamsMetrics,
                                                    final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, MEMTABLE_BYTES_FLUSHED);
        addAmountRateAndTotalMetricsToSensor(sensor,
            STATE_LEVEL_GROUP,
            streamsMetrics
                .storeLevelTagMap(metricContext.taskName(), metricContext.storeType(), metricContext.storeName()),
            MEMTABLE_BYTES_FLUSHED,
            "Average number of bytes flushed per second from the memtable to disk",
            "Total number of bytes flushed from the memtable to disk"
        );
        return sensor;
    }

    public static Sensor memtableHitRatioSensor(final StreamsMetricsImpl streamsMetrics,
                                                final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, MEMTABLE_HIT_RATIO);
        addValueMetricToSensor(sensor,
            STATE_LEVEL_GROUP,
            streamsMetrics
                .storeLevelTagMap(metricContext.taskName(), metricContext.storeType(), metricContext.storeName()),
            MEMTABLE_HIT_RATIO,
            "Ratio of memtable hits relative to all lookups to the memtable"
        );
        return sensor;
    }

    public static Sensor memtableAvgFlushTimeSensor(final StreamsMetricsImpl streamsMetrics,
                                                    final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, MEMTABLE_FLUSH_TIME + AVG_SUFFIX);
        addValueMetricToSensor(sensor,
            STATE_LEVEL_GROUP,
            streamsMetrics
                .storeLevelTagMap(metricContext.taskName(), metricContext.storeType(), metricContext.storeName()),
            MEMTABLE_FLUSH_TIME + AVG_SUFFIX,
            "Average time spent on flushing the memtable to disk in ms"
        );
        return sensor;
    }

    public static Sensor memtableMinFlushTimeSensor(final StreamsMetricsImpl streamsMetrics,
                                                    final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, MEMTABLE_FLUSH_TIME + MIN_SUFFIX);
        addValueMetricToSensor(sensor,
            STATE_LEVEL_GROUP,
            streamsMetrics
                .storeLevelTagMap(metricContext.taskName(), metricContext.storeType(), metricContext.storeName()),
            MEMTABLE_FLUSH_TIME + MIN_SUFFIX,
            "Minimum time spent on flushing the memtable to disk in ms"
        );
        return sensor;
    }

    public static Sensor memtableMaxFlushTimeSensor(final StreamsMetricsImpl streamsMetrics,
                                                    final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, MEMTABLE_FLUSH_TIME + MAX_SUFFIX);
        addValueMetricToSensor(sensor,
            STATE_LEVEL_GROUP,
            streamsMetrics
                .storeLevelTagMap(metricContext.taskName(), metricContext.storeType(), metricContext.storeName()),
            MEMTABLE_FLUSH_TIME + MAX_SUFFIX,
            "Maximum time spent on flushing the memtable to disk in ms"
        );
        return sensor;
    }

    public static Sensor writeStallDurationSensor(final StreamsMetricsImpl streamsMetrics,
                                                  final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, WRITE_STALL_DURATION);
        addAvgAndTotalMetricsToSensor(sensor,
            STATE_LEVEL_GROUP,
            streamsMetrics
                .storeLevelTagMap(metricContext.taskName(), metricContext.storeType(), metricContext.storeName()),
            WRITE_STALL_DURATION,
            "Moving average duration of write stalls in ms",
            "Total duration of write stalls in ms"
        );
        return sensor;
    }

    public static Sensor blockCacheDataHitRatioSensor(final StreamsMetricsImpl streamsMetrics,
                                                      final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, BLOCK_CACHE_DATA_HIT_RATIO);
        addValueMetricToSensor(sensor,
            STATE_LEVEL_GROUP,
            streamsMetrics
                .storeLevelTagMap(metricContext.taskName(), metricContext.storeType(), metricContext.storeName()),
            BLOCK_CACHE_DATA_HIT_RATIO,
            "Ratio of block cache hits for data relative to all lookups for data to the block cache"
        );
        return sensor;
    }

    public static Sensor blockCacheIndexHitRatioSensor(final StreamsMetricsImpl streamsMetrics,
                                                       final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, BLOCK_CACHE_INDEX_HIT_RATIO);
        addValueMetricToSensor(sensor,
                               STATE_LEVEL_GROUP,
                               streamsMetrics.storeLevelTagMap(metricContext.taskName(), metricContext.storeType(), metricContext.storeName()),
                               BLOCK_CACHE_INDEX_HIT_RATIO,
                               "Ratio of block cache hits for indexes relative to all lookups for indexes to"
                                   + " the block cache");
        return sensor;
    }

    public static Sensor blockCacheFilterHitRatioSensor(final StreamsMetricsImpl streamsMetrics,
                                                        final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, BLOCK_CACHE_FILTER_HIT_RATIO);
        addValueMetricToSensor(sensor,
            STATE_LEVEL_GROUP,
            streamsMetrics
                .storeLevelTagMap(metricContext.taskName(), metricContext.storeType(), metricContext.storeName()),
            BLOCK_CACHE_FILTER_HIT_RATIO,
            "Ratio of block cache hits for filters relative to all lookups for filters to"
                + " the block cache"
        );
        return sensor;
    }

    public static Sensor bytesReadDuringCompactionSensor(final StreamsMetricsImpl streamsMetrics,
                                                         final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, BYTES_READ_DURING_COMPACTION);
        addAmountRateMetricToSensor(sensor,
            STATE_LEVEL_GROUP,
            streamsMetrics
                .storeLevelTagMap(metricContext.taskName(), metricContext.storeType(), metricContext.storeName()),
            BYTES_READ_DURING_COMPACTION,
            "Average number of bytes read per second during compaction"
        );
        return sensor;
    }

    public static Sensor bytesWrittenDuringCompactionSensor(final StreamsMetricsImpl streamsMetrics,
                                                            final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, BYTES_WRITTEN_DURING_COMPACTION);
        addAmountRateMetricToSensor(sensor,
            STATE_LEVEL_GROUP,
            streamsMetrics
                .storeLevelTagMap(metricContext.taskName(), metricContext.storeType(), metricContext.storeName()),
            BYTES_WRITTEN_DURING_COMPACTION,
            "Average number of bytes written per second during compaction"
        );
        return sensor;
    }

    public static Sensor compactionTimeAvgSensor(final StreamsMetricsImpl streamsMetrics,
                                                 final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, COMPACTION_TIME + AVG_SUFFIX);
        addValueMetricToSensor(sensor,
            STATE_LEVEL_GROUP,
            streamsMetrics
                .storeLevelTagMap(metricContext.taskName(), metricContext.storeType(), metricContext.storeName()),
            COMPACTION_TIME + AVG_SUFFIX,
            "Average time spent on compaction in ms"
        );
        return sensor;
    }

    public static Sensor compactionTimeMinSensor(final StreamsMetricsImpl streamsMetrics,
                                                 final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, COMPACTION_TIME + MIN_SUFFIX);
        addValueMetricToSensor(sensor,
            STATE_LEVEL_GROUP,
            streamsMetrics
                .storeLevelTagMap(metricContext.taskName(), metricContext.storeType(), metricContext.storeName()),
            COMPACTION_TIME + MIN_SUFFIX,
            "Minimum time spent on compaction in ms"
        );
        return sensor;
    }

    public static Sensor compactionTimeMaxSensor(final StreamsMetricsImpl streamsMetrics,
                                                 final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, COMPACTION_TIME + MAX_SUFFIX);
        addValueMetricToSensor(sensor,
            STATE_LEVEL_GROUP,
            streamsMetrics
                .storeLevelTagMap(metricContext.taskName(), metricContext.storeType(), metricContext.storeName()),
            COMPACTION_TIME + MAX_SUFFIX,
            "Maximum time spent on compaction in ms"
        );
        return sensor;
    }

    public static Sensor numberOfOpenFilesSensor(final StreamsMetricsImpl streamsMetrics,
                                                 final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, NUMBER_OF_OPEN_FILES);
        addValueMetricToSensor(sensor,
            STATE_LEVEL_GROUP,
            streamsMetrics
                .storeLevelTagMap(metricContext.taskName(), metricContext.storeType(), metricContext.storeName()),
            NUMBER_OF_OPEN_FILES,
            "Number of currently open files"
        );
        return sensor;
    }

    public static Sensor numberOfFileErrorsSensor(final StreamsMetricsImpl streamsMetrics,
                                                  final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, NUMBER_OF_FILE_ERRORS);
        addTotalMetricToSensor(sensor,
            STATE_LEVEL_GROUP,
            streamsMetrics
                .storeLevelTagMap(metricContext.taskName(), metricContext.storeType(), metricContext.storeName()),
            NUMBER_OF_FILE_ERRORS,
            "Total number of file errors occurred"
        );
        return sensor;
    }

    private static Sensor createSensor(final StreamsMetricsImpl streamsMetrics,
                                       final RocksDBMetricContext metricContext,
                                       final String sensorName) {
        return streamsMetrics.storeLevelSensor(
            metricContext.taskName(),
            metricContext.storeName(),
            sensorName,
            RecordingLevel.DEBUG);
    }
}
