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

import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;

import java.math.BigInteger;
import java.util.Objects;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.AVG_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.MAX_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.MIN_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.RATIO_SUFFIX;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.STATE_STORE_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addRateOfSumMetricToSensor;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addRateOfSumAndSumMetricsToSensor;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addAvgAndSumMetricsToSensor;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addSumMetricToSensor;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addValueMetricToSensor;

public class RocksDBMetrics {
    private RocksDBMetrics() {}

    private static final String BYTES_WRITTEN_TO_DB = "bytes-written";
    private static final String BYTES_READ_FROM_DB = "bytes-read";
    private static final String MEMTABLE_BYTES_FLUSHED = "memtable-bytes-flushed";
    private static final String MEMTABLE_HIT_RATIO = "memtable-hit" + RATIO_SUFFIX;
    private static final String MEMTABLE_FLUSH_TIME = "memtable-flush-time";
    private static final String MEMTABLE_FLUSH_TIME_AVG = MEMTABLE_FLUSH_TIME + AVG_SUFFIX;
    private static final String MEMTABLE_FLUSH_TIME_MIN = MEMTABLE_FLUSH_TIME + MIN_SUFFIX;
    private static final String MEMTABLE_FLUSH_TIME_MAX = MEMTABLE_FLUSH_TIME + MAX_SUFFIX;
    private static final String WRITE_STALL_DURATION = "write-stall-duration";
    private static final String BLOCK_CACHE_DATA_HIT_RATIO = "block-cache-data-hit" + RATIO_SUFFIX;
    private static final String BLOCK_CACHE_INDEX_HIT_RATIO = "block-cache-index-hit" + RATIO_SUFFIX;
    private static final String BLOCK_CACHE_FILTER_HIT_RATIO = "block-cache-filter-hit" + RATIO_SUFFIX;
    private static final String BYTES_READ_DURING_COMPACTION = "bytes-read-compaction";
    private static final String BYTES_WRITTEN_DURING_COMPACTION = "bytes-written-compaction";
    private static final String COMPACTION_TIME = "compaction-time";
    private static final String COMPACTION_TIME_AVG = COMPACTION_TIME + AVG_SUFFIX;
    private static final String COMPACTION_TIME_MIN = COMPACTION_TIME + MIN_SUFFIX;
    private static final String COMPACTION_TIME_MAX = COMPACTION_TIME + MAX_SUFFIX;
    private static final String NUMBER_OF_OPEN_FILES = "number-open-files";
    private static final String NUMBER_OF_FILE_ERRORS = "number-file-errors";
    static final String NUMBER_OF_ENTRIES_ACTIVE_MEMTABLE = "num-entries-active-mem-table";

    private static final String BYTES_WRITTEN_TO_DB_RATE_DESCRIPTION =
        "Average number of bytes written per second to the RocksDB state store";
    private static final String BYTES_WRITTEN_TO_DB_TOTAL_DESCRIPTION =
        "Total number of bytes written to the RocksDB state store";
    private static final String BYTES_READ_FROM_DB_RATE_DESCRIPTION =
        "Average number of bytes read per second from the RocksDB state store";
    private static final String BYTES_READ_FROM_DB_TOTAL_DESCRIPTION =
        "Total number of bytes read from the RocksDB state store";
    private static final String MEMTABLE_BYTES_FLUSHED_RATE_DESCRIPTION =
        "Average number of bytes flushed per second from the memtable to disk";
    private static final String MEMTABLE_BYTES_FLUSHED_TOTAL_DESCRIPTION =
        "Total number of bytes flushed from the memtable to disk";
    private static final String MEMTABLE_HIT_RATIO_DESCRIPTION =
        "Ratio of memtable hits relative to all lookups to the memtable";
    private static final String MEMTABLE_FLUSH_TIME_AVG_DESCRIPTION =
        "Average time spent on flushing the memtable to disk in ms";
    private static final String MEMTABLE_FLUSH_TIME_MIN_DESCRIPTION =
        "Minimum time spent on flushing the memtable to disk in ms";
    private static final String MEMTABLE_FLUSH_TIME_MAX_DESCRIPTION =
        "Maximum time spent on flushing the memtable to disk in ms";
    private static final String WRITE_STALL_DURATION_AVG_DESCRIPTION = "Average duration of write stalls in ms";
    private static final String WRITE_STALL_DURATION_TOTAL_DESCRIPTION = "Total duration of write stalls in ms";
    private static final String BLOCK_CACHE_DATA_HIT_RATIO_DESCRIPTION =
        "Ratio of block cache hits for data relative to all lookups for data to the block cache";
    private static final String BLOCK_CACHE_INDEX_HIT_RATIO_DESCRIPTION =
        "Ratio of block cache hits for indexes relative to all lookups for indexes to the block cache";
    private static final String BLOCK_CACHE_FILTER_HIT_RATIO_DESCRIPTION =
        "Ratio of block cache hits for filters relative to all lookups for filters to the block cache";
    private static final String BYTES_READ_DURING_COMPACTION_DESCRIPTION =
        "Average number of bytes read per second during compaction";
    private static final String BYTES_WRITTEN_DURING_COMPACTION_DESCRIPTION =
        "Average number of bytes written per second during compaction";
    private static final String COMPACTION_TIME_AVG_DESCRIPTION = "Average time spent on compaction in ms";
    private static final String COMPACTION_TIME_MIN_DESCRIPTION = "Minimum time spent on compaction in ms";
    private static final String COMPACTION_TIME_MAX_DESCRIPTION = "Maximum time spent on compaction in ms";
    private static final String NUMBER_OF_OPEN_FILES_DESCRIPTION = "Number of currently open files";
    private static final String NUMBER_OF_FILE_ERRORS_DESCRIPTION = "Total number of file errors occurred";
    private static final String NUMBER_OF_ENTRIES_ACTIVE_MEMTABLE_DESCRIPTION =
            "Current total number of entries in the active memtable";

    public static class RocksDBMetricContext {
        private final String taskName;
        private final String metricsScope;
        private final String storeName;

        public RocksDBMetricContext(final String taskName,
                                    final String metricsScope,
                                    final String storeName) {
            this.taskName = taskName;
            this.metricsScope = metricsScope;
            this.storeName = storeName;
        }

        public String taskName() {
            return taskName;
        }
        public String metricsScope() {
            return metricsScope;
        }
        public String storeName() {
            return storeName;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final RocksDBMetricContext that = (RocksDBMetricContext) o;
            return Objects.equals(taskName, that.taskName) &&
                Objects.equals(metricsScope, that.metricsScope) &&
                Objects.equals(storeName, that.storeName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(taskName, metricsScope, storeName);
        }
    }

    public static Sensor bytesWrittenToDatabaseSensor(final StreamsMetricsImpl streamsMetrics,
                                                      final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, BYTES_WRITTEN_TO_DB);
        addRateOfSumAndSumMetricsToSensor(
            sensor,
            STATE_STORE_LEVEL_GROUP,
            streamsMetrics.storeLevelTagMap(
                metricContext.taskName(),
                metricContext.metricsScope(),
                metricContext.storeName()
            ),
            BYTES_WRITTEN_TO_DB,
            BYTES_WRITTEN_TO_DB_RATE_DESCRIPTION,
            BYTES_WRITTEN_TO_DB_TOTAL_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor bytesReadFromDatabaseSensor(final StreamsMetricsImpl streamsMetrics,
                                                     final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, BYTES_READ_FROM_DB);
        addRateOfSumAndSumMetricsToSensor(
            sensor,
            STATE_STORE_LEVEL_GROUP,
            streamsMetrics.storeLevelTagMap(
                metricContext.taskName(),
                metricContext.metricsScope(),
                metricContext.storeName()
            ),
            BYTES_READ_FROM_DB,
            BYTES_READ_FROM_DB_RATE_DESCRIPTION,
            BYTES_READ_FROM_DB_TOTAL_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor memtableBytesFlushedSensor(final StreamsMetricsImpl streamsMetrics,
                                                    final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, MEMTABLE_BYTES_FLUSHED);
        addRateOfSumAndSumMetricsToSensor(
            sensor,
            STATE_STORE_LEVEL_GROUP,
            streamsMetrics.storeLevelTagMap(
                metricContext.taskName(),
                metricContext.metricsScope(),
                metricContext.storeName()
            ),
            MEMTABLE_BYTES_FLUSHED,
            MEMTABLE_BYTES_FLUSHED_RATE_DESCRIPTION,
            MEMTABLE_BYTES_FLUSHED_TOTAL_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor memtableHitRatioSensor(final StreamsMetricsImpl streamsMetrics,
                                                final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, MEMTABLE_HIT_RATIO);
        addValueMetricToSensor(
            sensor,
            STATE_STORE_LEVEL_GROUP,
            streamsMetrics.storeLevelTagMap(
                metricContext.taskName(),
                metricContext.metricsScope(),
                metricContext.storeName()
            ),
            MEMTABLE_HIT_RATIO,
            MEMTABLE_HIT_RATIO_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor memtableAvgFlushTimeSensor(final StreamsMetricsImpl streamsMetrics,
                                                    final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, MEMTABLE_FLUSH_TIME_AVG);
        addValueMetricToSensor(
            sensor,
            STATE_STORE_LEVEL_GROUP,
            streamsMetrics.storeLevelTagMap(
                metricContext.taskName(),
                metricContext.metricsScope(),
                metricContext.storeName()
            ),
            MEMTABLE_FLUSH_TIME_AVG,
            MEMTABLE_FLUSH_TIME_AVG_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor memtableMinFlushTimeSensor(final StreamsMetricsImpl streamsMetrics,
                                                    final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, MEMTABLE_FLUSH_TIME_MIN);
        addValueMetricToSensor(
            sensor,
            STATE_STORE_LEVEL_GROUP,
            streamsMetrics.storeLevelTagMap(
                metricContext.taskName(),
                metricContext.metricsScope(),
                metricContext.storeName()
            ),
            MEMTABLE_FLUSH_TIME_MIN,
            MEMTABLE_FLUSH_TIME_MIN_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor memtableMaxFlushTimeSensor(final StreamsMetricsImpl streamsMetrics,
                                                    final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, MEMTABLE_FLUSH_TIME_MAX);
        addValueMetricToSensor(
            sensor,
            STATE_STORE_LEVEL_GROUP,
            streamsMetrics.storeLevelTagMap(
                metricContext.taskName(),
                metricContext.metricsScope(),
                metricContext.storeName()
            ),
            MEMTABLE_FLUSH_TIME_MAX,
            MEMTABLE_FLUSH_TIME_MAX_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor writeStallDurationSensor(final StreamsMetricsImpl streamsMetrics,
                                                  final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, WRITE_STALL_DURATION);
        addAvgAndSumMetricsToSensor(
            sensor,
            STATE_STORE_LEVEL_GROUP,
            streamsMetrics.storeLevelTagMap(
                metricContext.taskName(),
                metricContext.metricsScope(),
                metricContext.storeName()
            ),
            WRITE_STALL_DURATION,
            WRITE_STALL_DURATION_AVG_DESCRIPTION,
            WRITE_STALL_DURATION_TOTAL_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor blockCacheDataHitRatioSensor(final StreamsMetricsImpl streamsMetrics,
                                                      final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, BLOCK_CACHE_DATA_HIT_RATIO);
        addValueMetricToSensor(
            sensor,
            STATE_STORE_LEVEL_GROUP,
            streamsMetrics.storeLevelTagMap(
                metricContext.taskName(),
                metricContext.metricsScope(),
                metricContext.storeName()
            ),
            BLOCK_CACHE_DATA_HIT_RATIO,
            BLOCK_CACHE_DATA_HIT_RATIO_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor blockCacheIndexHitRatioSensor(final StreamsMetricsImpl streamsMetrics,
                                                       final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, BLOCK_CACHE_INDEX_HIT_RATIO);
        addValueMetricToSensor(
            sensor,
            STATE_STORE_LEVEL_GROUP,
            streamsMetrics.storeLevelTagMap(
                metricContext.taskName(),
                metricContext.metricsScope(),
                metricContext.storeName()
            ),
            BLOCK_CACHE_INDEX_HIT_RATIO,
            BLOCK_CACHE_INDEX_HIT_RATIO_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor blockCacheFilterHitRatioSensor(final StreamsMetricsImpl streamsMetrics,
                                                        final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, BLOCK_CACHE_FILTER_HIT_RATIO);
        addValueMetricToSensor(
            sensor,
            STATE_STORE_LEVEL_GROUP,
            streamsMetrics.storeLevelTagMap(
                metricContext.taskName(),
                metricContext.metricsScope(),
                metricContext.storeName()
            ),
            BLOCK_CACHE_FILTER_HIT_RATIO,
            BLOCK_CACHE_FILTER_HIT_RATIO_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor bytesReadDuringCompactionSensor(final StreamsMetricsImpl streamsMetrics,
                                                         final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, BYTES_READ_DURING_COMPACTION);
        addRateOfSumMetricToSensor(
            sensor,
            STATE_STORE_LEVEL_GROUP,
            streamsMetrics.storeLevelTagMap(
                metricContext.taskName(),
                metricContext.metricsScope(),
                metricContext.storeName()
            ),
            BYTES_READ_DURING_COMPACTION,
            BYTES_READ_DURING_COMPACTION_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor bytesWrittenDuringCompactionSensor(final StreamsMetricsImpl streamsMetrics,
                                                            final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, BYTES_WRITTEN_DURING_COMPACTION);
        addRateOfSumMetricToSensor(
            sensor,
            STATE_STORE_LEVEL_GROUP,
            streamsMetrics.storeLevelTagMap(
                metricContext.taskName(),
                metricContext.metricsScope(),
                metricContext.storeName()
            ),
            BYTES_WRITTEN_DURING_COMPACTION,
            BYTES_WRITTEN_DURING_COMPACTION_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor compactionTimeAvgSensor(final StreamsMetricsImpl streamsMetrics,
                                                 final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, COMPACTION_TIME_AVG);
        addValueMetricToSensor(
            sensor,
            STATE_STORE_LEVEL_GROUP,
            streamsMetrics.storeLevelTagMap(
                metricContext.taskName(),
                metricContext.metricsScope(),
                metricContext.storeName()
            ),
            COMPACTION_TIME_AVG,
            COMPACTION_TIME_AVG_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor compactionTimeMinSensor(final StreamsMetricsImpl streamsMetrics,
                                                 final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, COMPACTION_TIME_MIN);
        addValueMetricToSensor(
            sensor,
            STATE_STORE_LEVEL_GROUP,
            streamsMetrics.storeLevelTagMap(
                metricContext.taskName(),
                metricContext.metricsScope(),
                metricContext.storeName()
            ),
            COMPACTION_TIME_MIN,
            COMPACTION_TIME_MIN_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor compactionTimeMaxSensor(final StreamsMetricsImpl streamsMetrics,
                                                 final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, COMPACTION_TIME_MAX);
        addValueMetricToSensor(
            sensor,
            STATE_STORE_LEVEL_GROUP,
            streamsMetrics.storeLevelTagMap(
                metricContext.taskName(),
                metricContext.metricsScope(),
                metricContext.storeName()
            ),
            COMPACTION_TIME_MAX,
            COMPACTION_TIME_MAX_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor numberOfOpenFilesSensor(final StreamsMetricsImpl streamsMetrics,
                                                 final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, NUMBER_OF_OPEN_FILES);
        addSumMetricToSensor(
            sensor,
            STATE_STORE_LEVEL_GROUP,
            streamsMetrics.storeLevelTagMap(
                metricContext.taskName(),
                metricContext.metricsScope(),
                metricContext.storeName()
            ),
            NUMBER_OF_OPEN_FILES,
            false,
            NUMBER_OF_OPEN_FILES_DESCRIPTION
        );
        return sensor;
    }

    public static Sensor numberOfFileErrorsSensor(final StreamsMetricsImpl streamsMetrics,
                                                  final RocksDBMetricContext metricContext) {
        final Sensor sensor = createSensor(streamsMetrics, metricContext, NUMBER_OF_FILE_ERRORS);
        addSumMetricToSensor(
            sensor,
            STATE_STORE_LEVEL_GROUP,
            streamsMetrics.storeLevelTagMap(
                metricContext.taskName(),
                metricContext.metricsScope(),
                metricContext.storeName()
            ),
            NUMBER_OF_FILE_ERRORS,
            NUMBER_OF_FILE_ERRORS_DESCRIPTION
        );
        return sensor;
    }

    public static void addNumEntriesActiveMemTableMetric(final StreamsMetricsImpl streamsMetrics,
                                                         final RocksDBMetricContext metricContext,
                                                         final Gauge<BigInteger> valueProvider) {
        streamsMetrics.addStoreLevelMutableMetric(
            metricContext.taskName(),
            metricContext.metricsScope(),
            metricContext.storeName(),
            NUMBER_OF_ENTRIES_ACTIVE_MEMTABLE,
            NUMBER_OF_ENTRIES_ACTIVE_MEMTABLE_DESCRIPTION,
            RecordingLevel.INFO,
            valueProvider
        );
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