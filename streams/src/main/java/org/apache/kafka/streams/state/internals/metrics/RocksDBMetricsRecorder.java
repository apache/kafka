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
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.RocksDBMetricContext;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;
import org.rocksdb.TickerType;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RocksDBMetricsRecorder {
    private final Logger logger;

    private Sensor bytesWrittenToDatabaseSensor;
    private Sensor bytesReadFromDatabaseSensor;
    private Sensor memtableBytesFlushedSensor;
    private Sensor memtableHitRatioSensor;
    private Sensor writeStallDurationSensor;
    private Sensor blockCacheDataHitRatioSensor;
    private Sensor blockCacheIndexHitRatioSensor;
    private Sensor blockCacheFilterHitRatioSensor;
    private Sensor bytesReadDuringCompactionSensor;
    private Sensor bytesWrittenDuringCompactionSensor;
    private Sensor numberOfOpenFilesSensor;
    private Sensor numberOfFileErrorsSensor;

    private final Map<String, Statistics> statisticsToRecord = new ConcurrentHashMap<>();
    private final String metricsScope;
    private final String storeName;
    private TaskId taskId;
    private StreamsMetricsImpl streamsMetrics;
    private boolean isInitialized = false;

    public RocksDBMetricsRecorder(final String metricsScope, final String storeName) {
        this.metricsScope = metricsScope;
        this.storeName = storeName;
        final LogContext logContext = new LogContext(String.format("[RocksDB Metrics Recorder for %s] ", storeName));
        logger = logContext.logger(RocksDBMetricsRecorder.class);
    }

    public String storeName() {
        return storeName;
    }

    public TaskId taskId() {
        return taskId;
    }

    public void addStatistics(final String segmentName,
                              final Statistics statistics,
                              final StreamsMetricsImpl streamsMetrics,
                              final TaskId taskId) {
        if (!isInitialized) {
            initSensors(streamsMetrics, taskId);
            this.taskId = taskId;
            this.streamsMetrics = streamsMetrics;
            isInitialized = true;
        }
        if (this.taskId != taskId) {
            throw new IllegalStateException("Statistics of store \"" + segmentName + "\" for task " + taskId
                + " cannot be added to metrics recorder for task " + this.taskId + ". This is a bug in Kafka Streams.");
        }
        if (statisticsToRecord.isEmpty()) {
            logger.debug(
                "Adding metrics recorder of task {} to metrics recording trigger",
                taskId
            );
            streamsMetrics.rocksDBMetricsRecordingTrigger().addMetricsRecorder(this);
        } else if (statisticsToRecord.containsKey(segmentName)) {
            throw new IllegalStateException("Statistics for store \"" + segmentName + "\" of task " + taskId +
                " has been already added. This is a bug in Kafka Streams.");
        }
        statistics.setStatsLevel(StatsLevel.EXCEPT_DETAILED_TIMERS);
        logger.debug("Adding statistics for store {} of task {}", segmentName, taskId);
        statisticsToRecord.put(segmentName, statistics);
    }

    private void initSensors(final StreamsMetricsImpl streamsMetrics, final TaskId taskId) {
        final RocksDBMetricContext metricContext = new RocksDBMetricContext(taskId.toString(), metricsScope, storeName);
        bytesWrittenToDatabaseSensor = RocksDBMetrics.bytesWrittenToDatabaseSensor(streamsMetrics, metricContext);
        bytesReadFromDatabaseSensor = RocksDBMetrics.bytesReadFromDatabaseSensor(streamsMetrics, metricContext);
        memtableBytesFlushedSensor = RocksDBMetrics.memtableBytesFlushedSensor(streamsMetrics, metricContext);
        memtableHitRatioSensor = RocksDBMetrics.memtableHitRatioSensor(streamsMetrics, metricContext);
        writeStallDurationSensor = RocksDBMetrics.writeStallDurationSensor(streamsMetrics, metricContext);
        blockCacheDataHitRatioSensor = RocksDBMetrics.blockCacheDataHitRatioSensor(streamsMetrics, metricContext);
        blockCacheIndexHitRatioSensor = RocksDBMetrics.blockCacheIndexHitRatioSensor(streamsMetrics, metricContext);
        blockCacheFilterHitRatioSensor = RocksDBMetrics.blockCacheFilterHitRatioSensor(streamsMetrics, metricContext);
        bytesWrittenDuringCompactionSensor =
            RocksDBMetrics.bytesWrittenDuringCompactionSensor(streamsMetrics, metricContext);
        bytesReadDuringCompactionSensor = RocksDBMetrics.bytesReadDuringCompactionSensor(streamsMetrics, metricContext);
        numberOfOpenFilesSensor = RocksDBMetrics.numberOfOpenFilesSensor(streamsMetrics, metricContext);
        numberOfFileErrorsSensor = RocksDBMetrics.numberOfFileErrorsSensor(streamsMetrics, metricContext);
    }

    public void removeStatistics(final String segmentName) {
        logger.debug("Removing statistics for store {} of task {}", segmentName, taskId);
        final Statistics removedStatistics = statisticsToRecord.remove(segmentName);
        if (removedStatistics == null) {
            throw new IllegalStateException("No statistics for store \"" + segmentName + "\" of task " + taskId
                + " could be found. This is a bug in Kafka Streams.");
        }
        removedStatistics.close();
        if (statisticsToRecord.isEmpty()) {
            logger.debug(
                "Removing metrics recorder for store {} of task {} from metrics recording trigger",
                storeName,
                taskId
            );
            streamsMetrics.rocksDBMetricsRecordingTrigger().removeMetricsRecorder(this);
        }
    }

    public void record() {
        logger.debug("Recording metrics for store {}", storeName);
        long bytesWrittenToDatabase = 0;
        long bytesReadFromDatabase = 0;
        long memtableBytesFlushed = 0;
        long memtableHits = 0;
        long memtableMisses = 0;
        long blockCacheDataHits = 0;
        long blockCacheDataMisses = 0;
        long blockCacheIndexHits = 0;
        long blockCacheIndexMisses = 0;
        long blockCacheFilterHits = 0;
        long blockCacheFilterMisses = 0;
        long writeStallDuration = 0;
        long bytesWrittenDuringCompaction = 0;
        long bytesReadDuringCompaction = 0;
        long numberOfOpenFiles = 0;
        long numberOfFileErrors = 0;
        for (final Statistics statistics : statisticsToRecord.values()) {
            bytesWrittenToDatabase += statistics.getAndResetTickerCount(TickerType.BYTES_WRITTEN);
            bytesReadFromDatabase += statistics.getAndResetTickerCount(TickerType.BYTES_READ);
            memtableBytesFlushed += statistics.getAndResetTickerCount(TickerType.FLUSH_WRITE_BYTES);
            memtableHits += statistics.getAndResetTickerCount(TickerType.MEMTABLE_HIT);
            memtableMisses += statistics.getAndResetTickerCount(TickerType.MEMTABLE_MISS);
            blockCacheDataHits += statistics.getAndResetTickerCount(TickerType.BLOCK_CACHE_DATA_HIT);
            blockCacheDataMisses += statistics.getAndResetTickerCount(TickerType.BLOCK_CACHE_DATA_MISS);
            blockCacheIndexHits += statistics.getAndResetTickerCount(TickerType.BLOCK_CACHE_INDEX_HIT);
            blockCacheIndexMisses += statistics.getAndResetTickerCount(TickerType.BLOCK_CACHE_INDEX_MISS);
            blockCacheFilterHits += statistics.getAndResetTickerCount(TickerType.BLOCK_CACHE_FILTER_HIT);
            blockCacheFilterMisses += statistics.getAndResetTickerCount(TickerType.BLOCK_CACHE_FILTER_MISS);
            writeStallDuration += statistics.getAndResetTickerCount(TickerType.STALL_MICROS);
            bytesWrittenDuringCompaction += statistics.getAndResetTickerCount(TickerType.COMPACT_WRITE_BYTES);
            bytesReadDuringCompaction += statistics.getAndResetTickerCount(TickerType.COMPACT_READ_BYTES);
            numberOfOpenFiles += statistics.getAndResetTickerCount(TickerType.NO_FILE_OPENS)
                - statistics.getAndResetTickerCount(TickerType.NO_FILE_CLOSES);
            numberOfFileErrors += statistics.getAndResetTickerCount(TickerType.NO_FILE_ERRORS);
        }
        bytesWrittenToDatabaseSensor.record(bytesWrittenToDatabase);
        bytesReadFromDatabaseSensor.record(bytesReadFromDatabase);
        memtableBytesFlushedSensor.record(memtableBytesFlushed);
        memtableHitRatioSensor.record(computeHitRatio(memtableHits, memtableMisses));
        blockCacheDataHitRatioSensor.record(computeHitRatio(blockCacheDataHits, blockCacheDataMisses));
        blockCacheIndexHitRatioSensor.record(computeHitRatio(blockCacheIndexHits, blockCacheIndexMisses));
        blockCacheFilterHitRatioSensor.record(computeHitRatio(blockCacheFilterHits, blockCacheFilterMisses));
        writeStallDurationSensor.record(writeStallDuration);
        bytesWrittenDuringCompactionSensor.record(bytesWrittenDuringCompaction);
        bytesReadDuringCompactionSensor.record(bytesReadDuringCompaction);
        numberOfOpenFilesSensor.record(numberOfOpenFiles);
        numberOfFileErrorsSensor.record(numberOfFileErrors);
    }

    private double computeHitRatio(final long hits, final long misses) {
        if (hits == 0) {
            return 0;
        }
        return (double) hits / (hits + misses);
    }
}