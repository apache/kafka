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
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.RocksDBMetricContext;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;

import java.util.HashMap;
import java.util.Map;

public class RocksDBMetricsRecorder {

    private Sensor bytesWrittenToDatabaseSensor;
    private Sensor bytesReadToDatabaseSensor;
    private Sensor memtableBytesFlushedSensor;
    private Sensor memtableHitRatioSensor;
    private Sensor memtableAvgFlushTimeSensor;
    private Sensor memtableMinFlushTimeSensor;
    private Sensor memtableMaxFlushTimeSensor;
    private Sensor writeStallDurationSensor;
    private Sensor blockCacheDataHitRatioSensor;
    private Sensor blockCacheIndexHitRatioSensor;
    private Sensor blockCacheFilterHitRatioSensor;
    private Sensor bytesReadDuringCompactionSensor;
    private Sensor bytesWrittenDuringCompactionSensor;
    private Sensor compactionTimeAvgSensor;
    private Sensor compactionTimeMinSensor;
    private Sensor compactionTimeMaxSensor;
    private Sensor numberOfOpenFilesSensor;
    private Sensor numberOfFileErrorsSensor;

    final private Map<String, Statistics> statisticsToRecord = new HashMap<>();
    final private String metricsScope;
    final private String storeName;

    public RocksDBMetricsRecorder(final String metricsScope, final String storeName) {
        this.metricsScope = metricsScope;
        this.storeName = storeName;
    }

    private void init(final StreamsMetricsImpl streamsMetrics, final TaskId taskId) {
        final RocksDBMetricContext metricContext = new RocksDBMetricContext(taskId.toString(), metricsScope, storeName);
        bytesWrittenToDatabaseSensor = RocksDBMetrics.bytesWrittenToDatabaseSensor(streamsMetrics, metricContext);
        bytesReadToDatabaseSensor = RocksDBMetrics.bytesReadFromDatabaseSensor(streamsMetrics, metricContext);
        memtableBytesFlushedSensor = RocksDBMetrics.memtableBytesFlushedSensor(streamsMetrics, metricContext);
        memtableHitRatioSensor = RocksDBMetrics.memtableHitRatioSensor(streamsMetrics, metricContext);
        memtableAvgFlushTimeSensor = RocksDBMetrics.memtableAvgFlushTimeSensor(streamsMetrics, metricContext);
        memtableMinFlushTimeSensor = RocksDBMetrics.memtableMinFlushTimeSensor(streamsMetrics, metricContext);
        memtableMaxFlushTimeSensor = RocksDBMetrics.memtableMaxFlushTimeSensor(streamsMetrics, metricContext);
        writeStallDurationSensor = RocksDBMetrics.writeStallDurationSensor(streamsMetrics, metricContext);
        blockCacheDataHitRatioSensor = RocksDBMetrics.blockCacheDataHitRatioSensor(streamsMetrics, metricContext);
        blockCacheIndexHitRatioSensor = RocksDBMetrics.blockCacheIndexHitRatioSensor(streamsMetrics, metricContext);
        blockCacheFilterHitRatioSensor = RocksDBMetrics.blockCacheFilterHitRatioSensor(streamsMetrics, metricContext);
        bytesReadDuringCompactionSensor = RocksDBMetrics.bytesReadDuringCompactionSensor(streamsMetrics, metricContext);
        bytesWrittenDuringCompactionSensor =
            RocksDBMetrics.bytesWrittenDuringCompactionSensor(streamsMetrics, metricContext);
        compactionTimeAvgSensor = RocksDBMetrics.compactionTimeAvgSensor(streamsMetrics, metricContext);
        compactionTimeMinSensor = RocksDBMetrics.compactionTimeMinSensor(streamsMetrics, metricContext);
        compactionTimeMaxSensor = RocksDBMetrics.compactionTimeMaxSensor(streamsMetrics, metricContext);
        numberOfOpenFilesSensor = RocksDBMetrics.numberOfOpenFilesSensor(streamsMetrics, metricContext);
        numberOfFileErrorsSensor = RocksDBMetrics.numberOfFileErrorsSensor(streamsMetrics, metricContext);
    }

    public void addStatistics(final String storeName,
                              final Statistics statistics,
                              final StreamsMetricsImpl streamsMetrics,
                              final TaskId taskId) {
        if (statisticsToRecord.isEmpty()) {
            init(streamsMetrics, taskId);
        }
        if (statisticsToRecord.containsKey(storeName)) {
            throw new IllegalStateException("A statistics for store " + storeName + "has been already registered. "
                + "This is a bug in Kafka Streams.");
        }
        statistics.setStatsLevel(StatsLevel.EXCEPT_DETAILED_TIMERS);
        statisticsToRecord.put(storeName, statistics);
    }

    public void removeStatistics(final String storeName) {
        final Statistics removedStatistics = statisticsToRecord.remove(storeName);
        if (removedStatistics != null) {
            removedStatistics.close();
        }
    }

    public void record() {
        // TODO: this block of record calls merely avoids compiler warnings.
        //       The actual computations of the metrics will be implemented in the next PR
        bytesWrittenToDatabaseSensor.record(0);
        bytesReadToDatabaseSensor.record(0);
        memtableBytesFlushedSensor.record(0);
        memtableHitRatioSensor.record(0);
        memtableAvgFlushTimeSensor.record(0);
        memtableMinFlushTimeSensor.record(0);
        memtableMaxFlushTimeSensor.record(0);
        writeStallDurationSensor.record(0);
        blockCacheDataHitRatioSensor.record(0);
        blockCacheIndexHitRatioSensor.record(0);
        blockCacheFilterHitRatioSensor.record(0);
        bytesReadDuringCompactionSensor.record(0);
        bytesWrittenDuringCompactionSensor.record(0);
        compactionTimeAvgSensor.record(0);
        compactionTimeMinSensor.record(0);
        compactionTimeMaxSensor.record(0);
        numberOfOpenFilesSensor.record(0);
        numberOfFileErrorsSensor.record(0);
    }

    public void close() {
        for (final Statistics statistics : statisticsToRecord.values()) {
            statistics.close();
        }
        statisticsToRecord.clear();
    }
}