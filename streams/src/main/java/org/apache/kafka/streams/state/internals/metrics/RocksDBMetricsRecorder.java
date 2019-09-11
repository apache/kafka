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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.RocksDBMetricContext;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;
import org.rocksdb.TickerType;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RocksDBMetricsRecorder {
    private Logger log;

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

    private enum State { NEW, RUNNING, NOT_RUNNING, ERROR, MANUAL }
    private volatile State state = State.NEW;
    private final Duration recordingInterval = Duration.ofMinutes(10);
    private final boolean startRecordingThread;
    private Thread thread;

    public RocksDBMetricsRecorder(final String metricsScope, final String storeName) {
        this(metricsScope, storeName, true);
    }

    // visible for testing
    RocksDBMetricsRecorder(final String metricsScope, final String storeName, final boolean startRecordingThread) {
        this.metricsScope = metricsScope;
        this.storeName = storeName;
        this.startRecordingThread = startRecordingThread;
        final LogContext logContext = new LogContext(String.format("[RocksDB Metrics Recorder for %s] ", storeName));
        log = logContext.logger(RocksDBMetricsRecorder.class);
    }

    public boolean isRunning() {
        return state == State.RUNNING;
    }

    public boolean error() {
        return state == State.ERROR;
    }

    public void addStatistics(final String storeName,
                              final Statistics statistics,
                              final StreamsMetricsImpl streamsMetrics,
                              final TaskId taskId) {
        if (state == State.NEW) {
            initSensors(streamsMetrics, taskId);
            if (!startRecordingThread) {
                state = State.MANUAL;
            }
        }
        if (state == State.NEW || state == State.NOT_RUNNING) {
            createThread();
        } else {
            if (statisticsToRecord.containsKey(storeName)) {
                throw new IllegalStateException("Statistics for store \"" + storeName + "\" has been already added. "
                    + "This is a bug in Kafka Streams.");
            }
        }
        statistics.setStatsLevel(StatsLevel.EXCEPT_DETAILED_TIMERS);
        statisticsToRecord.put(storeName, statistics);
        log.debug("Added statistics for store {}", storeName);
        if (state == State.NEW || state == State.NOT_RUNNING) {
            state = State.RUNNING;
            thread.start();
        }
    }

    private void waitForThreadToDie() {
        if (thread == null) {
            return;
        }
        boolean wait = true;
        log.debug("Wait for recording thread to die");
        while (wait) {
            try {
                thread.join();
                wait = false;
            } catch (final InterruptedException e) {
                // go to next iteration
            }
        }
    }

    private void createThread() {
        thread = new Thread(this::recordLoop);
        thread.setName(storeName + "-RocksDB-metrics-recorder");
        thread.setUncaughtExceptionHandler((thread, exception) -> {
            state = State.ERROR;
            log.info(Utils.stackTrace(exception));
            close();
        });
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

    public void removeStatistics(final String storeName) {
        final Statistics removedStatistics = statisticsToRecord.remove(storeName);
        if (removedStatistics != null) {
            removedStatistics.close();
            log.debug("Removed statistics for store {}", storeName);
        } else {
            throw new IllegalStateException("No statistics for store \"" + storeName
                + "\" found. This is a bug in Kafka Streams.");
        }
        if (state == State.RUNNING && statisticsToRecord.isEmpty()) {
            state = State.NOT_RUNNING;
            thread.interrupt();
            waitForThreadToDie();
        }
    }

    private void recordLoop() {
        log.info("Started with recording interval {}", recordingInterval.toString());
        while (state == State.RUNNING) {
            recordOnce();
            try {
                Thread.sleep(recordingInterval.toMillis());
            } catch (final InterruptedException exception) {
                // do nothing and wait until next iteration
            }
        }
        log.info("Stopped");
    }

    public void recordOnce() {
        log.debug("Recording metrics for store {}", storeName);
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
            numberOfOpenFiles += statistics.getTickerCount(TickerType.NO_FILE_OPENS)
                - statistics.getTickerCount(TickerType.NO_FILE_CLOSES);
            numberOfFileErrors += statistics.getTickerCount(TickerType.NO_FILE_ERRORS);
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

    public void close() {
        final List<String> storeNames = new ArrayList<>(statisticsToRecord.keySet());
        for (final String storeName : storeNames) {
            removeStatistics(storeName);
        }
        log.debug("Closed");
    }
}