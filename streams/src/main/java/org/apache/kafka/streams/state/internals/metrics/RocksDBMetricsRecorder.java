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
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.RocksDBMetricContext;

import org.rocksdb.Cache;
import org.rocksdb.HistogramData;
import org.rocksdb.HistogramType;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;
import org.rocksdb.TickerType;
import org.slf4j.Logger;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.CAPACITY_OF_BLOCK_CACHE;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.COMPACTION_PENDING;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.CURRENT_SIZE_OF_ACTIVE_MEMTABLE;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.CURRENT_SIZE_OF_ALL_MEMTABLES;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.ESTIMATED_BYTES_OF_PENDING_COMPACTION;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.ESTIMATED_MEMORY_OF_TABLE_READERS;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.ESTIMATED_NUMBER_OF_KEYS;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.LIVE_SST_FILES_SIZE;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.MEMTABLE_FLUSH_PENDING;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.NUMBER_OF_BACKGROUND_ERRORS;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.NUMBER_OF_DELETES_ACTIVE_MEMTABLE;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.NUMBER_OF_DELETES_IMMUTABLE_MEMTABLES;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.NUMBER_OF_ENTRIES_ACTIVE_MEMTABLE;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.NUMBER_OF_ENTRIES_IMMUTABLE_MEMTABLES;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.NUMBER_OF_IMMUTABLE_MEMTABLES;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.NUMBER_OF_LIVE_VERSIONS;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.NUMBER_OF_RUNNING_COMPACTIONS;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.NUMBER_OF_RUNNING_FLUSHES;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.PINNED_USAGE_OF_BLOCK_CACHE;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.SIZE_OF_ALL_MEMTABLES;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.TOTAL_SST_FILES_SIZE;
import static org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.USAGE_OF_BLOCK_CACHE;

public class RocksDBMetricsRecorder {

    private static class DbAndCacheAndStatistics {
        public final RocksDB db;
        public final Cache cache;
        public final Statistics statistics;

        public DbAndCacheAndStatistics(final RocksDB db, final Cache cache, final Statistics statistics) {
            Objects.requireNonNull(db, "database instance must not be null");
            this.db = db;
            this.cache = cache;
            if (statistics != null) {
                statistics.setStatsLevel(StatsLevel.EXCEPT_DETAILED_TIMERS);
            }
            this.statistics = statistics;
        }
    }

    private static final String ROCKSDB_PROPERTIES_PREFIX = "rocksdb.";


    private final Logger logger;

    private Sensor bytesWrittenToDatabaseSensor;
    private Sensor bytesReadFromDatabaseSensor;
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

    private final Map<String, DbAndCacheAndStatistics> storeToValueProviders = new ConcurrentHashMap<>();
    private final String metricsScope;
    private final String storeName;
    private TaskId taskId;
    private StreamsMetricsImpl streamsMetrics;
    private boolean singleCache = true;

    public RocksDBMetricsRecorder(final String metricsScope,
                                  final String storeName) {
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

    /**
     * The initialisation of the metrics recorder is idempotent.
     */
    public void init(final StreamsMetricsImpl streamsMetrics,
                     final TaskId taskId) {
        Objects.requireNonNull(streamsMetrics, "Streams metrics must not be null");
        Objects.requireNonNull(taskId, "task ID must not be null");
        if (this.taskId != null && !this.taskId.equals(taskId)) {
            throw new IllegalStateException("Metrics recorder is re-initialised with different task: previous task is " +
                this.taskId + " whereas current task is " + taskId + ". This is a bug in Kafka Streams. " +
                "Please open a bug report under https://issues.apache.org/jira/projects/KAFKA/issues");
        }
        if (this.streamsMetrics != null && this.streamsMetrics != streamsMetrics) {
            throw new IllegalStateException("Metrics recorder is re-initialised with different Streams metrics. "
                + "This is a bug in Kafka Streams. " +
                "Please open a bug report under https://issues.apache.org/jira/projects/KAFKA/issues");
        }
        final RocksDBMetricContext metricContext = new RocksDBMetricContext(taskId.toString(), metricsScope, storeName);
        initSensors(streamsMetrics, metricContext);
        initGauges(streamsMetrics, metricContext);
        this.taskId = taskId;
        this.streamsMetrics = streamsMetrics;
    }

    public void addValueProviders(final String segmentName,
                                  final RocksDB db,
                                  final Cache cache,
                                  final Statistics statistics) {
        if (storeToValueProviders.isEmpty()) {
            logger.debug("Adding metrics recorder of task {} to metrics recording trigger", taskId);
            streamsMetrics.rocksDBMetricsRecordingTrigger().addMetricsRecorder(this);
        } else if (storeToValueProviders.containsKey(segmentName)) {
            throw new IllegalStateException("Value providers for store " + segmentName + " of task " + taskId +
                " has been already added. This is a bug in Kafka Streams. " +
                "Please open a bug report under https://issues.apache.org/jira/projects/KAFKA/issues");
        }
        verifyDbAndCacheAndStatistics(segmentName, db, cache, statistics);
        logger.debug("Adding value providers for store {} of task {}", segmentName, taskId);
        storeToValueProviders.put(segmentName, new DbAndCacheAndStatistics(db, cache, statistics));
    }

    private void verifyDbAndCacheAndStatistics(final String segmentName,
                                               final RocksDB db,
                                               final Cache cache,
                                               final Statistics statistics) {
        for (final DbAndCacheAndStatistics valueProviders : storeToValueProviders.values()) {
            verifyConsistencyOfValueProvidersAcrossSegments(segmentName, statistics, valueProviders.statistics, "statistics");
            verifyConsistencyOfValueProvidersAcrossSegments(segmentName, cache, valueProviders.cache, "cache");
            if (db == valueProviders.db) {
                throw new IllegalStateException("DB instance for store " + segmentName + " of task " + taskId +
                    " was already added for another segment as a value provider. This is a bug in Kafka Streams. " +
                    "Please open a bug report under https://issues.apache.org/jira/projects/KAFKA/issues");
            }
            if (storeToValueProviders.size() == 1 && cache != valueProviders.cache) {
                singleCache = false;
            } else if (singleCache && cache != valueProviders.cache || !singleCache && cache == valueProviders.cache) {
                throw new IllegalStateException("Caches for store " + storeName + " of task " + taskId +
                    " are either not all distinct or do not all refer to the same cache. This is a bug in Kafka Streams. " +
                    "Please open a bug report under https://issues.apache.org/jira/projects/KAFKA/issues");
            }
        }
    }

    private void verifyConsistencyOfValueProvidersAcrossSegments(final String segmentName,
                                                                 final Object newValueProvider,
                                                                 final Object oldValueProvider,
                                                                 final String valueProviderName) {
        if (newValueProvider == null && oldValueProvider != null ||
            newValueProvider != null && oldValueProvider == null) {

            final char capitalizedFirstChar = valueProviderName.toUpperCase(Locale.US).charAt(0);
            final StringBuilder capitalizedValueProviderName = new StringBuilder(valueProviderName);
            capitalizedValueProviderName.setCharAt(0, capitalizedFirstChar);
            throw new IllegalStateException(capitalizedValueProviderName +
                " for segment " + segmentName + " of task " + taskId +
                " is" + (newValueProvider == null ? " " : " not ") + "null although the " + valueProviderName +
                " of another segment in this metrics recorder is" + (newValueProvider != null ? " " : " not ") + "null. " +
                "This is a bug in Kafka Streams. " +
                "Please open a bug report under https://issues.apache.org/jira/projects/KAFKA/issues");
        }
    }

    private void initSensors(final StreamsMetricsImpl streamsMetrics, final RocksDBMetricContext metricContext) {
        bytesWrittenToDatabaseSensor = RocksDBMetrics.bytesWrittenToDatabaseSensor(streamsMetrics, metricContext);
        bytesReadFromDatabaseSensor = RocksDBMetrics.bytesReadFromDatabaseSensor(streamsMetrics, metricContext);
        memtableBytesFlushedSensor = RocksDBMetrics.memtableBytesFlushedSensor(streamsMetrics, metricContext);
        memtableHitRatioSensor = RocksDBMetrics.memtableHitRatioSensor(streamsMetrics, metricContext);
        memtableAvgFlushTimeSensor = RocksDBMetrics.memtableAvgFlushTimeSensor(streamsMetrics, metricContext);
        memtableMinFlushTimeSensor = RocksDBMetrics.memtableMinFlushTimeSensor(streamsMetrics, metricContext);
        memtableMaxFlushTimeSensor = RocksDBMetrics.memtableMaxFlushTimeSensor(streamsMetrics, metricContext);
        writeStallDurationSensor = RocksDBMetrics.writeStallDurationSensor(streamsMetrics, metricContext);
        blockCacheDataHitRatioSensor = RocksDBMetrics.blockCacheDataHitRatioSensor(streamsMetrics, metricContext);
        blockCacheIndexHitRatioSensor = RocksDBMetrics.blockCacheIndexHitRatioSensor(streamsMetrics, metricContext);
        blockCacheFilterHitRatioSensor = RocksDBMetrics.blockCacheFilterHitRatioSensor(streamsMetrics, metricContext);
        bytesWrittenDuringCompactionSensor =
            RocksDBMetrics.bytesWrittenDuringCompactionSensor(streamsMetrics, metricContext);
        bytesReadDuringCompactionSensor = RocksDBMetrics.bytesReadDuringCompactionSensor(streamsMetrics, metricContext);
        compactionTimeAvgSensor = RocksDBMetrics.compactionTimeAvgSensor(streamsMetrics, metricContext);
        compactionTimeMinSensor = RocksDBMetrics.compactionTimeMinSensor(streamsMetrics, metricContext);
        compactionTimeMaxSensor = RocksDBMetrics.compactionTimeMaxSensor(streamsMetrics, metricContext);
        numberOfOpenFilesSensor = RocksDBMetrics.numberOfOpenFilesSensor(streamsMetrics, metricContext);
        numberOfFileErrorsSensor = RocksDBMetrics.numberOfFileErrorsSensor(streamsMetrics, metricContext);
    }

    private void initGauges(final StreamsMetricsImpl streamsMetrics,
                            final RocksDBMetricContext metricContext) {
        RocksDBMetrics.addNumImmutableMemTableMetric(
            streamsMetrics,
            metricContext,
            gaugeToComputeSumOfProperties(NUMBER_OF_IMMUTABLE_MEMTABLES)
        );
        RocksDBMetrics.addCurSizeActiveMemTable(
            streamsMetrics,
            metricContext,
            gaugeToComputeSumOfProperties(CURRENT_SIZE_OF_ACTIVE_MEMTABLE)
        );
        RocksDBMetrics.addCurSizeAllMemTables(
            streamsMetrics,
            metricContext,
            gaugeToComputeSumOfProperties(CURRENT_SIZE_OF_ALL_MEMTABLES)
        );
        RocksDBMetrics.addSizeAllMemTables(
            streamsMetrics,
            metricContext,
            gaugeToComputeSumOfProperties(SIZE_OF_ALL_MEMTABLES)
        );
        RocksDBMetrics.addNumEntriesActiveMemTableMetric(
            streamsMetrics,
            metricContext,
            gaugeToComputeSumOfProperties(NUMBER_OF_ENTRIES_ACTIVE_MEMTABLE)
        );
        RocksDBMetrics.addNumDeletesActiveMemTableMetric(
            streamsMetrics,
            metricContext,
            gaugeToComputeSumOfProperties(NUMBER_OF_DELETES_ACTIVE_MEMTABLE)
        );
        RocksDBMetrics.addNumEntriesImmMemTablesMetric(
            streamsMetrics,
            metricContext,
            gaugeToComputeSumOfProperties(NUMBER_OF_ENTRIES_IMMUTABLE_MEMTABLES)
        );
        RocksDBMetrics.addNumDeletesImmMemTablesMetric(
            streamsMetrics,
            metricContext,
            gaugeToComputeSumOfProperties(NUMBER_OF_DELETES_IMMUTABLE_MEMTABLES)
        );
        RocksDBMetrics.addMemTableFlushPending(
            streamsMetrics,
            metricContext,
            gaugeToComputeSumOfProperties(MEMTABLE_FLUSH_PENDING)
        );
        RocksDBMetrics.addNumRunningFlushesMetric(
            streamsMetrics,
            metricContext,
            gaugeToComputeSumOfProperties(NUMBER_OF_RUNNING_FLUSHES)
        );
        RocksDBMetrics.addCompactionPendingMetric(
            streamsMetrics,
            metricContext,
            gaugeToComputeSumOfProperties(COMPACTION_PENDING)
        );
        RocksDBMetrics.addNumRunningCompactionsMetric(
            streamsMetrics,
            metricContext,
            gaugeToComputeSumOfProperties(NUMBER_OF_RUNNING_COMPACTIONS)
        );
        RocksDBMetrics.addEstimatePendingCompactionBytesMetric(
            streamsMetrics,
            metricContext,
            gaugeToComputeSumOfProperties(ESTIMATED_BYTES_OF_PENDING_COMPACTION)
        );
        RocksDBMetrics.addTotalSstFilesSizeMetric(
            streamsMetrics,
            metricContext,
            gaugeToComputeSumOfProperties(TOTAL_SST_FILES_SIZE)
        );
        RocksDBMetrics.addLiveSstFilesSizeMetric(
            streamsMetrics,
            metricContext,
            gaugeToComputeSumOfProperties(LIVE_SST_FILES_SIZE)
        );
        RocksDBMetrics.addNumLiveVersionMetric(
            streamsMetrics,
            metricContext,
            gaugeToComputeSumOfProperties(NUMBER_OF_LIVE_VERSIONS)
        );
        RocksDBMetrics.addEstimateNumKeysMetric(
            streamsMetrics,
            metricContext,
            gaugeToComputeSumOfProperties(ESTIMATED_NUMBER_OF_KEYS)
        );
        RocksDBMetrics.addEstimateTableReadersMemMetric(
            streamsMetrics,
            metricContext,
            gaugeToComputeSumOfProperties(ESTIMATED_MEMORY_OF_TABLE_READERS)
        );
        RocksDBMetrics.addBackgroundErrorsMetric(
            streamsMetrics,
            metricContext,
            gaugeToComputeSumOfProperties(NUMBER_OF_BACKGROUND_ERRORS)
        );
        RocksDBMetrics.addBlockCacheCapacityMetric(
            streamsMetrics,
            metricContext,
            gaugeToComputeBlockCacheMetrics(CAPACITY_OF_BLOCK_CACHE)
        );
        RocksDBMetrics.addBlockCacheUsageMetric(
            streamsMetrics,
            metricContext,
            gaugeToComputeBlockCacheMetrics(USAGE_OF_BLOCK_CACHE)
        );
        RocksDBMetrics.addBlockCachePinnedUsageMetric(
            streamsMetrics,
            metricContext,
            gaugeToComputeBlockCacheMetrics(PINNED_USAGE_OF_BLOCK_CACHE)
        );
    }

    private Gauge<BigInteger> gaugeToComputeSumOfProperties(final String propertyName) {
        return (metricsConfig, now) -> {
            BigInteger result = BigInteger.valueOf(0);
            for (final DbAndCacheAndStatistics valueProvider : storeToValueProviders.values()) {
                try {
                    // values of RocksDB properties are of type unsigned long in C++, i.e., in Java we need to use
                    // BigInteger and construct the object from the byte representation of the value
                    result = result.add(new BigInteger(1, longToBytes(
                        valueProvider.db.getAggregatedLongProperty(ROCKSDB_PROPERTIES_PREFIX + propertyName)
                    )));
                } catch (final RocksDBException e) {
                    throw new ProcessorStateException("Error recording RocksDB metric " + propertyName, e);
                }
            }
            return result;
        };
    }

    private Gauge<BigInteger> gaugeToComputeBlockCacheMetrics(final String propertyName) {
        return (metricsConfig, now) -> {
            BigInteger result = BigInteger.valueOf(0);
            for (final DbAndCacheAndStatistics valueProvider : storeToValueProviders.values()) {
                try {
                    if (singleCache) {
                        // values of RocksDB properties are of type unsigned long in C++, i.e., in Java we need to use
                        // BigInteger and construct the object from the byte representation of the value
                        result = new BigInteger(1, longToBytes(
                            valueProvider.db.getLongProperty(ROCKSDB_PROPERTIES_PREFIX + propertyName)
                        ));
                        break;
                    } else {
                        // values of RocksDB properties are of type unsigned long in C++, i.e., in Java we need to use
                        // BigInteger and construct the object from the byte representation of the value
                        result = result.add(new BigInteger(1, longToBytes(
                            valueProvider.db.getLongProperty(ROCKSDB_PROPERTIES_PREFIX + propertyName)
                        )));
                    }
                } catch (final RocksDBException e) {
                    throw new ProcessorStateException("Error recording RocksDB metric " + propertyName, e);
                }
            }
            return result;
        };
    }

    private static byte[] longToBytes(final long data) {
        final ByteBuffer conversionBuffer = ByteBuffer.allocate(Long.BYTES);
        conversionBuffer.putLong(0, data);
        return conversionBuffer.array();
    }

    public void removeValueProviders(final String segmentName) {
        logger.debug("Removing value providers for store {} of task {}", segmentName, taskId);
        final DbAndCacheAndStatistics removedValueProviders = storeToValueProviders.remove(segmentName);
        if (removedValueProviders == null) {
            throw new IllegalStateException("No value providers for store \"" + segmentName + "\" of task " + taskId +
                " could be found. This is a bug in Kafka Streams. " +
                "Please open a bug report under https://issues.apache.org/jira/projects/KAFKA/issues");
        }
        if (storeToValueProviders.isEmpty()) {
            logger.debug(
                "Removing metrics recorder for store {} of task {} from metrics recording trigger",
                storeName,
                taskId
            );
            streamsMetrics.rocksDBMetricsRecordingTrigger().removeMetricsRecorder(this);
        }
    }

    public void record(final long now) {
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
        long memtableFlushTimeSum = 0;
        long memtableFlushTimeCount = 0;
        double memtableFlushTimeMin = Double.MAX_VALUE;
        double memtableFlushTimeMax = 0.0;
        long compactionTimeSum = 0;
        long compactionTimeCount = 0;
        double compactionTimeMin = Double.MAX_VALUE;
        double compactionTimeMax = 0.0;
        boolean shouldRecord = true;
        for (final DbAndCacheAndStatistics valueProviders : storeToValueProviders.values()) {
            if (valueProviders.statistics == null) {
                shouldRecord = false;
                break;
            }
            bytesWrittenToDatabase += valueProviders.statistics.getAndResetTickerCount(TickerType.BYTES_WRITTEN);
            bytesReadFromDatabase += valueProviders.statistics.getAndResetTickerCount(TickerType.BYTES_READ);
            memtableBytesFlushed += valueProviders.statistics.getAndResetTickerCount(TickerType.FLUSH_WRITE_BYTES);
            memtableHits += valueProviders.statistics.getAndResetTickerCount(TickerType.MEMTABLE_HIT);
            memtableMisses += valueProviders.statistics.getAndResetTickerCount(TickerType.MEMTABLE_MISS);
            blockCacheDataHits += valueProviders.statistics.getAndResetTickerCount(TickerType.BLOCK_CACHE_DATA_HIT);
            blockCacheDataMisses += valueProviders.statistics.getAndResetTickerCount(TickerType.BLOCK_CACHE_DATA_MISS);
            blockCacheIndexHits += valueProviders.statistics.getAndResetTickerCount(TickerType.BLOCK_CACHE_INDEX_HIT);
            blockCacheIndexMisses += valueProviders.statistics.getAndResetTickerCount(TickerType.BLOCK_CACHE_INDEX_MISS);
            blockCacheFilterHits += valueProviders.statistics.getAndResetTickerCount(TickerType.BLOCK_CACHE_FILTER_HIT);
            blockCacheFilterMisses += valueProviders.statistics.getAndResetTickerCount(TickerType.BLOCK_CACHE_FILTER_MISS);
            writeStallDuration += valueProviders.statistics.getAndResetTickerCount(TickerType.STALL_MICROS);
            bytesWrittenDuringCompaction += valueProviders.statistics.getAndResetTickerCount(TickerType.COMPACT_WRITE_BYTES);
            bytesReadDuringCompaction += valueProviders.statistics.getAndResetTickerCount(TickerType.COMPACT_READ_BYTES);
            numberOfOpenFiles += valueProviders.statistics.getAndResetTickerCount(TickerType.NO_FILE_OPENS)
                - valueProviders.statistics.getAndResetTickerCount(TickerType.NO_FILE_CLOSES);
            numberOfFileErrors += valueProviders.statistics.getAndResetTickerCount(TickerType.NO_FILE_ERRORS);
            final HistogramData memtableFlushTimeData = valueProviders.statistics.getHistogramData(HistogramType.FLUSH_TIME);
            memtableFlushTimeSum += memtableFlushTimeData.getSum();
            memtableFlushTimeCount += memtableFlushTimeData.getCount();
            memtableFlushTimeMin = Double.min(memtableFlushTimeMin, memtableFlushTimeData.getMin());
            memtableFlushTimeMax = Double.max(memtableFlushTimeMax, memtableFlushTimeData.getMax());
            final HistogramData compactionTimeData = valueProviders.statistics.getHistogramData(HistogramType.COMPACTION_TIME);
            compactionTimeSum += compactionTimeData.getSum();
            compactionTimeCount += compactionTimeData.getCount();
            compactionTimeMin = Double.min(compactionTimeMin, compactionTimeData.getMin());
            compactionTimeMax = Double.max(compactionTimeMax, compactionTimeData.getMax());
        }
        if (shouldRecord) {
            bytesWrittenToDatabaseSensor.record(bytesWrittenToDatabase, now);
            bytesReadFromDatabaseSensor.record(bytesReadFromDatabase, now);
            memtableBytesFlushedSensor.record(memtableBytesFlushed, now);
            memtableHitRatioSensor.record(computeHitRatio(memtableHits, memtableMisses), now);
            memtableAvgFlushTimeSensor.record(computeAvg(memtableFlushTimeSum, memtableFlushTimeCount), now);
            memtableMinFlushTimeSensor.record(memtableFlushTimeMin, now);
            memtableMaxFlushTimeSensor.record(memtableFlushTimeMax, now);
            blockCacheDataHitRatioSensor.record(computeHitRatio(blockCacheDataHits, blockCacheDataMisses), now);
            blockCacheIndexHitRatioSensor.record(computeHitRatio(blockCacheIndexHits, blockCacheIndexMisses), now);
            blockCacheFilterHitRatioSensor.record(computeHitRatio(blockCacheFilterHits, blockCacheFilterMisses), now);
            writeStallDurationSensor.record(writeStallDuration, now);
            bytesWrittenDuringCompactionSensor.record(bytesWrittenDuringCompaction, now);
            bytesReadDuringCompactionSensor.record(bytesReadDuringCompaction, now);
            compactionTimeAvgSensor.record(computeAvg(compactionTimeSum, compactionTimeCount), now);
            compactionTimeMinSensor.record(compactionTimeMin, now);
            compactionTimeMaxSensor.record(compactionTimeMax, now);
            numberOfOpenFilesSensor.record(numberOfOpenFiles, now);
            numberOfFileErrorsSensor.record(numberOfFileErrors, now);
        }
    }

    private double computeHitRatio(final long hits, final long misses) {
        if (hits == 0) {
            return 0;
        }
        return (double) hits / (hits + misses);
    }

    private double computeAvg(final long sum, final long count) {
        if (count == 0) {
            return 0;
        }
        return (double) sum / count;
    }
}