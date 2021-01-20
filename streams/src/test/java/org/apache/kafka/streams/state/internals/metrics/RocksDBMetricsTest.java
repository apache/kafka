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
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.RocksDBMetricContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.math.BigInteger;
import java.util.Collections;
import java.util.Map;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.powermock.api.easymock.PowerMock.createStrictMock;
import static org.powermock.api.easymock.PowerMock.mockStatic;
import static org.powermock.api.easymock.PowerMock.replay;
import static org.powermock.api.easymock.PowerMock.replayAll;
import static org.powermock.api.easymock.PowerMock.verify;
import static org.powermock.api.easymock.PowerMock.verifyAll;

@RunWith(PowerMockRunner.class)
@PrepareForTest(StreamsMetricsImpl.class)
public class RocksDBMetricsTest {

    private static final String STATE_LEVEL_GROUP = "stream-state-metrics";
    private static final String TASK_ID = "test-task";
    private static final String STORE_TYPE = "test-store-type";
    private static final String STORE_NAME = "store";
    private static final RocksDBMetricContext ROCKSDB_METRIC_CONTEXT =
        new RocksDBMetricContext(TASK_ID, STORE_TYPE, STORE_NAME);
    private static final Gauge<BigInteger> VALUE_PROVIDER = (config, now) -> BigInteger.valueOf(10);

    private final Metrics metrics = new Metrics();
    private final Sensor sensor = metrics.sensor("dummy");
    private final StreamsMetricsImpl streamsMetrics = createStrictMock(StreamsMetricsImpl.class);
    private final Map<String, String> tags = Collections.singletonMap("hello", "world");

    private interface SensorCreator {
        Sensor sensor(final StreamsMetricsImpl streamsMetrics, final RocksDBMetricContext metricContext);
    }

    @Test
    public void shouldGetBytesWrittenSensor() {
        final String metricNamePrefix = "bytes-written";
        final String descriptionOfTotal = "Total number of bytes written to the RocksDB state store";
        final String descriptionOfRate = "Average number of bytes written per second to the RocksDB state store";
        verifyRateAndTotalSensor(
            metricNamePrefix,
            descriptionOfTotal,
            descriptionOfRate,
            RocksDBMetrics::bytesWrittenToDatabaseSensor
        );
    }

    @Test
    public void shouldGetBytesReadSensor() {
        final String metricNamePrefix = "bytes-read";
        final String descriptionOfTotal = "Total number of bytes read from the RocksDB state store";
        final String descriptionOfRate = "Average number of bytes read per second from the RocksDB state store";
        verifyRateAndTotalSensor(
            metricNamePrefix,
            descriptionOfTotal,
            descriptionOfRate,
            RocksDBMetrics::bytesReadFromDatabaseSensor
        );
    }

    @Test
    public void shouldGetMemtableHitRatioSensor() {
        final String metricNamePrefix = "memtable-hit-ratio";
        final String description = "Ratio of memtable hits relative to all lookups to the memtable";
        verifyValueSensor(metricNamePrefix, description, RocksDBMetrics::memtableHitRatioSensor);
    }

    @Test
    public void shouldGetMemtableBytesFlushedSensor() {
        final String metricNamePrefix = "memtable-bytes-flushed";
        final String descriptionOfTotal = "Total number of bytes flushed from the memtable to disk";
        final String descriptionOfRate = "Average number of bytes flushed per second from the memtable to disk";
        verifyRateAndTotalSensor(
            metricNamePrefix,
            descriptionOfTotal,
            descriptionOfRate,
            RocksDBMetrics::memtableBytesFlushedSensor
        );
    }

    @Test
    public void shouldGetMemtableAvgFlushTimeSensor() {
        final String metricNamePrefix = "memtable-flush-time-avg";
        final String description = "Average time spent on flushing the memtable to disk in ms";
        verifyValueSensor(metricNamePrefix, description, RocksDBMetrics::memtableAvgFlushTimeSensor);
    }

    @Test
    public void shouldGetMemtableMinFlushTimeSensor() {
        final String metricNamePrefix = "memtable-flush-time-min";
        final String description = "Minimum time spent on flushing the memtable to disk in ms";
        verifyValueSensor(metricNamePrefix, description, RocksDBMetrics::memtableMinFlushTimeSensor);
    }

    @Test
    public void shouldGetMemtableMaxFlushTimeSensor() {
        final String metricNamePrefix = "memtable-flush-time-max";
        final String description = "Maximum time spent on flushing the memtable to disk in ms";
        verifyValueSensor(metricNamePrefix, description, RocksDBMetrics::memtableMaxFlushTimeSensor);
    }

    @Test
    public void shouldGetWriteStallDurationSensor() {
        final String metricNamePrefix = "write-stall-duration";
        final String descriptionOfAvg = "Average duration of write stalls in ms";
        final String descriptionOfTotal = "Total duration of write stalls in ms";
        setupStreamsMetricsMock(metricNamePrefix);
        StreamsMetricsImpl.addAvgAndSumMetricsToSensor(
            sensor,
            STATE_LEVEL_GROUP,
            tags,
            metricNamePrefix,
            descriptionOfAvg,
            descriptionOfTotal
        );

        replayCallAndVerify(RocksDBMetrics::writeStallDurationSensor);
    }

    @Test
    public void shouldGetBlockCacheDataHitRatioSensor() {
        final String metricNamePrefix = "block-cache-data-hit-ratio";
        final String description =
            "Ratio of block cache hits for data relative to all lookups for data to the block cache";
        verifyValueSensor(metricNamePrefix, description, RocksDBMetrics::blockCacheDataHitRatioSensor);
    }

    @Test
    public void shouldGetBlockCacheIndexHitRatioSensor() {
        final String metricNamePrefix = "block-cache-index-hit-ratio";
        final String description =
            "Ratio of block cache hits for indexes relative to all lookups for indexes to the block cache";
        verifyValueSensor(metricNamePrefix, description, RocksDBMetrics::blockCacheIndexHitRatioSensor);
    }

    @Test
    public void shouldGetBlockCacheFilterHitRatioSensor() {
        final String metricNamePrefix = "block-cache-filter-hit-ratio";
        final String description =
            "Ratio of block cache hits for filters relative to all lookups for filters to the block cache";
        verifyValueSensor(metricNamePrefix, description, RocksDBMetrics::blockCacheFilterHitRatioSensor);
    }

    @Test
    public void shouldGetBytesReadDuringCompactionSensor() {
        final String metricNamePrefix = "bytes-read-compaction";
        final String description = "Average number of bytes read per second during compaction";
        verifyRateSensor(metricNamePrefix, description, RocksDBMetrics::bytesReadDuringCompactionSensor);
    }

    @Test
    public void shouldGetBytesWrittenDuringCompactionSensor() {
        final String metricNamePrefix = "bytes-written-compaction";
        final String description = "Average number of bytes written per second during compaction";
        verifyRateSensor(metricNamePrefix, description, RocksDBMetrics::bytesWrittenDuringCompactionSensor);
    }

    @Test
    public void shouldGetCompactionTimeAvgSensor() {
        final String metricNamePrefix = "compaction-time-avg";
        final String description = "Average time spent on compaction in ms";
        verifyValueSensor(metricNamePrefix, description, RocksDBMetrics::compactionTimeAvgSensor);
    }

    @Test
    public void shouldGetCompactionTimeMinSensor() {
        final String metricNamePrefix = "compaction-time-min";
        final String description = "Minimum time spent on compaction in ms";
        verifyValueSensor(metricNamePrefix, description, RocksDBMetrics::compactionTimeMinSensor);
    }

    @Test
    public void shouldGetCompactionTimeMaxSensor() {
        final String metricNamePrefix = "compaction-time-max";
        final String description = "Maximum time spent on compaction in ms";
        verifyValueSensor(metricNamePrefix, description, RocksDBMetrics::compactionTimeMaxSensor);
    }

    @Test
    public void shouldGetNumberOfOpenFilesSensor() {
        final String metricNamePrefix = "number-open-files";
        final String description = "Number of currently open files";
        verifySumSensor(metricNamePrefix, false, description, RocksDBMetrics::numberOfOpenFilesSensor);
    }

    @Test
    public void shouldGetNumberOfFilesErrors() {
        final String metricNamePrefix = "number-file-errors";
        final String description = "Total number of file errors occurred";
        verifySumSensor(metricNamePrefix, true, description, RocksDBMetrics::numberOfFileErrorsSensor);
    }

    @Test
    public void shouldAddNumEntriesActiveMemTableMetric() {
        final String name = "num-entries-active-mem-table";
        final String description = "Total number of entries in the active memtable";
        runAndVerifyMutableMetric(
            name,
            description,
            () -> RocksDBMetrics.addNumEntriesActiveMemTableMetric(streamsMetrics, ROCKSDB_METRIC_CONTEXT, VALUE_PROVIDER)
        );
    }

    @Test
    public void shouldAddNumberDeletesActiveTableMetric() {
        final String name = "num-deletes-active-mem-table";
        final String description = "Total number of delete entries in the active memtable";
        runAndVerifyMutableMetric(
            name,
            description,
            () -> RocksDBMetrics.addNumDeletesActiveMemTableMetric(streamsMetrics, ROCKSDB_METRIC_CONTEXT, VALUE_PROVIDER)
        );
    }

    @Test
    public void shouldAddNumEntriesImmutableMemTablesMetric() {
        final String name = "num-entries-imm-mem-tables";
        final String description = "Total number of entries in the unflushed immutable memtables";
        runAndVerifyMutableMetric(
            name,
            description,
            () -> RocksDBMetrics.addNumEntriesImmMemTablesMetric(streamsMetrics, ROCKSDB_METRIC_CONTEXT, VALUE_PROVIDER)
        );
    }

    @Test
    public void shouldAddNumDeletesImmutableMemTablesMetric() {
        final String name = "num-deletes-imm-mem-tables";
        final String description = "Total number of delete entries in the unflushed immutable memtables";
        runAndVerifyMutableMetric(
            name,
            description,
            () -> RocksDBMetrics.addNumDeletesImmMemTablesMetric(streamsMetrics, ROCKSDB_METRIC_CONTEXT, VALUE_PROVIDER)
        );
    }

    @Test
    public void shouldAddNumImmutableMemTablesMetric() {
        final String name = "num-immutable-mem-table";
        final String description = "Number of immutable memtables that have not yet been flushed";
        runAndVerifyMutableMetric(
            name,
            description,
            () -> RocksDBMetrics.addNumImmutableMemTableMetric(streamsMetrics, ROCKSDB_METRIC_CONTEXT, VALUE_PROVIDER)
        );
    }

    @Test
    public void shouldAddCurSizeActiveMemTableMetric() {
        final String name = "cur-size-active-mem-table";
        final String description = "Approximate size of active memtable in bytes";
        runAndVerifyMutableMetric(
            name,
            description,
            () -> RocksDBMetrics.addCurSizeActiveMemTable(streamsMetrics, ROCKSDB_METRIC_CONTEXT, VALUE_PROVIDER)
        );
    }

    @Test
    public void shouldAddCurSizeAllMemTablesMetric() {
        final String name = "cur-size-all-mem-tables";
        final String description = "Approximate size of active and unflushed immutable memtables in bytes";
        runAndVerifyMutableMetric(
            name,
            description,
            () -> RocksDBMetrics.addCurSizeAllMemTables(streamsMetrics, ROCKSDB_METRIC_CONTEXT, VALUE_PROVIDER)
        );
    }

    @Test
    public void shouldAddSizeAllMemTablesMetric() {
        final String name = "size-all-mem-tables";
        final String description = "Approximate size of active, unflushed immutable, and pinned immutable memtables in bytes";
        runAndVerifyMutableMetric(
            name,
            description,
            () -> RocksDBMetrics.addSizeAllMemTables(streamsMetrics, ROCKSDB_METRIC_CONTEXT, VALUE_PROVIDER)
        );
    }

    @Test
    public void shouldAddMemTableFlushPendingMetric() {
        final String name = "mem-table-flush-pending";
        final String description = "Reports 1 if a memtable flush is pending, otherwise it reports 0";
        runAndVerifyMutableMetric(
            name,
            description,
            () -> RocksDBMetrics.addMemTableFlushPending(streamsMetrics, ROCKSDB_METRIC_CONTEXT, VALUE_PROVIDER)
        );
    }

    @Test
    public void shouldAddNumRunningFlushesMetric() {
        final String name = "num-running-flushes";
        final String description = "Number of currently running flushes";
        runAndVerifyMutableMetric(
            name,
            description,
            () -> RocksDBMetrics.addNumRunningFlushesMetric(streamsMetrics, ROCKSDB_METRIC_CONTEXT, VALUE_PROVIDER)
        );
    }

    @Test
    public void shouldAddCompactionPendingMetric() {
        final String name = "compaction-pending";
        final String description = "Reports 1 if at least one compaction is pending, otherwise it reports 0";
        runAndVerifyMutableMetric(
            name,
            description,
            () -> RocksDBMetrics.addCompactionPendingMetric(streamsMetrics, ROCKSDB_METRIC_CONTEXT, VALUE_PROVIDER)
        );
    }

    @Test
    public void shouldAddNumRunningCompactionsMetric() {
        final String name = "num-running-compactions";
        final String description = "Number of currently running compactions";
        runAndVerifyMutableMetric(
            name,
            description,
            () -> RocksDBMetrics.addNumRunningCompactionsMetric(streamsMetrics, ROCKSDB_METRIC_CONTEXT, VALUE_PROVIDER)
        );
    }

    @Test
    public void shouldAddEstimatePendingCompactionBytesMetric() {
        final String name = "estimate-pending-compaction-bytes";
        final String description =
            "Estimated total number of bytes a compaction needs to rewrite on disk to get all levels down to under target size";
        runAndVerifyMutableMetric(
            name,
            description,
            () -> RocksDBMetrics.addEstimatePendingCompactionBytesMetric(streamsMetrics, ROCKSDB_METRIC_CONTEXT, VALUE_PROVIDER)
        );
    }

    @Test
    public void shouldAddTotalSstFilesSizeMetric() {
        final String name = "total-sst-files-size";
        final String description = "Total size in bytes of all SST files";
        runAndVerifyMutableMetric(
            name,
            description,
            () -> RocksDBMetrics.addTotalSstFilesSizeMetric(streamsMetrics, ROCKSDB_METRIC_CONTEXT, VALUE_PROVIDER)
        );
    }

    @Test
    public void shouldAddLiveSstFilesSizeMetric() {
        final String name = "live-sst-files-size";
        final String description = "Total size in bytes of all SST files that belong to the latest LSM tree";
        runAndVerifyMutableMetric(
            name,
            description,
            () -> RocksDBMetrics.addLiveSstFilesSizeMetric(streamsMetrics, ROCKSDB_METRIC_CONTEXT, VALUE_PROVIDER)
        );
    }

    @Test
    public void shouldAddNumLiveVersionMetric() {
        final String name = "num-live-versions";
        final String description = "Number of live versions of the LSM tree";
        runAndVerifyMutableMetric(
            name,
            description,
            () -> RocksDBMetrics.addNumLiveVersionMetric(streamsMetrics, ROCKSDB_METRIC_CONTEXT, VALUE_PROVIDER)
        );
    }

    @Test
    public void shouldAddBlockCacheCapacityMetric() {
        final String name = "block-cache-capacity";
        final String description = "Capacity of the block cache in bytes";
        runAndVerifyMutableMetric(
            name,
            description,
            () -> RocksDBMetrics.addBlockCacheCapacityMetric(streamsMetrics, ROCKSDB_METRIC_CONTEXT, VALUE_PROVIDER)
        );
    }

    @Test
    public void shouldAddBlockCacheUsageMetric() {
        final String name = "block-cache-usage";
        final String description = "Memory size of the entries residing in block cache in bytes";
        runAndVerifyMutableMetric(
            name,
            description,
            () -> RocksDBMetrics.addBlockCacheUsageMetric(streamsMetrics, ROCKSDB_METRIC_CONTEXT, VALUE_PROVIDER)
        );
    }

    @Test
    public void shouldAddBlockCachePinnedUsageMetric() {
        final String name = "block-cache-pinned-usage";
        final String description = "Memory size for the entries being pinned in the block cache in bytes";
        runAndVerifyMutableMetric(
            name,
            description,
            () -> RocksDBMetrics.addBlockCachePinnedUsageMetric(streamsMetrics, ROCKSDB_METRIC_CONTEXT, VALUE_PROVIDER)
        );
    }

    @Test
    public void shouldAddEstimateNumKeysMetric() {
        final String name = "estimate-num-keys";
        final String description =
            "Estimated number of keys in the active and unflushed immutable memtables and storage";
        runAndVerifyMutableMetric(
            name,
            description,
            () -> RocksDBMetrics.addEstimateNumKeysMetric(streamsMetrics, ROCKSDB_METRIC_CONTEXT, VALUE_PROVIDER)
        );
    }

    @Test
    public void shouldAddEstimateTableReadersMemMetric() {
        final String name = "estimate-table-readers-mem";
        final String description =
            "Estimated memory in bytes used for reading SST tables, excluding memory used in block cache";
        runAndVerifyMutableMetric(
            name,
            description,
            () -> RocksDBMetrics.addEstimateTableReadersMemMetric(streamsMetrics, ROCKSDB_METRIC_CONTEXT, VALUE_PROVIDER)
        );
    }

    @Test
    public void shouldAddBackgroundErrorsMetric() {
        final String name = "background-errors";
        final String description = "Total number of background errors";
        runAndVerifyMutableMetric(
            name,
            description,
            () -> RocksDBMetrics.addBackgroundErrorsMetric(streamsMetrics, ROCKSDB_METRIC_CONTEXT, VALUE_PROVIDER)
        );
    }

    private void runAndVerifyMutableMetric(final String name, final String description, final Runnable metricAdder) {
        streamsMetrics.addStoreLevelMutableMetric(
            eq(TASK_ID),
            eq(STORE_TYPE),
            eq(STORE_NAME),
            eq(name),
            eq(description),
            eq(RecordingLevel.INFO),
            eq(VALUE_PROVIDER)
        );
        replay(streamsMetrics);

        metricAdder.run();

        verify(streamsMetrics);
    }

    private void verifyRateAndTotalSensor(final String metricNamePrefix,
                                          final String descriptionOfTotal,
                                          final String descriptionOfRate,
                                          final SensorCreator sensorCreator) {
        setupStreamsMetricsMock(metricNamePrefix);
        StreamsMetricsImpl.addRateOfSumAndSumMetricsToSensor(
            sensor,
            STATE_LEVEL_GROUP,
            tags,
            metricNamePrefix,
            descriptionOfRate,
            descriptionOfTotal
        );

        replayCallAndVerify(sensorCreator);
    }

    private void verifyRateSensor(final String metricNamePrefix,
                                  final String description,
                                  final SensorCreator sensorCreator) {
        setupStreamsMetricsMock(metricNamePrefix);
        StreamsMetricsImpl.addRateOfSumMetricToSensor(sensor, STATE_LEVEL_GROUP, tags, metricNamePrefix, description);

        replayCallAndVerify(sensorCreator);
    }

    private void verifyValueSensor(final String metricNamePrefix,
                                   final String description,
                                   final SensorCreator sensorCreator) {
        setupStreamsMetricsMock(metricNamePrefix);
        StreamsMetricsImpl.addValueMetricToSensor(sensor, STATE_LEVEL_GROUP, tags, metricNamePrefix, description);

        replayCallAndVerify(sensorCreator);
    }

    private void verifySumSensor(final String metricNamePrefix,
                                 final boolean withSuffix,
                                 final String description,
                                 final SensorCreator sensorCreator) {
        setupStreamsMetricsMock(metricNamePrefix);
        if (withSuffix) {
            StreamsMetricsImpl.addSumMetricToSensor(sensor, STATE_LEVEL_GROUP, tags, metricNamePrefix, description);
        } else {
            StreamsMetricsImpl
                .addSumMetricToSensor(sensor, STATE_LEVEL_GROUP, tags, metricNamePrefix, withSuffix, description);
        }

        replayCallAndVerify(sensorCreator);
    }

    private void setupStreamsMetricsMock(final String metricNamePrefix) {
        mockStatic(StreamsMetricsImpl.class);
        expect(streamsMetrics.storeLevelSensor(
            TASK_ID,
            STORE_NAME,
            metricNamePrefix,
            RecordingLevel.DEBUG
        )).andReturn(sensor);
        expect(streamsMetrics.storeLevelTagMap(
            TASK_ID,
            STORE_TYPE,
            STORE_NAME
        )).andReturn(tags);
    }

    private void replayCallAndVerify(final SensorCreator sensorCreator) {
        replayAll();
        replay(StreamsMetricsImpl.class);

        final Sensor sensor = sensorCreator.sensor(streamsMetrics, ROCKSDB_METRIC_CONTEXT);

        verifyAll();
        verify(StreamsMetricsImpl.class);

        assertThat(sensor, is(this.sensor));
    }
}
