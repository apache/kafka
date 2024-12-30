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

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.RocksDBMetricContext;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.rocksdb.Cache;
import org.rocksdb.HistogramData;
import org.rocksdb.HistogramType;
import org.rocksdb.RocksDB;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;
import org.rocksdb.TickerType;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class RocksDBMetricsRecorderTest {
    private static final String METRICS_SCOPE = "metrics-scope";
    private static final TaskId TASK_ID1 = new TaskId(0, 0);
    private static final TaskId TASK_ID2 = new TaskId(0, 1);
    private static final String STORE_NAME = "store-name";
    private static final String SEGMENT_STORE_NAME_1 = "segment-store-name-1";
    private static final String SEGMENT_STORE_NAME_2 = "segment-store-name-2";
    private static final String SEGMENT_STORE_NAME_3 = "segment-store-name-3";

    private final RocksDB dbToAdd1 = mock(RocksDB.class);
    private final RocksDB dbToAdd2 = mock(RocksDB.class);
    private final RocksDB dbToAdd3 = mock(RocksDB.class);
    private final Cache cacheToAdd1 = mock(Cache.class);
    private final Cache cacheToAdd2 = mock(Cache.class);
    private final Statistics statisticsToAdd1 = mock(Statistics.class);
    private final Statistics statisticsToAdd2 = mock(Statistics.class);
    private final Statistics statisticsToAdd3 = mock(Statistics.class);

    private final Sensor bytesWrittenToDatabaseSensor = mock(Sensor.class);
    private final Sensor bytesReadFromDatabaseSensor = mock(Sensor.class);
    private final Sensor memtableBytesFlushedSensor = mock(Sensor.class);
    private final Sensor memtableHitRatioSensor = mock(Sensor.class);
    private final Sensor memtableAvgFlushTimeSensor = mock(Sensor.class);
    private final Sensor memtableMinFlushTimeSensor = mock(Sensor.class);
    private final Sensor memtableMaxFlushTimeSensor = mock(Sensor.class);
    private final Sensor writeStallDurationSensor = mock(Sensor.class);
    private final Sensor blockCacheDataHitRatioSensor = mock(Sensor.class);
    private final Sensor blockCacheIndexHitRatioSensor = mock(Sensor.class);
    private final Sensor blockCacheFilterHitRatioSensor = mock(Sensor.class);
    private final Sensor bytesReadDuringCompactionSensor = mock(Sensor.class);
    private final Sensor bytesWrittenDuringCompactionSensor = mock(Sensor.class);
    private final Sensor compactionTimeAvgSensor = mock(Sensor.class);
    private final Sensor compactionTimeMinSensor = mock(Sensor.class);
    private final Sensor compactionTimeMaxSensor = mock(Sensor.class);
    private final Sensor numberOfOpenFilesSensor = mock(Sensor.class);
    private final Sensor numberOfFileErrorsSensor = mock(Sensor.class);

    private final StreamsMetricsImpl streamsMetrics = mock(StreamsMetricsImpl.class);
    private final RocksDBMetricsRecordingTrigger recordingTrigger = mock(RocksDBMetricsRecordingTrigger.class);

    private final RocksDBMetricsRecorder recorder = new RocksDBMetricsRecorder(METRICS_SCOPE, STORE_NAME);
    private final RocksDBMetricContext metricsContext = new RocksDBMetricContext(TASK_ID1.toString(), METRICS_SCOPE, STORE_NAME);

    private MockedStatic<RocksDBMetrics> dbMetrics;

    @BeforeEach
    public void setUp() {
        setUpMetricsMock();
        when(streamsMetrics.rocksDBMetricsRecordingTrigger()).thenReturn(recordingTrigger);
        recorder.init(streamsMetrics, TASK_ID1);
    }

    @AfterEach
    public void cleanUpMocks() {
        dbMetrics.close();
    }

    @AfterAll
    public static void cleanUpMockito() {
        Mockito.framework().clearInlineMocks();
    }

    @Test
    public void shouldInitMetricsRecorder() {
        dbMetrics.verify(() -> RocksDBMetrics.bytesWrittenToDatabaseSensor(any(), any()));
        dbMetrics.verify(() -> RocksDBMetrics.bytesReadFromDatabaseSensor(any(), any()));
        dbMetrics.verify(() -> RocksDBMetrics.memtableBytesFlushedSensor(any(), any()));
        dbMetrics.verify(() -> RocksDBMetrics.memtableHitRatioSensor(any(), any()));
        dbMetrics.verify(() -> RocksDBMetrics.memtableAvgFlushTimeSensor(any(), any()));
        dbMetrics.verify(() -> RocksDBMetrics.memtableMinFlushTimeSensor(any(), any()));
        dbMetrics.verify(() -> RocksDBMetrics.memtableMaxFlushTimeSensor(any(), any()));
        dbMetrics.verify(() -> RocksDBMetrics.writeStallDurationSensor(any(), any()));
        dbMetrics.verify(() -> RocksDBMetrics.blockCacheDataHitRatioSensor(any(), any()));
        dbMetrics.verify(() -> RocksDBMetrics.blockCacheIndexHitRatioSensor(any(), any()));
        dbMetrics.verify(() -> RocksDBMetrics.blockCacheFilterHitRatioSensor(any(), any()));
        dbMetrics.verify(() -> RocksDBMetrics.bytesWrittenDuringCompactionSensor(any(), any()));
        dbMetrics.verify(() -> RocksDBMetrics.bytesReadDuringCompactionSensor(any(), any()));
        dbMetrics.verify(() -> RocksDBMetrics.compactionTimeAvgSensor(any(), any()));
        dbMetrics.verify(() -> RocksDBMetrics.compactionTimeMinSensor(any(), any()));
        dbMetrics.verify(() -> RocksDBMetrics.compactionTimeMaxSensor(any(), any()));
        dbMetrics.verify(() -> RocksDBMetrics.numberOfOpenFilesSensor(any(), any()));
        dbMetrics.verify(() -> RocksDBMetrics.numberOfFileErrorsSensor(any(), any()));
        dbMetrics.verify(() -> RocksDBMetrics.addNumImmutableMemTableMetric(eq(streamsMetrics), eq(metricsContext), any()));
        dbMetrics.verify(() -> RocksDBMetrics.addCurSizeActiveMemTable(eq(streamsMetrics), eq(metricsContext), any()));
        dbMetrics.verify(() -> RocksDBMetrics.addCurSizeAllMemTables(eq(streamsMetrics), eq(metricsContext), any()));
        dbMetrics.verify(() -> RocksDBMetrics.addSizeAllMemTables(eq(streamsMetrics), eq(metricsContext), any()));
        dbMetrics.verify(() -> RocksDBMetrics.addNumEntriesActiveMemTableMetric(eq(streamsMetrics), eq(metricsContext), any()));
        dbMetrics.verify(() -> RocksDBMetrics.addNumEntriesImmMemTablesMetric(eq(streamsMetrics), eq(metricsContext), any()));
        dbMetrics.verify(() -> RocksDBMetrics.addNumDeletesActiveMemTableMetric(eq(streamsMetrics), eq(metricsContext), any()));
        dbMetrics.verify(() -> RocksDBMetrics.addNumDeletesImmMemTablesMetric(eq(streamsMetrics), eq(metricsContext), any()));
        dbMetrics.verify(() -> RocksDBMetrics.addMemTableFlushPending(eq(streamsMetrics), eq(metricsContext), any()));
        dbMetrics.verify(() -> RocksDBMetrics.addNumRunningFlushesMetric(eq(streamsMetrics), eq(metricsContext), any()));
        dbMetrics.verify(() -> RocksDBMetrics.addCompactionPendingMetric(eq(streamsMetrics), eq(metricsContext), any()));
        dbMetrics.verify(() -> RocksDBMetrics.addNumRunningCompactionsMetric(eq(streamsMetrics), eq(metricsContext), any()));
        dbMetrics.verify(() -> RocksDBMetrics.addEstimatePendingCompactionBytesMetric(eq(streamsMetrics), eq(metricsContext), any()));
        dbMetrics.verify(() -> RocksDBMetrics.addTotalSstFilesSizeMetric(eq(streamsMetrics), eq(metricsContext), any()));
        dbMetrics.verify(() -> RocksDBMetrics.addLiveSstFilesSizeMetric(eq(streamsMetrics), eq(metricsContext), any()));
        dbMetrics.verify(() -> RocksDBMetrics.addNumLiveVersionMetric(eq(streamsMetrics), eq(metricsContext), any()));
        dbMetrics.verify(() -> RocksDBMetrics.addBlockCacheCapacityMetric(eq(streamsMetrics), eq(metricsContext), any()));
        dbMetrics.verify(() -> RocksDBMetrics.addBlockCacheUsageMetric(eq(streamsMetrics), eq(metricsContext), any()));
        dbMetrics.verify(() -> RocksDBMetrics.addBlockCachePinnedUsageMetric(eq(streamsMetrics), eq(metricsContext), any()));
        dbMetrics.verify(() -> RocksDBMetrics.addEstimateNumKeysMetric(eq(streamsMetrics), eq(metricsContext), any()));
        dbMetrics.verify(() -> RocksDBMetrics.addEstimateTableReadersMemMetric(eq(streamsMetrics), eq(metricsContext), any()));
        dbMetrics.verify(() -> RocksDBMetrics.addBackgroundErrorsMetric(eq(streamsMetrics), eq(metricsContext), any()));
        assertThat(recorder.taskId(), is(TASK_ID1));
    }

    @Test
    public void shouldThrowIfMetricRecorderIsReInitialisedWithDifferentTask() {
        assertThrows(
            IllegalStateException.class,
            () -> recorder.init(streamsMetrics, TASK_ID2)
        );
    }

    @Test
    public void shouldThrowIfMetricRecorderIsReInitialisedWithDifferentStreamsMetrics() {
        assertThrows(
            IllegalStateException.class,
            () -> recorder.init(
                new StreamsMetricsImpl(new Metrics(), "test-client", "processId", new MockTime()),
                TASK_ID1
            )
        );
    }

    @Test
    public void shouldSetStatsLevelToExceptDetailedTimersWhenValueProvidersWithStatisticsAreAdded() {
        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, statisticsToAdd1);
        verify(statisticsToAdd1).setStatsLevel(StatsLevel.EXCEPT_DETAILED_TIMERS);
    }

    @Test
    public void shouldNotSetStatsLevelToExceptDetailedTimersWhenValueProvidersWithoutStatisticsAreAdded() {
        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, null);
        verifyNoInteractions(statisticsToAdd1);
    }

    @Test
    public void shouldThrowIfValueProvidersForASegmentHasBeenAlreadyAdded() {
        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, statisticsToAdd1);

        final Throwable exception = assertThrows(
            IllegalStateException.class,
            () -> recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, statisticsToAdd2)
        );
        assertThat(
            exception.getMessage(),
            is("Value providers for store " + SEGMENT_STORE_NAME_1 + " of task " + TASK_ID1 +
                " has been already added. This is a bug in Kafka Streams. " +
                "Please open a bug report under https://issues.apache.org/jira/projects/KAFKA/issues")
        );
    }

    @Test
    public void shouldThrowIfStatisticsToAddIsNotNullButExistingStatisticsAreNull() {
        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, null);

        final Throwable exception = assertThrows(
            IllegalStateException.class,
            () -> recorder.addValueProviders(SEGMENT_STORE_NAME_2, dbToAdd2, cacheToAdd2, statisticsToAdd2)
        );
        assertThat(
            exception.getMessage(),
            is("Statistics for segment " + SEGMENT_STORE_NAME_2 + " of task " + TASK_ID1 +
                " is not null although the statistics of another segment in this metrics recorder is null. " +
                "This is a bug in Kafka Streams. " +
                "Please open a bug report under https://issues.apache.org/jira/projects/KAFKA/issues")
        );
    }

    @Test
    public void shouldThrowIfStatisticsToAddIsNullButExistingStatisticsAreNotNull() {
        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, statisticsToAdd1);

        final Throwable exception = assertThrows(
            IllegalStateException.class,
            () -> recorder.addValueProviders(SEGMENT_STORE_NAME_2, dbToAdd2, cacheToAdd2, null)
        );
        assertThat(
            exception.getMessage(),
            is("Statistics for segment " + SEGMENT_STORE_NAME_2 + " of task " + TASK_ID1 +
                " is null although the statistics of another segment in this metrics recorder is not null. " +
                "This is a bug in Kafka Streams. " +
                "Please open a bug report under https://issues.apache.org/jira/projects/KAFKA/issues")
        );
    }

    @Test
    public void shouldThrowIfCacheToAddIsNullButExistingCacheIsNotNull() {
        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, null, statisticsToAdd1);

        final Throwable exception = assertThrows(
            IllegalStateException.class,
            () -> recorder.addValueProviders(SEGMENT_STORE_NAME_2, dbToAdd2, cacheToAdd1, statisticsToAdd1)
        );
        assertThat(
            exception.getMessage(),
            is("Cache for segment " + SEGMENT_STORE_NAME_2 + " of task " + TASK_ID1 +
                " is not null although the cache of another segment in this metrics recorder is null. " +
                "This is a bug in Kafka Streams. " +
                "Please open a bug report under https://issues.apache.org/jira/projects/KAFKA/issues")
        );
    }

    @Test
    public void shouldThrowIfCacheToAddIsNotNullButExistingCacheIsNull() {
        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, statisticsToAdd1);

        final Throwable exception = assertThrows(
            IllegalStateException.class,
            () -> recorder.addValueProviders(SEGMENT_STORE_NAME_2, dbToAdd2, null, statisticsToAdd2)
        );
        assertThat(
            exception.getMessage(),
            is("Cache for segment " + SEGMENT_STORE_NAME_2 + " of task " + TASK_ID1 +
                " is null although the cache of another segment in this metrics recorder is not null. " +
                "This is a bug in Kafka Streams. " +
                "Please open a bug report under https://issues.apache.org/jira/projects/KAFKA/issues")
        );
    }

    @Test
    public void shouldThrowIfCacheToAddIsNotSameAsAllExistingCaches() {
        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, statisticsToAdd1);
        recorder.addValueProviders(SEGMENT_STORE_NAME_2, dbToAdd2, cacheToAdd1, statisticsToAdd2);

        final Throwable exception = assertThrows(
            IllegalStateException.class,
            () -> recorder.addValueProviders(SEGMENT_STORE_NAME_3, dbToAdd3, cacheToAdd2, statisticsToAdd3)
        );
        assertThat(
            exception.getMessage(),
            is("Caches for store " + STORE_NAME + " of task " + TASK_ID1 +
                " are either not all distinct or do not all refer to the same cache. This is a bug in Kafka Streams. " +
                "Please open a bug report under https://issues.apache.org/jira/projects/KAFKA/issues")
        );
    }

    @Test
    public void shouldThrowIfCacheToAddIsSameAsOnlyOneOfMultipleCaches() {
        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, statisticsToAdd1);
        recorder.addValueProviders(SEGMENT_STORE_NAME_2, dbToAdd2, cacheToAdd2, statisticsToAdd2);

        final Throwable exception = assertThrows(
            IllegalStateException.class,
            () -> recorder.addValueProviders(SEGMENT_STORE_NAME_3, dbToAdd3, cacheToAdd1, statisticsToAdd3)
        );
        assertThat(
            exception.getMessage(),
            is("Caches for store " + STORE_NAME + " of task " + TASK_ID1 +
                " are either not all distinct or do not all refer to the same cache. This is a bug in Kafka Streams. " +
                "Please open a bug report under https://issues.apache.org/jira/projects/KAFKA/issues")
        );
    }

    @Test
    public void shouldThrowIfDbToAddWasAlreadyAddedForOtherSegment() {
        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, statisticsToAdd1);

        final Throwable exception = assertThrows(
            IllegalStateException.class,
            () -> recorder.addValueProviders(SEGMENT_STORE_NAME_2, dbToAdd1, cacheToAdd2, statisticsToAdd2)
        );
        assertThat(
            exception.getMessage(),
            is("DB instance for store " + SEGMENT_STORE_NAME_2 + " of task " + TASK_ID1 +
                " was already added for another segment as a value provider. This is a bug in Kafka Streams. " +
                "Please open a bug report under https://issues.apache.org/jira/projects/KAFKA/issues")
        );
    }

    @Test
    public void shouldAddItselfToRecordingTriggerWhenFirstValueProvidersAreAddedToNewlyCreatedRecorder() {
        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, statisticsToAdd1);

        verify(recordingTrigger).addMetricsRecorder(recorder);
    }

    @Test
    public void shouldAddItselfToRecordingTriggerWhenFirstValueProvidersAreAddedAfterLastValueProvidersWereRemoved() {
        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, statisticsToAdd1);
        recorder.removeValueProviders(SEGMENT_STORE_NAME_1);
        recorder.addValueProviders(SEGMENT_STORE_NAME_2, dbToAdd2, cacheToAdd2, statisticsToAdd2);
        verify(recordingTrigger, times(2)).addMetricsRecorder(recorder);
    }

    @Test
    public void shouldNotAddItselfToRecordingTriggerWhenNotEmpty() {
        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, statisticsToAdd1);

        verify(recordingTrigger).addMetricsRecorder(recorder);

        recorder.addValueProviders(SEGMENT_STORE_NAME_2, dbToAdd2, cacheToAdd2, statisticsToAdd2);

        verifyNoMoreInteractions(recordingTrigger);
    }

    @Test
    public void shouldNotRemoveItselfFromRecordingTriggerWhenAtLeastOneValueProviderIsPresent() {
        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, statisticsToAdd1);
        recorder.addValueProviders(SEGMENT_STORE_NAME_2, dbToAdd2, cacheToAdd2, statisticsToAdd2);

        recorder.removeValueProviders(SEGMENT_STORE_NAME_1);

        verify(recordingTrigger, never()).removeMetricsRecorder(recorder);
    }

    @Test
    public void shouldRemoveItselfFromRecordingTriggerWhenAllValueProvidersAreRemoved() {
        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, statisticsToAdd1);
        recorder.addValueProviders(SEGMENT_STORE_NAME_2, dbToAdd2, cacheToAdd2, statisticsToAdd2);

        recorder.removeValueProviders(SEGMENT_STORE_NAME_1);
        recorder.removeValueProviders(SEGMENT_STORE_NAME_2);

        verify(recordingTrigger).removeMetricsRecorder(recorder);
    }

    @Test
    public void shouldThrowIfValueProvidersToRemoveNotFound() {
        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, statisticsToAdd1);

        assertThrows(
            IllegalStateException.class,
            () -> recorder.removeValueProviders(SEGMENT_STORE_NAME_2)
        );
    }

    @Test
    public void shouldRecordStatisticsBasedMetrics() {
        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, statisticsToAdd1);
        recorder.addValueProviders(SEGMENT_STORE_NAME_2, dbToAdd2, cacheToAdd2, statisticsToAdd2);
        final long now = 0L;

        when(statisticsToAdd1.getAndResetTickerCount(TickerType.BYTES_WRITTEN)).thenReturn(1L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.BYTES_WRITTEN)).thenReturn(2L);
        final double expectedBytesWrittenToDatabaseSensor = 1 + 2;

        when(statisticsToAdd1.getAndResetTickerCount(TickerType.BYTES_READ)).thenReturn(2L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.BYTES_READ)).thenReturn(3L);
        final double expectedBytesReadFromDatabaseSensor = 2 + 3;

        when(statisticsToAdd1.getAndResetTickerCount(TickerType.FLUSH_WRITE_BYTES)).thenReturn(3L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.FLUSH_WRITE_BYTES)).thenReturn(4L);
        final double expectedMemtableBytesFlushedSensor = 3 + 4;

        when(statisticsToAdd1.getAndResetTickerCount(TickerType.MEMTABLE_HIT)).thenReturn(1L);
        when(statisticsToAdd1.getAndResetTickerCount(TickerType.MEMTABLE_MISS)).thenReturn(2L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.MEMTABLE_HIT)).thenReturn(3L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.MEMTABLE_MISS)).thenReturn(4L);
        final double expectedMemtableHitRatioSensorRecord = (double) 4 / (4 + 6);

        final HistogramData memtableFlushTimeData1 = new HistogramData(0.0, 0.0, 0.0, 0.0, 0.0, 16.0, 2L, 10L, 3.0);
        final HistogramData memtableFlushTimeData2 = new HistogramData(0.0, 0.0, 0.0, 0.0, 0.0, 20.0, 4L, 8L, 10.0);
        when(statisticsToAdd1.getHistogramData(HistogramType.FLUSH_TIME)).thenReturn(memtableFlushTimeData1);
        when(statisticsToAdd2.getHistogramData(HistogramType.FLUSH_TIME)).thenReturn(memtableFlushTimeData2);
        final double expectedMemtableAvgFlushTimeSensor = (double) (10 + 8) / (2 + 4);
        final double expectedMemtableMinFlushTimeSensor = 3.0d;
        final double expectedMemtableMaxFlushTimeSensor = 20.0d;

        when(statisticsToAdd1.getAndResetTickerCount(TickerType.STALL_MICROS)).thenReturn(4L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.STALL_MICROS)).thenReturn(5L);
        final double expectedWriteStallDurationSensor = 4 + 5;

        when(statisticsToAdd1.getAndResetTickerCount(TickerType.BLOCK_CACHE_DATA_HIT)).thenReturn(5L);
        when(statisticsToAdd1.getAndResetTickerCount(TickerType.BLOCK_CACHE_DATA_MISS)).thenReturn(4L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.BLOCK_CACHE_DATA_HIT)).thenReturn(3L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.BLOCK_CACHE_DATA_MISS)).thenReturn(2L);
        final double expectedBlockCacheDataHitRatioSensor = (double) 8 / (8 + 6);

        when(statisticsToAdd1.getAndResetTickerCount(TickerType.BLOCK_CACHE_INDEX_HIT)).thenReturn(4L);
        when(statisticsToAdd1.getAndResetTickerCount(TickerType.BLOCK_CACHE_INDEX_MISS)).thenReturn(2L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.BLOCK_CACHE_INDEX_HIT)).thenReturn(2L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.BLOCK_CACHE_INDEX_MISS)).thenReturn(4L);
        final double expectedBlockCacheIndexHitRatioSensor = (double) 6 / (6 + 6);

        when(statisticsToAdd1.getAndResetTickerCount(TickerType.BLOCK_CACHE_FILTER_HIT)).thenReturn(2L);
        when(statisticsToAdd1.getAndResetTickerCount(TickerType.BLOCK_CACHE_FILTER_MISS)).thenReturn(4L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.BLOCK_CACHE_FILTER_HIT)).thenReturn(3L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.BLOCK_CACHE_FILTER_MISS)).thenReturn(5L);
        final double expectedBlockCacheFilterHitRatioSensor = (double) 5 / (5 + 9);

        when(statisticsToAdd1.getAndResetTickerCount(TickerType.COMPACT_WRITE_BYTES)).thenReturn(2L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.COMPACT_WRITE_BYTES)).thenReturn(4L);
        final double expectedBytesWrittenDuringCompactionSensor = 2 + 4;

        when(statisticsToAdd1.getAndResetTickerCount(TickerType.COMPACT_READ_BYTES)).thenReturn(5L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.COMPACT_READ_BYTES)).thenReturn(6L);
        final double expectedBytesReadDuringCompactionSensor = 5 + 6;

        final HistogramData compactionTimeData1 = new HistogramData(0.0, 0.0, 0.0, 0.0, 0.0, 16.0, 2L, 8L, 6.0);
        final HistogramData compactionTimeData2 = new HistogramData(0.0, 0.0, 0.0, 0.0, 0.0, 24.0, 2L, 8L, 4.0);
        when(statisticsToAdd1.getHistogramData(HistogramType.COMPACTION_TIME)).thenReturn(compactionTimeData1);
        when(statisticsToAdd2.getHistogramData(HistogramType.COMPACTION_TIME)).thenReturn(compactionTimeData2);
        final double expectedCompactionTimeAvgSensor = (double) (8 + 8) / (2 + 2);
        final double expectedCompactionTimeMinSensor = 4.0;
        final double expectedCompactionTimeMaxSensor = 24.0;

        when(statisticsToAdd1.getAndResetTickerCount(TickerType.NO_FILE_OPENS)).thenReturn(5L);
        when(statisticsToAdd1.getAndResetTickerCount(TickerType.NO_FILE_CLOSES)).thenReturn(3L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.NO_FILE_OPENS)).thenReturn(7L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.NO_FILE_CLOSES)).thenReturn(4L);
        final double expectedNumberOfOpenFilesSensor = (5 + 7) - (3 + 4);

        when(statisticsToAdd1.getAndResetTickerCount(TickerType.NO_FILE_ERRORS)).thenReturn(34L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.NO_FILE_ERRORS)).thenReturn(11L);
        final double expectedNumberOfFileErrorsSensor = 11 + 34;

        recorder.record(now);

        verify(statisticsToAdd1, times(17)).getAndResetTickerCount(isA(TickerType.class));
        verify(statisticsToAdd2, times(17)).getAndResetTickerCount(isA(TickerType.class));
        verify(statisticsToAdd1, times(2)).getHistogramData(isA(HistogramType.class));
        verify(statisticsToAdd2, times(2)).getHistogramData(isA(HistogramType.class));
        verify(bytesWrittenToDatabaseSensor).record(expectedBytesWrittenToDatabaseSensor, now);
        verify(bytesReadFromDatabaseSensor).record(expectedBytesReadFromDatabaseSensor, now);
        verify(memtableBytesFlushedSensor).record(expectedMemtableBytesFlushedSensor, now);
        verify(memtableHitRatioSensor).record(expectedMemtableHitRatioSensorRecord, now);
        verify(memtableAvgFlushTimeSensor).record(expectedMemtableAvgFlushTimeSensor, now);
        verify(memtableMinFlushTimeSensor).record(expectedMemtableMinFlushTimeSensor, now);
        verify(memtableMaxFlushTimeSensor).record(expectedMemtableMaxFlushTimeSensor, now);
        verify(writeStallDurationSensor).record(expectedWriteStallDurationSensor, now);
        verify(blockCacheDataHitRatioSensor).record(expectedBlockCacheDataHitRatioSensor, now);
        verify(blockCacheIndexHitRatioSensor).record(expectedBlockCacheIndexHitRatioSensor, now);
        verify(blockCacheFilterHitRatioSensor).record(expectedBlockCacheFilterHitRatioSensor, now);
        verify(bytesWrittenDuringCompactionSensor).record(expectedBytesWrittenDuringCompactionSensor, now);
        verify(bytesReadDuringCompactionSensor).record(expectedBytesReadDuringCompactionSensor, now);
        verify(compactionTimeAvgSensor).record(expectedCompactionTimeAvgSensor, now);
        verify(compactionTimeMinSensor).record(expectedCompactionTimeMinSensor, now);
        verify(compactionTimeMaxSensor).record(expectedCompactionTimeMaxSensor, now);
        verify(numberOfOpenFilesSensor).record(expectedNumberOfOpenFilesSensor, now);
        verify(numberOfFileErrorsSensor).record(expectedNumberOfFileErrorsSensor, now);
    }

    @Test
    public void shouldNotRecordStatisticsBasedMetricsIfStatisticsIsNull() {
        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, null);

        recorder.record(0L);

        verifyNoInteractions(
            bytesWrittenToDatabaseSensor,
            bytesReadFromDatabaseSensor,
            memtableBytesFlushedSensor,
            memtableHitRatioSensor,
            memtableAvgFlushTimeSensor,
            memtableMinFlushTimeSensor,
            memtableMaxFlushTimeSensor,
            writeStallDurationSensor,
            blockCacheDataHitRatioSensor,
            blockCacheIndexHitRatioSensor,
            blockCacheFilterHitRatioSensor,
            bytesWrittenDuringCompactionSensor,
            bytesReadDuringCompactionSensor,
            compactionTimeAvgSensor,
            compactionTimeMinSensor,
            compactionTimeMaxSensor,
            numberOfOpenFilesSensor,
            numberOfFileErrorsSensor
        );
    }

    @Test
    public void shouldCorrectlyHandleHitRatioRecordingsWithZeroHitsAndMisses() {
        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, statisticsToAdd1);
        when(statisticsToAdd1.getHistogramData(any())).thenReturn(new HistogramData(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0L, 0L, 0.0));
        when(statisticsToAdd1.getAndResetTickerCount(any())).thenReturn(0L);

        recorder.record(0L);

        verify(memtableHitRatioSensor).record(0d, 0L);
        verify(blockCacheDataHitRatioSensor).record(0d, 0L);
        verify(blockCacheIndexHitRatioSensor).record(0d, 0L);
        verify(blockCacheFilterHitRatioSensor).record(0d, 0L);
    }

    @Test
    public void shouldCorrectlyHandleAvgRecordingsWithZeroSumAndCount() {
        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, statisticsToAdd1);
        final long now = 0L;
        when(statisticsToAdd1.getHistogramData(any())).thenReturn(new HistogramData(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0L, 0L, 0.0));
        when(statisticsToAdd1.getAndResetTickerCount(any())).thenReturn(0L);

        recorder.record(now);

        verify(compactionTimeAvgSensor).record(0d, now);
        verify(memtableAvgFlushTimeSensor).record(0d, now);
    }

    private void setUpMetricsMock() {
        dbMetrics = mockStatic(RocksDBMetrics.class);
        dbMetrics.when(() -> RocksDBMetrics.bytesWrittenToDatabaseSensor(streamsMetrics, metricsContext))
            .thenReturn(bytesWrittenToDatabaseSensor);
        dbMetrics.when(() -> RocksDBMetrics.bytesReadFromDatabaseSensor(streamsMetrics, metricsContext))
            .thenReturn(bytesReadFromDatabaseSensor);
        dbMetrics.when(() -> RocksDBMetrics.memtableBytesFlushedSensor(streamsMetrics, metricsContext))
            .thenReturn(memtableBytesFlushedSensor);
        dbMetrics.when(() -> RocksDBMetrics.memtableHitRatioSensor(streamsMetrics, metricsContext))
            .thenReturn(memtableHitRatioSensor);
        dbMetrics.when(() -> RocksDBMetrics.memtableAvgFlushTimeSensor(streamsMetrics, metricsContext))
            .thenReturn(memtableAvgFlushTimeSensor);
        dbMetrics.when(() -> RocksDBMetrics.memtableMinFlushTimeSensor(streamsMetrics, metricsContext))
            .thenReturn(memtableMinFlushTimeSensor);
        dbMetrics.when(() -> RocksDBMetrics.memtableMaxFlushTimeSensor(streamsMetrics, metricsContext))
            .thenReturn(memtableMaxFlushTimeSensor);
        dbMetrics.when(() -> RocksDBMetrics.writeStallDurationSensor(streamsMetrics, metricsContext))
            .thenReturn(writeStallDurationSensor);
        dbMetrics.when(() -> RocksDBMetrics.blockCacheDataHitRatioSensor(streamsMetrics, metricsContext))
            .thenReturn(blockCacheDataHitRatioSensor);
        dbMetrics.when(() -> RocksDBMetrics.blockCacheIndexHitRatioSensor(streamsMetrics, metricsContext))
            .thenReturn(blockCacheIndexHitRatioSensor);
        dbMetrics.when(() -> RocksDBMetrics.blockCacheFilterHitRatioSensor(streamsMetrics, metricsContext))
            .thenReturn(blockCacheFilterHitRatioSensor);
        dbMetrics.when(() -> RocksDBMetrics.bytesWrittenDuringCompactionSensor(streamsMetrics, metricsContext))
            .thenReturn(bytesWrittenDuringCompactionSensor);
        dbMetrics.when(() -> RocksDBMetrics.bytesReadDuringCompactionSensor(streamsMetrics, metricsContext))
            .thenReturn(bytesReadDuringCompactionSensor);
        dbMetrics.when(() -> RocksDBMetrics.compactionTimeAvgSensor(streamsMetrics, metricsContext))
            .thenReturn(compactionTimeAvgSensor);
        dbMetrics.when(() -> RocksDBMetrics.compactionTimeMinSensor(streamsMetrics, metricsContext))
            .thenReturn(compactionTimeMinSensor);
        dbMetrics.when(() -> RocksDBMetrics.compactionTimeMaxSensor(streamsMetrics, metricsContext))
            .thenReturn(compactionTimeMaxSensor);
        dbMetrics.when(() -> RocksDBMetrics.numberOfOpenFilesSensor(streamsMetrics, metricsContext))
            .thenReturn(numberOfOpenFilesSensor);
        dbMetrics.when(() -> RocksDBMetrics.numberOfFileErrorsSensor(streamsMetrics, metricsContext))
            .thenReturn(numberOfFileErrorsSensor);
    }
}
