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
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.RocksDBMetricContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.rocksdb.Cache;
import org.rocksdb.RocksDB;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;
import org.rocksdb.TickerType;

import static org.easymock.EasyMock.anyObject;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.reset;

public class RocksDBMetricsRecorderTest {
    private final static String METRICS_SCOPE = "metrics-scope";
    private final static String THREAD_ID = "thread-id";
    private final static TaskId TASK_ID1 = new TaskId(0, 0);
    private final static TaskId TASK_ID2 = new TaskId(0, 1);
    private final static String STORE_NAME = "store-name";
    private final static String SEGMENT_STORE_NAME_1 = "segment-store-name-1";
    private final static String SEGMENT_STORE_NAME_2 = "segment-store-name-2";
    private final static String SEGMENT_STORE_NAME_3 = "segment-store-name-3";

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
    private final Sensor writeStallDurationSensor = mock(Sensor.class);
    private final Sensor blockCacheDataHitRatioSensor = mock(Sensor.class);
    private final Sensor blockCacheIndexHitRatioSensor = mock(Sensor.class);
    private final Sensor blockCacheFilterHitRatioSensor = mock(Sensor.class);
    private final Sensor bytesReadDuringCompactionSensor = mock(Sensor.class);
    private final Sensor bytesWrittenDuringCompactionSensor = mock(Sensor.class);
    private final Sensor numberOfOpenFilesSensor = mock(Sensor.class);
    private final Sensor numberOfFileErrorsSensor = mock(Sensor.class);

    private final StreamsMetricsImpl streamsMetrics = mock(StreamsMetricsImpl.class);
    private final RocksDBMetricsRecordingTrigger recordingTrigger = mock(RocksDBMetricsRecordingTrigger.class);

    private final RocksDBMetricsRecorder recorder = new RocksDBMetricsRecorder(METRICS_SCOPE, STORE_NAME);
    private MockedStatic<RocksDBMetrics> rocksDBMetricsMockedStatic;

    @Before
    public void setUp() {
        setUpMetricsStubMock();
        when(streamsMetrics.rocksDBMetricsRecordingTrigger()).thenReturn(recordingTrigger);
        recorder.init(streamsMetrics, TASK_ID1);
    }

    @After
    public void tearDown() {
        rocksDBMetricsMockedStatic.close();
    }

    @Test
    public void shouldInitMetricsRecorder() {
        setUpMetricsMock();

        recorder.init(streamsMetrics, TASK_ID1);

//        verify(RocksDBMetrics.class);
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
                        new StreamsMetricsImpl(new Metrics(), "test-client", StreamsConfig.METRICS_LATEST, new MockTime()),
                        TASK_ID1
                )
        );
    }

    @Test
    public void shouldSetStatsLevelToExceptDetailedTimersWhenValueProvidersWithStatisticsAreAdded() {
        statisticsToAdd1.setStatsLevel(StatsLevel.EXCEPT_DETAILED_TIMERS);

        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, statisticsToAdd1);
    }

    @Test
    public void shouldNotSetStatsLevelToExceptDetailedTimersWhenValueProvidersWithoutStatisticsAreAdded() {
        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, null);
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
    public void shouldThrowIfStatisticsToAddIsNotNullButExsitingStatisticsAreNull() {
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
    public void shouldThrowIfStatisticsToAddIsNullButExsitingStatisticsAreNotNull() {
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
    public void shouldThrowIfCacheToAddIsNullButExsitingCacheIsNotNull() {
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
        recordingTrigger.addMetricsRecorder(recorder);

        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, statisticsToAdd1);
    }

    @Test
    public void shouldAddItselfToRecordingTriggerWhenFirstValueProvidersAreAddedAfterLastValueProvidersWereRemoved() {
        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, statisticsToAdd1);
        recorder.removeValueProviders(SEGMENT_STORE_NAME_1);
        reset(recordingTrigger);
        recordingTrigger.addMetricsRecorder(recorder);

        recorder.addValueProviders(SEGMENT_STORE_NAME_2, dbToAdd2, cacheToAdd2, statisticsToAdd2);
    }

    @Test
    public void shouldNotAddItselfToRecordingTriggerWhenNotEmpty2() {
        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, statisticsToAdd1);
        reset(recordingTrigger);

        recorder.addValueProviders(SEGMENT_STORE_NAME_2, dbToAdd2, cacheToAdd2, statisticsToAdd2);
    }

    @Test
    public void shouldCloseStatisticsWhenValueProvidersAreRemoved() {
        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, statisticsToAdd1);
        reset(statisticsToAdd1);
        statisticsToAdd1.close();

        recorder.removeValueProviders(SEGMENT_STORE_NAME_1);
    }

    @Test
    public void shouldNotCloseStatisticsWhenValueProvidersWithoutStatisticsAreRemoved() {
        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, null);
        reset(statisticsToAdd1);

        recorder.removeValueProviders(SEGMENT_STORE_NAME_1);
    }

    @Test
    public void shouldRemoveItselfFromRecordingTriggerWhenLastValueProvidersAreRemoved() {
        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, statisticsToAdd1);
        recorder.addValueProviders(SEGMENT_STORE_NAME_2, dbToAdd2, cacheToAdd2, statisticsToAdd2);
        reset(recordingTrigger);

        recorder.removeValueProviders(SEGMENT_STORE_NAME_1);

        reset(recordingTrigger);
        recordingTrigger.removeMetricsRecorder(recorder);

        recorder.removeValueProviders(SEGMENT_STORE_NAME_2);
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
        reset(statisticsToAdd1);
        reset(statisticsToAdd2);

        when(statisticsToAdd1.getAndResetTickerCount(TickerType.BYTES_WRITTEN)).thenReturn(1L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.BYTES_WRITTEN)).thenReturn(2L);
        bytesWrittenToDatabaseSensor.record(1 + 2, 0L);

        when(statisticsToAdd1.getAndResetTickerCount(TickerType.BYTES_READ)).thenReturn(2L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.BYTES_READ)).thenReturn(3L);
        bytesReadFromDatabaseSensor.record(2 + 3, 0L);

        when(statisticsToAdd1.getAndResetTickerCount(TickerType.FLUSH_WRITE_BYTES)).thenReturn(3L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.FLUSH_WRITE_BYTES)).thenReturn(4L);
        memtableBytesFlushedSensor.record(3 + 4, 0L);

        when(statisticsToAdd1.getAndResetTickerCount(TickerType.MEMTABLE_HIT)).thenReturn(1L);
        when(statisticsToAdd1.getAndResetTickerCount(TickerType.MEMTABLE_MISS)).thenReturn(2L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.MEMTABLE_HIT)).thenReturn(3L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.MEMTABLE_MISS)).thenReturn(4L);
        memtableHitRatioSensor.record((double) 4 / (4 + 6), 0L);

        when(statisticsToAdd1.getAndResetTickerCount(TickerType.STALL_MICROS)).thenReturn(4L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.STALL_MICROS)).thenReturn(5L);
        writeStallDurationSensor.record(4 + 5, 0L);

        when(statisticsToAdd1.getAndResetTickerCount(TickerType.BLOCK_CACHE_DATA_HIT)).thenReturn(5L);
        when(statisticsToAdd1.getAndResetTickerCount(TickerType.BLOCK_CACHE_DATA_MISS)).thenReturn(4L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.BLOCK_CACHE_DATA_HIT)).thenReturn(3L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.BLOCK_CACHE_DATA_MISS)).thenReturn(2L);
        blockCacheDataHitRatioSensor.record((double) 8 / (8 + 6), 0L);

        when(statisticsToAdd1.getAndResetTickerCount(TickerType.BLOCK_CACHE_INDEX_HIT)).thenReturn(4L);
        when(statisticsToAdd1.getAndResetTickerCount(TickerType.BLOCK_CACHE_INDEX_MISS)).thenReturn(2L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.BLOCK_CACHE_INDEX_HIT)).thenReturn(2L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.BLOCK_CACHE_INDEX_MISS)).thenReturn(4L);
        blockCacheIndexHitRatioSensor.record((double) 6 / (6 + 6), 0L);

        when(statisticsToAdd1.getAndResetTickerCount(TickerType.BLOCK_CACHE_FILTER_HIT)).thenReturn(2L);
        when(statisticsToAdd1.getAndResetTickerCount(TickerType.BLOCK_CACHE_FILTER_MISS)).thenReturn(4L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.BLOCK_CACHE_FILTER_HIT)).thenReturn(3L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.BLOCK_CACHE_FILTER_MISS)).thenReturn(5L);
        blockCacheFilterHitRatioSensor.record((double) 5 / (5 + 9), 0L);

        when(statisticsToAdd1.getAndResetTickerCount(TickerType.COMPACT_WRITE_BYTES)).thenReturn(2L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.COMPACT_WRITE_BYTES)).thenReturn(4L);
        bytesWrittenDuringCompactionSensor.record(2 + 4, 0L);

        when(statisticsToAdd1.getAndResetTickerCount(TickerType.COMPACT_READ_BYTES)).thenReturn(5L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.COMPACT_READ_BYTES)).thenReturn(6L);
        bytesReadDuringCompactionSensor.record(5 + 6, 0L);

        when(statisticsToAdd1.getAndResetTickerCount(TickerType.NO_FILE_OPENS)).thenReturn(5L);
        when(statisticsToAdd1.getAndResetTickerCount(TickerType.NO_FILE_CLOSES)).thenReturn(3L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.NO_FILE_OPENS)).thenReturn(7L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.NO_FILE_CLOSES)).thenReturn(4L);
        numberOfOpenFilesSensor.record((5 + 7) - (3 + 4), 0L);

        when(statisticsToAdd1.getAndResetTickerCount(TickerType.NO_FILE_ERRORS)).thenReturn(34L);
        when(statisticsToAdd2.getAndResetTickerCount(TickerType.NO_FILE_ERRORS)).thenReturn(11L);
        numberOfFileErrorsSensor.record(11 + 34, 0L);

        recorder.record(0L);
    }

    @Test
    public void shouldNotRecordStatisticsBasedMetricsIfStatisticsIsNull() {
        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, null);

        recorder.record(0L);
    }

    @Test
    public void shouldCorrectlyHandleHitRatioRecordingsWithZeroHitsAndMisses() {
        reset(statisticsToAdd1);
        recorder.addValueProviders(SEGMENT_STORE_NAME_1, dbToAdd1, cacheToAdd1, statisticsToAdd1);
        when(statisticsToAdd1.getTickerCount(anyObject())).thenReturn(0L);
        memtableHitRatioSensor.record(0, 0L);
        blockCacheDataHitRatioSensor.record(0, 0L);
        blockCacheIndexHitRatioSensor.record(0, 0L);
        blockCacheFilterHitRatioSensor.record(0, 0L);

        recorder.record(0L);
    }

    private void setUpMetricsMock() {
        rocksDBMetricsMockedStatic.close();
        rocksDBMetricsMockedStatic = Mockito.mockStatic(RocksDBMetrics.class);
        final RocksDBMetricContext metricsContext =
                new RocksDBMetricContext(TASK_ID1.toString(), METRICS_SCOPE, STORE_NAME);
        rocksDBMetricsMockedStatic.when(() -> RocksDBMetrics.bytesWrittenToDatabaseSensor(streamsMetrics, metricsContext))
                .thenReturn(bytesWrittenToDatabaseSensor);
        rocksDBMetricsMockedStatic.when(() -> RocksDBMetrics.bytesReadFromDatabaseSensor(streamsMetrics, metricsContext))
                .thenReturn(bytesReadFromDatabaseSensor);
        rocksDBMetricsMockedStatic.when(() -> RocksDBMetrics.memtableBytesFlushedSensor(streamsMetrics, metricsContext))
                .thenReturn(memtableBytesFlushedSensor);
        rocksDBMetricsMockedStatic.when(() -> RocksDBMetrics.memtableHitRatioSensor(streamsMetrics, metricsContext))
                .thenReturn(memtableHitRatioSensor);
        rocksDBMetricsMockedStatic.when(() -> RocksDBMetrics.writeStallDurationSensor(streamsMetrics, metricsContext))
                .thenReturn(writeStallDurationSensor);
        rocksDBMetricsMockedStatic.when(() -> RocksDBMetrics.blockCacheDataHitRatioSensor(streamsMetrics, metricsContext))
                .thenReturn(blockCacheDataHitRatioSensor);
        rocksDBMetricsMockedStatic.when(() -> RocksDBMetrics.blockCacheIndexHitRatioSensor(streamsMetrics, metricsContext))
                .thenReturn(blockCacheIndexHitRatioSensor);
        rocksDBMetricsMockedStatic.when(() -> RocksDBMetrics.blockCacheFilterHitRatioSensor(streamsMetrics, metricsContext))
                .thenReturn(blockCacheFilterHitRatioSensor);
        rocksDBMetricsMockedStatic.when(() -> RocksDBMetrics.bytesWrittenDuringCompactionSensor(streamsMetrics, metricsContext))
                .thenReturn(bytesWrittenDuringCompactionSensor);
        rocksDBMetricsMockedStatic.when(() -> RocksDBMetrics.bytesReadDuringCompactionSensor(streamsMetrics, metricsContext))
                .thenReturn(bytesReadDuringCompactionSensor);
        rocksDBMetricsMockedStatic.when(() -> RocksDBMetrics.numberOfOpenFilesSensor(streamsMetrics, metricsContext))
                .thenReturn(numberOfOpenFilesSensor);
        rocksDBMetricsMockedStatic.when(() -> RocksDBMetrics.numberOfFileErrorsSensor(streamsMetrics, metricsContext))
                .thenReturn(numberOfFileErrorsSensor);
        RocksDBMetrics.addNumImmutableMemTableMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addCurSizeActiveMemTable(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addCurSizeAllMemTables(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addSizeAllMemTables(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addNumEntriesActiveMemTableMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addNumEntriesImmMemTablesMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addNumDeletesActiveMemTableMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addNumDeletesImmMemTablesMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addMemTableFlushPending(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addNumRunningFlushesMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addCompactionPendingMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addNumRunningCompactionsMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addEstimatePendingCompactionBytesMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addTotalSstFilesSizeMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addLiveSstFilesSizeMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addNumLiveVersionMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addBlockCacheCapacityMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addBlockCacheUsageMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addBlockCachePinnedUsageMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addEstimateNumKeysMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addEstimateTableReadersMemMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addBackgroundErrorsMetric(eq(streamsMetrics), eq(metricsContext), any());
    }

    private void setUpMetricsStubMock() {
        rocksDBMetricsMockedStatic = Mockito.mockStatic(RocksDBMetrics.class);
        final RocksDBMetricContext metricsContext =
                new RocksDBMetricContext(TASK_ID1.toString(), METRICS_SCOPE, STORE_NAME);

        rocksDBMetricsMockedStatic.when(() -> RocksDBMetrics.bytesWrittenToDatabaseSensor(streamsMetrics, metricsContext))
                .thenReturn(bytesWrittenToDatabaseSensor);
        rocksDBMetricsMockedStatic.when(() -> RocksDBMetrics.bytesReadFromDatabaseSensor(streamsMetrics, metricsContext))
                .thenReturn(bytesReadFromDatabaseSensor);
        rocksDBMetricsMockedStatic.when(() -> RocksDBMetrics.memtableBytesFlushedSensor(streamsMetrics, metricsContext))
                .thenReturn(memtableBytesFlushedSensor);
        rocksDBMetricsMockedStatic.when(() -> RocksDBMetrics.memtableHitRatioSensor(streamsMetrics, metricsContext))
                .thenReturn(memtableHitRatioSensor);
        rocksDBMetricsMockedStatic.when(() -> RocksDBMetrics.writeStallDurationSensor(streamsMetrics, metricsContext))
                .thenReturn(writeStallDurationSensor);
        rocksDBMetricsMockedStatic.when(() -> RocksDBMetrics.blockCacheDataHitRatioSensor(streamsMetrics, metricsContext))
                .thenReturn(blockCacheDataHitRatioSensor);
        rocksDBMetricsMockedStatic.when(() -> RocksDBMetrics.blockCacheIndexHitRatioSensor(streamsMetrics, metricsContext))
                .thenReturn(blockCacheIndexHitRatioSensor);
        rocksDBMetricsMockedStatic.when(() -> RocksDBMetrics.blockCacheFilterHitRatioSensor(streamsMetrics, metricsContext))
                .thenReturn(blockCacheFilterHitRatioSensor);
        rocksDBMetricsMockedStatic.when(() -> RocksDBMetrics.bytesWrittenDuringCompactionSensor(streamsMetrics, metricsContext))
                .thenReturn(bytesWrittenDuringCompactionSensor);
        rocksDBMetricsMockedStatic.when(() -> RocksDBMetrics.bytesReadDuringCompactionSensor(streamsMetrics, metricsContext))
                .thenReturn(bytesReadDuringCompactionSensor);
        rocksDBMetricsMockedStatic.when(() -> RocksDBMetrics.numberOfOpenFilesSensor(streamsMetrics, metricsContext))
                .thenReturn(numberOfOpenFilesSensor);
        rocksDBMetricsMockedStatic.when(() -> RocksDBMetrics.numberOfFileErrorsSensor(streamsMetrics, metricsContext))
                .thenReturn(numberOfFileErrorsSensor);
        RocksDBMetrics.addNumImmutableMemTableMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addCurSizeActiveMemTable(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addCurSizeAllMemTables(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addSizeAllMemTables(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addNumEntriesActiveMemTableMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addNumEntriesImmMemTablesMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addNumDeletesActiveMemTableMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addNumDeletesImmMemTablesMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addMemTableFlushPending(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addNumRunningFlushesMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addCompactionPendingMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addNumRunningCompactionsMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addEstimatePendingCompactionBytesMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addTotalSstFilesSizeMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addLiveSstFilesSizeMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addNumLiveVersionMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addBlockCacheCapacityMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addBlockCacheUsageMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addBlockCachePinnedUsageMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addEstimateNumKeysMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addEstimateTableReadersMemMetric(eq(streamsMetrics), eq(metricsContext), any());
        RocksDBMetrics.addBackgroundErrorsMetric(eq(streamsMetrics), eq(metricsContext), any());
    }
}