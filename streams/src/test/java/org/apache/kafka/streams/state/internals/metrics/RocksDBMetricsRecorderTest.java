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
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetrics.RocksDBMetricContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;
import org.rocksdb.TickerType;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.niceMock;
import static org.easymock.EasyMock.resetToNice;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.powermock.api.easymock.PowerMock.reset;
import static org.powermock.api.easymock.PowerMock.createMock;
import static org.powermock.api.easymock.PowerMock.mockStatic;
import static org.powermock.api.easymock.PowerMock.replay;
import static org.powermock.api.easymock.PowerMock.verify;

@RunWith(PowerMockRunner.class)
@PrepareForTest({RocksDBMetrics.class, Sensor.class})
public class RocksDBMetricsRecorderTest {
    private final static String METRICS_SCOPE = "metrics-scope";
    private final static String THREAD_ID = "thread-id";
    private final static String STORE_NAME = "store-name";
    private final static String SEGMENT_STORE_NAME_1 = "segment-store-name-1";
    private final static String SEGMENT_STORE_NAME_2 = "segment-store-name-2";

    private final Statistics statisticsToAdd1 = mock(Statistics.class);
    private final Statistics statisticsToAdd2 = mock(Statistics.class);

    private final Sensor bytesWrittenToDatabaseSensor = createMock(Sensor.class);
    private final Sensor bytesReadFromDatabaseSensor = createMock(Sensor.class);
    private final Sensor memtableBytesFlushedSensor = createMock(Sensor.class);
    private final Sensor memtableHitRatioSensor = createMock(Sensor.class);
    private final Sensor writeStallDurationSensor = createMock(Sensor.class);
    private final Sensor blockCacheDataHitRatioSensor = createMock(Sensor.class);
    private final Sensor blockCacheIndexHitRatioSensor = createMock(Sensor.class);
    private final Sensor blockCacheFilterHitRatioSensor = createMock(Sensor.class);
    private final Sensor bytesReadDuringCompactionSensor = createMock(Sensor.class);
    private final Sensor bytesWrittenDuringCompactionSensor = createMock(Sensor.class);
    private final Sensor numberOfOpenFilesSensor = createMock(Sensor.class);
    private final Sensor numberOfFileErrorsSensor = createMock(Sensor.class);

    private final StreamsMetricsImpl streamsMetrics = niceMock(StreamsMetricsImpl.class);
    private final RocksDBMetricsRecordingTrigger recordingTrigger = mock(RocksDBMetricsRecordingTrigger.class);
    private final TaskId taskId1 = new TaskId(0, 0);
    private final TaskId taskId2 = new TaskId(0, 1);

    private final RocksDBMetricsRecorder recorder = new RocksDBMetricsRecorder(METRICS_SCOPE, THREAD_ID, STORE_NAME);

    @Before
    public void setUp() {
        setUpMetricsStubMock();
        expect(streamsMetrics.rocksDBMetricsRecordingTrigger()).andStubReturn(recordingTrigger);
        replay(streamsMetrics);
        recorder.init(streamsMetrics, taskId1);
    }

    @Test
    public void shouldInitMetricsRecorder() {
        setUpMetricsMock();

        recorder.init(streamsMetrics, taskId1);

        verify(RocksDBMetrics.class);
        assertThat(recorder.taskId(), is(taskId1));
    }

    @Test
    public void shouldThrowIfMetricRecorderIsReInitialisedWithDifferentTask() {
        setUpMetricsStubMock();
        recorder.init(streamsMetrics, taskId1);

        assertThrows(
            IllegalStateException.class,
            () -> recorder.init(streamsMetrics, taskId2)
        );
    }

    @Test
    public void shouldThrowIfMetricRecorderIsReInitialisedWithDifferentStreamsMetrics() {
        setUpMetricsStubMock();
        recorder.init(streamsMetrics, taskId1);

        assertThrows(
            IllegalStateException.class,
            () -> recorder.init(
                new StreamsMetricsImpl(new Metrics(), "test-client", StreamsConfig.METRICS_LATEST),
                taskId1
            )
        );
    }

    @Test
    public void shouldSetStatsLevelToExceptDetailedTimersWhenStatisticsIsAdded() {
        statisticsToAdd1.setStatsLevel(StatsLevel.EXCEPT_DETAILED_TIMERS);
        replay(statisticsToAdd1);

        recorder.addStatistics(SEGMENT_STORE_NAME_1, statisticsToAdd1);

        verify(statisticsToAdd1);
    }

    @Test
    public void shouldThrowIfStatisticsToAddHasBeenAlreadyAdded() {
        recorder.addStatistics(SEGMENT_STORE_NAME_1, statisticsToAdd1);

        assertThrows(
            IllegalStateException.class,
            () -> recorder.addStatistics(SEGMENT_STORE_NAME_1, statisticsToAdd1)
        );
    }

    @Test
    public void shouldAddItselfToRecordingTriggerWhenFirstStatisticsIsAddedToNewlyCreatedRecorder() {
        recordingTrigger.addMetricsRecorder(recorder);
        replay(recordingTrigger);

        recorder.addStatistics(SEGMENT_STORE_NAME_1, statisticsToAdd1);

        verify(recordingTrigger);
    }

    @Test
    public void shouldAddItselfToRecordingTriggerWhenFirstStatisticsIsAddedAfterLastStatisticsWasRemoved() {
        recorder.addStatistics(SEGMENT_STORE_NAME_1, statisticsToAdd1);
        recorder.removeStatistics(SEGMENT_STORE_NAME_1);
        reset(recordingTrigger);
        recordingTrigger.addMetricsRecorder(recorder);
        replay(recordingTrigger);

        recorder.addStatistics(SEGMENT_STORE_NAME_2, statisticsToAdd2);

        verify(recordingTrigger);
    }

    @Test
    public void shouldNotAddItselfToRecordingTriggerWhenNotEmpty() {
        recorder.addStatistics(SEGMENT_STORE_NAME_1, statisticsToAdd1);
        reset(recordingTrigger);
        replay(recordingTrigger);

        recorder.addStatistics(SEGMENT_STORE_NAME_2, statisticsToAdd2);

        verify(recordingTrigger);
    }

    @Test
    public void shouldCloseStatisticsWhenStatisticsIsRemoved() {
        recorder.addStatistics(SEGMENT_STORE_NAME_1, statisticsToAdd1);
        reset(statisticsToAdd1);
        statisticsToAdd1.close();
        replay(statisticsToAdd1);

        recorder.removeStatistics(SEGMENT_STORE_NAME_1);

        verify(statisticsToAdd1);
    }

    @Test
    public void shouldRemoveItselfFromRecordingTriggerWhenLastStatisticsIsRemoved() {
        recorder.addStatistics(SEGMENT_STORE_NAME_1, statisticsToAdd1);
        recorder.addStatistics(SEGMENT_STORE_NAME_2, statisticsToAdd2);
        reset(recordingTrigger);
        replay(recordingTrigger);

        recorder.removeStatistics(SEGMENT_STORE_NAME_1);

        verify(recordingTrigger);

        reset(recordingTrigger);
        recordingTrigger.removeMetricsRecorder(recorder);
        replay(recordingTrigger);

        recorder.removeStatistics(SEGMENT_STORE_NAME_2);

        verify(recordingTrigger);
    }

    @Test
    public void shouldThrowIfStatisticsToRemoveNotFound() {
        recorder.addStatistics(SEGMENT_STORE_NAME_1, statisticsToAdd1);

        assertThrows(
            IllegalStateException.class,
            () -> recorder.removeStatistics(SEGMENT_STORE_NAME_2)
        );
    }

    @Test
    public void shouldRecordMetrics() {
        recorder.addStatistics(SEGMENT_STORE_NAME_1, statisticsToAdd1);
        recorder.addStatistics(SEGMENT_STORE_NAME_2, statisticsToAdd2);
        reset(statisticsToAdd1);
        reset(statisticsToAdd2);

        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.BYTES_WRITTEN)).andReturn(1L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.BYTES_WRITTEN)).andReturn(2L);
        bytesWrittenToDatabaseSensor.record(1 + 2, 0L);
        replay(bytesWrittenToDatabaseSensor);

        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.BYTES_READ)).andReturn(2L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.BYTES_READ)).andReturn(3L);
        bytesReadFromDatabaseSensor.record(2 + 3, 0L);
        replay(bytesReadFromDatabaseSensor);

        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.FLUSH_WRITE_BYTES)).andReturn(3L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.FLUSH_WRITE_BYTES)).andReturn(4L);
        memtableBytesFlushedSensor.record(3 + 4, 0L);
        replay(memtableBytesFlushedSensor);

        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.MEMTABLE_HIT)).andReturn(1L);
        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.MEMTABLE_MISS)).andReturn(2L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.MEMTABLE_HIT)).andReturn(3L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.MEMTABLE_MISS)).andReturn(4L);
        memtableHitRatioSensor.record((double) 4 / (4 + 6), 0L);
        replay(memtableHitRatioSensor);

        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.STALL_MICROS)).andReturn(4L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.STALL_MICROS)).andReturn(5L);
        writeStallDurationSensor.record(4 + 5, 0L);
        replay(writeStallDurationSensor);

        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.BLOCK_CACHE_DATA_HIT)).andReturn(5L);
        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.BLOCK_CACHE_DATA_MISS)).andReturn(4L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.BLOCK_CACHE_DATA_HIT)).andReturn(3L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.BLOCK_CACHE_DATA_MISS)).andReturn(2L);
        blockCacheDataHitRatioSensor.record((double) 8 / (8 + 6), 0L);
        replay(blockCacheDataHitRatioSensor);

        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.BLOCK_CACHE_INDEX_HIT)).andReturn(4L);
        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.BLOCK_CACHE_INDEX_MISS)).andReturn(2L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.BLOCK_CACHE_INDEX_HIT)).andReturn(2L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.BLOCK_CACHE_INDEX_MISS)).andReturn(4L);
        blockCacheIndexHitRatioSensor.record((double) 6 / (6 + 6), 0L);
        replay(blockCacheIndexHitRatioSensor);

        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.BLOCK_CACHE_FILTER_HIT)).andReturn(2L);
        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.BLOCK_CACHE_FILTER_MISS)).andReturn(4L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.BLOCK_CACHE_FILTER_HIT)).andReturn(3L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.BLOCK_CACHE_FILTER_MISS)).andReturn(5L);
        blockCacheFilterHitRatioSensor.record((double) 5 / (5 + 9), 0L);
        replay(blockCacheFilterHitRatioSensor);

        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.COMPACT_WRITE_BYTES)).andReturn(2L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.COMPACT_WRITE_BYTES)).andReturn(4L);
        bytesWrittenDuringCompactionSensor.record(2 + 4, 0L);
        replay(bytesWrittenDuringCompactionSensor);

        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.COMPACT_READ_BYTES)).andReturn(5L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.COMPACT_READ_BYTES)).andReturn(6L);
        bytesReadDuringCompactionSensor.record(5 + 6, 0L);
        replay(bytesReadDuringCompactionSensor);

        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.NO_FILE_OPENS)).andReturn(5L);
        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.NO_FILE_CLOSES)).andReturn(3L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.NO_FILE_OPENS)).andReturn(7L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.NO_FILE_CLOSES)).andReturn(4L);
        numberOfOpenFilesSensor.record((5 + 7) - (3 + 4), 0L);
        replay(numberOfOpenFilesSensor);

        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.NO_FILE_ERRORS)).andReturn(34L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.NO_FILE_ERRORS)).andReturn(11L);
        numberOfFileErrorsSensor.record(11 + 34, 0L);
        replay(numberOfFileErrorsSensor);

        replay(statisticsToAdd1);
        replay(statisticsToAdd2);

        recorder.record(0L);

        verify(statisticsToAdd1);
        verify(statisticsToAdd2);
        verify(bytesWrittenToDatabaseSensor);
    }

    @Test
    public void shouldCorrectlyHandleHitRatioRecordingsWithZeroHitsAndMisses() {
        resetToNice(statisticsToAdd1);
        recorder.addStatistics(SEGMENT_STORE_NAME_1, statisticsToAdd1);
        expect(statisticsToAdd1.getTickerCount(anyObject())).andStubReturn(0L);
        replay(statisticsToAdd1);
        memtableHitRatioSensor.record(0, 0L);
        blockCacheDataHitRatioSensor.record(0, 0L);
        blockCacheIndexHitRatioSensor.record(0, 0L);
        blockCacheFilterHitRatioSensor.record(0, 0L);
        replay(memtableHitRatioSensor);
        replay(blockCacheDataHitRatioSensor);
        replay(blockCacheIndexHitRatioSensor);
        replay(blockCacheFilterHitRatioSensor);

        recorder.record(0L);

        verify(memtableHitRatioSensor);
        verify(blockCacheDataHitRatioSensor);
        verify(blockCacheIndexHitRatioSensor);
        verify(blockCacheFilterHitRatioSensor);
    }

    private void setUpMetricsMock() {
        mockStatic(RocksDBMetrics.class);
        final RocksDBMetricContext metricsContext =
            new RocksDBMetricContext(THREAD_ID, taskId1.toString(), METRICS_SCOPE, STORE_NAME);
        expect(RocksDBMetrics.bytesWrittenToDatabaseSensor(eq(streamsMetrics), eq(metricsContext)))
            .andReturn(bytesWrittenToDatabaseSensor);
        expect(RocksDBMetrics.bytesReadFromDatabaseSensor(eq(streamsMetrics), eq(metricsContext)))
            .andReturn(bytesReadFromDatabaseSensor);
        expect(RocksDBMetrics.memtableBytesFlushedSensor(eq(streamsMetrics), eq(metricsContext)))
            .andReturn(memtableBytesFlushedSensor);
        expect(RocksDBMetrics.memtableHitRatioSensor(eq(streamsMetrics), eq(metricsContext)))
            .andReturn(memtableHitRatioSensor);
        expect(RocksDBMetrics.writeStallDurationSensor(eq(streamsMetrics), eq(metricsContext)))
            .andReturn(writeStallDurationSensor);
        expect(RocksDBMetrics.blockCacheDataHitRatioSensor(eq(streamsMetrics), eq(metricsContext)))
            .andReturn(blockCacheDataHitRatioSensor);
        expect(RocksDBMetrics.blockCacheIndexHitRatioSensor(eq(streamsMetrics), eq(metricsContext)))
            .andReturn(blockCacheIndexHitRatioSensor);
        expect(RocksDBMetrics.blockCacheFilterHitRatioSensor(eq(streamsMetrics), eq(metricsContext)))
            .andReturn(blockCacheFilterHitRatioSensor);
        expect(RocksDBMetrics.bytesWrittenDuringCompactionSensor(eq(streamsMetrics), eq(metricsContext)))
            .andReturn(bytesWrittenDuringCompactionSensor);
        expect(RocksDBMetrics.bytesReadDuringCompactionSensor(eq(streamsMetrics), eq(metricsContext)))
            .andReturn(bytesReadDuringCompactionSensor);
        expect(RocksDBMetrics.numberOfOpenFilesSensor(eq(streamsMetrics), eq(metricsContext)))
            .andReturn(numberOfOpenFilesSensor);
        expect(RocksDBMetrics.numberOfFileErrorsSensor(eq(streamsMetrics), eq(metricsContext)))
            .andReturn(numberOfFileErrorsSensor);
        replay(RocksDBMetrics.class);
    }

    private void setUpMetricsStubMock() {
        mockStatic(RocksDBMetrics.class);
        final RocksDBMetricContext metricsContext =
            new RocksDBMetricContext(THREAD_ID, taskId1.toString(), METRICS_SCOPE, STORE_NAME);
        expect(RocksDBMetrics.bytesWrittenToDatabaseSensor(streamsMetrics, metricsContext))
            .andStubReturn(bytesWrittenToDatabaseSensor);
        expect(RocksDBMetrics.bytesReadFromDatabaseSensor(streamsMetrics, metricsContext))
            .andStubReturn(bytesReadFromDatabaseSensor);
        expect(RocksDBMetrics.memtableBytesFlushedSensor(streamsMetrics, metricsContext))
            .andStubReturn(memtableBytesFlushedSensor);
        expect(RocksDBMetrics.memtableHitRatioSensor(streamsMetrics, metricsContext))
            .andStubReturn(memtableHitRatioSensor);
        expect(RocksDBMetrics.writeStallDurationSensor(streamsMetrics, metricsContext))
            .andStubReturn(writeStallDurationSensor);
        expect(RocksDBMetrics.blockCacheDataHitRatioSensor(streamsMetrics, metricsContext))
            .andStubReturn(blockCacheDataHitRatioSensor);
        expect(RocksDBMetrics.blockCacheIndexHitRatioSensor(streamsMetrics, metricsContext))
            .andStubReturn(blockCacheIndexHitRatioSensor);
        expect(RocksDBMetrics.blockCacheFilterHitRatioSensor(streamsMetrics, metricsContext))
            .andStubReturn(blockCacheFilterHitRatioSensor);
        expect(RocksDBMetrics.bytesWrittenDuringCompactionSensor(streamsMetrics, metricsContext))
            .andStubReturn(bytesWrittenDuringCompactionSensor);
        expect(RocksDBMetrics.bytesReadDuringCompactionSensor(streamsMetrics, metricsContext))
            .andStubReturn(bytesReadDuringCompactionSensor);
        expect(RocksDBMetrics.numberOfOpenFilesSensor(streamsMetrics, metricsContext))
            .andStubReturn(numberOfOpenFilesSensor);
        expect(RocksDBMetrics.numberOfFileErrorsSensor(streamsMetrics, metricsContext))
            .andStubReturn(numberOfFileErrorsSensor);
        replay(RocksDBMetrics.class);
    }
}