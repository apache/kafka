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
import static org.junit.Assert.assertThrows;
import static org.powermock.api.easymock.PowerMock.reset;
import static org.powermock.api.easymock.PowerMock.createMock;
import static org.powermock.api.easymock.PowerMock.mockStatic;
import static org.powermock.api.easymock.PowerMock.mockStaticNice;
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
    private final TaskId taskId2 = new TaskId(0, 2);

    private final RocksDBMetricsRecorder recorder = new RocksDBMetricsRecorder(METRICS_SCOPE, THREAD_ID, STORE_NAME);

    @Before
    public void setUp() {
        expect(streamsMetrics.rocksDBMetricsRecordingTrigger()).andStubReturn(recordingTrigger);
        replay(streamsMetrics);
    }

    @Test
    public void shouldSetStatsLevelToExceptDetailedTimersWhenStatisticsIsAdded() {
        mockStaticNice(RocksDBMetrics.class);
        replay(RocksDBMetrics.class);
        statisticsToAdd1.setStatsLevel(StatsLevel.EXCEPT_DETAILED_TIMERS);
        replay(statisticsToAdd1);

        recorder.addStatistics(SEGMENT_STORE_NAME_1, statisticsToAdd1, streamsMetrics, taskId1);

        verify(statisticsToAdd1);
    }

    @Test
    public void shouldThrowIfTaskIdOfStatisticsToAddDiffersFromInitialisedOne() {
        mockStaticNice(RocksDBMetrics.class);
        replay(RocksDBMetrics.class);
        recorder.addStatistics(SEGMENT_STORE_NAME_1, statisticsToAdd1, streamsMetrics, taskId1);
        assertThrows(
            IllegalStateException.class,
            () -> recorder.addStatistics(SEGMENT_STORE_NAME_2, statisticsToAdd2, streamsMetrics, taskId2)
        );
    }

    @Test
    public void shouldThrowIfStatisticsToAddHasBeenAlreadyAdded() {
        mockStaticNice(RocksDBMetrics.class);
        replay(RocksDBMetrics.class);
        recorder.addStatistics(SEGMENT_STORE_NAME_1, statisticsToAdd1, streamsMetrics, taskId1);

        assertThrows(
            IllegalStateException.class,
            () -> recorder.addStatistics(SEGMENT_STORE_NAME_1, statisticsToAdd1, streamsMetrics, taskId1)
        );
    }

    @Test
    public void shouldInitMetricsAndAddItselfToRecordingTriggerOnlyWhenFirstStatisticsIsAdded() {
        setUpMetricsMock();
        recordingTrigger.addMetricsRecorder(recorder);
        replay(recordingTrigger);

        recorder.addStatistics(SEGMENT_STORE_NAME_1, statisticsToAdd1, streamsMetrics, taskId1);

        verify(recordingTrigger);
        verify(RocksDBMetrics.class);

        mockStatic(RocksDBMetrics.class);
        replay(RocksDBMetrics.class);
        reset(recordingTrigger);
        replay(recordingTrigger);

        recorder.addStatistics(SEGMENT_STORE_NAME_2, statisticsToAdd2, streamsMetrics, taskId1);

        verify(recordingTrigger);
        verify(RocksDBMetrics.class);
    }

    @Test
    public void shouldAddItselfToRecordingTriggerWhenEmptyButInitialised() {
        mockStaticNice(RocksDBMetrics.class);
        replay(RocksDBMetrics.class);
        recorder.addStatistics(SEGMENT_STORE_NAME_1, statisticsToAdd1, streamsMetrics, taskId1);
        recorder.removeStatistics(SEGMENT_STORE_NAME_1);
        reset(recordingTrigger);
        recordingTrigger.addMetricsRecorder(recorder);
        replay(recordingTrigger);

        recorder.addStatistics(SEGMENT_STORE_NAME_2, statisticsToAdd2, streamsMetrics, taskId1);

        verify(recordingTrigger);
    }

    @Test
    public void shouldNotAddItselfToRecordingTriggerWhenNotEmpty() {
        mockStaticNice(RocksDBMetrics.class);
        replay(RocksDBMetrics.class);
        recorder.addStatistics(SEGMENT_STORE_NAME_1, statisticsToAdd1, streamsMetrics, taskId1);
        reset(recordingTrigger);
        replay(recordingTrigger);

        recorder.addStatistics(SEGMENT_STORE_NAME_2, statisticsToAdd2, streamsMetrics, taskId1);

        verify(recordingTrigger);
    }

    @Test
    public void shouldCloseStatisticsWhenStatisticsIsRemoved() {
        mockStaticNice(RocksDBMetrics.class);
        replay(RocksDBMetrics.class);
        recorder.addStatistics(SEGMENT_STORE_NAME_1, statisticsToAdd1, streamsMetrics, taskId1);
        reset(statisticsToAdd1);
        statisticsToAdd1.close();
        replay(statisticsToAdd1);

        recorder.removeStatistics(SEGMENT_STORE_NAME_1);

        verify(statisticsToAdd1);
    }

    @Test
    public void shouldRemoveItselfFromRecordingTriggerWhenLastStatisticsIsRemoved() {
        mockStaticNice(RocksDBMetrics.class);
        replay(RocksDBMetrics.class);
        recorder.addStatistics(SEGMENT_STORE_NAME_1, statisticsToAdd1, streamsMetrics, taskId1);
        recorder.addStatistics(SEGMENT_STORE_NAME_2, statisticsToAdd2, streamsMetrics, taskId1);
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
        mockStaticNice(RocksDBMetrics.class);
        replay(RocksDBMetrics.class);
        recorder.addStatistics(SEGMENT_STORE_NAME_1, statisticsToAdd1, streamsMetrics, taskId1);
        assertThrows(
            IllegalStateException.class,
            () -> recorder.removeStatistics(SEGMENT_STORE_NAME_2)
        );
    }

    @Test
    public void shouldRecordMetrics() {
        setUpMetricsMock();
        recorder.addStatistics(SEGMENT_STORE_NAME_1, statisticsToAdd1, streamsMetrics, taskId1);
        recorder.addStatistics(SEGMENT_STORE_NAME_2, statisticsToAdd2, streamsMetrics, taskId1);
        reset(statisticsToAdd1);
        reset(statisticsToAdd2);

        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.BYTES_WRITTEN)).andReturn(1L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.BYTES_WRITTEN)).andReturn(2L);
        bytesWrittenToDatabaseSensor.record(1 + 2);
        replay(bytesWrittenToDatabaseSensor);

        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.BYTES_READ)).andReturn(2L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.BYTES_READ)).andReturn(3L);
        bytesReadFromDatabaseSensor.record(2 + 3);
        replay(bytesReadFromDatabaseSensor);

        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.FLUSH_WRITE_BYTES)).andReturn(3L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.FLUSH_WRITE_BYTES)).andReturn(4L);
        memtableBytesFlushedSensor.record(3 + 4);
        replay(memtableBytesFlushedSensor);

        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.MEMTABLE_HIT)).andReturn(1L);
        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.MEMTABLE_MISS)).andReturn(2L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.MEMTABLE_HIT)).andReturn(3L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.MEMTABLE_MISS)).andReturn(4L);
        memtableHitRatioSensor.record((double) 4 / (4 + 6));
        replay(memtableHitRatioSensor);

        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.STALL_MICROS)).andReturn(4L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.STALL_MICROS)).andReturn(5L);
        writeStallDurationSensor.record(4 + 5);
        replay(writeStallDurationSensor);

        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.BLOCK_CACHE_DATA_HIT)).andReturn(5L);
        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.BLOCK_CACHE_DATA_MISS)).andReturn(4L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.BLOCK_CACHE_DATA_HIT)).andReturn(3L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.BLOCK_CACHE_DATA_MISS)).andReturn(2L);
        blockCacheDataHitRatioSensor.record((double) 8 / (8 + 6));
        replay(blockCacheDataHitRatioSensor);

        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.BLOCK_CACHE_INDEX_HIT)).andReturn(4L);
        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.BLOCK_CACHE_INDEX_MISS)).andReturn(2L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.BLOCK_CACHE_INDEX_HIT)).andReturn(2L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.BLOCK_CACHE_INDEX_MISS)).andReturn(4L);
        blockCacheIndexHitRatioSensor.record((double) 6 / (6 + 6));
        replay(blockCacheIndexHitRatioSensor);

        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.BLOCK_CACHE_FILTER_HIT)).andReturn(2L);
        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.BLOCK_CACHE_FILTER_MISS)).andReturn(4L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.BLOCK_CACHE_FILTER_HIT)).andReturn(3L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.BLOCK_CACHE_FILTER_MISS)).andReturn(5L);
        blockCacheFilterHitRatioSensor.record((double) 5 / (5 + 9));
        replay(blockCacheFilterHitRatioSensor);

        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.COMPACT_WRITE_BYTES)).andReturn(2L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.COMPACT_WRITE_BYTES)).andReturn(4L);
        bytesWrittenDuringCompactionSensor.record(2 + 4);
        replay(bytesWrittenDuringCompactionSensor);

        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.COMPACT_READ_BYTES)).andReturn(5L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.COMPACT_READ_BYTES)).andReturn(6L);
        bytesReadDuringCompactionSensor.record(5 + 6);
        replay(bytesReadDuringCompactionSensor);

        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.NO_FILE_OPENS)).andReturn(5L);
        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.NO_FILE_CLOSES)).andReturn(3L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.NO_FILE_OPENS)).andReturn(7L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.NO_FILE_CLOSES)).andReturn(4L);
        numberOfOpenFilesSensor.record((5 + 7) - (3 + 4));
        replay(numberOfOpenFilesSensor);

        expect(statisticsToAdd1.getAndResetTickerCount(TickerType.NO_FILE_ERRORS)).andReturn(34L);
        expect(statisticsToAdd2.getAndResetTickerCount(TickerType.NO_FILE_ERRORS)).andReturn(11L);
        numberOfFileErrorsSensor.record(11 + 34);
        replay(numberOfFileErrorsSensor);

        replay(statisticsToAdd1);
        replay(statisticsToAdd2);

        recorder.record();

        verify(statisticsToAdd1);
        verify(statisticsToAdd2);
        verify(bytesWrittenToDatabaseSensor);
    }

    @Test
    public void shouldCorrectlyHandleHitRatioRecordingsWithZeroHitsAndMisses() {
        setUpMetricsMock();
        recorder.addStatistics(SEGMENT_STORE_NAME_1, statisticsToAdd1, streamsMetrics, taskId1);
        resetToNice(statisticsToAdd1);
        expect(statisticsToAdd1.getTickerCount(anyObject())).andReturn(0L).anyTimes();
        replay(statisticsToAdd1);
        memtableHitRatioSensor.record(0);
        blockCacheDataHitRatioSensor.record(0);
        blockCacheIndexHitRatioSensor.record(0);
        blockCacheFilterHitRatioSensor.record(0);
        replay(memtableHitRatioSensor);
        replay(blockCacheDataHitRatioSensor);
        replay(blockCacheIndexHitRatioSensor);
        replay(blockCacheFilterHitRatioSensor);

        recorder.record();

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
}