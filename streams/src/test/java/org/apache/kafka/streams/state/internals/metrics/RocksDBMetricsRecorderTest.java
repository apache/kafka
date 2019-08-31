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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
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
    private final static String STORE_NAME = "store name";
    private final static String SEGMENT_STORE_NAME_1 = "segment store name 1";
    private final static String SEGMENT_STORE_NAME_2 = "segment  name 2";

    private final Statistics statisticsToAdd1 = mock(Statistics.class);
    private final Statistics statisticsToAdd2 = mock(Statistics.class);
    private final Sensor sensor = createMock(Sensor.class);
    private final StreamsMetricsImpl streamsMetrics = mock(StreamsMetricsImpl.class);
    private final TaskId taskId = new TaskId(0, 0);

    private final RocksDBMetricsRecorder recorder = new RocksDBMetricsRecorder(METRICS_SCOPE, STORE_NAME);

    @Test
    public void shouldSetStatsLevelToExceptDetailedTimers() {
        mockStaticNice(RocksDBMetrics.class);
        replay(RocksDBMetrics.class);
        statisticsToAdd1.setStatsLevel(StatsLevel.EXCEPT_DETAILED_TIMERS);
        replay(statisticsToAdd1);

        recorder.addStatistics(SEGMENT_STORE_NAME_1, statisticsToAdd1, streamsMetrics, taskId);

        verify(statisticsToAdd1);
    }

    @Test
    public void shouldInitMetricsOnlyWhenFirstStatisticsIsAdded() {
        replayMetricsInitialization();
        statisticsToAdd1.setStatsLevel(StatsLevel.EXCEPT_DETAILED_TIMERS);
        replay(statisticsToAdd1);
        recorder.addStatistics(SEGMENT_STORE_NAME_1, statisticsToAdd1, streamsMetrics, taskId);
        verify(RocksDBMetrics.class);

        mockStatic(RocksDBMetrics.class);
        replay(RocksDBMetrics.class);
        recorder.addStatistics(SEGMENT_STORE_NAME_2, statisticsToAdd2, streamsMetrics, taskId);
        verify(RocksDBMetrics.class);
    }

    @Test
    public void shouldCloseStatisticsWhenRecorderIsClosed() {
        mockStaticNice(RocksDBMetrics.class);
        replay(RocksDBMetrics.class);
        recorder.addStatistics(SEGMENT_STORE_NAME_1, statisticsToAdd1, streamsMetrics, taskId);
        recorder.addStatistics(SEGMENT_STORE_NAME_2, statisticsToAdd2, streamsMetrics, taskId);
        reset(statisticsToAdd1);
        reset(statisticsToAdd2);
        statisticsToAdd1.close();
        statisticsToAdd2.close();
        replay(statisticsToAdd1);
        replay(statisticsToAdd2);

        recorder.close();

        verify(statisticsToAdd1);
        verify(statisticsToAdd2);
    }

    @Test
    public void shouldCloseStatisticsWhenStatisticsIsRemoved() {
        mockStaticNice(RocksDBMetrics.class);
        replay(RocksDBMetrics.class);
        recorder.addStatistics(SEGMENT_STORE_NAME_1, statisticsToAdd1, streamsMetrics, taskId);
        reset(statisticsToAdd1);
        statisticsToAdd1.close();
        replay(statisticsToAdd1);

        recorder.removeStatistics(SEGMENT_STORE_NAME_1);

        verify(statisticsToAdd1);
    }

    private void replayMetricsInitialization() {
        mockStatic(RocksDBMetrics.class);
        final RocksDBMetricContext metricsContext =
            new RocksDBMetricContext(taskId.toString(), METRICS_SCOPE, STORE_NAME);
        expect(RocksDBMetrics.bytesWrittenToDatabaseSensor(eq(streamsMetrics), eq(metricsContext))).andReturn(sensor);
        expect(RocksDBMetrics.bytesReadFromDatabaseSensor(eq(streamsMetrics), eq(metricsContext))).andReturn(sensor);
        expect(RocksDBMetrics.memtableBytesFlushedSensor(eq(streamsMetrics), eq(metricsContext))).andReturn(sensor);
        expect(RocksDBMetrics.memtableHitRatioSensor(eq(streamsMetrics), eq(metricsContext))).andReturn(sensor);
        expect(RocksDBMetrics.memtableAvgFlushTimeSensor(eq(streamsMetrics), eq(metricsContext))).andReturn(sensor);
        expect(RocksDBMetrics.memtableMinFlushTimeSensor(eq(streamsMetrics), eq(metricsContext))).andReturn(sensor);
        expect(RocksDBMetrics.memtableMaxFlushTimeSensor(eq(streamsMetrics), eq(metricsContext))).andReturn(sensor);
        expect(RocksDBMetrics.writeStallDurationSensor(eq(streamsMetrics), eq(metricsContext))).andReturn(sensor);
        expect(RocksDBMetrics.blockCacheDataHitRatioSensor(eq(streamsMetrics), eq(metricsContext))).andReturn(sensor);
        expect(RocksDBMetrics.blockCacheIndexHitRatioSensor(eq(streamsMetrics), eq(metricsContext))).andReturn(sensor);
        expect(RocksDBMetrics.blockCacheFilterHitRatioSensor(eq(streamsMetrics), eq(metricsContext))).andReturn(sensor);
        expect(
            RocksDBMetrics.bytesReadDuringCompactionSensor(eq(streamsMetrics), eq(metricsContext))
        ).andReturn(sensor);
        expect(
            RocksDBMetrics.bytesWrittenDuringCompactionSensor(eq(streamsMetrics), eq(metricsContext))
        ).andReturn(sensor);
        expect(RocksDBMetrics.compactionTimeMinSensor(eq(streamsMetrics), eq(metricsContext))).andReturn(sensor);
        expect(RocksDBMetrics.compactionTimeMaxSensor(eq(streamsMetrics), eq(metricsContext))).andReturn(sensor);
        expect(RocksDBMetrics.compactionTimeAvgSensor(eq(streamsMetrics), eq(metricsContext))).andReturn(sensor);
        expect(RocksDBMetrics.numberOfOpenFilesSensor(eq(streamsMetrics), eq(metricsContext))).andReturn(sensor);
        expect(RocksDBMetrics.numberOfFileErrorsSensor(eq(streamsMetrics), eq(metricsContext))).andReturn(sensor);
        replay(RocksDBMetrics.class);
    }
}