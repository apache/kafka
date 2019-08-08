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

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.powermock.api.easymock.PowerMock.createMock;
import static org.powermock.api.easymock.PowerMock.mockStatic;
import static org.powermock.api.easymock.PowerMock.mockStaticNice;
import static org.powermock.api.easymock.PowerMock.replay;
import static org.powermock.api.easymock.PowerMock.replayAll;
import static org.powermock.api.easymock.PowerMock.verify;
import static org.powermock.api.easymock.PowerMock.verifyAll;

@RunWith(PowerMockRunner.class)
@PrepareForTest({RocksDBMetrics.class, Sensor.class})
public class RocksDBMetricsRecorderTest {

    private final static String METRICS_SCOPE = "test-state-id";
    private final static String STORE_NAME = "test store";
    private final RocksDBMetricsRecorder recorder = new RocksDBMetricsRecorder(METRICS_SCOPE, STORE_NAME);

    private final Statistics statisticsToAdd1 = createMock(Statistics.class);
    private final Statistics statisticsToAdd2 = createMock(Statistics.class);
    private final String statisticsId1 = "statistics ID 1";
    private final String statisticsId2 = "statistics ID 2";

    private final Sensor sensor = createMock(Sensor.class);
    private final StreamsMetricsImpl streamsMetrics = createMock(StreamsMetricsImpl.class);
    private final TaskId taskId = new TaskId(0, 0);

    @Test
    public void shouldInitMetricsOnlyWhenFirstStatisticsIsAdded() {
        replayMetricsInitialization();
        recorder.addStatistics(statisticsId1, statisticsToAdd1, streamsMetrics, taskId);
        verify(RocksDBMetrics.class);

        mockStatic(RocksDBMetrics.class);
        replay(RocksDBMetrics.class);
        recorder.addStatistics(statisticsId2, statisticsToAdd2, streamsMetrics, taskId);
        verify(RocksDBMetrics.class);
    }

    @Test
    public void shouldCloseStatisticsWhenRecorderIsClosed() {
        mockStaticNice(RocksDBMetrics.class);
        statisticsToAdd1.close();
        statisticsToAdd2.close();
        replayAll();
        recorder.addStatistics(statisticsId1, statisticsToAdd1, streamsMetrics, taskId);
        recorder.addStatistics(statisticsId2, statisticsToAdd2, streamsMetrics, taskId);

        recorder.close();

        verifyAll();
    }

    @Test
    public void shouldCloseStatisticsWhenStatisticsWithSameIdIsAdded() {
        mockStaticNice(RocksDBMetrics.class);
        statisticsToAdd1.close();
        replayAll();
        recorder.addStatistics(statisticsId1, statisticsToAdd1, streamsMetrics, taskId);

        recorder.addStatistics(statisticsId1, statisticsToAdd2, streamsMetrics, taskId);

        verifyAll();
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