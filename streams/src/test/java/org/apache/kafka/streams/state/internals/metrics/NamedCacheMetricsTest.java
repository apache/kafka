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
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.Version;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Map;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.powermock.api.easymock.PowerMock.createMock;
import static org.powermock.api.easymock.PowerMock.mockStatic;
import static org.powermock.api.easymock.PowerMock.replay;
import static org.powermock.api.easymock.PowerMock.verify;


@RunWith(PowerMockRunner.class)
@PrepareForTest({StreamsMetricsImpl.class, Sensor.class})
public class NamedCacheMetricsTest {

    private static final String THREAD_ID = "test-thread";
    private static final String TASK_ID = "test-task";
    private static final String STORE_NAME = "storeName";
    private static final String HIT_RATIO_AVG_DESCRIPTION = "The average cache hit ratio";
    private static final String HIT_RATIO_MIN_DESCRIPTION = "The minimum cache hit ratio";
    private static final String HIT_RATIO_MAX_DESCRIPTION = "The maximum cache hit ratio";

    private final StreamsMetricsImpl streamsMetrics = createMock(StreamsMetricsImpl.class);
    private final Sensor expectedSensor = mock(Sensor.class);
    private final Map<String, String> tagMap = mkMap(mkEntry("key", "value"));

    @Test
    public void shouldGetHitRatioSensorWithBuiltInMetricsVersionCurrent() {
        final String hitRatio = "hit-ratio";
        mockStatic(StreamsMetricsImpl.class);
        setUpStreamsMetrics(Version.LATEST, hitRatio);
        replay(streamsMetrics);
        replay(StreamsMetricsImpl.class);

        final Sensor sensor = NamedCacheMetrics.hitRatioSensor(streamsMetrics, THREAD_ID, TASK_ID, STORE_NAME);

        verifyResult(sensor);
    }

    @Test
    public void shouldGetHitRatioSensorWithBuiltInMetricsVersionBefore24() {
        final Map<String, String> parentTagMap = mkMap(mkEntry("key", "all"));
        final String hitRatio = "hitRatio";
        final RecordingLevel recordingLevel = RecordingLevel.DEBUG;
        mockStatic(StreamsMetricsImpl.class);
        final Sensor parentSensor = mock(Sensor.class);
        expect(streamsMetrics.taskLevelSensor(THREAD_ID, TASK_ID, hitRatio, recordingLevel)).andReturn(parentSensor);
        expect(streamsMetrics.cacheLevelTagMap(THREAD_ID, TASK_ID, StreamsMetricsImpl.ROLLUP_VALUE))
            .andReturn(parentTagMap);
        StreamsMetricsImpl.addAvgAndMinAndMaxToSensor(
            parentSensor,
            StreamsMetricsImpl.CACHE_LEVEL_GROUP,
            parentTagMap,
            hitRatio,
            HIT_RATIO_AVG_DESCRIPTION,
            HIT_RATIO_MIN_DESCRIPTION,
            HIT_RATIO_MAX_DESCRIPTION);
        setUpStreamsMetrics(Version.FROM_0100_TO_24, hitRatio, parentSensor);
        replay(streamsMetrics);
        replay(StreamsMetricsImpl.class);

        final Sensor sensor = NamedCacheMetrics.hitRatioSensor(streamsMetrics, THREAD_ID, TASK_ID, STORE_NAME);

        verifyResult(sensor);
    }

    private void setUpStreamsMetrics(final Version builtInMetricsVersion,
                                     final String hitRatio,
                                     final Sensor... parents) {
        expect(streamsMetrics.version()).andReturn(builtInMetricsVersion);
        expect(streamsMetrics.cacheLevelSensor(THREAD_ID, TASK_ID, STORE_NAME, hitRatio, RecordingLevel.DEBUG, parents))
            .andReturn(expectedSensor);
        expect(streamsMetrics.cacheLevelTagMap(THREAD_ID, TASK_ID, STORE_NAME)).andReturn(tagMap);
        StreamsMetricsImpl.addAvgAndMinAndMaxToSensor(
            expectedSensor,
            StreamsMetricsImpl.CACHE_LEVEL_GROUP,
            tagMap,
            hitRatio,
            HIT_RATIO_AVG_DESCRIPTION,
            HIT_RATIO_MIN_DESCRIPTION,
            HIT_RATIO_MAX_DESCRIPTION);
    }

    private void verifyResult(final Sensor sensor) {
        verify(streamsMetrics);
        verify(StreamsMetricsImpl.class);
        assertThat(sensor, is(expectedSensor));
    }
}