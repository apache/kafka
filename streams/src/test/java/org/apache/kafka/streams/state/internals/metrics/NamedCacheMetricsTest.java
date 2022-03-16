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
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class NamedCacheMetricsTest {

    private static final String THREAD_ID = "test-thread";
    private static final String TASK_ID = "test-task";
    private static final String STORE_NAME = "storeName";
    private static final String HIT_RATIO_AVG_DESCRIPTION = "The average cache hit ratio";
    private static final String HIT_RATIO_MIN_DESCRIPTION = "The minimum cache hit ratio";
    private static final String HIT_RATIO_MAX_DESCRIPTION = "The maximum cache hit ratio";

    private final StreamsMetricsImpl streamsMetrics = mock(StreamsMetricsImpl.class);
    private final Sensor expectedSensor = mock(Sensor.class);
    private final Map<String, String> tagMap = mkMap(mkEntry("key", "value"));

    @Test
    public void shouldGetHitRatioSensorWithBuiltInMetricsVersionCurrent() {
        final String hitRatio = "hit-ratio";
        when(streamsMetrics.cacheLevelSensor(THREAD_ID, TASK_ID, STORE_NAME, hitRatio, RecordingLevel.DEBUG)).thenReturn(expectedSensor);
        when(streamsMetrics.cacheLevelTagMap(THREAD_ID, TASK_ID, STORE_NAME)).thenReturn(tagMap);
        StreamsMetricsImpl.addAvgAndMinAndMaxToSensor(
            expectedSensor,
            StreamsMetricsImpl.CACHE_LEVEL_GROUP,
            tagMap,
            hitRatio,
            HIT_RATIO_AVG_DESCRIPTION,
            HIT_RATIO_MIN_DESCRIPTION,
            HIT_RATIO_MAX_DESCRIPTION);

        final Sensor sensor = NamedCacheMetrics.hitRatioSensor(streamsMetrics, THREAD_ID, TASK_ID, STORE_NAME);

        assertThat(sensor, is(expectedSensor));
    }

}
