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
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.CACHE_LEVEL_GROUP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addAvgAndMinAndMaxToSensor;

public class NamedCacheMetrics {
    private NamedCacheMetrics() {}

    private static final String HIT_RATIO = "hit-ratio";
    private static final String HIT_RATIO_AVG_DESCRIPTION = "The average cache hit ratio";
    private static final String HIT_RATIO_MIN_DESCRIPTION = "The minimum cache hit ratio";
    private static final String HIT_RATIO_MAX_DESCRIPTION = "The maximum cache hit ratio";


    public static Sensor hitRatioSensor(final StreamsMetricsImpl streamsMetrics,
                                        final String threadId,
                                        final String taskName,
                                        final String storeName) {

        final Sensor hitRatioSensor;
        final String hitRatioName;
        hitRatioName = HIT_RATIO;
        hitRatioSensor = streamsMetrics.cacheLevelSensor(
            threadId,
            taskName,
            storeName,
            hitRatioName,
            Sensor.RecordingLevel.DEBUG
        );
        addAvgAndMinAndMaxToSensor(
            hitRatioSensor,
            CACHE_LEVEL_GROUP,
            streamsMetrics.cacheLevelTagMap(threadId, taskName, storeName),
            hitRatioName,
            HIT_RATIO_AVG_DESCRIPTION,
            HIT_RATIO_MIN_DESCRIPTION,
            HIT_RATIO_MAX_DESCRIPTION
        );
        return hitRatioSensor;
    }
}
