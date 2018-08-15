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

import java.util.Map;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addAvgMaxLatency;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.addInvocationRateAndCount;

public final class Sensors {
    
    private Sensors() {}

    public static Sensor createTaskAndStoreLatencyAndThroughputSensors(final Sensor.RecordingLevel level,
                                                                       final String operation,
                                                                       final StreamsMetricsImpl metrics,
                                                                       final String metricsGroup,
                                                                       final String taskName,
                                                                       final String storeName,
                                                                       final Map<String, String> taskTags,
                                                                       final Map<String, String> storeTags) {
        final Sensor taskSensor = metrics.taskLevelSensor(taskName, operation, level);
        addAvgMaxLatency(taskSensor, metricsGroup, taskTags, operation);
        addInvocationRateAndCount(taskSensor, metricsGroup, taskTags, operation);
        final Sensor sensor = metrics.storeLevelSensor(taskName, storeName, operation, level, taskSensor);
        addAvgMaxLatency(sensor, metricsGroup, storeTags, operation);
        addInvocationRateAndCount(sensor, metricsGroup, storeTags, operation);
        return sensor;
    }
}

