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
package org.apache.kafka.streams.kstream.internals.metrics;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.LATE_RECORD_DROP;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.PROCESSOR_NODE_LEVEL_GROUP;

public class Sensors {
    private Sensors() {}

    public static Sensor lateRecordDropSensor(final InternalProcessorContext context) {
        final StreamsMetricsImpl metrics = context.metrics();
        final String threadId = Thread.currentThread().getName();

        final Sensor sensor = metrics.nodeLevelSensor(
            threadId,
            context.taskId().toString(),
            context.currentNode().name(),
            LATE_RECORD_DROP,
            Sensor.RecordingLevel.INFO
        );
        StreamsMetricsImpl.addInvocationRateAndCountToSensor(
            sensor,
            PROCESSOR_NODE_LEVEL_GROUP,
            metrics.nodeLevelTagMap(threadId, context.taskId().toString(), context.currentNode().name()),
            LATE_RECORD_DROP
        );
        return sensor;
    }
}
