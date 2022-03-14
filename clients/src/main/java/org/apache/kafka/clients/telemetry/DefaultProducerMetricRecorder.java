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
package org.apache.kafka.clients.telemetry;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;

/**
 * A sensor registry that exposes {@link Sensor}s used to record the producer-level metrics.
 *
 * @see ProducerTopicMetricRecorder for details on recording topic-level metrics.
 */

public class DefaultProducerMetricRecorder extends MetricRecorder implements ProducerMetricRecorder {

    private static final String GROUP_NAME = "producer-telemetry";

    private final MetricName recordQueueBytes;

    private final MetricName recordQueueMaxBytes;

    private final MetricName recordQueueCount;

    private final MetricName recordQeueMaxCount;

    public DefaultProducerMetricRecorder(Metrics metrics) {
        super(metrics);

        this.recordQueueBytes = createMetricName(RECORD_QUEUE_BYTES_NAME, GROUP_NAME, RECORD_QUEUE_BYTES_DESCRIPTION);
        this.recordQueueMaxBytes = createMetricName(RECORD_QUEUE_MAX_BYTES_NAME, GROUP_NAME, RECORD_QUEUE_MAX_BYTES_DESCRIPTION);
        this.recordQueueCount = createMetricName(RECORD_QUEUE_COUNT_NAME, GROUP_NAME, RECORD_QUEUE_COUNT_DESCRIPTION);
        this.recordQeueMaxCount = createMetricName(RECORD_QUEUE_MAX_COUNT_NAME, GROUP_NAME, RECORD_QUEUE_MAX_COUNT_DESCRIPTION);
    }

    @Override
    public void incrementRecordQueueBytes(long amount) {
        gaugeUpdateSensor(recordQueueBytes).record(amount);
    }

    @Override
    public void incrementRecordQueueMaxBytes(long amount) {
        gaugeUpdateSensor(recordQueueMaxBytes).record(amount);
    }

    @Override
    public void incrementRecordQueueCount(long amount) {
        gaugeUpdateSensor(recordQueueCount).record(amount);
    }

    @Override
    public void incrementRecordQueueMaxCount(long amount) {
        gaugeUpdateSensor(recordQeueMaxCount).record(amount);
    }

}
