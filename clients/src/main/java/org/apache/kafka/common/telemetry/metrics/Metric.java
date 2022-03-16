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
package org.apache.kafka.common.telemetry.metrics;

import java.time.Instant;
import java.util.StringJoiner;

public class Metric implements MetricKeyable {

    private final MetricKeyable metricKeyable;

    private final MetricType metricType;

    private final Number value;

    private final Instant timestamp;

    private final Instant startTimestamp;

    private final boolean deltaTemporality;

    public Metric(MetricKeyable metricKeyable, MetricType metricType, Number value, Instant timestamp) {
        this(metricKeyable, metricType, value, timestamp, null, false);
    }

    public Metric(MetricKeyable metricKeyable,
        MetricType metricType,
        Number value,
        Instant timestamp,
        Instant startTimestamp,
        boolean deltaTemporality) {
        this.metricKeyable = metricKeyable;
        this.metricType = metricType;
        this.value = value;
        this.timestamp = timestamp;
        this.startTimestamp = startTimestamp;
        this.deltaTemporality = deltaTemporality;
    }

    @Override
    public MetricKey key() {
        return metricKeyable.key();
    }

    public MetricType metricType() {
        return metricType;
    }

    public Number value() {
        return value;
    }

    public Instant timestamp() {
        return timestamp;
    }

    public Instant startTimestamp() {
        return startTimestamp;
    }

    public boolean deltaTemporality() {
        return deltaTemporality;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", Metric.class.getSimpleName() + "[", "]")
            .add("metricKeyable=" + metricKeyable)
            .add("metricType=" + metricType)
            .add("value=" + value)
            .add("timestamp=" + timestamp)
            .add("startTimestamp=" + startTimestamp)
            .add("deltaTemporality=" + deltaTemporality)
            .toString();
    }
}
