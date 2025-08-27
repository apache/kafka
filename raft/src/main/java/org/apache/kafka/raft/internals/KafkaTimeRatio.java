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

package org.apache.kafka.raft.internals;

import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.MetricConfig;

/**
 * Kafka Metrics facade for TimeRatio.
 * This facade adapts the TimeRatio core implementation to work with
 * Kafka Metrics by implementing MeasurableStat interface.
 */
public class KafkaTimeRatio implements MeasurableStat {
    private final TimeRatio timeRatio;

    public KafkaTimeRatio(double defaultRatio) {
        this.timeRatio = new TimeRatio(defaultRatio);
    }

    @Override
    public double measure(MetricConfig config, long currentTimestampMs) {
        return timeRatio.measure();
    }

    @Override
    public void record(MetricConfig config, double value, long currentTimestampMs) {
        timeRatio.record(value, currentTimestampMs);
    }
}