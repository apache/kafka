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

package org.apache.kafka.controller.metrics;

import com.yammer.metrics.core.Gauge;
import org.apache.kafka.raft.internals.TimeRatio;

/**
 * Yammer Metrics facade for TimeRatio.
 * This class provides a Gauge interface for Yammer metrics registry
 * while using the same shared TimeRatio implementation that
 * the Kafka Metrics KafkaTimeRatio uses.
 */
public class YammerTimeRatio extends Gauge<Double> {
    private final TimeRatio timeRatio;

    public YammerTimeRatio(double defaultRatio) {
        this.timeRatio = new TimeRatio(defaultRatio);
    }

    /**
     * Record an idle/wait duration.
     *
     * @param idleDurationMs The duration of the idle/wait period in milliseconds
     * @param currentTimeMs The current time in milliseconds
     */
    public void record(double idleDurationMs, long currentTimeMs) {
        timeRatio.record(idleDurationMs, currentTimeMs);
    }

    /**
     * Get the current idle ratio for Yammer Metrics.
     * 
     * @return The ratio of idle time to total time (between 0.0 and 1.0)
     */
    @Override
    public Double value() { return timeRatio.measure(); }
}