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

package org.apache.kafka.controller;

import org.apache.kafka.queue.EventQueueMetrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

public final class QuorumControllerMetrics implements ControllerMetrics, EventQueueMetrics {
    private final static MetricName ACTIVE_CONTROLLER_COUNT = new MetricName(
        "kafka.controller", "KafkaController", "ActiveControllerCount", null);
    private final static MetricName EVENT_QUEUE_TIME_MS = new MetricName(
        "kafka.controller", "ControllerEventManager", "EventQueueTimeMs", null);
    private final static MetricName EVENT_QUEUE_PROCESSING_TIME_MS = new MetricName(
        "kafka.controller", "ControllerEventManager", "EventQueueProcessingTimeMs", null);
    private final static MetricName EVENT_QUEUE_SIZE = new MetricName(
        "kafka.controller", "ControllerEventManager", "EventQueueSize", null);
    private final static MetricName GLOBAL_TOPIC_COUNT = new MetricName(
        "kafka.controller", "KafkaController", "GlobalTopicCount", null);
    private final static MetricName GLOBAL_PARTITION_COUNT = new MetricName(
        "kafka.controller", "KafkaController", "GlobalPartitionCount", null);

    private volatile boolean active;
    private volatile int globalTopicCount;
    private volatile int globalPartitionCount;
    private volatile int eventQueueSize;
    private final Gauge<Integer> activeControllerCount;
    private final Gauge<Integer> globalPartitionCountGauge;
    private final Gauge<Integer> globalTopicCountGauge;
    private final Gauge<Integer> eventQueueSizeGauge;
    private final Histogram eventQueueTime;
    private final Histogram eventQueueProcessingTime;

    public QuorumControllerMetrics(MetricsRegistry registry) {
        this.active = false;
        this.globalTopicCount = 0;
        this.globalPartitionCount = 0;
        this.eventQueueSize = 0;
        this.activeControllerCount = registry.newGauge(ACTIVE_CONTROLLER_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return active ? 1 : 0;
            }
        });
        this.eventQueueTime = registry.newHistogram(EVENT_QUEUE_TIME_MS, true);
        this.eventQueueProcessingTime = registry.newHistogram(EVENT_QUEUE_PROCESSING_TIME_MS, true);
        this.eventQueueSizeGauge = registry.newGauge(EVENT_QUEUE_SIZE, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return eventQueueSize;
            }
        });
        this.globalTopicCountGauge = registry.newGauge(GLOBAL_TOPIC_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return globalTopicCount;
            }
        });
        this.globalPartitionCountGauge = registry.newGauge(GLOBAL_PARTITION_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return globalPartitionCount;
            }
        });
    }

    @Override
    public void setActive(boolean active) {
        this.active = active;
    }

    @Override
    public boolean active() {
        return this.active;
    }

    @Override
    public void updateEventQueueTime(long durationMs) {
        eventQueueTime.update(durationMs);
    }

    @Override
    public void updateEventQueueProcessingTime(long durationMs) {
        eventQueueProcessingTime.update(durationMs);
    }

    @Override
    public void setEventQueueSize(int eventQueueSize) {
        this.eventQueueSize = eventQueueSize;
    }

    @Override
    public int eventQueueSize() {
        return this.eventQueueSize;
    }

    @Override
    public void setGlobalTopicsCount(int topicCount) {
        this.globalTopicCount = topicCount;
    }

    @Override
    public int globalTopicsCount() {
        return this.globalTopicCount;
    }

    @Override
    public void setGlobalPartitionCount(int partitionCount) {
        this.globalPartitionCount = partitionCount;
    }

    @Override
    public int globalPartitionCount() {
        return this.globalPartitionCount;
    }
}
