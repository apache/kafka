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

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;


public final class QuorumControllerMetrics implements ControllerMetrics {
    private final static MetricName ACTIVE_CONTROLLER_COUNT = new MetricName(
        "kafka.controller", "KafkaController", "ActiveControllerCount", null);
    private final static MetricName EVENT_QUEUE_TIME_MS = new MetricName(
        "kafka.controller", "ControllerEventManager", "EventQueueTimeMs", null);
    private final static MetricName EVENT_QUEUE_PROCESSING_TIME_MS = new MetricName(
        "kafka.controller", "ControllerEventManager", "EventQueueProcessingTimeMs", null);
    private final static MetricName GLOBAL_TOPIC_COUNT = new MetricName(
        "kafka.controller", "ReplicationControlManager", "GlobalTopicCount", null);
    private final static MetricName GLOBAL_PARTITION_COUNT = new MetricName(
        "kafka.controller", "ReplicationControlManager", "GlobalPartitionCount", null);
    

    private volatile boolean active;
    private final Gauge<Integer> activeControllerCount;
    private final Histogram eventQueueTime;
    private final Histogram eventQueueProcessingTime;
    private int topicCount;
    private final Gauge<Integer> globalTopicCount;
    private int partitionCount;
    private final Gauge<Integer> globalPartitionCount;

    public QuorumControllerMetrics(MetricsRegistry registry) {
        this.active = false;
        this.activeControllerCount = registry.newGauge(ACTIVE_CONTROLLER_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return active ? 1 : 0;
            }
        });
        this.eventQueueTime = registry.newHistogram(EVENT_QUEUE_TIME_MS, true);
        this.eventQueueProcessingTime = registry.newHistogram(EVENT_QUEUE_PROCESSING_TIME_MS, true);
        this.topicCount = 0;
        this.globalTopicCount = registry.newGauge(GLOBAL_TOPIC_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return topicCount;
            }
        });
        this.globalPartitionCount = registry.newGauge(GLOBAL_TOPIC_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return partitionCount;
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
        eventQueueTime.update(durationMs);
    }

    @Override
    public int topicCount() {
        return topicCount;
    }

    @Override
    public void incTopicCount() {
        topicCount++;
    }

    @Override
    public void decTopicCount() {
        topicCount--;
    }

    @Override
    public int partitionCount() {
        return partitionCount;
    }

    @Override
    public void incPartitionCount() {
        partitionCount++;
    }

    @Override
    public void decPartitionCount() {
        partitionCount--;
    }
}
