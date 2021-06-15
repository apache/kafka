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
    private final static MetricName REGISTERED_BROKER_COUNT = new MetricName(
        "kafka.controller", "KafkaController", "RegisteredBrokerCount", null);
    private final static MetricName UNFENCED_BROKER_COUNT = new MetricName(
        "kafka.controller", "KafkaController", "UnfencedBrokerCount", null);
    private final static MetricName GLOBAL_TOPIC_COUNT = new MetricName(
        "kafka.controller", "KafkaController", "GlobalTopicCount", null);
    private final static MetricName GLOBAL_PARTITION_COUNT = new MetricName(
        "kafka.controller", "KafkaController", "GlobalPartitionCount", null);
    private final static MetricName OFFLINE_PARTITION_COUNT = new MetricName(
        "kafka.controller", "KafkaController", "OfflinePartitionCount", null);
    private final static MetricName PREFERRED_REPLICA_IMBALANCE_COUNT = new MetricName(
        "kafka.controller", "KafkaController", "PreferredReplicaImbalanceCount", null);
    
    private volatile boolean active;
    private volatile int registeredBrokerCount;
    private volatile int unfencedBrokerCount;
    private volatile int globalTopicCount;
    private volatile int globalPartitionCount;
    private volatile int offlinePartitionCount;
    private volatile int preferredReplicaImbalanceCount;
    private final Gauge<Integer> activeControllerCount;
    private final Gauge<Integer> registeredBrokerCountGauge;
    private final Gauge<Integer> unfencedBrokerCountGauge;
    private final Gauge<Integer> globalPartitionCountGauge;
    private final Gauge<Integer> globalTopicCountGauge;
    private final Gauge<Integer> offlinePartitionCountGauge;
    private final Gauge<Integer> preferredReplicaImbalanceCountGauge;
    private final Histogram eventQueueTime;
    private final Histogram eventQueueProcessingTime;

    public QuorumControllerMetrics(MetricsRegistry registry) {
        this.active = false;
        this.registeredBrokerCount = 0;
        this.unfencedBrokerCount = 0;
        this.globalTopicCount = 0;
        this.globalPartitionCount = 0;
        this.offlinePartitionCount = 0;
        this.preferredReplicaImbalanceCount = 0;
        this.activeControllerCount = registry.newGauge(ACTIVE_CONTROLLER_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return active ? 1 : 0;
            }
        });
        this.eventQueueTime = registry.newHistogram(EVENT_QUEUE_TIME_MS, true);
        this.eventQueueProcessingTime = registry.newHistogram(EVENT_QUEUE_PROCESSING_TIME_MS, true);
        this.registeredBrokerCountGauge = registry.newGauge(REGISTERED_BROKER_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return registeredBrokerCount;
            }
        });
        this.unfencedBrokerCountGauge = registry.newGauge(UNFENCED_BROKER_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return unfencedBrokerCount;
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
        this.offlinePartitionCountGauge = registry.newGauge(OFFLINE_PARTITION_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return offlinePartitionCount;
            }
        });
        this.preferredReplicaImbalanceCountGauge = registry.newGauge(PREFERRED_REPLICA_IMBALANCE_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return preferredReplicaImbalanceCount;
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
    public void setRegisteredBrokerCount(int brokerCount) {
        this.registeredBrokerCount = brokerCount;
    }

    @Override
    public int registeredBrokerCount() {
        return this.registeredBrokerCount;
    }

    public void setUnfencedBrokerCount(int unfencedBrokerCount) {
        this.unfencedBrokerCount = unfencedBrokerCount;
    }

    @Override
    public int unfencedBrokerCount() {
        return this.unfencedBrokerCount;
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

    @Override
    public void setOfflinePartitionCount(int offlinePartitions) {
        this.offlinePartitionCount = offlinePartitions;
    }

    @Override
    public int offlinePartitionCount() {
        return this.offlinePartitionCount;
    }

    @Override
    public void setPreferredReplicaImbalanceCount(int replicaImbalances) {
        this.preferredReplicaImbalanceCount = replicaImbalances;
    }

    @Override
    public int preferredReplicaImbalanceCount() {
        return this.preferredReplicaImbalanceCount;
    }
}
