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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class QuorumControllerMetrics implements ControllerMetrics {
    private final static MetricName ACTIVE_CONTROLLER_COUNT = getMetricName(
        "KafkaController", "ActiveControllerCount");
    private final static MetricName EVENT_QUEUE_TIME_MS = getMetricName(
        "ControllerEventManager", "EventQueueTimeMs");
    private final static MetricName EVENT_QUEUE_PROCESSING_TIME_MS = getMetricName(
        "ControllerEventManager", "EventQueueProcessingTimeMs");
    private final static MetricName FENCED_BROKER_COUNT = getMetricName(
        "KafkaController", "FencedBrokerCount");
    private final static MetricName ACTIVE_BROKER_COUNT = getMetricName(
        "KafkaController", "ActiveBrokerCount");
    private final static MetricName GLOBAL_TOPIC_COUNT = getMetricName(
        "KafkaController", "GlobalTopicCount");
    private final static MetricName GLOBAL_PARTITION_COUNT = getMetricName(
        "KafkaController", "GlobalPartitionCount");
    private final static MetricName OFFLINE_PARTITION_COUNT = getMetricName(
        "KafkaController", "OfflinePartitionsCount");
    private final static MetricName PREFERRED_REPLICA_IMBALANCE_COUNT = getMetricName(
        "KafkaController", "PreferredReplicaImbalanceCount");
    private final static MetricName METADATA_ERROR_COUNT = getMetricName(
        "KafkaController", "MetadataErrorCount");
    private final static MetricName LAST_APPLIED_RECORD_OFFSET = getMetricName(
        "KafkaController", "LastAppliedRecordOffset");
    private final static MetricName LAST_COMMITTED_RECORD_OFFSET = getMetricName(
        "KafkaController", "LastCommittedRecordOffset");
    private final static MetricName LAST_APPLIED_RECORD_TIMESTAMP = getMetricName(
        "KafkaController", "LastAppliedRecordTimestamp");
    private final static MetricName LAST_APPLIED_RECORD_LAG_MS = getMetricName(
        "KafkaController", "LastAppliedRecordLagMs");

    private final MetricsRegistry registry;
    private volatile boolean active;
    private volatile int fencedBrokerCount;
    private volatile int activeBrokerCount;
    private volatile int globalTopicCount;
    private volatile int globalPartitionCount;
    private volatile int offlinePartitionCount;
    private volatile int preferredReplicaImbalanceCount;
    private final AtomicInteger metadataErrorCount;
    private final AtomicLong lastAppliedRecordOffset = new AtomicLong(0);
    private final AtomicLong lastCommittedRecordOffset = new AtomicLong(0);
    private final AtomicLong lastAppliedRecordTimestamp = new AtomicLong(0);
    private final Gauge<Integer> activeControllerCount;
    private final Gauge<Integer> fencedBrokerCountGauge;
    private final Gauge<Integer> activeBrokerCountGauge;
    private final Gauge<Integer> globalPartitionCountGauge;
    private final Gauge<Integer> globalTopicCountGauge;
    private final Gauge<Integer> offlinePartitionCountGauge;
    private final Gauge<Integer> preferredReplicaImbalanceCountGauge;
    private final Gauge<Integer> metadataErrorCountGauge;
    private final Gauge<Long> lastAppliedRecordOffsetGauge;
    private final Gauge<Long> lastCommittedRecordOffsetGauge;
    private final Gauge<Long> lastAppliedRecordTimestampGauge;
    private final Gauge<Long> lastAppliedRecordLagMsGauge;
    private final Histogram eventQueueTime;
    private final Histogram eventQueueProcessingTime;

    public QuorumControllerMetrics(
        MetricsRegistry registry,
        Time time
    ) {
        this.registry = Objects.requireNonNull(registry);
        this.active = false;
        this.fencedBrokerCount = 0;
        this.activeBrokerCount = 0;
        this.globalTopicCount = 0;
        this.globalPartitionCount = 0;
        this.offlinePartitionCount = 0;
        this.preferredReplicaImbalanceCount = 0;
        this.metadataErrorCount = new AtomicInteger(0);
        this.activeControllerCount = registry.newGauge(ACTIVE_CONTROLLER_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return active ? 1 : 0;
            }
        });
        this.eventQueueTime = registry.newHistogram(EVENT_QUEUE_TIME_MS, true);
        this.eventQueueProcessingTime = registry.newHistogram(EVENT_QUEUE_PROCESSING_TIME_MS, true);
        this.fencedBrokerCountGauge = registry.newGauge(FENCED_BROKER_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return fencedBrokerCount;
            }
        });
        this.activeBrokerCountGauge = registry.newGauge(ACTIVE_BROKER_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return activeBrokerCount;
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
        this.metadataErrorCountGauge = registry.newGauge(METADATA_ERROR_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return metadataErrorCount.get();
            }
        });
        lastAppliedRecordOffsetGauge = registry.newGauge(LAST_APPLIED_RECORD_OFFSET, new Gauge<Long>() {
            @Override
            public Long value() {
                return lastAppliedRecordOffset.get();
            }
        });
        lastCommittedRecordOffsetGauge = registry.newGauge(LAST_COMMITTED_RECORD_OFFSET, new Gauge<Long>() {
            @Override
            public Long value() {
                return lastCommittedRecordOffset.get();
            }
        });
        lastAppliedRecordTimestampGauge = registry.newGauge(LAST_APPLIED_RECORD_TIMESTAMP, new Gauge<Long>() {
            @Override
            public Long value() {
                return lastAppliedRecordTimestamp.get();
            }
        });
        lastAppliedRecordLagMsGauge = registry.newGauge(LAST_APPLIED_RECORD_LAG_MS, new Gauge<Long>() {
            @Override
            public Long value() {
                return time.milliseconds() - lastAppliedRecordTimestamp.get();
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
    public void setFencedBrokerCount(int brokerCount) {
        this.fencedBrokerCount = brokerCount;
    }

    @Override
    public int fencedBrokerCount() {
        return this.fencedBrokerCount;
    }

    public void setActiveBrokerCount(int brokerCount) {
        this.activeBrokerCount = brokerCount;
    }

    @Override
    public int activeBrokerCount() {
        return this.activeBrokerCount;
    }

    @Override
    public void setGlobalTopicCount(int topicCount) {
        this.globalTopicCount = topicCount;
    }

    @Override
    public int globalTopicCount() {
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

    @Override
    public void incrementMetadataErrorCount() {
        this.metadataErrorCount.getAndIncrement();
    }

    @Override
    public int metadataErrorCount() {
        return this.metadataErrorCount.get();
    }
    @Override
    public void setLastAppliedRecordOffset(long offset) {
        lastAppliedRecordOffset.set(offset);
    }

    @Override
    public long lastAppliedRecordOffset() {
        return lastAppliedRecordOffset.get();
    }

    @Override
    public void setLastCommittedRecordOffset(long offset) {
        lastCommittedRecordOffset.set(offset);
    }

    @Override
    public long lastCommittedRecordOffset() {
        return lastCommittedRecordOffset.get();
    }

    @Override
    public void setLastAppliedRecordTimestamp(long timestamp) {
        lastAppliedRecordTimestamp.set(timestamp);
    }

    @Override
    public long lastAppliedRecordTimestamp() {
        return lastAppliedRecordTimestamp.get();
    }

    @Override
    public void close() {
        Arrays.asList(
            ACTIVE_CONTROLLER_COUNT,
            FENCED_BROKER_COUNT,
            ACTIVE_BROKER_COUNT,
            EVENT_QUEUE_TIME_MS,
            EVENT_QUEUE_PROCESSING_TIME_MS,
            GLOBAL_TOPIC_COUNT,
            GLOBAL_PARTITION_COUNT,
            OFFLINE_PARTITION_COUNT,
            PREFERRED_REPLICA_IMBALANCE_COUNT,
            METADATA_ERROR_COUNT,
            LAST_APPLIED_RECORD_OFFSET,
            LAST_COMMITTED_RECORD_OFFSET,
            LAST_APPLIED_RECORD_TIMESTAMP,
            LAST_APPLIED_RECORD_LAG_MS
        ).forEach(registry::removeMetric);
    }

    private static MetricName getMetricName(String type, String name) {
        return KafkaYammerMetrics.getMetricName("kafka.controller", type, name);
    }
}
