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

import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * These are the metrics which are managed by the ControllerServer class. They generally pertain to
 * aspects of the metadata, like how many topics or partitions we have.
 * All of these except MetadataErrorCount are managed by ControllerMetadataMetricsPublisher.
 *
 * IMPORTANT: Metrics which are managed by the QuorumController class itself should go in
 * {@link org.apache.kafka.controller.metrics.QuorumControllerMetrics}, not here.
 */
public final class ControllerMetadataMetrics implements AutoCloseable {
    private static final MetricName FENCED_BROKER_COUNT = getMetricName(
        "KafkaController", "FencedBrokerCount");
    private static final MetricName ACTIVE_BROKER_COUNT = getMetricName(
        "KafkaController", "ActiveBrokerCount");
    private static final MetricName GLOBAL_TOPIC_COUNT = getMetricName(
        "KafkaController", "GlobalTopicCount");
    private static final MetricName GLOBAL_PARTITION_COUNT = getMetricName(
        "KafkaController", "GlobalPartitionCount");
    private static final MetricName OFFLINE_PARTITION_COUNT = getMetricName(
        "KafkaController", "OfflinePartitionsCount");
    private static final MetricName PREFERRED_REPLICA_IMBALANCE_COUNT = getMetricName(
        "KafkaController", "PreferredReplicaImbalanceCount");
    private static final MetricName METADATA_ERROR_COUNT = getMetricName(
        "KafkaController", "MetadataErrorCount");
    private static final MetricName UNCLEAN_LEADER_ELECTIONS_PER_SEC = getMetricName(
        "ControllerStats", "UncleanLeaderElectionsPerSec");
    private static final MetricName IGNORED_STATIC_VOTERS = getMetricName(
        "KafkaController", "IgnoredStaticVoters");

    private final Optional<MetricsRegistry> registry;
    private final AtomicInteger fencedBrokerCount = new AtomicInteger(0);
    private final AtomicInteger activeBrokerCount = new AtomicInteger(0);
    private final AtomicInteger globalTopicCount = new AtomicInteger(0);
    private final AtomicInteger globalPartitionCount = new AtomicInteger(0);
    private final AtomicInteger offlinePartitionCount = new AtomicInteger(0);
    private final AtomicInteger preferredReplicaImbalanceCount = new AtomicInteger(0);
    private final AtomicInteger metadataErrorCount = new AtomicInteger(0);
    private Optional<Meter> uncleanLeaderElectionMeter = Optional.empty();
    private final AtomicBoolean ignoredStaticVoters = new AtomicBoolean(false);

    /**
     * Create a new ControllerMetadataMetrics object.
     *
     * @param registry The metrics registry, or Optional.empty if this is a test and we don't have one.
     */
    public ControllerMetadataMetrics(Optional<MetricsRegistry> registry) {
        this.registry = registry;
        registry.ifPresent(r -> r.newGauge(FENCED_BROKER_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return fencedBrokerCount();
            }
        }));
        registry.ifPresent(r -> r.newGauge(ACTIVE_BROKER_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return activeBrokerCount();
            }
        }));
        registry.ifPresent(r -> r.newGauge(GLOBAL_TOPIC_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return globalTopicCount();
            }
        }));
        registry.ifPresent(r -> r.newGauge(GLOBAL_PARTITION_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return globalPartitionCount();
            }
        }));
        registry.ifPresent(r -> r.newGauge(OFFLINE_PARTITION_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return offlinePartitionCount();
            }
        }));
        registry.ifPresent(r -> r.newGauge(PREFERRED_REPLICA_IMBALANCE_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return preferredReplicaImbalanceCount();
            }
        }));
        registry.ifPresent(r -> r.newGauge(METADATA_ERROR_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return metadataErrorCount();
            }
        }));
        registry.ifPresent(r -> uncleanLeaderElectionMeter =
                Optional.of(registry.get().newMeter(UNCLEAN_LEADER_ELECTIONS_PER_SEC, "elections", TimeUnit.SECONDS)));

        registry.ifPresent(r -> r.newGauge(IGNORED_STATIC_VOTERS, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return ignoredStaticVoters() ? 1 : 0;
            }
        }));
    }

    public void setFencedBrokerCount(int brokerCount) {
        this.fencedBrokerCount.set(brokerCount);
    }

    public void addToFencedBrokerCount(int brokerCountDelta) {
        this.fencedBrokerCount.addAndGet(brokerCountDelta);
    }

    public int fencedBrokerCount() {
        return this.fencedBrokerCount.get();
    }

    public void setActiveBrokerCount(int brokerCount) {
        this.activeBrokerCount.set(brokerCount);
    }

    public void addToActiveBrokerCount(int brokerCountDelta) {
        this.activeBrokerCount.addAndGet(brokerCountDelta);
    }

    public int activeBrokerCount() {
        return this.activeBrokerCount.get();
    }
    
    public void setGlobalTopicCount(int topicCount) {
        this.globalTopicCount.set(topicCount);
    }

    public void addToGlobalTopicCount(int topicCountDelta) {
        this.globalTopicCount.addAndGet(topicCountDelta);
    }

    public int globalTopicCount() {
        return this.globalTopicCount.get();
    }

    public void setGlobalPartitionCount(int partitionCount) {
        this.globalPartitionCount.set(partitionCount);
    }

    public void addToGlobalPartitionCount(int partitionCountDelta) {
        this.globalPartitionCount.addAndGet(partitionCountDelta);
    }

    public int globalPartitionCount() {
        return this.globalPartitionCount.get();
    }

    public void setOfflinePartitionCount(int offlinePartitions) {
        this.offlinePartitionCount.set(offlinePartitions);
    }

    public void addToOfflinePartitionCount(int offlinePartitionsDelta) {
        this.offlinePartitionCount.addAndGet(offlinePartitionsDelta);
    }

    public int offlinePartitionCount() {
        return this.offlinePartitionCount.get();
    }

    public void setPreferredReplicaImbalanceCount(int replicaImbalances) {
        this.preferredReplicaImbalanceCount.set(replicaImbalances);
    }

    public void addToPreferredReplicaImbalanceCount(int replicaImbalancesCount) {
        this.preferredReplicaImbalanceCount.addAndGet(replicaImbalancesCount);
    }

    public int preferredReplicaImbalanceCount() {
        return this.preferredReplicaImbalanceCount.get();
    }

    public void incrementMetadataErrorCount() {
        this.metadataErrorCount.getAndIncrement();
    }

    public int metadataErrorCount() {
        return this.metadataErrorCount.get();
    }
    
    public void updateUncleanLeaderElection(int count) {
        this.uncleanLeaderElectionMeter.ifPresent(m -> m.mark(count));
    }

    public void setIgnoredStaticVoters(boolean ignored) {
        ignoredStaticVoters.set(ignored);
    }

    public boolean ignoredStaticVoters() {
        return ignoredStaticVoters.get();
    }

    @Override
    public void close() {
        registry.ifPresent(r -> Arrays.asList(
            FENCED_BROKER_COUNT,
            ACTIVE_BROKER_COUNT,
            GLOBAL_TOPIC_COUNT,
            GLOBAL_PARTITION_COUNT,
            OFFLINE_PARTITION_COUNT,
            PREFERRED_REPLICA_IMBALANCE_COUNT,
            METADATA_ERROR_COUNT,
            UNCLEAN_LEADER_ELECTIONS_PER_SEC,
            IGNORED_STATIC_VOTERS
        ).forEach(r::removeMetric));
    }

    private static MetricName getMetricName(String type, String name) {
        return KafkaYammerMetrics.getMetricName("kafka.controller", type, name);
    }
}
