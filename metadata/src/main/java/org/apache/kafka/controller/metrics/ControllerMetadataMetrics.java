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

import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.controller.metrics.BrokerRegistrationState.getBrokerRegistrationState;

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
    private static final MetricName CONTROLLED_SHUTDOWN_BROKER_COUNT = getMetricName(
        "KafkaController", "ControlledShutdownBrokerCount"
    );
    private static final String BROKER_REGISTRATION_STATE_METRIC_NAME = "BrokerRegistrationState";
    private static final String BROKER_ID_TAG = "broker";
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
    private static final MetricName ELECTION_FROM_ELIGIBLE_LEADER_REPLICAS_PER_SEC = getMetricName(
        "ControllerStats", "ElectionFromEligibleLeaderReplicasPerSec");
    private static final MetricName IGNORED_STATIC_VOTERS = getMetricName(
        "KafkaController", "IgnoredStaticVoters");

    private final Optional<MetricsRegistry> registry;
    private final AtomicInteger fencedBrokerCount = new AtomicInteger(0);
    private final AtomicInteger activeBrokerCount = new AtomicInteger(0);
    private final AtomicInteger controllerShutdownBrokerCount = new AtomicInteger(0);
    private final Map<Integer, Integer> brokerRegistrationStates = new ConcurrentHashMap<>();
    private final AtomicInteger globalTopicCount = new AtomicInteger(0);
    private final AtomicInteger globalPartitionCount = new AtomicInteger(0);
    private final AtomicInteger offlinePartitionCount = new AtomicInteger(0);
    private final AtomicInteger preferredReplicaImbalanceCount = new AtomicInteger(0);
    private final AtomicInteger metadataErrorCount = new AtomicInteger(0);
    private Optional<Meter> uncleanLeaderElectionMeter = Optional.empty();
    private Optional<Meter> electionFromEligibleLeaderReplicasMeter = Optional.empty();
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
        registry.ifPresent(r -> r.newGauge(CONTROLLED_SHUTDOWN_BROKER_COUNT, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return controlledShutdownBrokerCount();
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
        registry.ifPresent(r -> electionFromEligibleLeaderReplicasMeter =
                Optional.of(registry.get().newMeter(ELECTION_FROM_ELIGIBLE_LEADER_REPLICAS_PER_SEC, "elections", TimeUnit.SECONDS)));

        registry.ifPresent(r -> r.newGauge(IGNORED_STATIC_VOTERS, new Gauge<Integer>() {
            @Override
            public Integer value() {
                return ignoredStaticVoters() ? 1 : 0;
            }
        }));
    }

    public void addBrokerRegistrationStateMetric(int brokerId) {
        registry.ifPresent(r -> r.newGauge(
            getBrokerIdTagMetricName(
                "KafkaController",
                BROKER_REGISTRATION_STATE_METRIC_NAME,
                brokerId
            ),
            new Gauge<Integer>() {
                @Override
                public Integer value() {
                    return brokerRegistrationState(brokerId);
                }
            }
        ));
    }

    public void removeBrokerRegistrationStateMetric(int brokerId) {
        registry.ifPresent(r -> r.removeMetric(
            getBrokerIdTagMetricName(
                "KafkaController",
                BROKER_REGISTRATION_STATE_METRIC_NAME,
                brokerId
            )
        ));
    }

    public Optional<MetricsRegistry> registry() {
        return registry;
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

    public void setControlledShutdownBrokerCount(int brokerCount) {
        this.controllerShutdownBrokerCount.set(brokerCount);
    }

    public void addToControlledShutdownBrokerCount(int brokerCountDelta) {
        this.controllerShutdownBrokerCount.addAndGet(brokerCountDelta);
    }

    public int controlledShutdownBrokerCount() {
        return this.controllerShutdownBrokerCount.get();
    }

    public void setBrokerRegistrationState(int brokerId, BrokerRegistration brokerRegistration) {
        // if the broker is unregistered, remove the metric and state
        if (brokerRegistration == null) {
            removeBrokerRegistrationStateMetric(brokerId);
            brokerRegistrationStates.remove(brokerId);
            return;
        }
        BrokerRegistrationState brokerState = getBrokerRegistrationState(brokerRegistration);
        brokerRegistrationStates.put(brokerId, brokerState.state());
    }

    public int brokerRegistrationState(int brokerId) {
        return this.brokerRegistrationStates.getOrDefault(
            brokerId,
            BrokerRegistrationState.UNREGISTERED.state()
        );
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

    public void updateElectionFromEligibleLeaderReplicasCount(int count) {
        this.electionFromEligibleLeaderReplicasMeter.ifPresent(m -> m.mark(count));
    }

    public void setIgnoredStaticVoters(boolean ignored) {
        ignoredStaticVoters.set(ignored);
    }

    public boolean ignoredStaticVoters() {
        return ignoredStaticVoters.get();
    }

    @Override
    public void close() {
        registry.ifPresent(r -> List.of(
            FENCED_BROKER_COUNT,
            ACTIVE_BROKER_COUNT,
            CONTROLLED_SHUTDOWN_BROKER_COUNT,
            GLOBAL_TOPIC_COUNT,
            GLOBAL_PARTITION_COUNT,
            OFFLINE_PARTITION_COUNT,
            PREFERRED_REPLICA_IMBALANCE_COUNT,
            METADATA_ERROR_COUNT,
            UNCLEAN_LEADER_ELECTIONS_PER_SEC,
            ELECTION_FROM_ELIGIBLE_LEADER_REPLICAS_PER_SEC,
            IGNORED_STATIC_VOTERS
        ).forEach(r::removeMetric));
        for (int brokerId : brokerRegistrationStates.keySet()) {
            removeBrokerRegistrationStateMetric(brokerId);
        }
    }

    private static MetricName getMetricName(String type, String name) {
        return KafkaYammerMetrics.getMetricName("kafka.controller", type, name);
    }

    private static MetricName getBrokerIdTagMetricName(String type, String name, int brokerId) {
        LinkedHashMap<String, String> brokerIdTag = new LinkedHashMap<>();
        brokerIdTag.put(BROKER_ID_TAG, Integer.toString(brokerId));
        return KafkaYammerMetrics.getMetricName("kafka.controller", type, name, brokerIdTag);
    }
}
