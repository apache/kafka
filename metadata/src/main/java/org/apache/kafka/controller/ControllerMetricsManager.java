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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.BrokerRegistrationChangeRecord;
import org.apache.kafka.common.metadata.FenceBrokerRecord;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.UnfenceBrokerRecord;
import org.apache.kafka.common.metadata.UnregisterBrokerRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.metadata.BrokerRegistrationFencingChange;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import static org.apache.kafka.metadata.LeaderConstants.NO_LEADER;
import static org.apache.kafka.metadata.LeaderConstants.NO_LEADER_CHANGE;

/**
 * Type for updating controller metrics based on metadata records.
 */
final class ControllerMetricsManager {
    private final static class PartitionState {
        final int leader;
        final int preferredReplica;

        PartitionState(int leader, int preferredReplica) {
            this.leader = leader;
            this.preferredReplica = preferredReplica;
        }

        int leader() {
            return leader;
        }

        int preferredReplica() {
            return preferredReplica;
        }
    }

    private final Set<Integer> registeredBrokers = new HashSet<>();

    private final Set<Integer> fencedBrokers = new HashSet<>();

    private int topicCount = 0;

    private final Map<TopicIdPartition, PartitionState> topicPartitions = new HashMap<>();

    private final Set<TopicIdPartition> offlineTopicPartitions = new HashSet<>();

    private final Set<TopicIdPartition> imbalancedTopicPartitions = new HashSet<>();

    private final ControllerMetrics controllerMetrics;

    ControllerMetricsManager(ControllerMetrics controllerMetrics) {
        this.controllerMetrics = controllerMetrics;
    }

    void replayBatch(long baseOffset, List<ApiMessageAndVersion> messages) {
        int i = 1;
        for (ApiMessageAndVersion message : messages) {
            try {
                replay(message.message());
            } catch (Exception e) {
                String failureMessage = String.format(
                    "Unable to update controller metrics for %s record, it was %d of %d record(s) " +
                    "in the batch with baseOffset %d.",
                    message.message().getClass().getSimpleName(),
                    i,
                    messages.size(),
                    baseOffset
                );
                throw new IllegalArgumentException(failureMessage, e);
            }
            i++;
        }
    }

    /**
     * Update controller metrics by replaying a metadata record.
     *
     * This method assumes that the provided ApiMessage is one of the type covered by MetadataRecordType.
     *
     * @param message a metadata record
     */
    @SuppressWarnings("checkstyle:cyclomaticComplexity")
    void replay(ApiMessage message) {
        MetadataRecordType type = MetadataRecordType.fromId(message.apiKey());
        switch (type) {
            case REGISTER_BROKER_RECORD:
                replay((RegisterBrokerRecord) message);
                break;
            case UNREGISTER_BROKER_RECORD:
                replay((UnregisterBrokerRecord) message);
                break;
            case FENCE_BROKER_RECORD:
                replay((FenceBrokerRecord) message);
                break;
            case UNFENCE_BROKER_RECORD:
                replay((UnfenceBrokerRecord) message);
                break;
            case BROKER_REGISTRATION_CHANGE_RECORD:
                replay((BrokerRegistrationChangeRecord) message);
                break;
            case TOPIC_RECORD:
                replay((TopicRecord) message);
                break;
            case PARTITION_RECORD:
                replay((PartitionRecord) message);
                break;
            case PARTITION_CHANGE_RECORD:
                replay((PartitionChangeRecord) message);
                break;
            case REMOVE_TOPIC_RECORD:
                replay((RemoveTopicRecord) message);
                break;
            case CONFIG_RECORD:
            case FEATURE_LEVEL_RECORD:
            case CLIENT_QUOTA_RECORD:
            case PRODUCER_IDS_RECORD:
            case ACCESS_CONTROL_ENTRY_RECORD:
            case REMOVE_ACCESS_CONTROL_ENTRY_RECORD:
            case NO_OP_RECORD:
                // These record types do not affect metrics
                break;
            default:
                throw new RuntimeException("Unhandled record type " + type);
        }
    }

    private void replay(RegisterBrokerRecord record) {
        Integer brokerId = record.brokerId();
        registeredBrokers.add(brokerId);
        if (record.fenced()) {
            fencedBrokers.add(brokerId);
        } else {
            fencedBrokers.remove(brokerId);
        }

        updateBrokerStateMetrics();
    }

    private void replay(UnregisterBrokerRecord record) {
        Integer brokerId = record.brokerId();
        registeredBrokers.remove(brokerId);
        fencedBrokers.remove(brokerId);

        updateBrokerStateMetrics();
    }

    private void replay(FenceBrokerRecord record) {
        handleFencingChange(record.id(), BrokerRegistrationFencingChange.FENCE);
    }

    private void replay(UnfenceBrokerRecord record) {
        handleFencingChange(record.id(), BrokerRegistrationFencingChange.UNFENCE);
    }

    private void replay(BrokerRegistrationChangeRecord record) {
        BrokerRegistrationFencingChange fencingChange = BrokerRegistrationFencingChange
            .fromValue(record.fenced())
            .orElseThrow(() -> {
                return new IllegalArgumentException(
                    String.format(
                        "Registration change record for %d has unknown value for fenced field: %x",
                        record.brokerId(),
                        record.fenced()
                    )
                );
            });

        handleFencingChange(record.brokerId(), fencingChange);
    }

    private void handleFencingChange(Integer brokerId, BrokerRegistrationFencingChange fencingChange) {
        if (!registeredBrokers.contains(brokerId)) {
            throw new IllegalArgumentException(String.format("Broker with id %s is not registered", brokerId));
        }

        if (fencingChange == BrokerRegistrationFencingChange.FENCE) {
            fencedBrokers.add(brokerId);
            updateBrokerStateMetrics();
        } else if (fencingChange == BrokerRegistrationFencingChange.UNFENCE) {
            fencedBrokers.remove(brokerId);
            updateBrokerStateMetrics();
        } else {
            // The fencingChange value is NONE. In this case the controller doesn't need to update the broker
            // state metrics.
        }
    }

    private void updateBrokerStateMetrics() {
        controllerMetrics.setFencedBrokerCount(fencedBrokers.size());

        Set<Integer> activeBrokers = new HashSet<>(registeredBrokers);
        activeBrokers.removeAll(fencedBrokers);
        controllerMetrics.setActiveBrokerCount(activeBrokers.size());
    }

    private void replay(TopicRecord record) {
        topicCount++;

        controllerMetrics.setGlobalTopicCount(topicCount);
    }

    private void replay(PartitionRecord record) {
        TopicIdPartition tp = new TopicIdPartition(record.topicId(), record.partitionId());

        PartitionState partitionState = new PartitionState(record.leader(), record.replicas().get(0));
        topicPartitions.put(tp, partitionState);

        updateBasedOnPartitionState(tp, partitionState);

        updateTopicAndPartitionMetrics();
    }

    private void replay(PartitionChangeRecord record) {
        TopicIdPartition tp = new TopicIdPartition(record.topicId(), record.partitionId());
        if (!topicPartitions.containsKey(tp)) {
            throw new IllegalArgumentException(String.format("Unknown topic partitions %s", tp));
        }

        PartitionState partitionState = topicPartitions.computeIfPresent(
            tp,
            (key, oldValue) -> {
                PartitionState newValue = oldValue;
                // Update replicas
                if (record.replicas() != null) {
                    newValue = new PartitionState(newValue.leader(), record.replicas().get(0));
                }

                if (record.leader() != NO_LEADER_CHANGE) {
                    newValue = new PartitionState(record.leader(), newValue.preferredReplica());
                }

                return newValue;
            }
        );

        updateBasedOnPartitionState(tp, partitionState);

        updateTopicAndPartitionMetrics();
    }

    private void replay(RemoveTopicRecord record) {
        Uuid topicId = record.topicId();
        Predicate<TopicIdPartition> matchesTopic = tp -> tp.topicId() == topicId;

        topicCount--;
        topicPartitions.keySet().removeIf(matchesTopic);
        offlineTopicPartitions.removeIf(matchesTopic);
        imbalancedTopicPartitions.removeIf(matchesTopic);

        updateTopicAndPartitionMetrics();
    }

    private void updateBasedOnPartitionState(TopicIdPartition tp, PartitionState partitionState) {
        if (partitionState.leader() == NO_LEADER) {
            offlineTopicPartitions.add(tp);
        } else {
            offlineTopicPartitions.remove(tp);
        }

        if (partitionState.leader() == partitionState.preferredReplica()) {
            imbalancedTopicPartitions.remove(tp);
        } else {
            imbalancedTopicPartitions.add(tp);
        }
    }

    private void updateTopicAndPartitionMetrics() {
        controllerMetrics.setGlobalTopicCount(topicCount);
        controllerMetrics.setGlobalPartitionCount(topicPartitions.size());
        controllerMetrics.setOfflinePartitionCount(offlineTopicPartitions.size());
        controllerMetrics.setPreferredReplicaImbalanceCount(imbalancedTopicPartitions.size());
    }

    /**
     * Resets the value of all of the metrics.
     *
     * Resets all of the state tracked by this type and resets all of the related controller metrics.
     */
    void reset() {
        registeredBrokers.clear();
        fencedBrokers.clear();
        topicCount = 0;
        topicPartitions.clear();
        offlineTopicPartitions.clear();
        imbalancedTopicPartitions.clear();

        updateBrokerStateMetrics();
        updateTopicAndPartitionMetrics();
    }
}
