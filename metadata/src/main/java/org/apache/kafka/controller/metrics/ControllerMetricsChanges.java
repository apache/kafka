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

import org.apache.kafka.image.TopicDelta;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.PartitionRegistration;

import java.util.Map.Entry;


/**
 * The ControllerMetricsChanges class is used inside ControllerMetricsPublisher to track the
 * metrics changes triggered by a series of deltas.
 */
class ControllerMetricsChanges {
    /**
     * Calculates the change between two boolean values, expressed as an integer.
     */
    static int delta(boolean prev, boolean next) {
        if (prev) {
            return next ? 0 : -1;
        } else {
            return next ? 1 : 0;
        }
    }

    private int fencedBrokersChange = 0;
    private int activeBrokersChange = 0;
    private int migratingZkBrokersChange = 0;
    private int globalTopicsChange = 0;
    private int globalPartitionsChange = 0;
    private int offlinePartitionsChange = 0;
    private int partitionsWithoutPreferredLeaderChange = 0;

    public int fencedBrokersChange() {
        return fencedBrokersChange;
    }

    public int activeBrokersChange() {
        return activeBrokersChange;
    }

    public int migratingZkBrokersChange() {
        return migratingZkBrokersChange;
    }

    public int globalTopicsChange() {
        return globalTopicsChange;
    }

    public int globalPartitionsChange() {
        return globalPartitionsChange;
    }

    public int offlinePartitionsChange() {
        return offlinePartitionsChange;
    }

    public int partitionsWithoutPreferredLeaderChange() {
        return partitionsWithoutPreferredLeaderChange;
    }

    void handleBrokerChange(BrokerRegistration prev, BrokerRegistration next) {
        boolean wasFenced = false;
        boolean wasActive = false;
        boolean wasZk = false;
        if (prev != null) {
            wasFenced = prev.fenced();
            wasActive = !prev.fenced();
            wasZk = prev.isMigratingZkBroker();
        }
        boolean isFenced = false;
        boolean isActive = false;
        boolean isZk = false;
        if (next != null) {
            isFenced = next.fenced();
            isActive = !next.fenced();
            isZk = next.isMigratingZkBroker();
        }
        fencedBrokersChange += delta(wasFenced, isFenced);
        activeBrokersChange += delta(wasActive, isActive);
        migratingZkBrokersChange += delta(wasZk, isZk);
    }

    void handleDeletedTopic(TopicImage deletedTopic) {
        deletedTopic.partitions().values().forEach(prev -> handlePartitionChange(prev, null));
        globalTopicsChange--;
    }

    void handleTopicChange(TopicImage prev, TopicDelta topicDelta) {
        if (prev == null) {
            globalTopicsChange++;
            for (PartitionRegistration nextPartition : topicDelta.partitionChanges().values()) {
                handlePartitionChange(null, nextPartition);
            }
        } else {
            for (Entry<Integer, PartitionRegistration> entry : topicDelta.partitionChanges().entrySet()) {
                int partitionId = entry.getKey();
                PartitionRegistration nextPartition = entry.getValue();
                handlePartitionChange(prev.partitions().get(partitionId), nextPartition);
            }
        }
    }

    void handlePartitionChange(PartitionRegistration prev, PartitionRegistration next) {
        boolean wasPresent = false;
        boolean wasOffline = false;
        boolean wasWithoutPreferredLeader = false;
        if (prev != null) {
            wasPresent = true;
            wasOffline = !prev.hasLeader();
            wasWithoutPreferredLeader = !prev.hasPreferredLeader();
        }
        boolean isPresent = false;
        boolean isOffline = false;
        boolean isWithoutPreferredLeader = false;
        if (next != null) {
            isPresent = true;
            isOffline = !next.hasLeader();
            isWithoutPreferredLeader = !next.hasPreferredLeader();
        }
        globalPartitionsChange += delta(wasPresent, isPresent);
        offlinePartitionsChange += delta(wasOffline, isOffline);
        partitionsWithoutPreferredLeaderChange += delta(wasWithoutPreferredLeader, isWithoutPreferredLeader);
    }

    /**
     * Apply these changes to the metrics object.
     */
    void apply(ControllerMetadataMetrics metrics) {
        if (fencedBrokersChange != 0) {
            metrics.addToFencedBrokerCount(fencedBrokersChange);
        }
        if (activeBrokersChange != 0) {
            metrics.addToActiveBrokerCount(activeBrokersChange);
        }
        if (migratingZkBrokersChange != 0) {
            metrics.addToMigratingZkBrokerCount(migratingZkBrokersChange);
        }
        if (globalTopicsChange != 0) {
            metrics.addToGlobalTopicCount(globalTopicsChange);
        }
        if (globalPartitionsChange != 0) {
            metrics.addToGlobalPartitionCount(globalPartitionsChange);
        }
        if (offlinePartitionsChange != 0) {
            metrics.addToOfflinePartitionCount(offlinePartitionsChange);
        }
        if (partitionsWithoutPreferredLeaderChange != 0) {
            metrics.addToPreferredReplicaImbalanceCount(partitionsWithoutPreferredLeaderChange);
        }
    }
}
