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
package org.apache.kafka.clients.consumer;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor;
import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignorTest;
import org.apache.kafka.common.TopicPartition;

public class CooperativeStickyAssignorTest extends AbstractStickyAssignorTest {

    @Override
    public AbstractStickyAssignor createAssignor() {
        return new CooperativeStickyAssignor();
    }

    @Override
    public Subscription buildSubscription(List<String> topics, List<TopicPartition> partitions) {
        return new Subscription(topics, assignor.subscriptionUserData(new HashSet<>(topics)), partitions);
    }

    /**
     * The cooperative assignor must do some additional work and verification of some assignments relative to the eager
     * assignor, since it may or may not need to trigger a second follow-up rebalance.
     * <p>
     * In addition to the validity requirements described in
     * {@link org.apache.kafka.clients.consumer.internals.AbstractStickyAssignorTest#verifyValidityAndBalance(Map, Map, Map)},
     * we must verify that no partition is being revoked and reassigned during the same rebalance. This means the initial
     * assignment may be unbalanced, so if we do detect partitions being revoked we should trigger a second "rebalance"
     * to get the final assignment and then verify that it is both valid and balanced.
     */
    @Override
    public void verifyValidityAndBalance(Map<String, Subscription> subscriptions,
                                         Map<String, List<TopicPartition>> assignments,
                                         Map<String, Integer> partitionsPerTopic) {
        int rebalances = 0;
        // partitions are being revoked, we must go through another assignment to get the final state
        while (verifyCooperativeValidity(subscriptions, assignments)) {

            // update the subscriptions with the now owned partitions
            for (Map.Entry<String, List<TopicPartition>> entry : assignments.entrySet()) {
                String consumer = entry.getKey();
                Subscription oldSubscription = subscriptions.get(consumer);
                subscriptions.put(consumer, buildSubscription(oldSubscription.topics(), entry.getValue()));
            }

            assignments.clear();
            assignments.putAll(assignor.assign(partitionsPerTopic, subscriptions));
            ++rebalances;

            assertTrue(rebalances <= 4);
        }

        // Check the validity and balance of the final assignment
        super.verifyValidityAndBalance(subscriptions, assignments, partitionsPerTopic);
    }

    // Returns true if partitions are being revoked, indicating a second rebalance will be triggered
    private boolean verifyCooperativeValidity(Map<String, Subscription> subscriptions, Map<String, List<TopicPartition>> assignments) {
        Set<TopicPartition> allAddedPartitions = new HashSet<>();
        Set<TopicPartition> allRevokedPartitions = new HashSet<>();
        for (Map.Entry<String, List<TopicPartition>> entry : assignments.entrySet()) {
            List<TopicPartition> ownedPartitions = subscriptions.get(entry.getKey()).ownedPartitions();
            List<TopicPartition> assignedPartitions = entry.getValue();

            Set<TopicPartition> revokedPartitions = new HashSet<>(ownedPartitions);
            revokedPartitions.removeAll(assignedPartitions);

            Set<TopicPartition> addedPartitions = new HashSet<>(assignedPartitions);
            addedPartitions.removeAll(ownedPartitions);

            allAddedPartitions.addAll(addedPartitions);
            allRevokedPartitions.addAll(revokedPartitions);
        }

        Set<TopicPartition> intersection = new HashSet<>(allAddedPartitions);
        intersection.retainAll(allRevokedPartitions);
        assertTrue(intersection.isEmpty(),
            "Error: Some partitions were assigned to a new consumer during the same rebalance they are being " +
            "revoked from their previous owner. Partitions: " + intersection);

        return !allRevokedPartitions.isEmpty();
    }
}
