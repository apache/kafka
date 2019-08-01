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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor;
import org.apache.kafka.common.TopicPartition;

/**
 * A cooperative version of the {@link AbstractStickyAssignor AbstractStickyAssignor}. This follows the same (sticky)
 * assignment logic as {@link StickyAssignor StickyAssignor} but allows for cooperative rebalancing while the
 * {@link StickyAssignor StickyAssignor} follows the eager rebalancing protocol.
 *
 * In eager rebalancing, a consumer will revoke all its owned partitions before sending a JoinGroup request and
 * relinquish any associated resources before getting its new assignment. Of course when using a sticky assignor, many
 * of the partitions in its new assignment (if not all) may also have been assigned previously, meaning the consumer
 * needlessly had its partitions revoked and reassigned. Incremental cooperative rebalancing tries to improve the total
 * rebalancing latency by allowing a consumer to hang on to its owned partitions before sending the JoinGroup, and only
 * revoke those partitions that will ultimately be assigned to another consumer after the rebalance.
 *
 *
 * To turn on cooperative rebalancing you must set all your consumers to use this {@code PartitionAssignor},
 * or implement a custom one that returns {@code RebalanceProtocol.COOPERATIVE} in
 * {@link CooperativeStickyAssignor#supportedProtocols supportedProtocols()}.
 * <p>
 * IMPORTANT: if upgrading from 2.3 or earlier, you must follow a specific upgrade path in order to safely turn on
 * cooperative rebalancing. See the upgrade guide for details.
 */
public class CooperativeStickyAssignor extends AbstractStickyAssignor {

    @Override
    public String name() {
        return "cooperative";
    }

    @Override
    protected MemberData memberData(Subscription subscription) {
        return new MemberData(subscription.ownedPartitions(), Optional.empty());
    }

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {

        final Map<String, List<TopicPartition>> assignments = super.assign(partitionsPerTopic, subscriptions);
        adjustAssignment(subscriptions, assignments);
        return assignments;
    }

    // Following the cooperative rebalancing protocol requires removing partitions that must first be revoked from the assignment
    private void adjustAssignment(final Map<String, Subscription> subscriptions,
                                  final Map<String, List<TopicPartition>> assignments) {

        Map<TopicPartition, String> allOwnedPartitions = new HashMap<>();
        Map<TopicPartition, String> allAssignedPartitions = new HashMap<>();
        Set<TopicPartition> allRevokedPartitions = new HashSet<>();

        for (final Map.Entry<String, List<TopicPartition>> entry : assignments.entrySet()) {
            final String consumer = entry.getKey();

            final List<TopicPartition> ownedPartitions = subscriptions.get(consumer).ownedPartitions();
            for (TopicPartition tp : ownedPartitions) {
                allOwnedPartitions.put(tp, consumer);
            }

            final List<TopicPartition> assignedPartitions = entry.getValue();
            for (TopicPartition tp : assignedPartitions) {
                allAssignedPartitions.put(tp, consumer);
            }

            final Set<TopicPartition> revokedPartitions = new HashSet<>(ownedPartitions);
            revokedPartitions.removeAll(assignedPartitions);
            allRevokedPartitions.addAll(revokedPartitions);
        }

        // for all owned but not assigned partitions, blindly add them to assignment
        for (final Map.Entry<TopicPartition, String> entry : allOwnedPartitions.entrySet()) {
            final TopicPartition tp = entry.getKey();
            if (!allAssignedPartitions.containsKey(tp)) {
                assignments.get(entry.getValue()).add(tp);
            }
        }

        // remove any partitions to be revoked from the current assignment
        for (TopicPartition tp : allRevokedPartitions) {
            // if partition is being migrated to another consumer, don't assign it there yet
            if (allAssignedPartitions.containsKey(tp)) {
                String assignedConsumer = allAssignedPartitions.get(tp);
                assignments.get(assignedConsumer).remove(tp);
            }
        }

    }

}
