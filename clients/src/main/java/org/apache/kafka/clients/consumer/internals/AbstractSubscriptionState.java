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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

import java.util.Collection;
import java.util.Set;

/**
 * The subscription-state surface required by {@link AbstractMembershipManager} while it drives
 * reconciliation of a group member's assignment. Both the classic consumer
 * ({@link ConsumerSubscriptionState}) and the share consumer ({@link ShareSubscriptionState})
 * implement this interface, allowing the shared membership-manager logic to operate over either
 * without depending on the offset/position/fetch machinery that only the classic consumer needs.
 */
public interface AbstractSubscriptionState {

    /**
     * Set the topic IDs of the currently assigned topics.
     */
    void setAssignedTopicIds(Set<Uuid> assignedTopicIds);

    /**
     * @return The set of currently assigned partitions.
     */
    Set<TopicPartition> assignedPartitions();

    /**
     * @return {@code true} if partitions are assigned automatically from a subscription (as
     * opposed to manual assignment).
     */
    boolean hasAutoAssignedPartitions();

    /**
     * Change the assignment to the specified partitions returned from the coordinator.
     */
    void assignFromSubscribed(Collection<TopicPartition> assignments);

    /**
     * Remove the subscription and clear the assignment.
     */
    void unsubscribe();

    /**
     * Enable fetching for the given partitions once any assignment callback has completed.
     */
    default void enablePartitionsAwaitingCallback(Collection<TopicPartition> partitions) {}

    /**
     * Mark the given partitions as pending revocation so that fetching for them stops.
     */
    default void markPendingRevocation(Set<TopicPartition> tps) {}
}
