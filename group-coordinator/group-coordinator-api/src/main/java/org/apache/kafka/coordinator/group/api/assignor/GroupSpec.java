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
package org.apache.kafka.coordinator.group.api.assignor;

import org.apache.kafka.common.Uuid;

import java.util.Collection;

/**
 * The group metadata specifications required to compute the target assignment.
 */
public interface GroupSpec {
    /**
     * @return All the member Ids of the consumer group.
     */
    Collection<String> memberIds();

    /**
     * @return The group's subscription type.
     */
    SubscriptionType subscriptionType();

    /**
     * Determine whether a topic id and partition have been assigned to
     * a member. This method functions the same for all types of groups.
     *
     * @param topicId           Uuid corresponding to the partition's topic.
     * @param partitionId       Partition Id within topic.
     * @return True, if the partition is currently assigned to a member.
     *         False, otherwise.
     */
    boolean isPartitionAssigned(Uuid topicId, int partitionId);

    /**
     * For share groups, a partition can only be assigned once its initialization is complete.
     * For other group types, this initialization is not required and all partitions returned
     * by the SubscribedTopicDescriber are always assignable.
     *
     * @param topicId           Uuid corresponding to the partition's topic.
     * @param partitionId       Partition Id within topic.
     * @return True, if the partition is assignable.
     */
    boolean isPartitionAssignable(Uuid topicId, int partitionId);

    /**
     * Gets the member subscription specification for a member.
     *
     * @param memberId The member Id.
     * @return The member's subscription metadata.
     * @throws IllegalArgumentException If the member Id isn't found.
     */
    MemberSubscription memberSubscription(String memberId);

    /**
     * Gets the current assignment of the member.
     *
     * @param memberId The member Id.
     * @return The member's assignment or an empty assignment if the
     *         member does not have one.
     */
    MemberAssignment memberAssignment(String memberId);
}
