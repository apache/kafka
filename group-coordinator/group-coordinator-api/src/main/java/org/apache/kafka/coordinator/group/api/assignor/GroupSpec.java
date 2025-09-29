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
     * @return True, if the partition is currently assigned to a member.
     *         False, otherwise.
     */
    boolean isPartitionAssigned(Uuid topicId, int partitionId);

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
