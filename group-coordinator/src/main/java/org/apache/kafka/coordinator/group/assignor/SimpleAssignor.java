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
package org.apache.kafka.coordinator.group.assignor;

import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.api.assignor.ShareGroupPartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HOMOGENEOUS;

/**
 * A simple partition assignor for share groups that assigns partitions of the subscribed topics
 * to different members based on the rules defined in KIP-932. It is not rack-aware.
 * <p>
 * Assignments are done according to the following principles:
 * <ol>
 *   <li>Balance:          Ensure partitions are distributed equally among all members.
 *                         The difference in assignments sizes between any two members
 *                         should not exceed one partition.</li>
 *   <li>Stickiness:       Minimize partition movements among members by retaining
 *                         as much of the existing assignment as possible.</li>
 * </ol>
 * <p>
 * Balance is prioritized above stickiness.
 */
public class SimpleAssignor implements ShareGroupPartitionAssignor {
    private static final Logger log = LoggerFactory.getLogger(SimpleAssignor.class);
    private static final String SIMPLE_ASSIGNOR_NAME = "simple";

    /**
     * Unique name for this assignor.
     */
    @Override
    public String name() {
        return SIMPLE_ASSIGNOR_NAME;
    }

    /**
     * Assigns partitions to group members based on the given assignment specification and topic metadata.
     *
     * @param groupSpec                The assignment spec which includes member metadata.
     * @param subscribedTopicDescriber The topic and partition metadata describer.
     * @return The new assignment for the group.
     */
    @Override
    public GroupAssignment assign(GroupSpec groupSpec, SubscribedTopicDescriber subscribedTopicDescriber) throws PartitionAssignorException {
        if (groupSpec.memberIds().isEmpty()) {
            return new GroupAssignment(Map.of());
        }

        if (groupSpec.subscriptionType().equals(HOMOGENEOUS)) {
            log.debug("Detected that all members are subscribed to the same set of topics, invoking the homogeneous assignment algorithm");
            return new SimpleHomogeneousAssignmentBuilder(groupSpec, subscribedTopicDescriber).build();
        } else {
            log.debug("Detected that the members are subscribed to different sets of topics, invoking the heterogeneous assignment algorithm");
            return new SimpleHeterogeneousAssignmentBuilder(groupSpec, subscribedTopicDescriber).build();
        }
    }
}
