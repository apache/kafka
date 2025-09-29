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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.GroupSpec;
import org.apache.kafka.coordinator.group.api.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.api.assignor.MemberSubscription;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.api.assignor.ShareGroupPartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.SubscribedTopicDescriber;
import org.apache.kafka.coordinator.group.modern.MemberAssignmentImpl;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HOMOGENEOUS;

/**
 * A simple partition assignor that assigns each member all partitions of the subscribed topics.
 */
public class SimpleAssignor implements ShareGroupPartitionAssignor {

    private static final String SIMPLE_ASSIGNOR_NAME = "simple";

    @Override
    public String name() {
        return SIMPLE_ASSIGNOR_NAME;
    }

    @Override
    public GroupAssignment assign(
        GroupSpec groupSpec,
        SubscribedTopicDescriber subscribedTopicDescriber
    ) throws PartitionAssignorException {
        if (groupSpec.memberIds().isEmpty())
            return new GroupAssignment(Collections.emptyMap());

        if (groupSpec.subscriptionType().equals(HOMOGENEOUS)) {
            return assignHomogenous(groupSpec, subscribedTopicDescriber);
        } else {
            return assignHeterogeneous(groupSpec, subscribedTopicDescriber);
        }
    }

    private GroupAssignment assignHomogenous(
        GroupSpec groupSpec,
        SubscribedTopicDescriber subscribedTopicDescriber
    ) {
        Set<Uuid> subscribeTopicIds = groupSpec.memberSubscription(groupSpec.memberIds().iterator().next())
            .subscribedTopicIds();
        if (subscribeTopicIds.isEmpty())
            return new GroupAssignment(Collections.emptyMap());

        Map<Uuid, Set<Integer>> targetPartitions = computeTargetPartitions(
            subscribeTopicIds, subscribedTopicDescriber);

        return new GroupAssignment(groupSpec.memberIds().stream().collect(Collectors.toMap(
            Function.identity(), memberId -> new MemberAssignmentImpl(targetPartitions))));
    }

    private GroupAssignment assignHeterogeneous(
        GroupSpec groupSpec,
        SubscribedTopicDescriber subscribedTopicDescriber
    ) {
        Map<String, MemberAssignment> members = new HashMap<>();
        for (String memberId : groupSpec.memberIds()) {
            MemberSubscription spec = groupSpec.memberSubscription(memberId);
            if (spec.subscribedTopicIds().isEmpty())
                continue;

            Map<Uuid, Set<Integer>> targetPartitions = computeTargetPartitions(
                spec.subscribedTopicIds(), subscribedTopicDescriber);

            members.put(memberId, new MemberAssignmentImpl(targetPartitions));
        }
        return new GroupAssignment(members);
    }

    private Map<Uuid, Set<Integer>> computeTargetPartitions(
        Set<Uuid> subscribeTopicIds,
        SubscribedTopicDescriber subscribedTopicDescriber
    ) {
        Map<Uuid, Set<Integer>> targetPartitions = new HashMap<>();
        subscribeTopicIds.forEach(topicId -> {
            int numPartitions = subscribedTopicDescriber.numPartitions(topicId);
            if (numPartitions == -1) {
                throw new PartitionAssignorException(
                    "Members are subscribed to topic " + topicId
                        + " which doesn't exist in the topic metadata."
                );
            }

            Set<Integer> partitions = new HashSet<>();
            for (int i = 0; i < numPartitions; i++) {
                partitions.add(i);
            }
            targetPartitions.put(topicId, partitions);
        });
        return targetPartitions;
    }
}
