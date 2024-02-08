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
package org.apache.kafka.coordinator.group.share;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.group.assignor.AssignmentMemberSpec;
import org.apache.kafka.coordinator.group.assignor.AssignmentSpec;
import org.apache.kafka.coordinator.group.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.assignor.PartitionAssignor;
import org.apache.kafka.coordinator.group.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.assignor.SubscribedTopicDescriber;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A simple partition assignor that assigns each member all partitions of the subscribed topics.
 */
public class SimpleAssignor implements PartitionAssignor {

  public static final String SIMPLE_ASSIGNOR_NAME = "simple";

  @Override
  public String name() {
    return SIMPLE_ASSIGNOR_NAME;
  }

  @Override
  public GroupAssignment assign(
      AssignmentSpec assignmentSpec,
      SubscribedTopicDescriber subscribedTopicDescriber
  ) throws PartitionAssignorException {
    Map<String, MemberAssignment> members = new HashMap<>();
    assignmentSpec.members().keySet().forEach(memberId -> {
      Map<Uuid, Set<Integer>> targetPartitions = new HashMap<>();
      AssignmentMemberSpec spec = assignmentSpec.members().get(memberId);
      spec.subscribedTopicIds().forEach(uuid -> {
        int numPartitions = subscribedTopicDescriber.numPartitions(uuid);
        HashSet<Integer> partitions = new HashSet<>();
        for (int i = 0; i < numPartitions; i++) {
          partitions.add(i);
        }
        targetPartitions.put(uuid, partitions);
      });

      members.put(memberId, new MemberAssignment(targetPartitions));
    });

    return new GroupAssignment(members);
  }
}