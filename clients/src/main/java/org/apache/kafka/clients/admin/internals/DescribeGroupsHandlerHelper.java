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

package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.ShareGroupDescribeResponseData;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class DescribeGroupsHandlerHelper {

  public static Set<AclOperation> validAclOperations(final int authorizedOperations) {
    if (authorizedOperations == MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED) {
      return null;
    }
    return Utils.from32BitField(authorizedOperations)
        .stream()
        .map(AclOperation::fromCode)
        .filter(operation -> operation != AclOperation.UNKNOWN
            && operation != AclOperation.ALL
            && operation != AclOperation.ANY)
        .collect(Collectors.toSet());
  }

  public static List<MemberDescription> memberDescriptions(List<DescribeGroupsResponseData.DescribedGroupMember> members) {
    final List<MemberDescription> memberDescriptions = new ArrayList<>(members.size());
    if (members.size() == 0) {
      return memberDescriptions;
    }
    for (DescribeGroupsResponseData.DescribedGroupMember groupMember : members) {
      Set<TopicPartition> partitions = Collections.emptySet();
      if (groupMember.memberAssignment().length > 0) {
        final ConsumerPartitionAssignor.Assignment assignment = ConsumerProtocol.
            deserializeAssignment(ByteBuffer.wrap(groupMember.memberAssignment()));
        partitions = new HashSet<>(assignment.partitions());
      }
      memberDescriptions.add(new MemberDescription(
          groupMember.memberId(),
          Optional.ofNullable(groupMember.groupInstanceId()),
          groupMember.clientId(),
          groupMember.clientHost(),
          new MemberAssignment(partitions)));
    }
    return memberDescriptions;
  }

  public static List<MemberDescription> memberDescriptionsShare(List<ShareGroupDescribeResponseData.Member> members) {
    final List<MemberDescription> memberDescriptions = new ArrayList<>(members.size());
    if (members.size() == 0) {
      return memberDescriptions;
    }
    for (ShareGroupDescribeResponseData.Member groupMember : members) {
      Set<TopicPartition> partitions = new HashSet<>();
      if (groupMember.assignment() != null && groupMember.assignment().topicPartitions() != null) {
        for (ShareGroupDescribeResponseData.TopicPartitions tp : groupMember.assignment().topicPartitions()) {
          for (int partition : tp.partitions()) {
            partitions.add(new TopicPartition(tp.topicName(), partition));
          }
        }
      }

      memberDescriptions.add(new MemberDescription(
          groupMember.memberId(),
          Optional.empty(),
          groupMember.clientId(),
          groupMember.clientHost(),
          new MemberAssignment(partitions)));
    }
    return memberDescriptions;
  }
}
