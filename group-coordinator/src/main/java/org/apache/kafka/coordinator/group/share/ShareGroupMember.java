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
import org.apache.kafka.common.message.ShareGroupDescribeResponseData;
import org.apache.kafka.coordinator.group.GroupMember;
import org.apache.kafka.coordinator.group.Utils;
import org.apache.kafka.coordinator.group.common.MemberState;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupMemberMetadataValue;
import org.apache.kafka.image.TopicsImage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

/**
 * ShareGroupMember contains all the information related to a member
 * within a share group. This class is immutable.
 */
public class ShareGroupMember extends GroupMember {
  /**
   * A builder that facilitates the creation of a new member or the update of
   * an existing one.
   * <p>
   * Please refer to the javadoc of {{@link ShareGroupMember}} for the
   * definition of the fields.
   */
  public static class Builder {
    private final String memberId;
    private int memberEpoch = 0;
    private int previousMemberEpoch = -1;
    private MemberState state = MemberState.STABLE;
    private String rackId = null;
    private int rebalanceTimeoutMs = -1;
    private String clientId = "";
    private String clientHost = "";
    private List<String> subscribedTopicNames = Collections.emptyList();
    private Map<Uuid, Set<Integer>> assignedPartitions = Collections.emptyMap();

    public Builder(String memberId) {
      this.memberId = Objects.requireNonNull(memberId);
    }

    public Builder(ShareGroupMember member) {
      Objects.requireNonNull(member);

      this.memberId = member.memberId();
      this.memberEpoch = member.memberEpoch;
      this.previousMemberEpoch = member.previousMemberEpoch;
      this.rackId = member.rackId;
      this.rebalanceTimeoutMs = member.rebalanceTimeoutMs;
      this.clientId = member.clientId;
      this.clientHost = member.clientHost;
      this.subscribedTopicNames = member.subscribedTopicNames;
      this.assignedPartitions = member.assignedPartitions;
    }

    public Builder setMemberEpoch(int memberEpoch) {
      this.memberEpoch = memberEpoch;
      return this;
    }

    public Builder setPreviousMemberEpoch(int previousMemberEpoch) {
      this.previousMemberEpoch = previousMemberEpoch;
      return this;
    }

    public Builder setRackId(String rackId) {
      this.rackId = rackId;
      return this;
    }

    public Builder maybeUpdateRackId(Optional<String> rackId) {
      this.rackId = rackId.orElse(this.rackId);
      return this;
    }

    public Builder setRebalanceTimeoutMs(int rebalanceTimeoutMs) {
      this.rebalanceTimeoutMs = rebalanceTimeoutMs;
      return this;
    }

    public Builder maybeUpdateRebalanceTimeoutMs(OptionalInt rebalanceTimeoutMs) {
      this.rebalanceTimeoutMs = rebalanceTimeoutMs.orElse(this.rebalanceTimeoutMs);
      return this;
    }

    public Builder setClientId(String clientId) {
      this.clientId = clientId;
      return this;
    }

    public Builder setClientHost(String clientHost) {
      this.clientHost = clientHost;
      return this;
    }

    public Builder setSubscribedTopicNames(List<String> subscribedTopicNames) {
      this.subscribedTopicNames = subscribedTopicNames;
      this.subscribedTopicNames.sort(Comparator.naturalOrder());
      return this;
    }

    public Builder maybeUpdateSubscribedTopicNames(Optional<List<String>> subscribedTopicNames) {
      this.subscribedTopicNames = subscribedTopicNames.orElse(this.subscribedTopicNames);
      this.subscribedTopicNames.sort(Comparator.naturalOrder());
      return this;
    }

    public Builder setState(MemberState state) {
      this.state = state;
      return this;
    }

    public Builder setAssignedPartitions(Map<Uuid, Set<Integer>> assignedPartitions) {
      this.assignedPartitions = assignedPartitions;
      return this;
    }

    public Builder updateWith(ShareGroupMemberMetadataValue record) {
      setRackId(record.rackId());
      setClientId(record.clientId());
      setClientHost(record.clientHost());
      setSubscribedTopicNames(record.subscribedTopicNames());
      setRebalanceTimeoutMs(record.rebalanceTimeoutMs());
      return this;
    }

    public Builder updateWith(
        ConsumerGroupCurrentMemberAssignmentValue record) {
      setMemberEpoch(record.memberEpoch());
      setPreviousMemberEpoch(record.previousMemberEpoch());
      setState(MemberState.fromValue(record.state()));
      setAssignedPartitions(assignmentFromTopicPartitions(record.assignedPartitions()));
      return this;
    }

    public ShareGroupMember build() {
      return new ShareGroupMember(
              memberId,
              memberEpoch,
              previousMemberEpoch,
              rackId,
              rebalanceTimeoutMs,
              clientId,
              clientHost,
              subscribedTopicNames,
              state,
              assignedPartitions
      );
    }
  }

  private ShareGroupMember(
          String memberId,
          int memberEpoch,
          int previousMemberEpoch,
          String rackId,
          int rebalanceTimeoutMs,
          String clientId,
          String clientHost,
          List<String> subscribedTopicNames,
          MemberState state,
          Map<Uuid, Set<Integer>> assignedPartitions
  ) {
    this.memberId = memberId;
    this.memberEpoch = memberEpoch;
    this.previousMemberEpoch = previousMemberEpoch;
    this.rackId = rackId;
    this.rebalanceTimeoutMs = rebalanceTimeoutMs;
    this.clientId = clientId;
    this.clientHost = clientHost;
    this.subscribedTopicNames = subscribedTopicNames;
    this.state = state;
    this.assignedPartitions = assignedPartitions;
  }

  /**
   * @return A string representation of the current assignment state.
   */
  public String currentAssignmentSummary() {
    return "CurrentAssignment(memberEpoch=" + memberEpoch +
            ", previousMemberEpoch=" + previousMemberEpoch +
            ", state=" + state +
            ", assignedPartitions=" + assignedPartitions +
            ')';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ShareGroupMember that = (ShareGroupMember) o;
    return memberEpoch == that.memberEpoch
            && previousMemberEpoch == that.previousMemberEpoch
            && rebalanceTimeoutMs == that.rebalanceTimeoutMs
            && Objects.equals(memberId, that.memberId)
            && Objects.equals(rackId, that.rackId)
            && Objects.equals(clientId, that.clientId)
            && Objects.equals(clientHost, that.clientHost)
            && Objects.equals(subscribedTopicNames, that.subscribedTopicNames)
            && Objects.equals(assignedPartitions, that.assignedPartitions);
  }

  @Override
  public int hashCode() {
    int result = memberId != null ? memberId.hashCode() : 0;
    result = 31 * result + memberEpoch;
    result = 31 * result + previousMemberEpoch;
    result = 31 * result + Objects.hashCode(rackId);
    result = 31 * result + rebalanceTimeoutMs;
    result = 31 * result + Objects.hashCode(clientId);
    result = 31 * result + Objects.hashCode(clientHost);
    result = 31 * result + Objects.hashCode(subscribedTopicNames);
    result = 31 * result + Objects.hashCode(assignedPartitions);
    return result;
  }

  @Override
  public String toString() {
    return "ConsumerGroupMember(" +
            "memberId='" + memberId + '\'' +
            ", memberEpoch=" + memberEpoch +
            ", previousMemberEpoch=" + previousMemberEpoch +
            ", rackId='" + rackId + '\'' +
            ", rebalanceTimeoutMs=" + rebalanceTimeoutMs +
            ", clientId='" + clientId + '\'' +
            ", clientHost='" + clientHost + '\'' +
            ", subscribedTopicNames=" + subscribedTopicNames +
            ", state=" + state +
            ", assignedPartitions=" + assignedPartitions +
            ')';
  }

  private static List<ShareGroupDescribeResponseData.TopicPartitions> topicPartitionsFromMap(
      Map<Uuid, Set<Integer>> partitions,
      TopicsImage topicsImage
  ) {
    List<ShareGroupDescribeResponseData.TopicPartitions> topicPartitions = new ArrayList<>();
    partitions.forEach((topicId, partitionSet) -> {
      String topicName = Utils.lookupTopicNameById(topicId, topicsImage);
      if (topicName != null) {
        topicPartitions.add(new ShareGroupDescribeResponseData.TopicPartitions()
            .setTopicId(topicId)
            .setTopicName(topicName)
            .setPartitions(new ArrayList<>(partitionSet)));
      }
    });
    return topicPartitions;
  }

  /**
   * @param topicsImage: Topics image objec to search for a specific topic id
   *
   * @return The ShareGroupMember mapped as ShareGroupDescribeResponseData.Member.
   */
  public ShareGroupDescribeResponseData.Member asShareGroupDescribeMember(
      TopicsImage topicsImage
  ) {
    return new ShareGroupDescribeResponseData.Member()
        .setMemberEpoch(memberEpoch)
        .setMemberId(memberId)
        .setAssignment(new ShareGroupDescribeResponseData.Assignment()
            .setTopicPartitions(topicPartitionsFromMap(assignedPartitions, topicsImage)))
        .setClientHost(clientHost)
        .setClientId(clientId)
        .setRackId(rackId)
        .setSubscribedTopicNames(subscribedTopicNames);
  }
}
