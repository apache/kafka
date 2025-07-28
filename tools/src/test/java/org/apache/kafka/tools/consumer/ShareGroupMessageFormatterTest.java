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
package org.apache.kafka.tools.consumer;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.coordinator.group.generated.ShareGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupStatePartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupStatePartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupTargetAssignmentMetadataValue;

import org.junit.jupiter.params.provider.Arguments;

import java.util.List;
import java.util.stream.Stream;

public class ShareGroupMessageFormatterTest extends CoordinatorRecordMessageFormatterTest {
    private static final ShareGroupMemberMetadataKey SHARE_GROUP_MEMBER_METADATA_KEY = new ShareGroupMemberMetadataKey()
        .setGroupId("group-id")
        .setMemberId("member-id");
    private static final ShareGroupMemberMetadataValue SHARE_GROUP_MEMBER_METADATA_VALUE = new ShareGroupMemberMetadataValue()
        .setRackId("rack-a")
        .setClientId("client-id")
        .setClientHost("1.2.3.4")
        .setSubscribedTopicNames(List.of("topic"));
    private static final ShareGroupMetadataKey SHARE_GROUP_METADATA_KEY = new ShareGroupMetadataKey()
        .setGroupId("group-id");
    private static final ShareGroupMetadataValue SHARE_GROUP_METADATA_VALUE = new ShareGroupMetadataValue()
        .setEpoch(1)
        .setMetadataHash(1);
    private static final ShareGroupTargetAssignmentMetadataKey SHARE_GROUP_TARGET_ASSIGNMENT_METADATA_KEY = new ShareGroupTargetAssignmentMetadataKey()
        .setGroupId("group-id");
    private static final ShareGroupTargetAssignmentMetadataValue SHARE_GROUP_TARGET_ASSIGNMENT_METADATA_VALUE = new ShareGroupTargetAssignmentMetadataValue()
        .setAssignmentEpoch(1);
    private static final ShareGroupTargetAssignmentMemberKey SHARE_GROUP_TARGET_ASSIGNMENT_MEMBER_KEY = new ShareGroupTargetAssignmentMemberKey()
        .setGroupId("group-id")
        .setMemberId("member-id");
    private static final ShareGroupTargetAssignmentMemberValue SHARE_GROUP_TARGET_ASSIGNMENT_MEMBER_VALUE = new ShareGroupTargetAssignmentMemberValue()
        .setTopicPartitions(List.of(new ShareGroupTargetAssignmentMemberValue.TopicPartition()
            .setTopicId(Uuid.ONE_UUID)
            .setPartitions(List.of(0, 1)))
        );
    private static final ShareGroupCurrentMemberAssignmentKey SHARE_GROUP_CURRENT_MEMBER_ASSIGNMENT_KEY = new ShareGroupCurrentMemberAssignmentKey()
        .setGroupId("group-id")
        .setMemberId("member-id");
    private static final ShareGroupCurrentMemberAssignmentValue SHARE_GROUP_CURRENT_MEMBER_ASSIGNMENT_VALUE = new ShareGroupCurrentMemberAssignmentValue()
        .setMemberEpoch(1)
        .setPreviousMemberEpoch(0)
        .setState((byte) 0)
        .setAssignedPartitions(List.of(new ShareGroupCurrentMemberAssignmentValue.TopicPartitions()
            .setTopicId(Uuid.ONE_UUID)
            .setPartitions(List.of(0, 1)))
        );
    private static final ShareGroupStatePartitionMetadataKey SHARE_GROUP_STATE_PARTITION_METADATA_KEY = new ShareGroupStatePartitionMetadataKey()
        .setGroupId("group-id");
    private static final ShareGroupStatePartitionMetadataValue SHARE_GROUP_STATE_PARTITION_METADATA_VALUE = new ShareGroupStatePartitionMetadataValue()
        .setInitializingTopics(List.of(new ShareGroupStatePartitionMetadataValue.TopicPartitionsInfo()
            .setTopicId(Uuid.ONE_UUID)
            .setTopicName("topic")
            .setPartitions(List.of(1)))
        )
        .setInitializedTopics(List.of(new ShareGroupStatePartitionMetadataValue.TopicPartitionsInfo()
            .setTopicId(Uuid.ONE_UUID)
            .setTopicName("topic")
            .setPartitions(List.of(0)))
        )
        .setDeletingTopics(List.of(new ShareGroupStatePartitionMetadataValue.TopicInfo()
            .setTopicId(Uuid.ONE_UUID)
            .setTopicName("topic"))
        );

    @Override
    protected CoordinatorRecordMessageFormatter formatter() {
        return new ShareGroupMessageFormatter();
    }

    @Override
    protected Stream<Arguments> parameters() {
        return Stream.of(
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 10, SHARE_GROUP_MEMBER_METADATA_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, SHARE_GROUP_MEMBER_METADATA_VALUE).array(),
                """
                    {"key":{"type":10,"data":{"groupId":"group-id","memberId":"member-id"}},
                     "value":{"version":0,
                              "data":{"rackId":"rack-a",
                                      "clientId":"client-id",
                                      "clientHost":"1.2.3.4",
                                      "subscribedTopicNames":["topic"]}}}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 10, SHARE_GROUP_MEMBER_METADATA_KEY).array(),
                null,
                """
                    {"key":{"type":10,"data":{"groupId":"group-id","memberId":"member-id"}},"value":null}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 11, SHARE_GROUP_METADATA_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, SHARE_GROUP_METADATA_VALUE).array(),
                """
                    {"key":{"type":11,"data":{"groupId":"group-id"}},
                     "value":{"version":0,
                              "data":{"epoch":1,
                                      "metadataHash":1}}}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 11, SHARE_GROUP_METADATA_KEY).array(),
                null,
                """
                    {"key":{"type":11,"data":{"groupId":"group-id"}},"value":null}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 12, SHARE_GROUP_TARGET_ASSIGNMENT_METADATA_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, SHARE_GROUP_TARGET_ASSIGNMENT_METADATA_VALUE).array(),
                """
                    {"key":{"type":12,"data":{"groupId":"group-id"}},
                     "value":{"version":0,
                              "data":{"assignmentEpoch":1}}}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 12, SHARE_GROUP_TARGET_ASSIGNMENT_METADATA_KEY).array(),
                null,
                """
                    {"key":{"type":12,"data":{"groupId":"group-id"}},"value":null}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 13, SHARE_GROUP_TARGET_ASSIGNMENT_MEMBER_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, SHARE_GROUP_TARGET_ASSIGNMENT_MEMBER_VALUE).array(),
                """
                    {"key":{"type":13,"data":{"groupId":"group-id","memberId":"member-id"}},
                     "value":{"version":0,
                              "data":{"topicPartitions":[{"topicId":"AAAAAAAAAAAAAAAAAAAAAQ",
                                                          "partitions":[0,1]}]}}}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 13, SHARE_GROUP_TARGET_ASSIGNMENT_MEMBER_KEY).array(),
                null,
                """
                    {"key":{"type":13,"data":{"groupId":"group-id","memberId":"member-id"}},"value":null}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 14, SHARE_GROUP_CURRENT_MEMBER_ASSIGNMENT_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, SHARE_GROUP_CURRENT_MEMBER_ASSIGNMENT_VALUE).array(),
                """
                    {"key":{"type":14,"data":{"groupId":"group-id","memberId":"member-id"}},
                     "value":{"version":0,
                              "data":{"memberEpoch":1,
                                      "previousMemberEpoch":0,
                                      "state":0,
                                      "assignedPartitions":[{"topicId":"AAAAAAAAAAAAAAAAAAAAAQ",
                                                             "partitions":[0,1]}]}}}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 14, SHARE_GROUP_CURRENT_MEMBER_ASSIGNMENT_KEY).array(),
                null,
                """
                    {"key":{"type":14,"data":{"groupId":"group-id","memberId":"member-id"}},"value":null}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 15, SHARE_GROUP_STATE_PARTITION_METADATA_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, SHARE_GROUP_STATE_PARTITION_METADATA_VALUE).array(),
                """
                    {"key":{"type":15,"data":{"groupId":"group-id"}},
                     "value":{"version":0,
                              "data":{"initializingTopics":[{"topicId":"AAAAAAAAAAAAAAAAAAAAAQ",
                                                             "topicName":"topic",
                                                             "partitions":[1]}],
                                      "initializedTopics":[{"topicId":"AAAAAAAAAAAAAAAAAAAAAQ",
                                                            "topicName":"topic",
                                                            "partitions":[0]}],
                                      "deletingTopics":[{"topicId":"AAAAAAAAAAAAAAAAAAAAAQ",
                                                         "topicName":"topic"}]}}}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 15, SHARE_GROUP_STATE_PARTITION_METADATA_KEY).array(),
                null,
                """
                    {"key":{"type":15,"data":{"groupId":"group-id"}},"value":null}
                """
            )
        );
    }
}
