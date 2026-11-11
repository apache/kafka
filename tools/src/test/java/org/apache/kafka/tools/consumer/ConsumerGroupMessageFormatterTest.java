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
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupRegularExpressionKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupRegularExpressionValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataValue;
import org.apache.kafka.coordinator.group.generated.GroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.GroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.OffsetCommitKey;
import org.apache.kafka.coordinator.group.generated.OffsetCommitValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ShareGroupMetadataValue;

import org.junit.jupiter.params.provider.Arguments;

import java.util.List;
import java.util.stream.Stream;

public class ConsumerGroupMessageFormatterTest extends CoordinatorRecordMessageFormatterTest {
    private static final ConsumerGroupMetadataKey CONSUMER_GROUP_METADATA_KEY = new ConsumerGroupMetadataKey()
        .setGroupId("group-id");
    private static final ConsumerGroupMetadataValue CONSUMER_GROUP_METADATA_VALUE = new ConsumerGroupMetadataValue()
        .setEpoch(1)
        .setMetadataHash(1);
    private static final ConsumerGroupPartitionMetadataKey CONSUMER_GROUP_PARTITION_METADATA_KEY = new ConsumerGroupPartitionMetadataKey()
        .setGroupId("group-id");
    private static final ConsumerGroupPartitionMetadataValue CONSUMER_GROUP_PARTITION_METADATA_VALUE = new ConsumerGroupPartitionMetadataValue()
        .setTopics(List.of(new ConsumerGroupPartitionMetadataValue.TopicMetadata()
            .setTopicId(Uuid.ONE_UUID)
            .setTopicName("topic")
            .setNumPartitions(1)
            .setPartitionMetadata(List.of(new ConsumerGroupPartitionMetadataValue.PartitionMetadata()
                .setPartition(0)
                .setRacks(List.of("rack-a"))))
        ));
    private static final ConsumerGroupMemberMetadataKey CONSUMER_GROUP_MEMBER_METADATA_KEY = new ConsumerGroupMemberMetadataKey()
        .setGroupId("group-id")
        .setMemberId("member-id");
    private static final ConsumerGroupMemberMetadataValue CONSUMER_GROUP_MEMBER_METADATA_VALUE = new ConsumerGroupMemberMetadataValue()
        .setInstanceId("instance-id")
        .setRackId("rack-a")
        .setClientId("client-id")
        .setClientHost("1.2.3.4")
        .setSubscribedTopicNames(List.of("topic"))
        .setSubscribedTopicRegex("topic.*")
        .setRebalanceTimeoutMs(1000)
        .setServerAssignor("uniform");
    private static final ConsumerGroupTargetAssignmentMetadataKey CONSUMER_GROUP_TARGET_ASSIGNMENT_METADATA_KEY = new ConsumerGroupTargetAssignmentMetadataKey()
        .setGroupId("group-id");
    private static final ConsumerGroupTargetAssignmentMetadataValue CONSUMER_GROUP_TARGET_ASSIGNMENT_METADATA_VALUE = new ConsumerGroupTargetAssignmentMetadataValue()
        .setAssignmentEpoch(1)
        .setAssignmentTimestamp(1234);
    private static final ConsumerGroupTargetAssignmentMemberKey CONSUMER_GROUP_TARGET_ASSIGNMENT_MEMBER_KEY = new ConsumerGroupTargetAssignmentMemberKey()
        .setGroupId("group-id")
        .setMemberId("member-id");
    private static final ConsumerGroupTargetAssignmentMemberValue CONSUMER_GROUP_TARGET_ASSIGNMENT_MEMBER_VALUE = new ConsumerGroupTargetAssignmentMemberValue()
        .setTopicPartitions(List.of(new ConsumerGroupTargetAssignmentMemberValue.TopicPartition()
            .setTopicId(Uuid.ONE_UUID)
            .setPartitions(List.of(0, 1)))
        );
    private static final ConsumerGroupCurrentMemberAssignmentKey CONSUMER_GROUP_CURRENT_MEMBER_ASSIGNMENT_KEY = new ConsumerGroupCurrentMemberAssignmentKey()
        .setGroupId("group-id")
        .setMemberId("member-id");
    private static final ConsumerGroupCurrentMemberAssignmentValue CONSUMER_GROUP_CURRENT_MEMBER_ASSIGNMENT_VALUE = new ConsumerGroupCurrentMemberAssignmentValue()
        .setMemberEpoch(1)
        .setPreviousMemberEpoch(0)
        .setState((byte) 0)
        .setAssignedPartitions(List.of(new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
            .setTopicId(Uuid.ONE_UUID)
            .setPartitions(List.of(0, 1)))
        )
        .setPartitionsPendingRevocation(List.of(new ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions()
            .setTopicId(Uuid.ONE_UUID)
            .setPartitions(List.of(2)))
        );
    private static final ConsumerGroupRegularExpressionKey CONSUMER_GROUP_REGULAR_EXPRESSION_KEY = new ConsumerGroupRegularExpressionKey()
        .setGroupId("group-id")
        .setRegularExpression("topic.*");
    private static final ConsumerGroupRegularExpressionValue CONSUMER_GROUP_REGULAR_EXPRESSION_VALUE = new ConsumerGroupRegularExpressionValue()
        .setTopics(List.of("topic"))
        .setVersion(1)
        .setTimestamp(1234);
    private static final GroupMetadataKey GROUP_METADATA_KEY = new GroupMetadataKey()
        .setGroup("group-id");
    private static final GroupMetadataValue GROUP_METADATA_VALUE = new GroupMetadataValue()
        .setProtocolType("consumer")
        .setGeneration(1)
        .setProtocol("range")
        .setLeader("leader")
        .setMembers(List.of());
    private static final OffsetCommitKey OFFSET_COMMIT_KEY = new OffsetCommitKey()
        .setGroup("group-id")
        .setTopic("topic")
        .setPartition(0);
    private static final OffsetCommitValue OFFSET_COMMIT_VALUE = new OffsetCommitValue()
        .setOffset(100L)
        .setLeaderEpoch(10)
        .setMetadata("metadata")
        .setCommitTimestamp(1234L);
    private static final ShareGroupMetadataKey SHARE_GROUP_METADATA_KEY = new ShareGroupMetadataKey()
        .setGroupId("group-id");
    private static final ShareGroupMetadataValue SHARE_GROUP_METADATA_VALUE = new ShareGroupMetadataValue()
        .setEpoch(1)
        .setMetadataHash(1);

    @Override
    protected CoordinatorRecordMessageFormatter formatter() {
        return new ConsumerGroupMessageFormatter();
    }

    @Override
    protected Stream<Arguments> parameters() {
        return Stream.of(
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 3, CONSUMER_GROUP_METADATA_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, CONSUMER_GROUP_METADATA_VALUE).array(),
                """
                    {"key":{"type":3,"data":{"groupId":"group-id"}},
                     "value":{"version":0,
                              "data":{"epoch":1,
                                      "metadataHash":1}}}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 3, CONSUMER_GROUP_METADATA_KEY).array(),
                null,
                """
                    {"key":{"type":3,"data":{"groupId":"group-id"}},"value":null}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 4, CONSUMER_GROUP_PARTITION_METADATA_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, CONSUMER_GROUP_PARTITION_METADATA_VALUE).array(),
                """
                    {"key":{"type":4,"data":{"groupId":"group-id"}},
                     "value":{"version":0,
                              "data":{"topics":[{"topicId":"AAAAAAAAAAAAAAAAAAAAAQ",
                                                 "topicName":"topic",
                                                 "numPartitions":1,
                                                 "partitionMetadata":[{"partition":0,
                                                                       "racks":["rack-a"]}]}]}}}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 4, CONSUMER_GROUP_PARTITION_METADATA_KEY).array(),
                null,
                """
                    {"key":{"type":4,"data":{"groupId":"group-id"}},"value":null}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 5, CONSUMER_GROUP_MEMBER_METADATA_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, CONSUMER_GROUP_MEMBER_METADATA_VALUE).array(),
                """
                    {"key":{"type":5,"data":{"groupId":"group-id","memberId":"member-id"}},
                     "value":{"version":0,
                              "data":{"instanceId":"instance-id",
                                      "rackId":"rack-a",
                                      "clientId":"client-id",
                                      "clientHost":"1.2.3.4",
                                      "subscribedTopicNames":["topic"],
                                      "subscribedTopicRegex":"topic.*",
                                      "rebalanceTimeoutMs":1000,
                                      "serverAssignor":"uniform"}}}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 5, CONSUMER_GROUP_MEMBER_METADATA_KEY).array(),
                null,
                """
                    {"key":{"type":5,"data":{"groupId":"group-id","memberId":"member-id"}},"value":null}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 6, CONSUMER_GROUP_TARGET_ASSIGNMENT_METADATA_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, CONSUMER_GROUP_TARGET_ASSIGNMENT_METADATA_VALUE).array(),
                """
                    {"key":{"type":6,"data":{"groupId":"group-id"}},
                     "value":{"version":0,
                              "data":{"assignmentEpoch":1,
                                      "assignmentTimestamp":1234}}}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 6, CONSUMER_GROUP_TARGET_ASSIGNMENT_METADATA_KEY).array(),
                null,
                """
                    {"key":{"type":6,"data":{"groupId":"group-id"}},"value":null}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 7, CONSUMER_GROUP_TARGET_ASSIGNMENT_MEMBER_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, CONSUMER_GROUP_TARGET_ASSIGNMENT_MEMBER_VALUE).array(),
                """
                    {"key":{"type":7,"data":{"groupId":"group-id","memberId":"member-id"}},
                     "value":{"version":0,
                              "data":{"topicPartitions":[{"topicId":"AAAAAAAAAAAAAAAAAAAAAQ",
                                                          "partitions":[0,1]}]}}}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 7, CONSUMER_GROUP_TARGET_ASSIGNMENT_MEMBER_KEY).array(),
                null,
                """
                    {"key":{"type":7,"data":{"groupId":"group-id","memberId":"member-id"}},"value":null}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 8, CONSUMER_GROUP_CURRENT_MEMBER_ASSIGNMENT_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, CONSUMER_GROUP_CURRENT_MEMBER_ASSIGNMENT_VALUE).array(),
                """
                    {"key":{"type":8,"data":{"groupId":"group-id","memberId":"member-id"}},
                     "value":{"version":0,
                              "data":{"memberEpoch":1,
                                      "previousMemberEpoch":0,
                                      "state":0,
                                      "assignedPartitions":[{"topicId":"AAAAAAAAAAAAAAAAAAAAAQ",
                                                             "partitions":[0,1]}],
                                      "partitionsPendingRevocation":[{"topicId":"AAAAAAAAAAAAAAAAAAAAAQ",
                                                                      "partitions":[2]}]}}}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 8, CONSUMER_GROUP_CURRENT_MEMBER_ASSIGNMENT_KEY).array(),
                null,
                """
                    {"key":{"type":8,"data":{"groupId":"group-id","memberId":"member-id"}},"value":null}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 16, CONSUMER_GROUP_REGULAR_EXPRESSION_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, CONSUMER_GROUP_REGULAR_EXPRESSION_VALUE).array(),
                """
                    {"key":{"type":16,"data":{"groupId":"group-id","regularExpression":"topic.*"}},
                     "value":{"version":0,
                              "data":{"topics":["topic"],
                                      "version":1,
                                      "timestamp":1234}}}
                """
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 16, CONSUMER_GROUP_REGULAR_EXPRESSION_KEY).array(),
                null,
                """
                    {"key":{"type":16,"data":{"groupId":"group-id","regularExpression":"topic.*"}},"value":null}
                """
            ),
            Arguments.of(
                null,
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, CONSUMER_GROUP_METADATA_VALUE).array(),
                ""
            ),
            Arguments.of(null, null, ""),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 1, OFFSET_COMMIT_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, OFFSET_COMMIT_VALUE).array(),
                ""
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 2, GROUP_METADATA_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, GROUP_METADATA_VALUE).array(),
                ""
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer((short) 11, SHARE_GROUP_METADATA_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, SHARE_GROUP_METADATA_VALUE).array(),
                ""
            ),
            Arguments.of(
                MessageUtil.toVersionPrefixedByteBuffer(Short.MAX_VALUE, CONSUMER_GROUP_METADATA_KEY).array(),
                MessageUtil.toVersionPrefixedByteBuffer((short) 0, CONSUMER_GROUP_METADATA_VALUE).array(),
                ""
            )
        );
    }
}
