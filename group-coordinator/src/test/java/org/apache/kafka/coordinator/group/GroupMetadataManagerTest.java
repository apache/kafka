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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.FencedMemberEpochException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupMaxSizeReachedException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnsupportedAssignorException;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.coordinator.group.assignor.AssignmentSpec;
import org.apache.kafka.coordinator.group.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.assignor.MemberAssignment;
import org.apache.kafka.coordinator.group.assignor.PartitionAssignor;
import org.apache.kafka.coordinator.group.assignor.PartitionAssignorException;
import org.apache.kafka.coordinator.group.consumer.Assignment;
import org.apache.kafka.coordinator.group.consumer.ConsumerGroup;
import org.apache.kafka.coordinator.group.consumer.ConsumerGroupMember;
import org.apache.kafka.coordinator.group.consumer.TopicMetadata;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataValue;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsDelta;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkAssignment;
import static org.apache.kafka.coordinator.group.AssignmentTestUtil.mkTopicAssignment;
import static org.junit.jupiter.api.AssertionFailureBuilder.assertionFailure;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GroupMetadataManagerTest {
    static class MockPartitionAssignor implements PartitionAssignor {
        private final String name;
        private GroupAssignment prepareGroupAssignment = null;

        MockPartitionAssignor(String name) {
            this.name = name;
        }

        public void prepareGroupAssignment(GroupAssignment prepareGroupAssignment) {
            this.prepareGroupAssignment = prepareGroupAssignment;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public GroupAssignment assign(AssignmentSpec assignmentSpec) throws PartitionAssignorException {
            return prepareGroupAssignment;
        }
    }

    public static class TopicsImageBuilder {
        private TopicsDelta delta = new TopicsDelta(TopicsImage.EMPTY);

        public TopicsImageBuilder addTopic(
            Uuid topicId,
            String topicName,
            int numPartitions
        ) {
            delta.replay(new TopicRecord().setTopicId(topicId).setName(topicName));
            for (int i = 0; i < numPartitions; i++) {
                delta.replay(new PartitionRecord()
                    .setTopicId(topicId)
                    .setPartitionId(i));
            }
            return this;
        }

        public TopicsImage build() {
            return delta.apply();
        }
    }

    static class ConsumerGroupBuilder {
        private final String groupId;
        private final int groupEpoch;
        private int assignmentEpoch;
        private final Map<String, ConsumerGroupMember> members = new HashMap<>();
        private final Map<String, Assignment> assignments = new HashMap<>();

        public ConsumerGroupBuilder(String groupId, int groupEpoch) {
            this.groupId = groupId;
            this.groupEpoch = groupEpoch;
            this.assignmentEpoch = 0;
        }

        public ConsumerGroupBuilder withMember(ConsumerGroupMember member) {
            this.members.put(member.memberId(), member);
            return this;
        }

        public ConsumerGroupBuilder withAssignment(String memberId, Map<Uuid, Set<Integer>> assignment) {
            this.assignments.put(memberId, new Assignment(assignment));
            return this;
        }

        public ConsumerGroupBuilder withAssignmentEpoch(int assignmentEpoch) {
            this.assignmentEpoch = assignmentEpoch;
            return this;
        }

        public List<Record> build(TopicsImage topicsImage) {
            List<Record> records = new ArrayList<>();

            // Add subscription records for members.
            members.forEach((memberId, member) -> {
                records.add(RecordHelpers.newMemberSubscriptionRecord(groupId, member));
            });

            // Add subscription metadata.
            Map<String, TopicMetadata> subscriptionMetadata = new HashMap<>();
            members.forEach((memberId, member) -> {
                member.subscribedTopicNames().forEach(topicName -> {
                    TopicImage topicImage = topicsImage.getTopic(topicName);
                    if (topicImage != null) {
                        subscriptionMetadata.put(topicName, new TopicMetadata(
                            topicImage.id(),
                            topicImage.name(),
                            topicImage.partitions().size()
                        ));
                    }
                });
            });

            if (!subscriptionMetadata.isEmpty()) {
                records.add(RecordHelpers.newGroupSubscriptionMetadataRecord(groupId, subscriptionMetadata));
            }

            // Add group epoch record.
            records.add(RecordHelpers.newGroupEpochRecord(groupId, groupEpoch));

            // Add target assignment records.
            assignments.forEach((memberId, assignment) -> {
                records.add(RecordHelpers.newTargetAssignmentRecord(groupId, memberId, assignment.partitions()));
            });

            // Add target assignment epoch.
            records.add(RecordHelpers.newTargetAssignmentEpochRecord(groupId, assignmentEpoch));

            // Add current assignment records for members.
            members.forEach((memberId, member) -> {
                records.add(RecordHelpers.newCurrentAssignmentRecord(groupId, member));
            });

            return records;
        }
    }

    static class GroupMetadataManagerTestContext {
        static class Builder {
            final private LogContext logContext = new LogContext();
            final private SnapshotRegistry snapshotRegistry = new SnapshotRegistry(logContext);
            private TopicsImage topicsImage;
            private List<PartitionAssignor> assignors;
            private List<ConsumerGroupBuilder> consumerGroupBuilders = new ArrayList<>();
            private int consumerGroupMaxSize = Integer.MAX_VALUE;

            public Builder withTopicsImage(TopicsImage topicsImage) {
                this.topicsImage = topicsImage;
                return this;
            }

            public Builder withAssignors(List<PartitionAssignor> assignors) {
                this.assignors = assignors;
                return this;
            }

            public Builder withConsumerGroup(ConsumerGroupBuilder builder) {
                this.consumerGroupBuilders.add(builder);
                return this;
            }

            public Builder withConsumerGroupMaxSize(int consumerGroupMaxSize) {
                this.consumerGroupMaxSize = consumerGroupMaxSize;
                return this;
            }

            public GroupMetadataManagerTestContext build() {
                if (topicsImage == null) topicsImage = TopicsImage.EMPTY;
                if (assignors == null) assignors = Collections.emptyList();

                GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext(
                    snapshotRegistry,
                    new GroupMetadataManager.Builder()
                        .withSnapshotRegistry(snapshotRegistry)
                        .withLogContext(logContext)
                        .withTopicsImage(topicsImage)
                        .withConsumerGroupHeartbeatInterval(5000)
                        .withConsumerGroupMaxSize(consumerGroupMaxSize)
                        .withAssignors(assignors)
                        .build()
                );

                consumerGroupBuilders.forEach(builder -> {
                    builder.build(topicsImage).forEach(context::replay);
                });

                context.commit();

                return context;
            }
        }

        final SnapshotRegistry snapshotRegistry;
        final GroupMetadataManager groupMetadataManager;

        long lastCommittedOffset = 0L;
        long lastWrittenOffset = 0L;

        public GroupMetadataManagerTestContext(
            SnapshotRegistry snapshotRegistry,
            GroupMetadataManager groupMetadataManager
        ) {
            this.snapshotRegistry = snapshotRegistry;
            this.groupMetadataManager = groupMetadataManager;
        }

        public void commit() {
            long lastCommittedOffset = this.lastCommittedOffset;
            this.lastCommittedOffset = lastWrittenOffset;
            snapshotRegistry.deleteSnapshotsUpTo(lastCommittedOffset);
        }

        public void rollback() {
            lastWrittenOffset = lastCommittedOffset;
            snapshotRegistry.revertToSnapshot(lastCommittedOffset);
        }

        public ConsumerGroup.ConsumerGroupState consumerGroupState(
            String groupId
        ) {
            return groupMetadataManager
                .getOrMaybeCreateConsumerGroup(groupId, false)
                .state();
        }

        public ConsumerGroupMember.MemberState consumerGroupMemberState(
            String groupId,
            String memberId
        ) {
            return groupMetadataManager
                .getOrMaybeCreateConsumerGroup(groupId, false)
                .getOrMaybeCreateMember(memberId, false)
                .state();
        }

        public Result<ConsumerGroupHeartbeatResponseData> consumerGroupHeartbeat(
            ConsumerGroupHeartbeatRequestData request
        ) {
            snapshotRegistry.getOrCreateSnapshot(lastCommittedOffset);

            RequestContext context = new RequestContext(
                new RequestHeader(
                    ApiKeys.CONSUMER_GROUP_HEARTBEAT,
                    ApiKeys.CONSUMER_GROUP_HEARTBEAT.latestVersion(),
                    "client",
                    0
                ),
                "1",
                InetAddress.getLoopbackAddress(),
                KafkaPrincipal.ANONYMOUS,
                ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
                SecurityProtocol.PLAINTEXT,
                ClientInformation.EMPTY,
                false
            );

            Result<ConsumerGroupHeartbeatResponseData> result = groupMetadataManager.consumerGroupHeartbeat(
                context,
                request
            );

            result.records().forEach(this::replay);
            return result;
        }

        private ApiMessage messageOrNull(ApiMessageAndVersion apiMessageAndVersion) {
            if (apiMessageAndVersion == null) {
                return null;
            } else {
                return apiMessageAndVersion.message();
            }
        }

        private void replay(
            Record record
        ) {
            ApiMessageAndVersion key = record.key();
            ApiMessageAndVersion value = record.value();

            if (key == null) {
                throw new IllegalStateException("Received a null key in " + record);
            }

            switch (key.version()) {
                case ConsumerGroupMemberMetadataKey.HIGHEST_SUPPORTED_VERSION:
                    groupMetadataManager.replay(
                        (ConsumerGroupMemberMetadataKey) key.message(),
                        (ConsumerGroupMemberMetadataValue) messageOrNull(value)
                    );
                    break;

                case ConsumerGroupMetadataKey.HIGHEST_SUPPORTED_VERSION:
                    groupMetadataManager.replay(
                        (ConsumerGroupMetadataKey) key.message(),
                        (ConsumerGroupMetadataValue) messageOrNull(value)
                    );
                    break;

                case ConsumerGroupPartitionMetadataKey.HIGHEST_SUPPORTED_VERSION:
                    groupMetadataManager.replay(
                        (ConsumerGroupPartitionMetadataKey) key.message(),
                        (ConsumerGroupPartitionMetadataValue) messageOrNull(value)
                    );
                    break;

                case ConsumerGroupTargetAssignmentMemberKey.HIGHEST_SUPPORTED_VERSION:
                    groupMetadataManager.replay(
                        (ConsumerGroupTargetAssignmentMemberKey) key.message(),
                        (ConsumerGroupTargetAssignmentMemberValue) messageOrNull(value)
                    );
                    break;

                case ConsumerGroupTargetAssignmentMetadataKey.HIGHEST_SUPPORTED_VERSION:
                    groupMetadataManager.replay(
                        (ConsumerGroupTargetAssignmentMetadataKey) key.message(),
                        (ConsumerGroupTargetAssignmentMetadataValue) messageOrNull(value)
                    );
                    break;

                case ConsumerGroupCurrentMemberAssignmentKey.HIGHEST_SUPPORTED_VERSION:
                    groupMetadataManager.replay(
                        (ConsumerGroupCurrentMemberAssignmentKey) key.message(),
                        (ConsumerGroupCurrentMemberAssignmentValue) messageOrNull(value)
                    );
                    break;

                default:
                    throw new IllegalStateException("Received an unknown record type " + key.version()
                        + " in " + record);
            }

            lastWrittenOffset++;
        }
    }

    @Test
    public void testConsumerHeartbeatRequestValidation() {
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .build();
        Exception ex;

        // GroupId must be present in all requests.
        ex = assertThrows(InvalidRequestException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()));
        assertEquals("GroupId can't be empty.", ex.getMessage());

        // RebalanceTimeoutMs must be present in the first request (epoch == 0).
        ex = assertThrows(InvalidRequestException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberEpoch(0)));
        assertEquals("RebalanceTimeoutMs must be provided in first request.", ex.getMessage());

        // TopicPartitions must be present and empty in the first request (epoch == 0).
        ex = assertThrows(InvalidRequestException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)));
        assertEquals("TopicPartitions must be empty when (re-)joining.", ex.getMessage());

        // SubscribedTopicNames must be present and empty in the first request (epoch == 0).
        ex = assertThrows(InvalidRequestException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setTopicPartitions(Collections.emptyList())));
        assertEquals("SubscribedTopicNames must be set in first request.", ex.getMessage());

        // MemberId must be non-empty in all requests except for the first one where it
        // could be empty (epoch != 0).
        ex = assertThrows(InvalidRequestException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberEpoch(1)));
        assertEquals("MemberId can't be empty.", ex.getMessage());

        // InstanceId must be non-empty if provided in all requests.
        ex = assertThrows(InvalidRequestException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberId(Uuid.randomUuid().toString())
                .setMemberEpoch(1)
                .setInstanceId("")));
        assertEquals("InstanceId can't be empty.", ex.getMessage());

        // RackId must be non-empty if provided in all requests.
        ex = assertThrows(InvalidRequestException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberId(Uuid.randomUuid().toString())
                .setMemberEpoch(1)
                .setRackId("")));
        assertEquals("RackId can't be empty.", ex.getMessage());

        // ServerAssignor must exist if provided in all requests.
        ex = assertThrows(UnsupportedAssignorException.class, () -> context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId("foo")
                .setMemberId(Uuid.randomUuid().toString())
                .setMemberEpoch(1)
                .setServerAssignor("bar")));
        assertEquals("ServerAssignor bar is not supported. Supported assignors: range.", ex.getMessage());
    }

    @Test
    public void testMemberIdGeneration() {
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .withTopicsImage(TopicsImage.EMPTY)
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Collections.emptyMap()
        ));

        Result<ConsumerGroupHeartbeatResponseData> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId("group-foo")
                .setMemberEpoch(0)
                .setServerAssignor("range")
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                .setTopicPartitions(Collections.emptyList()));

        // Verify that a member id was generated for the new member.
        String memberId = result.response().memberId();
        assertNotNull(memberId);
        assertNotEquals("", memberId);

        // The response should get a bumped epoch and should not
        // contain any assignment because we did not provide
        // topics metadata.
        assertEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(1)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()),
            result.response()
        );
    }

    @Test
    public void testUnknownGroupId() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .build();

        assertThrows(GroupIdNotFoundException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(100) // Epoch must be > 0.
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                    .setTopicPartitions(Collections.emptyList())));
    }

    @Test
    public void testUnknownMemberIdJoinsConsumerGroup() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(Collections.emptyMap()));

        // A first member joins to create the group.
        context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setServerAssignor("range")
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                .setTopicPartitions(Collections.emptyList()));

        // The second member is rejected because the member id is unknown and
        // the member epoch is not zero.
        assertThrows(UnknownMemberIdException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(Uuid.randomUuid().toString())
                    .setMemberEpoch(1)
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                    .setTopicPartitions(Collections.emptyList())));
    }

    @Test
    public void testConsumerGroupMemberEpochValidation() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();
        Uuid fooTopicId = Uuid.randomUuid();

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .build();

        ConsumerGroupMember member = new ConsumerGroupMember.Builder(memberId)
            .setMemberEpoch(100)
            .setPreviousMemberEpoch(99)
            .setTargetMemberEpoch(100)
            .setRebalanceTimeoutMs(5000)
            .setClientId("client")
            .setClientHost("localhost/127.0.0.1")
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignment(mkTopicAssignment(fooTopicId, 1, 2, 3)))
            .build();

        context.replay(RecordHelpers.newMemberSubscriptionRecord(groupId, member));

        context.replay(RecordHelpers.newGroupEpochRecord(groupId, 100));

        context.replay(RecordHelpers.newTargetAssignmentRecord(groupId, memberId, mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2, 3)
        )));

        context.replay(RecordHelpers.newTargetAssignmentEpochRecord(groupId, 100));

        context.replay(RecordHelpers.newCurrentAssignmentRecord(groupId, member));

        // Member epoch is greater than the expected epoch.
        assertThrows(FencedMemberEpochException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(200)
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))));

        // Member epoch is smaller than the expected epoch.
        assertThrows(FencedMemberEpochException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(50)
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))));

        // Member joins with previous epoch but without providing partitions.
        assertThrows(FencedMemberEpochException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId)
                    .setMemberEpoch(99)
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))));

        // Member joins with previous epoch and has a subset of the owned partitions. This
        // is accepted as the response with the bumped epoch may have been lost. In this
        // case, we provide back the correct epoch to the member.
        Result<ConsumerGroupHeartbeatResponseData> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(99)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                .setTopicPartitions(Collections.singletonList(new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(fooTopicId)
                    .setPartitions(Arrays.asList(1, 2)))));
        assertEquals(100, result.response().memberEpoch());
    }

    @Test
    public void testMemberJoinsEmptyConsumerGroup() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .withTopicsImage(new TopicsImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .build())
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Collections.singletonMap(memberId, new MemberAssignment(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)
            )))
        ));

        assertThrows(GroupIdNotFoundException.class, () ->
            context.groupMetadataManager.getOrMaybeCreateConsumerGroup(groupId, false));

        Result<ConsumerGroupHeartbeatResponseData> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(0)
                .setServerAssignor("range")
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(1)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setAssignedTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(0, 1, 2, 3, 4, 5)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(Arrays.asList(0, 1, 2))
                    ))),
            result.response()
        );

        ConsumerGroupMember expectedMember = new ConsumerGroupMember.Builder(memberId)
            .setMemberEpoch(1)
            .setPreviousMemberEpoch(0)
            .setTargetMemberEpoch(1)
            .setClientId("client")
            .setClientHost("localhost/127.0.0.1")
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)))
            .build();

        List<Record> expectedRecords = Arrays.asList(
            RecordHelpers.newMemberSubscriptionRecord(groupId, expectedMember),
            RecordHelpers.newGroupSubscriptionMetadataRecord(groupId, new HashMap<String, TopicMetadata>() {{
                    put(fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 6));
                    put(barTopicName, new TopicMetadata(barTopicId, barTopicName, 3));
                }}),
            RecordHelpers.newGroupEpochRecord(groupId, 1),
            RecordHelpers.newTargetAssignmentRecord(groupId, memberId, mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)
            )),
            RecordHelpers.newTargetAssignmentEpochRecord(groupId, 1),
            RecordHelpers.newCurrentAssignmentRecord(groupId, expectedMember)
        );

        assertRecordsEquals(expectedRecords, result.records());
    }

    @Test
    public void testUpdatingSubscriptionTriggersNewTargetAssignment() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .withTopicsImage(new TopicsImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setTargetMemberEpoch(10)
                    .setClientId("client")
                    .setClientHost("localhost/127.0.0.1")
                    .setSubscribedTopicNames(Arrays.asList("foo"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
                    .build())
                .withAssignment(memberId, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5)))
                .withAssignmentEpoch(10))
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            Collections.singletonMap(memberId, new MemberAssignment(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)
            )))
        ));

        Result<ConsumerGroupHeartbeatResponseData> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setMemberEpoch(10)
                .setSubscribedTopicNames(Arrays.asList("foo", "bar")));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setAssignedTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(0, 1, 2, 3, 4, 5)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(Arrays.asList(0, 1, 2))
                    ))),
            result.response()
        );

        ConsumerGroupMember expectedMember = new ConsumerGroupMember.Builder(memberId)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setTargetMemberEpoch(11)
            .setClientId("client")
            .setClientHost("localhost/127.0.0.1")
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
            .setServerAssignorName("range")
            .setAssignedPartitions(mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)))
            .build();

        List<Record> expectedRecords = Arrays.asList(
            RecordHelpers.newMemberSubscriptionRecord(groupId, expectedMember),
            RecordHelpers.newGroupSubscriptionMetadataRecord(groupId, new HashMap<String, TopicMetadata>() {
                {
                    put(fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 6));
                    put(barTopicName, new TopicMetadata(barTopicId, barTopicName, 3));
                }
            }),
            RecordHelpers.newGroupEpochRecord(groupId, 11),
            RecordHelpers.newTargetAssignmentRecord(groupId, memberId, mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1, 2, 3, 4, 5),
                mkTopicAssignment(barTopicId, 0, 1, 2)
            )),
            RecordHelpers.newTargetAssignmentEpochRecord(groupId, 11),
            RecordHelpers.newCurrentAssignmentRecord(groupId, expectedMember)
        );

        assertRecordsEquals(expectedRecords, result.records());
    }

    @Test
    public void testNewJoiningMemberTriggersNewTargetAssignment() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String memberId3 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .withTopicsImage(new TopicsImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setTargetMemberEpoch(10)
                    .setClientId("client")
                    .setClientHost("localhost/127.0.0.1")
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2),
                        mkTopicAssignment(barTopicId, 0, 1)))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setTargetMemberEpoch(10)
                    .setClientId("client")
                    .setClientHost("localhost/127.0.0.1")
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5),
                        mkTopicAssignment(barTopicId, 2)))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10))
            .build();

        assignor.prepareGroupAssignment(new GroupAssignment(
            new HashMap<String, MemberAssignment>() {
                {
                    put(memberId1, new MemberAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1),
                        mkTopicAssignment(barTopicId, 0)
                    )));
                    put(memberId2, new MemberAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 2, 3),
                        mkTopicAssignment(barTopicId, 1)
                    )));
                    put(memberId3, new MemberAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 4, 5),
                        mkTopicAssignment(barTopicId, 2)
                    )));
                }
            }
        ));

        // Member 3 joins the consumer group.
        Result<ConsumerGroupHeartbeatResponseData> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId3)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                .setServerAssignor("range")
                .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setPendingTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(4, 5)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(Arrays.asList(2))
                    ))),
            result.response()
        );

        ConsumerGroupMember expectedMember3 = new ConsumerGroupMember.Builder(memberId3)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(0)
            .setTargetMemberEpoch(11)
            .setClientId("client")
            .setClientHost("localhost/127.0.0.1")
            .setRebalanceTimeoutMs(5000)
            .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
            .setServerAssignorName("range")
            .setPartitionsPendingAssignment(mkAssignment(
                mkTopicAssignment(fooTopicId, 4, 5),
                mkTopicAssignment(barTopicId, 2)))
            .build();

        List<Record> expectedRecords = Arrays.asList(
            RecordHelpers.newMemberSubscriptionRecord(groupId, expectedMember3),
            RecordHelpers.newGroupEpochRecord(groupId, 11),
            RecordHelpers.newTargetAssignmentRecord(groupId, memberId1, mkAssignment(
                mkTopicAssignment(fooTopicId, 0, 1),
                mkTopicAssignment(barTopicId, 0)
            )),
            RecordHelpers.newTargetAssignmentRecord(groupId, memberId2, mkAssignment(
                mkTopicAssignment(fooTopicId, 2, 3),
                mkTopicAssignment(barTopicId, 1)
            )),
            RecordHelpers.newTargetAssignmentRecord(groupId, memberId3, mkAssignment(
                mkTopicAssignment(fooTopicId, 4, 5),
                mkTopicAssignment(barTopicId, 2)
            )),
            RecordHelpers.newTargetAssignmentEpochRecord(groupId, 11),
            RecordHelpers.newCurrentAssignmentRecord(groupId, expectedMember3)
        );

        assertRecordsEquals(expectedRecords.subList(0, 2), result.records().subList(0, 2));
        assertUnorderedListEquals(expectedRecords.subList(2, 5), result.records().subList(2, 5));
        assertRecordsEquals(expectedRecords.subList(5, 7), result.records().subList(5, 7));
    }

    @Test
    public void testLeavingMemberBumpsGroupEpoch() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";
        Uuid zarTopicId = Uuid.randomUuid();
        String zarTopicName = "zar";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");

        // Consumer group with two members.
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .withTopicsImage(new TopicsImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .addTopic(zarTopicId, zarTopicName, 1)
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setTargetMemberEpoch(10)
                    .setClientId("client")
                    .setClientHost("localhost/127.0.0.1")
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2),
                        mkTopicAssignment(barTopicId, 0, 1)))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setTargetMemberEpoch(10)
                    .setClientId("client")
                    .setClientHost("localhost/127.0.0.1")
                    // Use zar only here to ensure that metadata needs to be recomputed.
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar", "zar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5),
                        mkTopicAssignment(barTopicId, 2)))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10))
            .build();

        // Member 2 leaves the consumer group.
        Result<ConsumerGroupHeartbeatResponseData> result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(-1)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(-1),
            result.response()
        );

        List<Record> expectedRecords = Arrays.asList(
            RecordHelpers.newCurrentAssignmentTombstoneRecord(groupId, memberId2),
            RecordHelpers.newTargetAssignmentTombstoneRecord(groupId, memberId2),
            RecordHelpers.newMemberSubscriptionTombstoneRecord(groupId, memberId2),
            // Subscription metadata is recomputed because zar is no longer there.
            RecordHelpers.newGroupSubscriptionMetadataRecord(groupId, new HashMap<String, TopicMetadata>() {
                {
                    put(fooTopicName, new TopicMetadata(fooTopicId, fooTopicName, 6));
                    put(barTopicName, new TopicMetadata(barTopicId, barTopicName, 3));
                }
            }),
            RecordHelpers.newGroupEpochRecord(groupId, 11)
        );

        assertRecordsEquals(expectedRecords, result.records());
    }

    @Test
    public void testReconciliationProcess() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String memberId3 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        // Create a context with one consumer group containing two members.
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .withTopicsImage(new TopicsImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setTargetMemberEpoch(10)
                    .setClientId("client")
                    .setClientHost("localhost/127.0.0.1")
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2),
                        mkTopicAssignment(barTopicId, 0, 1)))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setTargetMemberEpoch(10)
                    .setClientId("client")
                    .setClientHost("localhost/127.0.0.1")
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5),
                        mkTopicAssignment(barTopicId, 2)))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10))
            .build();

        // Prepare new assignment for the group.
        assignor.prepareGroupAssignment(new GroupAssignment(
            new HashMap<String, MemberAssignment>() {
                {
                    put(memberId1, new MemberAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1),
                        mkTopicAssignment(barTopicId, 0)
                    )));
                    put(memberId2, new MemberAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 2, 3),
                        mkTopicAssignment(barTopicId, 2)
                    )));
                    put(memberId3, new MemberAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 4, 5),
                        mkTopicAssignment(barTopicId, 1)
                    )));
                }
            }
        ));

        Result<ConsumerGroupHeartbeatResponseData> result;

        // Members in the group are in Stable state.
        assertEquals(ConsumerGroupMember.MemberState.STABLE, context.consumerGroupMemberState(groupId, memberId1));
        assertEquals(ConsumerGroupMember.MemberState.STABLE, context.consumerGroupMemberState(groupId, memberId2));
        assertEquals(ConsumerGroup.ConsumerGroupState.STABLE, context.consumerGroupState(groupId));

        // Member 3 joins the group. This triggers the computation of a new target assignment
        // for the group. Member 3 does not get any assigned partitions yet because they are
        // all owned by other members. However, it transitions to epoch 11 / Assigning state.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId3)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                .setServerAssignor("range")
                .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setPendingTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(4, 5)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(Arrays.asList(1))
                    ))),
            result.response()
        );

        // We only check the last record as the subscription/target assignment updates are
        // already covered by other tests.
        assertRecordEquals(
            RecordHelpers.newCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId3)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(0)
                .setTargetMemberEpoch(11)
                .setPartitionsPendingAssignment(mkAssignment(
                    mkTopicAssignment(fooTopicId, 4, 5),
                    mkTopicAssignment(barTopicId, 1)))
                .build()),
            result.records().get(result.records().size() - 1)
        );

        assertEquals(ConsumerGroupMember.MemberState.ASSIGNING, context.consumerGroupMemberState(groupId, memberId3));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 1 heartbeats. It remains at epoch 10 but transitions to Revoking state until
        // it acknowledges the revocation of its partitions. The response contains the new
        // assignment without the partitions that must be revoked.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId1)
            .setMemberEpoch(10));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setAssignedTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(0, 1)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(Arrays.asList(0))
                    ))),
            result.response()
        );

        assertRecordsEquals(Collections.singletonList(
            RecordHelpers.newCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId1)
                .setMemberEpoch(10)
                .setPreviousMemberEpoch(9)
                .setTargetMemberEpoch(11)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1),
                    mkTopicAssignment(barTopicId, 0)))
                .setPartitionsPendingRevocation(mkAssignment(
                    mkTopicAssignment(fooTopicId, 2),
                    mkTopicAssignment(barTopicId, 1)))
                .build())),
            result.records()
        );

        assertEquals(ConsumerGroupMember.MemberState.REVOKING, context.consumerGroupMemberState(groupId, memberId1));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 2 heartbeats. It remains at epoch 10 but transitions to Revoking state until
        // it acknowledges the revocation of its partitions. The response contains the new
        // assignment without the partitions that must be revoked.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId2)
            .setMemberEpoch(10));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setAssignedTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(3)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(Arrays.asList(2))
                    ))),
            result.response()
        );

        assertRecordsEquals(Collections.singletonList(
            RecordHelpers.newCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId2)
                .setMemberEpoch(10)
                .setPreviousMemberEpoch(9)
                .setTargetMemberEpoch(11)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(fooTopicId, 3),
                    mkTopicAssignment(barTopicId, 2)))
                .setPartitionsPendingRevocation(mkAssignment(
                    mkTopicAssignment(fooTopicId, 4, 5)))
                .setPartitionsPendingAssignment(mkAssignment(
                    mkTopicAssignment(fooTopicId, 2)))
                .build())),
            result.records()
        );

        assertEquals(ConsumerGroupMember.MemberState.REVOKING, context.consumerGroupMemberState(groupId, memberId2));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 3 heartbeats. The response does not contain any assignment
        // because the member is still waiting on other members to revoke partitions.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId3)
            .setMemberEpoch(11));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000),
            result.response()
        );

        assertEquals(Collections.emptyList(), result.records());
        assertEquals(ConsumerGroupMember.MemberState.ASSIGNING, context.consumerGroupMemberState(groupId, memberId3));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 1 acknowledges the revocation of the partitions. It does so by providing the
        // partitions that it still owns in the request. This allows him to transition to epoch 11
        // and to the Stable state.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId1)
            .setMemberEpoch(10)
            .setTopicPartitions(Arrays.asList(
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(fooTopicId)
                    .setPartitions(Arrays.asList(0, 1)),
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(barTopicId)
                    .setPartitions(Arrays.asList(0))
            )));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setAssignedTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(0, 1)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(Arrays.asList(0))
                    ))),
            result.response()
        );

        assertRecordsEquals(Collections.singletonList(
            RecordHelpers.newCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId1)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setTargetMemberEpoch(11)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1),
                    mkTopicAssignment(barTopicId, 0)))
                .build())),
            result.records()
        );

        assertEquals(ConsumerGroupMember.MemberState.STABLE, context.consumerGroupMemberState(groupId, memberId1));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 2 heartbeats but without acknowledging the revocation yet. This is basically a no-op.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId2)
            .setMemberEpoch(10));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000),
            result.response()
        );

        assertEquals(Collections.emptyList(), result.records());
        assertEquals(ConsumerGroupMember.MemberState.REVOKING, context.consumerGroupMemberState(groupId, memberId2));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 3 heartbeats. It receives the partitions revoked by member 1 but remains
        // in Assigning state because it still waits on other partitions.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId3)
            .setMemberEpoch(11));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setAssignedTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(Arrays.asList(1))))
                    .setPendingTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(4, 5))))),
            result.response()
        );

        assertRecordsEquals(Collections.singletonList(
            RecordHelpers.newCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId3)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(11)
                .setTargetMemberEpoch(11)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(barTopicId, 1)))
                .setPartitionsPendingAssignment(mkAssignment(
                    mkTopicAssignment(fooTopicId, 4, 5)))
                .build())),
            result.records()
        );

        assertEquals(ConsumerGroupMember.MemberState.ASSIGNING, context.consumerGroupMemberState(groupId, memberId3));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 3 heartbeats. Member 2 has not acknowledged the revocation of its partition so
        // member keeps its current assignment.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId3)
            .setMemberEpoch(11));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000),
            result.response()
        );

        assertEquals(Collections.emptyList(), result.records());
        assertEquals(ConsumerGroupMember.MemberState.ASSIGNING, context.consumerGroupMemberState(groupId, memberId3));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 2 acknowledges the revocation of the partitions. It does so by providing the
        // partitions that it still owns in the request. This allows him to transition to epoch 11
        // and to the Stable state.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId2)
            .setMemberEpoch(10)
            .setTopicPartitions(Arrays.asList(
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(fooTopicId)
                    .setPartitions(Arrays.asList(3)),
                new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                    .setTopicId(barTopicId)
                    .setPartitions(Arrays.asList(2))
            )));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setAssignedTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(2, 3)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(Arrays.asList(2))
                    ))),
            result.response()
        );

        assertRecordsEquals(Collections.singletonList(
            RecordHelpers.newCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId2)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(10)
                .setTargetMemberEpoch(11)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(fooTopicId, 2, 3),
                    mkTopicAssignment(barTopicId, 2)))
                .build())),
            result.records()
        );

        assertEquals(ConsumerGroupMember.MemberState.STABLE, context.consumerGroupMemberState(groupId, memberId2));
        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        // Member 3 heartbeats. It receives all its partitions and transitions to Stable.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId3)
            .setMemberEpoch(11));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setAssignedTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(4, 5)),
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(barTopicId)
                            .setPartitions(Arrays.asList(1))))),
            result.response()
        );

        assertRecordsEquals(Collections.singletonList(
            RecordHelpers.newCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId3)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(11)
                .setTargetMemberEpoch(11)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(fooTopicId, 4, 5),
                    mkTopicAssignment(barTopicId, 1)))
                .build())),
            result.records()
        );

        assertEquals(ConsumerGroupMember.MemberState.STABLE, context.consumerGroupMemberState(groupId, memberId3));
        assertEquals(ConsumerGroup.ConsumerGroupState.STABLE, context.consumerGroupState(groupId));
    }

    @Test
    public void testReconciliationRestartsWhenNewTargetAssignmentIsInstalled() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String memberId3 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        // Create a context with one consumer group containing one member.
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .withTopicsImage(new TopicsImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .build())
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setTargetMemberEpoch(10)
                    .setClientId("client")
                    .setClientHost("localhost/127.0.0.1")
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2)))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2)))
                .withAssignmentEpoch(10))
            .build();

        Result<ConsumerGroupHeartbeatResponseData> result;

        // Prepare new assignment for the group.
        assignor.prepareGroupAssignment(new GroupAssignment(
            new HashMap<String, MemberAssignment>() {
                {
                    put(memberId1, new MemberAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1)
                    )));
                    put(memberId2, new MemberAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 2)
                    )));
                }
            }
        ));

        // Member 2 joins.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId2)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                .setServerAssignor("range")
                .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(11)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setPendingTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(2))
                    ))),
            result.response()
        );

        assertRecordEquals(
            RecordHelpers.newCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId2)
                .setMemberEpoch(11)
                .setPreviousMemberEpoch(0)
                .setTargetMemberEpoch(11)
                .setPartitionsPendingAssignment(mkAssignment(
                    mkTopicAssignment(fooTopicId, 2)))
                .build()),
            result.records().get(result.records().size() - 1)
        );

        assertEquals(ConsumerGroupMember.MemberState.ASSIGNING, context.consumerGroupMemberState(groupId, memberId2));

        // Member 1 heartbeats and transitions to Revoking.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId1)
            .setMemberEpoch(10));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setAssignedTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(0, 1))))),
            result.response()
        );

        assertRecordsEquals(Collections.singletonList(
            RecordHelpers.newCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId1)
                .setMemberEpoch(10)
                .setPreviousMemberEpoch(9)
                .setTargetMemberEpoch(11)
                .setAssignedPartitions(mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1)))
                .setPartitionsPendingRevocation(mkAssignment(
                    mkTopicAssignment(fooTopicId, 2)))
                .build())),
            result.records()
        );

        assertEquals(ConsumerGroupMember.MemberState.REVOKING, context.consumerGroupMemberState(groupId, memberId1));

        // Prepare new assignment for the group.
        assignor.prepareGroupAssignment(new GroupAssignment(
            new HashMap<String, MemberAssignment>() {
                {
                    put(memberId1, new MemberAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0)
                    )));
                    put(memberId2, new MemberAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 2)
                    )));
                    put(memberId3, new MemberAssignment(mkAssignment(
                        mkTopicAssignment(fooTopicId, 1)
                    )));
                }
            }
        ));

        // Member 3 joins.
        result = context.consumerGroupHeartbeat(
            new ConsumerGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId3)
                .setMemberEpoch(0)
                .setRebalanceTimeoutMs(5000)
                .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                .setServerAssignor("range")
                .setTopicPartitions(Collections.emptyList()));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId3)
                .setMemberEpoch(12)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setPendingTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(1))
                    ))),
            result.response()
        );

        assertRecordEquals(
            RecordHelpers.newCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId3)
                .setMemberEpoch(12)
                .setPreviousMemberEpoch(0)
                .setTargetMemberEpoch(12)
                .setPartitionsPendingAssignment(mkAssignment(
                    mkTopicAssignment(fooTopicId, 1)))
                .build()),
            result.records().get(result.records().size() - 1)
        );

        assertEquals(ConsumerGroupMember.MemberState.ASSIGNING, context.consumerGroupMemberState(groupId, memberId3));

        // When member 1 heartbeats, it transitions to Revoke again but an updated state.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId1)
            .setMemberEpoch(10));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId1)
                .setMemberEpoch(10)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setAssignedTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(0))))),
            result.response()
        );

        assertRecordsEquals(Collections.singletonList(
                RecordHelpers.newCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId1)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setTargetMemberEpoch(12)
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0)))
                    .setPartitionsPendingRevocation(mkAssignment(
                        mkTopicAssignment(fooTopicId, 1, 2)))
                    .build())),
            result.records()
        );

        assertEquals(ConsumerGroupMember.MemberState.REVOKING, context.consumerGroupMemberState(groupId, memberId1));

        // When member 2 heartbeats, it transitions to Assign again but with an updated state.
        result = context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(groupId)
            .setMemberId(memberId2)
            .setMemberEpoch(11));

        assertResponseEquals(
            new ConsumerGroupHeartbeatResponseData()
                .setMemberId(memberId2)
                .setMemberEpoch(12)
                .setHeartbeatIntervalMs(5000)
                .setAssignment(new ConsumerGroupHeartbeatResponseData.Assignment()
                    .setPendingTopicPartitions(Arrays.asList(
                        new ConsumerGroupHeartbeatResponseData.TopicPartitions()
                            .setTopicId(fooTopicId)
                            .setPartitions(Arrays.asList(2))))),
            result.response()
        );

        assertRecordsEquals(Collections.singletonList(
            RecordHelpers.newCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId2)
                .setMemberEpoch(12)
                .setPreviousMemberEpoch(11)
                .setTargetMemberEpoch(12)
                .setPartitionsPendingAssignment(mkAssignment(
                    mkTopicAssignment(fooTopicId, 2)))
                .build())),
            result.records()
        );

        assertEquals(ConsumerGroupMember.MemberState.ASSIGNING, context.consumerGroupMemberState(groupId, memberId2));
    }

    @Test
    public void testNewMemberIsRejectedWithMaximumMembersIsReached() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();
        String memberId2 = Uuid.randomUuid().toString();
        String memberId3 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        // Create a context with one consumer group containing two members.
        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .withTopicsImage(new TopicsImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .build())
            .withConsumerGroupMaxSize(2)
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10)
                .withMember(new ConsumerGroupMember.Builder(memberId1)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setTargetMemberEpoch(10)
                    .setClientId("client")
                    .setClientHost("localhost/127.0.0.1")
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 0, 1, 2),
                        mkTopicAssignment(barTopicId, 0, 1)))
                    .build())
                .withMember(new ConsumerGroupMember.Builder(memberId2)
                    .setMemberEpoch(10)
                    .setPreviousMemberEpoch(9)
                    .setTargetMemberEpoch(10)
                    .setClientId("client")
                    .setClientHost("localhost/127.0.0.1")
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                    .setServerAssignorName("range")
                    .setAssignedPartitions(mkAssignment(
                        mkTopicAssignment(fooTopicId, 3, 4, 5),
                        mkTopicAssignment(barTopicId, 2)))
                    .build())
                .withAssignment(memberId1, mkAssignment(
                    mkTopicAssignment(fooTopicId, 0, 1, 2),
                    mkTopicAssignment(barTopicId, 0, 1)))
                .withAssignment(memberId2, mkAssignment(
                    mkTopicAssignment(fooTopicId, 3, 4, 5),
                    mkTopicAssignment(barTopicId, 2)))
                .withAssignmentEpoch(10))
            .build();

        assertThrows(GroupMaxSizeReachedException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId3)
                    .setMemberEpoch(0)
                    .setServerAssignor("range")
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                    .setTopicPartitions(Collections.emptyList())));
    }

    @Test
    public void testConsumerGroupStates() {
        String groupId = "fooup";
        String memberId1 = Uuid.randomUuid().toString();
        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";

        MockPartitionAssignor assignor = new MockPartitionAssignor("range");
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .withConsumerGroup(new ConsumerGroupBuilder(groupId, 10))
            .build();

        assertEquals(ConsumerGroup.ConsumerGroupState.EMPTY, context.consumerGroupState(groupId));

        context.replay(RecordHelpers.newMemberSubscriptionRecord(groupId, new ConsumerGroupMember.Builder(memberId1)
            .setSubscribedTopicNames(Collections.singletonList(fooTopicName))
            .build()));
        context.replay(RecordHelpers.newGroupEpochRecord(groupId, 11));

        assertEquals(ConsumerGroup.ConsumerGroupState.ASSIGNING, context.consumerGroupState(groupId));

        context.replay(RecordHelpers.newTargetAssignmentRecord(groupId, memberId1, mkAssignment(
            mkTopicAssignment(fooTopicId, 1, 2, 3))));
        context.replay(RecordHelpers.newTargetAssignmentEpochRecord(groupId, 11));

        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        context.replay(RecordHelpers.newCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId1)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setTargetMemberEpoch(11)
            .setAssignedPartitions(mkAssignment(mkTopicAssignment(fooTopicId, 1, 2)))
            .setPartitionsPendingAssignment(mkAssignment(mkTopicAssignment(fooTopicId, 3)))
            .build()));

        assertEquals(ConsumerGroup.ConsumerGroupState.RECONCILING, context.consumerGroupState(groupId));

        context.replay(RecordHelpers.newCurrentAssignmentRecord(groupId, new ConsumerGroupMember.Builder(memberId1)
            .setMemberEpoch(11)
            .setPreviousMemberEpoch(10)
            .setTargetMemberEpoch(11)
            .setAssignedPartitions(mkAssignment(mkTopicAssignment(fooTopicId, 1, 2, 3)))
            .build()));

        assertEquals(ConsumerGroup.ConsumerGroupState.STABLE, context.consumerGroupState(groupId));
    }

    @Test
    public void testPartitionAssignorExceptionOnRegularHeartbeat() {
        String groupId = "fooup";
        // Use a static member id as it makes the test easier.
        String memberId1 = Uuid.randomUuid().toString();

        Uuid fooTopicId = Uuid.randomUuid();
        String fooTopicName = "foo";
        Uuid barTopicId = Uuid.randomUuid();
        String barTopicName = "bar";

        PartitionAssignor assignor = mock(PartitionAssignor.class);
        when(assignor.name()).thenReturn("range");
        when(assignor.assign(any())).thenThrow(new PartitionAssignorException("Assignment failed."));

        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withAssignors(Collections.singletonList(assignor))
            .withTopicsImage(new TopicsImageBuilder()
                .addTopic(fooTopicId, fooTopicName, 6)
                .addTopic(barTopicId, barTopicName, 3)
                .build())
            .build();

        // Member 1 joins the consumer group. The request fails because the
        // target assignment computation failed.
        assertThrows(UnknownServerException.class, () ->
            context.consumerGroupHeartbeat(
                new ConsumerGroupHeartbeatRequestData()
                    .setGroupId(groupId)
                    .setMemberId(memberId1)
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(5000)
                    .setSubscribedTopicNames(Arrays.asList("foo", "bar"))
                    .setServerAssignor("range")
                    .setTopicPartitions(Collections.emptyList())));
    }

    private <T> void assertUnorderedListEquals(
        List<T> expected,
        List<T> actual
    ) {
        assertEquals(new HashSet<>(expected), new HashSet<>(actual));
    }

    private void assertResponseEquals(
        ConsumerGroupHeartbeatResponseData expected,
        ConsumerGroupHeartbeatResponseData actual
    ) {
        if (!responseEquals(expected, actual)) {
            assertionFailure()
                .expected(expected)
                .actual(actual)
                .buildAndThrow();
        }
    }

    private boolean responseEquals(
        ConsumerGroupHeartbeatResponseData expected,
        ConsumerGroupHeartbeatResponseData actual
    ) {
        if (expected.throttleTimeMs() != actual.throttleTimeMs()) return false;
        if (expected.errorCode() != actual.errorCode()) return false;
        if (!Objects.equals(expected.errorMessage(), actual.errorMessage())) return false;
        if (!Objects.equals(expected.memberId(), actual.memberId())) return false;
        if (expected.memberEpoch() != actual.memberEpoch()) return false;
        if (expected.shouldComputeAssignment() != actual.shouldComputeAssignment()) return false;
        if (expected.heartbeatIntervalMs() != actual.heartbeatIntervalMs()) return false;
        // Unordered comparison of the assignments.
        return responseAssignmentEquals(expected.assignment(), actual.assignment());
    }

    private boolean responseAssignmentEquals(
        ConsumerGroupHeartbeatResponseData.Assignment expected,
        ConsumerGroupHeartbeatResponseData.Assignment actual
    ) {
        if (expected == actual) return true;
        if (expected == null) return false;
        if (actual == null) return false;

        if (!Objects.equals(fromAssignment(expected.pendingTopicPartitions()), fromAssignment(actual.pendingTopicPartitions())))
            return false;

        return Objects.equals(fromAssignment(expected.assignedTopicPartitions()), fromAssignment(actual.assignedTopicPartitions()));
    }

    private Map<Uuid, Set<Integer>> fromAssignment(
        List<ConsumerGroupHeartbeatResponseData.TopicPartitions> assignment
    ) {
        if (assignment == null) return null;

        Map<Uuid, Set<Integer>> assigmentMap = new HashMap<>();
        assignment.forEach(topicPartitions -> {
            assigmentMap.put(topicPartitions.topicId(), new HashSet<>(topicPartitions.partitions()));
        });
        return assigmentMap;
    }

    private void assertRecordsEquals(
        List<Record> expectedRecords,
        List<Record> actualRecords
    ) {
        try {
            assertEquals(expectedRecords.size(), actualRecords.size());

            for (int i = 0; i < expectedRecords.size(); i++) {
                Record expectedRecord = expectedRecords.get(i);
                Record actualRecord = actualRecords.get(i);
                assertRecordEquals(expectedRecord, actualRecord);
            }
        } catch (AssertionFailedError e) {
            assertionFailure()
                .expected(expectedRecords)
                .actual(actualRecords)
                .buildAndThrow();
        }
    }

    private void assertRecordEquals(
        Record expected,
        Record actual
    ) {
        try {
            assertApiMessageAndVersionEquals(expected.key(), actual.key());
            assertApiMessageAndVersionEquals(expected.value(), actual.value());
        } catch (AssertionFailedError e) {
            assertionFailure()
                .expected(expected)
                .actual(actual)
                .buildAndThrow();
        }
    }

    private void assertApiMessageAndVersionEquals(
        ApiMessageAndVersion expected,
        ApiMessageAndVersion actual
    ) {
        if (expected == actual) return;

        assertEquals(expected.version(), actual.version());

        if (actual.message() instanceof ConsumerGroupCurrentMemberAssignmentValue) {
            // The order of the topics stored in ConsumerGroupCurrentMemberAssignmentValue is not
            // always guaranteed. Therefore, we need a special comparator.
            ConsumerGroupCurrentMemberAssignmentValue expectedValue =
                (ConsumerGroupCurrentMemberAssignmentValue) expected.message();
            ConsumerGroupCurrentMemberAssignmentValue actualValue =
                (ConsumerGroupCurrentMemberAssignmentValue) actual.message();

            assertEquals(expectedValue.memberEpoch(), actualValue.memberEpoch());
            assertEquals(expectedValue.previousMemberEpoch(), actualValue.previousMemberEpoch());
            assertEquals(expectedValue.targetMemberEpoch(), actualValue.targetMemberEpoch());
            assertEquals(expectedValue.error(), actualValue.error());
            assertEquals(expectedValue.metadataVersion(), actualValue.metadataVersion());
            assertEquals(expectedValue.metadataBytes(), actualValue.metadataBytes());

            // We transform those to Maps before comparing them.
            assertEquals(fromTopicPartitions(expectedValue.assignedPartitions()),
                fromTopicPartitions(actualValue.assignedPartitions()));
            assertEquals(fromTopicPartitions(expectedValue.partitionsPendingRevocation()),
                fromTopicPartitions(actualValue.partitionsPendingRevocation()));
            assertEquals(fromTopicPartitions(expectedValue.partitionsPendingAssignment()),
                fromTopicPartitions(actualValue.partitionsPendingAssignment()));
        } else {
            assertEquals(expected.message(), actual.message());
        }
    }

    private Map<Uuid, Set<Integer>> fromTopicPartitions(
        List<ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions> assignment
    ) {
        Map<Uuid, Set<Integer>> assignmentMap = new HashMap<>();
        assignment.forEach(topicPartitions -> {
            assignmentMap.put(topicPartitions.topicId(), new HashSet<>(topicPartitions.partitions()));
        });
        return assignmentMap;
    }
}
