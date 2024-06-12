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

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.GroupMetadataValue;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.opentest4j.AssertionFailedError;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.junit.jupiter.api.AssertionFailureBuilder.assertionFailure;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class Assertions {
    public static <T> void assertUnorderedListEquals(
        List<T> expected,
        List<T> actual
    ) {
        assertEquals(new HashSet<>(expected), new HashSet<>(actual));
    }

    public static void assertResponseEquals(
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

    private static boolean responseEquals(
        ConsumerGroupHeartbeatResponseData expected,
        ConsumerGroupHeartbeatResponseData actual
    ) {
        if (expected.throttleTimeMs() != actual.throttleTimeMs()) return false;
        if (expected.errorCode() != actual.errorCode()) return false;
        if (!Objects.equals(expected.errorMessage(), actual.errorMessage())) return false;
        if (!Objects.equals(expected.memberId(), actual.memberId())) return false;
        if (expected.memberEpoch() != actual.memberEpoch()) return false;
        if (expected.heartbeatIntervalMs() != actual.heartbeatIntervalMs()) return false;
        // Unordered comparison of the assignments.
        return responseAssignmentEquals(expected.assignment(), actual.assignment());
    }

    private static boolean responseAssignmentEquals(
        ConsumerGroupHeartbeatResponseData.Assignment expected,
        ConsumerGroupHeartbeatResponseData.Assignment actual
    ) {
        if (expected == actual) return true;
        if (expected == null) return false;
        if (actual == null) return false;

        return Objects.equals(fromAssignment(expected.topicPartitions()), fromAssignment(actual.topicPartitions()));
    }

    private static Map<Uuid, Set<Integer>> fromAssignment(
        List<ConsumerGroupHeartbeatResponseData.TopicPartitions> assignment
    ) {
        if (assignment == null) return null;

        Map<Uuid, Set<Integer>> assignmentMap = new HashMap<>();
        assignment.forEach(topicPartitions ->
            assignmentMap.put(topicPartitions.topicId(), new HashSet<>(topicPartitions.partitions()))
        );
        return assignmentMap;
    }

    public static void assertRecordsEquals(
        List<CoordinatorRecord> expectedRecords,
        List<CoordinatorRecord> actualRecords
    ) {
        try {
            assertEquals(expectedRecords.size(), actualRecords.size());

            for (int i = 0; i < expectedRecords.size(); i++) {
                CoordinatorRecord expectedRecord = expectedRecords.get(i);
                CoordinatorRecord actualRecord = actualRecords.get(i);
                assertRecordEquals(expectedRecord, actualRecord);
            }
        } catch (AssertionFailedError e) {
            assertionFailure()
                .expected(expectedRecords)
                .actual(actualRecords)
                .buildAndThrow();
        }
    }

    public static void assertRecordEquals(
        CoordinatorRecord expected,
        CoordinatorRecord actual
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

    private static void assertApiMessageAndVersionEquals(
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

            // We transform those to Maps before comparing them.
            assertEquals(fromTopicPartitions(expectedValue.assignedPartitions()),
                fromTopicPartitions(actualValue.assignedPartitions()));
            assertEquals(fromTopicPartitions(expectedValue.partitionsPendingRevocation()),
                fromTopicPartitions(actualValue.partitionsPendingRevocation()));
        } else if (actual.message() instanceof ConsumerGroupPartitionMetadataValue) {
            // The order of the racks stored in the PartitionMetadata of the ConsumerGroupPartitionMetadataValue
            // is not always guaranteed. Therefore, we need a special comparator.
            ConsumerGroupPartitionMetadataValue expectedValue =
                (ConsumerGroupPartitionMetadataValue) expected.message().duplicate();
            ConsumerGroupPartitionMetadataValue actualValue =
                (ConsumerGroupPartitionMetadataValue) actual.message().duplicate();

            List<ConsumerGroupPartitionMetadataValue.TopicMetadata> expectedTopicMetadataList =
                expectedValue.topics();
            List<ConsumerGroupPartitionMetadataValue.TopicMetadata> actualTopicMetadataList =
                actualValue.topics();

            if (expectedTopicMetadataList.size() != actualTopicMetadataList.size()) {
                fail("Topic metadata lists have different sizes");
            }

            expectedTopicMetadataList.sort(Comparator.comparing(ConsumerGroupPartitionMetadataValue.TopicMetadata::topicId));
            actualTopicMetadataList.sort(Comparator.comparing(ConsumerGroupPartitionMetadataValue.TopicMetadata::topicId));

            for (int i = 0; i < expectedTopicMetadataList.size(); i++) {
                ConsumerGroupPartitionMetadataValue.TopicMetadata expectedTopicMetadata =
                    expectedTopicMetadataList.get(i);
                ConsumerGroupPartitionMetadataValue.TopicMetadata actualTopicMetadata =
                    actualTopicMetadataList.get(i);

                assertEquals(expectedTopicMetadata.topicId(), actualTopicMetadata.topicId());
                assertEquals(expectedTopicMetadata.topicName(), actualTopicMetadata.topicName());
                assertEquals(expectedTopicMetadata.numPartitions(), actualTopicMetadata.numPartitions());

                List<ConsumerGroupPartitionMetadataValue.PartitionMetadata> expectedPartitionMetadataList =
                    expectedTopicMetadata.partitionMetadata();
                List<ConsumerGroupPartitionMetadataValue.PartitionMetadata> actualPartitionMetadataList =
                    actualTopicMetadata.partitionMetadata();

                // If the list is empty, rack information wasn't available for any replica of
                // the partition and hence, the entry wasn't added to the record.
                if (expectedPartitionMetadataList.size() != actualPartitionMetadataList.size()) {
                    fail("Partition metadata lists have different sizes");
                } else if (!expectedPartitionMetadataList.isEmpty() && !actualPartitionMetadataList.isEmpty()) {
                    for (int j = 0; j < expectedPartitionMetadataList.size(); j++) {
                        ConsumerGroupPartitionMetadataValue.PartitionMetadata expectedPartitionMetadata =
                            expectedPartitionMetadataList.get(j);
                        ConsumerGroupPartitionMetadataValue.PartitionMetadata actualPartitionMetadata =
                            actualPartitionMetadataList.get(j);

                        assertEquals(expectedPartitionMetadata.partition(), actualPartitionMetadata.partition());
                        assertUnorderedListEquals(expectedPartitionMetadata.racks(), actualPartitionMetadata.racks());
                    }
                }
            }
        } else if (actual.message() instanceof GroupMetadataValue) {
            GroupMetadataValue expectedValue = (GroupMetadataValue) expected.message().duplicate();
            GroupMetadataValue actualValue = (GroupMetadataValue) actual.message().duplicate();

            Comparator<GroupMetadataValue.MemberMetadata> comparator =
                Comparator.comparing(GroupMetadataValue.MemberMetadata::memberId);
            expectedValue.members().sort(comparator);
            actualValue.members().sort(comparator);
            try {
                Arrays.asList(expectedValue, actualValue).forEach(value ->
                    value.members().forEach(memberMetadata -> {
                        // Sort topics and ownedPartitions in Subscription.
                        ConsumerPartitionAssignor.Subscription subscription =
                            ConsumerProtocol.deserializeSubscription(ByteBuffer.wrap(memberMetadata.subscription()));
                        subscription.topics().sort(String::compareTo);
                        subscription.ownedPartitions().sort(
                            Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition)
                        );
                        memberMetadata.setSubscription(Utils.toArray(ConsumerProtocol.serializeSubscription(
                            subscription,
                            ConsumerProtocol.deserializeVersion(ByteBuffer.wrap(memberMetadata.subscription()))
                        )));

                        // Sort partitions in Assignment.
                        ConsumerPartitionAssignor.Assignment assignment =
                            ConsumerProtocol.deserializeAssignment(ByteBuffer.wrap(memberMetadata.assignment()));
                        assignment.partitions().sort(
                            Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition)
                        );
                        memberMetadata.setAssignment(Utils.toArray(ConsumerProtocol.serializeAssignment(
                            assignment,
                            ConsumerProtocol.deserializeVersion(ByteBuffer.wrap(memberMetadata.assignment()))
                        )));
                    })
                );
            } catch (SchemaException ex) {
                fail("Failed deserialization: " + ex.getMessage());
            }
            assertEquals(expectedValue, actualValue);
        } else if (actual.message() instanceof ConsumerGroupTargetAssignmentMemberValue) {
            ConsumerGroupTargetAssignmentMemberValue expectedValue =
                (ConsumerGroupTargetAssignmentMemberValue) expected.message().duplicate();
            ConsumerGroupTargetAssignmentMemberValue actualValue =
                (ConsumerGroupTargetAssignmentMemberValue) actual.message().duplicate();

            Comparator<ConsumerGroupTargetAssignmentMemberValue.TopicPartition> comparator =
                Comparator.comparing(ConsumerGroupTargetAssignmentMemberValue.TopicPartition::topicId);
            expectedValue.topicPartitions().sort(comparator);
            actualValue.topicPartitions().sort(comparator);

            assertEquals(expectedValue, actualValue);
        } else {
            assertEquals(expected.message(), actual.message());
        }
    }

    private static Map<Uuid, Set<Integer>> fromTopicPartitions(
        List<ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions> assignment
    ) {
        Map<Uuid, Set<Integer>> assignmentMap = new HashMap<>();
        assignment.forEach(topicPartitions ->
            assignmentMap.put(topicPartitions.topicId(), new HashSet<>(topicPartitions.partitions()))
        );
        return assignmentMap;
    }

    public static void assertSyncGroupResponseEquals(
        SyncGroupResponseData expected,
        SyncGroupResponseData actual
    ) {
        SyncGroupResponseData expectedDuplicate = expected.duplicate();
        SyncGroupResponseData actualDuplicate = actual.duplicate();

        Arrays.asList(expectedDuplicate, actualDuplicate).forEach(duplicate -> {
            try {
                ConsumerPartitionAssignor.Assignment assignment =
                    ConsumerProtocol.deserializeAssignment(ByteBuffer.wrap(duplicate.assignment()));
                assignment.partitions().sort(
                    Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition)
                );
                duplicate.setAssignment(Utils.toArray(ConsumerProtocol.serializeAssignment(
                    assignment,
                    ConsumerProtocol.deserializeVersion(ByteBuffer.wrap(duplicate.assignment()))
                )));
            } catch (SchemaException ex) {
                fail("Failed deserialization: " + ex.getMessage());
            }
        });
        assertEquals(expectedDuplicate, actualDuplicate);
    }
}
