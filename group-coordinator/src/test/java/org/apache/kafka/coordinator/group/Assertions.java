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
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.ShareGroupHeartbeatResponseData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.GroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ShareGroupPartitionMetadataValue;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import org.opentest4j.AssertionFailedError;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.junit.jupiter.api.AssertionFailureBuilder.assertionFailure;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

public class Assertions {
    private static final BiConsumer<ApiMessage, ApiMessage> API_MESSAGE_DEFAULT_COMPARATOR = org.junit.jupiter.api.Assertions::assertEquals;
    private static final Map<Class<?>, BiConsumer<ApiMessage, ApiMessage>> API_MESSAGE_COMPARATORS = Map.of(
        // Register request/response comparators.
        ConsumerGroupHeartbeatResponseData.class, Assertions::assertConsumerGroupHeartbeatResponse,
        ShareGroupHeartbeatResponseData.class, Assertions::assertShareGroupHeartbeatResponse,
        SyncGroupResponseData.class, Assertions::assertSyncGroupResponse,

        // Register record comparators.
        ConsumerGroupCurrentMemberAssignmentValue.class, Assertions::assertConsumerGroupCurrentMemberAssignmentValue,
        ConsumerGroupPartitionMetadataValue.class, Assertions::assertConsumerGroupPartitionMetadataValue,
        GroupMetadataValue.class, Assertions::assertGroupMetadataValue,
        ConsumerGroupTargetAssignmentMemberValue.class, Assertions::assertConsumerGroupTargetAssignmentMemberValue,
        ShareGroupPartitionMetadataValue.class, Assertions::assertShareGroupPartitionMetadataValue
    );

    public static <T> void assertUnorderedListEquals(
        List<T> expected,
        List<T> actual
    ) {
        assertEquals(new HashSet<>(expected), new HashSet<>(actual));
    }

    public static void assertResponseEquals(
        ApiMessage expected,
        ApiMessage actual
    ) {
        BiConsumer<ApiMessage, ApiMessage> asserter = API_MESSAGE_COMPARATORS
            .getOrDefault(expected.getClass(), API_MESSAGE_DEFAULT_COMPARATOR);
        asserter.accept(expected, actual);
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

    private static void assertConsumerGroupHeartbeatResponse(
        ApiMessage exp,
        ApiMessage act
    ) {
        ConsumerGroupHeartbeatResponseData expected = (ConsumerGroupHeartbeatResponseData) exp.duplicate();
        ConsumerGroupHeartbeatResponseData actual = (ConsumerGroupHeartbeatResponseData) act.duplicate();

        Consumer<ConsumerGroupHeartbeatResponseData> normalize = message -> {
            if (message.assignment() != null) {
                message.assignment().topicPartitions().sort(Comparator.comparing(ConsumerGroupHeartbeatResponseData.TopicPartitions::topicId));
                message.assignment().topicPartitions().forEach(topic -> topic.partitions().sort(Integer::compareTo));
            }
        };

        normalize.accept(expected);
        normalize.accept(actual);

        assertEquals(expected, actual);
    }

    private static void assertShareGroupHeartbeatResponse(
        ApiMessage exp,
        ApiMessage act
    ) {
        ShareGroupHeartbeatResponseData expected = (ShareGroupHeartbeatResponseData) exp.duplicate();
        ShareGroupHeartbeatResponseData actual = (ShareGroupHeartbeatResponseData) act.duplicate();

        Consumer<ShareGroupHeartbeatResponseData> normalize = message -> {
            if (message.assignment() != null) {
                message.assignment().topicPartitions().sort(Comparator.comparing(ShareGroupHeartbeatResponseData.TopicPartitions::topicId));
                message.assignment().topicPartitions().forEach(topic -> topic.partitions().sort(Integer::compareTo));
            }
        };

        normalize.accept(expected);
        normalize.accept(actual);

        assertEquals(expected, actual);
    }

    private static void assertApiMessageAndVersionEquals(
        ApiMessageAndVersion expected,
        ApiMessageAndVersion actual
    ) {
        if (expected == actual) return;
        assertNotNull(expected);
        assertNotNull(actual);
        assertEquals(expected.version(), actual.version());
        BiConsumer<ApiMessage, ApiMessage> asserter = API_MESSAGE_COMPARATORS
            .getOrDefault(expected.message().getClass(), API_MESSAGE_DEFAULT_COMPARATOR);
        asserter.accept(expected.message(), actual.message());
    }

    private static void assertConsumerGroupCurrentMemberAssignmentValue(
        ApiMessage exp,
        ApiMessage act
    ) {
        // The order of the topics stored in ConsumerGroupCurrentMemberAssignmentValue is not
        // always guaranteed. Therefore, we need a special comparator.
        ConsumerGroupCurrentMemberAssignmentValue expected = (ConsumerGroupCurrentMemberAssignmentValue) exp.duplicate();
        ConsumerGroupCurrentMemberAssignmentValue actual = (ConsumerGroupCurrentMemberAssignmentValue) act.duplicate();

        Consumer<List<ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions>> sortTopicsAndPartitions = topicPartitions -> {
            topicPartitions.sort(Comparator.comparing(ConsumerGroupCurrentMemberAssignmentValue.TopicPartitions::topicId));
            topicPartitions.forEach(topic -> topic.partitions().sort(Integer::compareTo));
        };

        Consumer<ConsumerGroupCurrentMemberAssignmentValue> normalize = message -> {
            sortTopicsAndPartitions.accept(message.assignedPartitions());
            sortTopicsAndPartitions.accept(message.partitionsPendingRevocation());
        };

        normalize.accept(expected);
        normalize.accept(actual);

        assertEquals(expected, actual);
    }

    private static void assertConsumerGroupPartitionMetadataValue(
        ApiMessage exp,
        ApiMessage act
    ) {
        // The order of the racks stored in the PartitionMetadata of the ConsumerGroupPartitionMetadataValue
        // is not always guaranteed. Therefore, we need a special comparator.
        ConsumerGroupPartitionMetadataValue expected = (ConsumerGroupPartitionMetadataValue) exp.duplicate();
        ConsumerGroupPartitionMetadataValue actual = (ConsumerGroupPartitionMetadataValue) act.duplicate();

        Consumer<ConsumerGroupPartitionMetadataValue> normalize = message -> {
            message.topics().sort(Comparator.comparing(ConsumerGroupPartitionMetadataValue.TopicMetadata::topicId));
            message.topics().forEach(topic -> {
                topic.partitionMetadata().sort(Comparator.comparing(ConsumerGroupPartitionMetadataValue.PartitionMetadata::partition));
                topic.partitionMetadata().forEach(partition -> partition.racks().sort(String::compareTo));
            });
        };

        normalize.accept(expected);
        normalize.accept(actual);

        assertEquals(expected, actual);
    }

    private static void assertShareGroupPartitionMetadataValue(
        ApiMessage exp,
        ApiMessage act
    ) {
        // The order of the racks stored in the PartitionMetadata of the ShareGroupPartitionMetadataValue
        // is not always guaranteed. Therefore, we need a special comparator.
        ShareGroupPartitionMetadataValue expected = (ShareGroupPartitionMetadataValue) exp.duplicate();
        ShareGroupPartitionMetadataValue actual = (ShareGroupPartitionMetadataValue) act.duplicate();

        Consumer<ShareGroupPartitionMetadataValue> normalize = message -> {
            message.topics().sort(Comparator.comparing(ShareGroupPartitionMetadataValue.TopicMetadata::topicId));
            message.topics().forEach(topic -> {
                topic.partitionMetadata().sort(Comparator.comparing(ShareGroupPartitionMetadataValue.PartitionMetadata::partition));
                topic.partitionMetadata().forEach(partition -> partition.racks().sort(String::compareTo));
            });
        };

        normalize.accept(expected);
        normalize.accept(actual);

        assertEquals(expected, actual);
    }

    private static void assertGroupMetadataValue(
        ApiMessage exp,
        ApiMessage act
    ) {
        GroupMetadataValue expected = (GroupMetadataValue) exp.duplicate();
        GroupMetadataValue actual = (GroupMetadataValue) act.duplicate();

        Consumer<GroupMetadataValue> normalize = message -> {
            message.members().sort(Comparator.comparing(GroupMetadataValue.MemberMetadata::memberId));
            try {
                message.members().forEach(memberMetadata -> {
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
                });
            } catch (SchemaException ex) {
                fail("Failed deserialization: " + ex.getMessage());
            }
        };

        normalize.accept(expected);
        normalize.accept(actual);

        assertEquals(expected, actual);
    }

    private static void assertConsumerGroupTargetAssignmentMemberValue(
        ApiMessage exp,
        ApiMessage act
    ) {
        ConsumerGroupTargetAssignmentMemberValue expected = (ConsumerGroupTargetAssignmentMemberValue) exp.duplicate();
        ConsumerGroupTargetAssignmentMemberValue actual = (ConsumerGroupTargetAssignmentMemberValue) act.duplicate();

        Consumer<ConsumerGroupTargetAssignmentMemberValue> normalize = message -> {
            message.topicPartitions().sort(Comparator.comparing(ConsumerGroupTargetAssignmentMemberValue.TopicPartition::topicId));
            message.topicPartitions().forEach(topic -> topic.partitions().sort(Integer::compareTo));
        };

        normalize.accept(expected);
        normalize.accept(actual);

        assertEquals(expected, actual);
    }

    private static void assertSyncGroupResponse(
        ApiMessage exp,
        ApiMessage act
    ) {
        SyncGroupResponseData expected = (SyncGroupResponseData) exp.duplicate();
        SyncGroupResponseData actual = (SyncGroupResponseData) act.duplicate();

        Consumer<SyncGroupResponseData> normalize = message -> {
            try {
                ConsumerPartitionAssignor.Assignment assignment =
                    ConsumerProtocol.deserializeAssignment(ByteBuffer.wrap(message.assignment()));
                assignment.partitions().sort(
                    Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition)
                );
                message.setAssignment(Utils.toArray(ConsumerProtocol.serializeAssignment(
                    assignment,
                    ConsumerProtocol.deserializeVersion(ByteBuffer.wrap(message.assignment()))
                )));
            } catch (SchemaException ex) {
                fail("Failed deserialization: " + ex.getMessage());
            }
        };

        normalize.accept(expected);
        normalize.accept(actual);

        assertEquals(expected, actual);
    }
}
