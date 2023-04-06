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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ConsumerProtocolAssignment;
import org.apache.kafka.common.message.ConsumerProtocolSubscription;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor.DEFAULT_GENERATION;
import static org.apache.kafka.test.TestUtils.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsumerProtocolTest {

    private final TopicPartition tp1 = new TopicPartition("foo", 1);
    private final TopicPartition tp2 = new TopicPartition("bar", 2);
    private final Optional<String> groupInstanceId = Optional.of("instance.id");
    private final int generationId = 1;
    private final Optional<String> rackId = Optional.of("rack-a");

    @Test
    public void serializeDeserializeSubscriptionAllVersions() {
        List<TopicPartition> ownedPartitions = Arrays.asList(
            new TopicPartition("foo", 0),
            new TopicPartition("bar", 0));
        Subscription subscription = new Subscription(Arrays.asList("foo", "bar"),
            ByteBuffer.wrap("hello".getBytes()), ownedPartitions, generationId, rackId);

        for (short version = ConsumerProtocolSubscription.LOWEST_SUPPORTED_VERSION; version <= ConsumerProtocolSubscription.HIGHEST_SUPPORTED_VERSION; version++) {
            ByteBuffer buffer = ConsumerProtocol.serializeSubscription(subscription, version);
            Subscription parsedSubscription = ConsumerProtocol.deserializeSubscription(buffer);

            assertEquals(toSet(subscription.topics()), toSet(parsedSubscription.topics()));
            assertEquals(subscription.userData(), parsedSubscription.userData());
            assertFalse(parsedSubscription.groupInstanceId().isPresent());

            if (version >= 1) {
                assertEquals(toSet(subscription.ownedPartitions()), toSet(parsedSubscription.ownedPartitions()));
            } else {
                assertEquals(Collections.emptyList(), parsedSubscription.ownedPartitions());
            }

            if (version >= 2) {
                assertEquals(generationId, parsedSubscription.generationId().orElse(DEFAULT_GENERATION));
            } else {
                assertFalse(parsedSubscription.generationId().isPresent());
            }

            if (version >= 3) {
                assertEquals(rackId, parsedSubscription.rackId());
            } else {
                assertEquals(Optional.empty(), parsedSubscription.rackId());
            }
        }
    }

    @Test
    public void serializeDeserializeMetadata() {
        Subscription subscription = new Subscription(Arrays.asList("foo", "bar"), ByteBuffer.wrap(new byte[0]));
        ByteBuffer buffer = ConsumerProtocol.serializeSubscription(subscription);
        Subscription parsedSubscription = ConsumerProtocol.deserializeSubscription(buffer);
        assertEquals(toSet(subscription.topics()), toSet(parsedSubscription.topics()));
        assertEquals(0, parsedSubscription.userData().limit());
        assertFalse(parsedSubscription.groupInstanceId().isPresent());
        assertFalse(parsedSubscription.generationId().isPresent());
        assertFalse(parsedSubscription.rackId().isPresent());
    }

    @Test
    public void serializeDeserializeMetadataAndGroupInstanceId() {
        Subscription subscription = new Subscription(Arrays.asList("foo", "bar"), ByteBuffer.wrap(new byte[0]));
        ByteBuffer buffer = ConsumerProtocol.serializeSubscription(subscription);

        Subscription parsedSubscription = ConsumerProtocol.deserializeSubscription(buffer);
        parsedSubscription.setGroupInstanceId(groupInstanceId);
        assertEquals(toSet(subscription.topics()), toSet(parsedSubscription.topics()));
        assertEquals(0, parsedSubscription.userData().limit());
        assertEquals(groupInstanceId, parsedSubscription.groupInstanceId());
        assertFalse(parsedSubscription.generationId().isPresent());
        assertFalse(parsedSubscription.rackId().isPresent());
    }

    @Test
    public void serializeDeserializeNullSubscriptionUserData() {
        Subscription subscription = new Subscription(Arrays.asList("foo", "bar"), null);
        ByteBuffer buffer = ConsumerProtocol.serializeSubscription(subscription);
        Subscription parsedSubscription = ConsumerProtocol.deserializeSubscription(buffer);
        assertEquals(toSet(subscription.topics()), toSet(parsedSubscription.topics()));
        assertNull(parsedSubscription.userData());
        assertFalse(parsedSubscription.rackId().isPresent());
    }

    @Test
    public void serializeSubscriptionShouldOrderTopics() {
        assertEquals(
            ConsumerProtocol.serializeSubscription(
                new Subscription(Arrays.asList("foo", "bar"), null, Arrays.asList(tp1, tp2))
            ),
            ConsumerProtocol.serializeSubscription(
                new Subscription(Arrays.asList("bar", "foo"), null, Arrays.asList(tp1, tp2))
            )
        );
    }

    @Test
    public void serializeSubscriptionShouldOrderOwnedPartitions() {
        assertEquals(
            ConsumerProtocol.serializeSubscription(
                new Subscription(Arrays.asList("foo", "bar"), null, Arrays.asList(tp1, tp2))
            ),
            ConsumerProtocol.serializeSubscription(
                new Subscription(Arrays.asList("foo", "bar"), null, Arrays.asList(tp2, tp1))
            )
        );
    }

    @Test
    public void deserializeOldSubscriptionVersion() {
        Subscription subscription = new Subscription(Arrays.asList("foo", "bar"), null);
        ByteBuffer buffer = ConsumerProtocol.serializeSubscription(subscription, (short) 0);
        Subscription parsedSubscription = ConsumerProtocol.deserializeSubscription(buffer);
        assertEquals(toSet(parsedSubscription.topics()), toSet(parsedSubscription.topics()));
        assertNull(parsedSubscription.userData());
        assertTrue(parsedSubscription.ownedPartitions().isEmpty());
        assertFalse(parsedSubscription.generationId().isPresent());
        assertFalse(parsedSubscription.rackId().isPresent());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void deserializeNewSubscriptionWithOldVersion(boolean hasGenerationIdAndRack) {
        Subscription subscription;
        if (hasGenerationIdAndRack) {
            subscription = new Subscription(Arrays.asList("foo", "bar"), null, Collections.singletonList(tp2), generationId, rackId);
        } else {
            subscription = new Subscription(Arrays.asList("foo", "bar"), null, Collections.singletonList(tp2));
        }
        ByteBuffer buffer = ConsumerProtocol.serializeSubscription(subscription);
        // ignore the version assuming it is the old byte code, as it will blindly deserialize as V0
        ConsumerProtocol.deserializeVersion(buffer);
        Subscription parsedSubscription = ConsumerProtocol.deserializeSubscription(buffer, (short) 0);
        assertEquals(toSet(subscription.topics()), toSet(parsedSubscription.topics()));
        assertNull(parsedSubscription.userData());
        assertTrue(parsedSubscription.ownedPartitions().isEmpty());
        assertFalse(parsedSubscription.groupInstanceId().isPresent());
        assertFalse(parsedSubscription.generationId().isPresent());
        assertFalse(parsedSubscription.rackId().isPresent());
    }

    @Test
    public void deserializeFutureSubscriptionVersion() {
        ByteBuffer buffer = generateFutureSubscriptionVersionData();

        Subscription subscription = ConsumerProtocol.deserializeSubscription(buffer);
        subscription.setGroupInstanceId(groupInstanceId);
        assertEquals(Collections.singleton("topic"), toSet(subscription.topics()));
        assertEquals(Collections.singleton(tp2), toSet(subscription.ownedPartitions()));
        assertEquals(groupInstanceId, subscription.groupInstanceId());
        assertEquals(generationId, subscription.generationId().orElse(DEFAULT_GENERATION));
        assertEquals(rackId, subscription.rackId());
    }

    @Test
    public void serializeDeserializeAssignmentAllVersions() {
        List<TopicPartition> partitions = Arrays.asList(tp1, tp2);
        Assignment assignment = new Assignment(partitions, ByteBuffer.wrap("hello".getBytes()));

        for (short version = ConsumerProtocolAssignment.LOWEST_SUPPORTED_VERSION; version <= ConsumerProtocolAssignment.HIGHEST_SUPPORTED_VERSION; version++) {
            ByteBuffer buffer = ConsumerProtocol.serializeAssignment(assignment, version);
            Assignment parsedAssignment = ConsumerProtocol.deserializeAssignment(buffer);
            assertEquals(toSet(partitions), toSet(parsedAssignment.partitions()));
            assertEquals(assignment.userData(), parsedAssignment.userData());
        }
    }

    @Test
    public void serializeDeserializeAssignment() {
        List<TopicPartition> partitions = Arrays.asList(tp1, tp2);
        ByteBuffer buffer = ConsumerProtocol.serializeAssignment(new Assignment(partitions, ByteBuffer.wrap(new byte[0])));
        Assignment parsedAssignment = ConsumerProtocol.deserializeAssignment(buffer);
        assertEquals(toSet(partitions), toSet(parsedAssignment.partitions()));
        assertEquals(0, parsedAssignment.userData().limit());
    }

    @Test
    public void deserializeNullAssignmentUserData() {
        List<TopicPartition> partitions = Arrays.asList(tp1, tp2);
        ByteBuffer buffer = ConsumerProtocol.serializeAssignment(new Assignment(partitions, null));
        Assignment parsedAssignment = ConsumerProtocol.deserializeAssignment(buffer);
        assertEquals(toSet(partitions), toSet(parsedAssignment.partitions()));
        assertNull(parsedAssignment.userData());
    }

    @Test
    public void deserializeFutureAssignmentVersion() {
        // verify that a new version which adds a field is still parseable
        short version = 100;

        Schema assignmentSchemaV100 = new Schema(
            new Field("assigned_partitions", new ArrayOf(
                ConsumerProtocolAssignment.TopicPartition.SCHEMA_0)),
            new Field("user_data", Type.BYTES),
            new Field("foo", Type.STRING));

        Struct assignmentV100 = new Struct(assignmentSchemaV100);
        assignmentV100.set("assigned_partitions",
            new Object[]{new Struct(ConsumerProtocolAssignment.TopicPartition.SCHEMA_0)
                .set("topic", tp1.topic())
                .set("partitions", new Object[]{tp1.partition()})});
        assignmentV100.set("user_data", ByteBuffer.wrap(new byte[0]));
        assignmentV100.set("foo", "bar");

        Struct headerV100 = new Struct(new Schema(new Field("version", Type.INT16)));
        headerV100.set("version", version);

        ByteBuffer buffer = ByteBuffer.allocate(assignmentV100.sizeOf() + headerV100.sizeOf());
        headerV100.writeTo(buffer);
        assignmentV100.writeTo(buffer);

        buffer.flip();

        Assignment assignment = ConsumerProtocol.deserializeAssignment(buffer);
        assertEquals(toSet(Collections.singletonList(tp1)), toSet(assignment.partitions()));
    }

    private ByteBuffer generateFutureSubscriptionVersionData() {
        // verify that a new version which adds a field is still parseable
        short version = 100;

        Schema subscriptionSchemaV100 = new Schema(
            new Field("topics", new ArrayOf(Type.STRING)),
            new Field("user_data", Type.NULLABLE_BYTES),
            new Field("owned_partitions", new ArrayOf(
                ConsumerProtocolSubscription.TopicPartition.SCHEMA_1)),
            new Field("generation_id", Type.INT32),
            new Field("rack_id", Type.STRING),
            new Field("bar", Type.STRING));

        Struct subscriptionV100 = new Struct(subscriptionSchemaV100);
        subscriptionV100.set("topics", new Object[]{"topic"});
        subscriptionV100.set("user_data", ByteBuffer.wrap(new byte[0]));
        subscriptionV100.set("owned_partitions", new Object[]{new Struct(
            ConsumerProtocolSubscription.TopicPartition.SCHEMA_1)
            .set("topic", tp2.topic())
            .set("partitions", new Object[]{tp2.partition()})});
        subscriptionV100.set("generation_id", generationId);
        subscriptionV100.set("rack_id", rackId.orElse(null));
        subscriptionV100.set("bar", "bar");

        Struct headerV100 = new Struct(new Schema(new Field("version", Type.INT16)));
        headerV100.set("version", version);

        ByteBuffer buffer = ByteBuffer.allocate(subscriptionV100.sizeOf() + headerV100.sizeOf());
        headerV100.writeTo(buffer);
        subscriptionV100.writeTo(buffer);

        buffer.flip();

        return buffer;
    }
}
