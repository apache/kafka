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

import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Subscription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ConsumerProtocolTest {

    @Test
    public void serializeDeserializeMetadata() {
        Subscription subscription = new Subscription(Arrays.asList("foo", "bar"));
        ByteBuffer buffer = ConsumerProtocol.serializeSubscription(subscription);
        Subscription parsedSubscription = ConsumerProtocol.deserializeSubscription(buffer);
        assertEquals(subscription.topics(), parsedSubscription.topics());
    }

    @Test
    public void serializeDeserializeNullSubscriptionUserData() {
        Subscription subscription = new Subscription(Arrays.asList("foo", "bar"), null);
        ByteBuffer buffer = ConsumerProtocol.serializeSubscription(subscription);
        Subscription parsedSubscription = ConsumerProtocol.deserializeSubscription(buffer);
        assertEquals(subscription.topics(), parsedSubscription.topics());
        assertNull(subscription.userData());
    }

    @Test
    public void deserializeNewSubscriptionVersion() {
        // verify that a new version which adds a field is still parseable
        short version = 100;

        Schema subscriptionSchemaV100 = new Schema(
                new Field(ConsumerProtocol.TOPICS_KEY_NAME, new ArrayOf(Type.STRING)),
                new Field(ConsumerProtocol.USER_DATA_KEY_NAME, Type.BYTES),
                new Field("foo", Type.STRING));

        Struct subscriptionV100 = new Struct(subscriptionSchemaV100);
        subscriptionV100.set(ConsumerProtocol.TOPICS_KEY_NAME, new Object[]{"topic"});
        subscriptionV100.set(ConsumerProtocol.USER_DATA_KEY_NAME, ByteBuffer.wrap(new byte[0]));
        subscriptionV100.set("foo", "bar");

        Struct headerV100 = new Struct(ConsumerProtocol.CONSUMER_PROTOCOL_HEADER_SCHEMA);
        headerV100.set(ConsumerProtocol.VERSION_KEY_NAME, version);

        ByteBuffer buffer = ByteBuffer.allocate(subscriptionV100.sizeOf() + headerV100.sizeOf());
        headerV100.writeTo(buffer);
        subscriptionV100.writeTo(buffer);

        buffer.flip();

        Subscription subscription = ConsumerProtocol.deserializeSubscription(buffer);
        assertEquals(Arrays.asList("topic"), subscription.topics());
    }

    @Test
    public void serializeDeserializeAssignment() {
        List<TopicPartition> partitions = Arrays.asList(new TopicPartition("foo", 0), new TopicPartition("bar", 2));
        ByteBuffer buffer = ConsumerProtocol.serializeAssignment(new PartitionAssignor.Assignment(partitions));
        PartitionAssignor.Assignment parsedAssignment = ConsumerProtocol.deserializeAssignment(buffer);
        assertEquals(toSet(partitions), toSet(parsedAssignment.partitions()));
    }

    @Test
    public void deserializeNullAssignmentUserData() {
        List<TopicPartition> partitions = Arrays.asList(new TopicPartition("foo", 0), new TopicPartition("bar", 2));
        ByteBuffer buffer = ConsumerProtocol.serializeAssignment(new PartitionAssignor.Assignment(partitions, null));
        PartitionAssignor.Assignment parsedAssignment = ConsumerProtocol.deserializeAssignment(buffer);
        assertEquals(toSet(partitions), toSet(parsedAssignment.partitions()));
        assertNull(parsedAssignment.userData());
    }

    @Test
    public void deserializeNewAssignmentVersion() {
        // verify that a new version which adds a field is still parseable
        short version = 100;

        Schema assignmentSchemaV100 = new Schema(
                new Field(ConsumerProtocol.TOPIC_PARTITIONS_KEY_NAME, new ArrayOf(ConsumerProtocol.TOPIC_ASSIGNMENT_V0)),
                new Field(ConsumerProtocol.USER_DATA_KEY_NAME, Type.BYTES),
                new Field("foo", Type.STRING));

        Struct assignmentV100 = new Struct(assignmentSchemaV100);
        assignmentV100.set(ConsumerProtocol.TOPIC_PARTITIONS_KEY_NAME,
                new Object[]{new Struct(ConsumerProtocol.TOPIC_ASSIGNMENT_V0)
                        .set(ConsumerProtocol.TOPIC_KEY_NAME, "foo")
                        .set(ConsumerProtocol.PARTITIONS_KEY_NAME, new Object[]{1})});
        assignmentV100.set(ConsumerProtocol.USER_DATA_KEY_NAME, ByteBuffer.wrap(new byte[0]));
        assignmentV100.set("foo", "bar");

        Struct headerV100 = new Struct(ConsumerProtocol.CONSUMER_PROTOCOL_HEADER_SCHEMA);
        headerV100.set(ConsumerProtocol.VERSION_KEY_NAME, version);

        ByteBuffer buffer = ByteBuffer.allocate(assignmentV100.sizeOf() + headerV100.sizeOf());
        headerV100.writeTo(buffer);
        assignmentV100.writeTo(buffer);

        buffer.flip();

        PartitionAssignor.Assignment assignment = ConsumerProtocol.deserializeAssignment(buffer);
        assertEquals(toSet(Arrays.asList(new TopicPartition("foo", 1))), toSet(assignment.partitions()));
    }

    private static <T> Set<T> toSet(Collection<T> collection) {
        return new HashSet<>(collection);
    }
}
