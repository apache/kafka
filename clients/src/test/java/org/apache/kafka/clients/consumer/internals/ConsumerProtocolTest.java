/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

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
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class ConsumerProtocolTest {
    private ConsumerProtocol protocol = new ConsumerProtocol();

    @Test
    public void serializeDeserializeMetadata() {
        GroupProtocol.GenericType<ConsumerProtocol.Subscription> schema = protocol.metadataSchema();

        ConsumerProtocol.Subscription subscription = new ConsumerProtocol.Subscription(
                Arrays.asList("foo", "bar"));

        ByteBuffer buffer = ByteBuffer.allocate(schema.sizeOf(subscription));
        schema.write(buffer, subscription);

        buffer.flip();

        Object parsedObj = schema.read(buffer);
        ConsumerProtocol.Subscription parsedSubscription = schema.validate(parsedObj);
        assertEquals(subscription.topics(), parsedSubscription.topics());
    }

    @Test
    public void deserializeNewSubscriptionVersion() {
        // verify that a new version which adds a field is still parseable
        short version = 100;

        Schema subscriptionSchemaV100 = new Schema(
                new Field(ConsumerProtocol.SUBSCRIPTION_KEY_NAME, new ArrayOf(Type.STRING)),
                new Field("foo", Type.STRING));

        Struct subscriptionV100 = new Struct(subscriptionSchemaV100);
        subscriptionV100.set(ConsumerProtocol.SUBSCRIPTION_KEY_NAME, new Object[]{"foo", "bar"});
        subscriptionV100.set("foo", "bar");

        Struct headerV100 = new Struct(ConsumerProtocol.CONSUMER_PROTOCOL_HEADER_SCHEMA);
        headerV100.set(ConsumerProtocol.VERSION_KEY_NAME, version);

        ByteBuffer buffer = ByteBuffer.allocate(subscriptionV100.sizeOf() + headerV100.sizeOf());
        headerV100.writeTo(buffer);
        subscriptionV100.writeTo(buffer);

        buffer.flip();

        Object subscriptionObj = protocol.metadataSchema().read(buffer);
        ConsumerProtocol.Subscription subscription = protocol.metadataSchema().validate(subscriptionObj);

        assertEquals(version, subscription.version());
        assertEquals(Arrays.asList("foo", "bar"), subscription.topics());
    }

    @Test
    public void serializeDeserializeAssignment() {
        GroupProtocol.GenericType<ConsumerProtocol.Assignment> schema = protocol.assignmentSchema();

        ConsumerProtocol.Assignment assignment = new ConsumerProtocol.Assignment(
                Arrays.asList(new TopicPartition("foo", 0), new TopicPartition("bar", 2)));

        ByteBuffer buffer = ByteBuffer.allocate(schema.sizeOf(assignment));
        schema.write(buffer, assignment);

        buffer.flip();

        Object parsedObj = schema.read(buffer);
        ConsumerProtocol.Assignment parsedAssignment = schema.validate(parsedObj);
        assertEquals(toSet(assignment.partitions()), toSet(parsedAssignment.partitions()));
    }

    @Test
    public void deserializeNewAssignmentVersion() {
        // verify that a new version which adds a field is still parseable
        short version = 100;

        Schema assignmentSchemaV100 = new Schema(
                new Field(ConsumerProtocol.ASSIGNMENT_KEY_NAME, new ArrayOf(ConsumerProtocol.TOPIC_ASSIGNMENT_V0)),
                new Field("foo", Type.STRING));

        Struct assignmentV100 = new Struct(assignmentSchemaV100);
        assignmentV100.set(ConsumerProtocol.ASSIGNMENT_KEY_NAME,
                new Object[]{new Struct(ConsumerProtocol.TOPIC_ASSIGNMENT_V0)
                        .set(ConsumerProtocol.TOPIC_KEY_NAME, "foo")
                        .set(ConsumerProtocol.PARTITIONS_KEY_NAME, new Object[]{1})});
        assignmentV100.set("foo", "bar");

        Struct headerV100 = new Struct(ConsumerProtocol.CONSUMER_PROTOCOL_HEADER_SCHEMA);
        headerV100.set(ConsumerProtocol.VERSION_KEY_NAME, version);

        ByteBuffer buffer = ByteBuffer.allocate(assignmentV100.sizeOf() + headerV100.sizeOf());
        headerV100.writeTo(buffer);
        assignmentV100.writeTo(buffer);

        buffer.flip();

        Object assignmentObj = protocol.assignmentSchema().read(buffer);
        ConsumerProtocol.Assignment assignment = protocol.assignmentSchema().validate(assignmentObj);

        assertEquals(version, assignment.version());
        assertEquals(toSet(Arrays.asList(new TopicPartition("foo", 1))), toSet(assignment.partitions()));
    }

    private static <T> Set<T> toSet(Collection<T> collection) {
        return new HashSet<>(collection);
    }

}
