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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.consumer.AbstractPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.AbstractPartitionAssignor.Subscription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.GenericType;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RangeAssignorTest {

    private RangeAssignor assignor = new RangeAssignor();

    @Test
    public void serializeDeserializeMetadata() {
        GenericType<Subscription> schema = assignor.subscriptionSchema();

        PartitionAssignor.Subscription subscription = new Subscription(Arrays.asList("foo", "bar"));

        ByteBuffer buffer = ByteBuffer.allocate(schema.sizeOf(subscription));
        schema.write(buffer, subscription);

        buffer.flip();

        Object parsedObj = schema.read(buffer);
        PartitionAssignor.Subscription parsedSubscription = schema.validate(parsedObj);
        assertEquals(subscription.topics(), parsedSubscription.topics());
    }

    @Test
    public void deserializeNewSubscriptionVersion() {
        // verify that a new version which adds a field is still parseable
        short version = 100;

        Schema subscriptionSchemaV100 = new Schema(
                new Field(AbstractPartitionAssignor.TOPICS_KEY_NAME, new ArrayOf(Type.STRING)),
                new Field("foo", Type.STRING));

        Struct subscriptionV100 = new Struct(subscriptionSchemaV100);
        subscriptionV100.set(AbstractPartitionAssignor.TOPICS_KEY_NAME, new Object[]{"foo", "bar"});
        subscriptionV100.set("foo", "bar");

        Struct headerV100 = new Struct(AbstractPartitionAssignor.CONSUMER_PROTOCOL_HEADER_SCHEMA);
        headerV100.set(AbstractPartitionAssignor.VERSION_KEY_NAME, version);

        ByteBuffer buffer = ByteBuffer.allocate(subscriptionV100.sizeOf() + headerV100.sizeOf());
        headerV100.writeTo(buffer);
        subscriptionV100.writeTo(buffer);

        buffer.flip();

        Object subscriptionObj = assignor.subscriptionSchema().read(buffer);
        Subscription subscription = assignor.subscriptionSchema().validate(subscriptionObj);

        assertEquals(version, subscription.version());
        assertEquals(Arrays.asList("foo", "bar"), subscription.topics());
    }

    @Test
    public void serializeDeserializeAssignment() {
        GenericType<Assignment> schema = assignor.assignmentSchema();

        Assignment assignment = new Assignment(Arrays.asList(new TopicPartition("foo", 0),
                new TopicPartition("bar", 2)));

        ByteBuffer buffer = ByteBuffer.allocate(schema.sizeOf(assignment));
        schema.write(buffer, assignment);

        buffer.flip();

        Object parsedObj = schema.read(buffer);
        Assignment parsedAssignment = schema.validate(parsedObj);
        assertEquals(toSet(assignment.partitions()), toSet(parsedAssignment.partitions()));
    }

    @Test
    public void deserializeNewAssignmentVersion() {
        // verify that a new version which adds a field is still parseable
        short version = 100;

        Schema assignmentSchemaV100 = new Schema(
                new Field(AbstractPartitionAssignor.TOPIC_PARTITIONS_KEY_NAME, new ArrayOf(AbstractPartitionAssignor.TOPIC_ASSIGNMENT_V0)),
                new Field("foo", Type.STRING));

        Struct assignmentV100 = new Struct(assignmentSchemaV100);
        assignmentV100.set(AbstractPartitionAssignor.TOPIC_PARTITIONS_KEY_NAME,
                new Object[]{new Struct(AbstractPartitionAssignor.TOPIC_ASSIGNMENT_V0)
                        .set(AbstractPartitionAssignor.TOPIC_KEY_NAME, "foo")
                        .set(AbstractPartitionAssignor.PARTITIONS_KEY_NAME, new Object[]{1})});
        assignmentV100.set("foo", "bar");

        Struct headerV100 = new Struct(AbstractPartitionAssignor.CONSUMER_PROTOCOL_HEADER_SCHEMA);
        headerV100.set(AbstractPartitionAssignor.VERSION_KEY_NAME, version);

        ByteBuffer buffer = ByteBuffer.allocate(assignmentV100.sizeOf() + headerV100.sizeOf());
        headerV100.writeTo(buffer);
        assignmentV100.writeTo(buffer);

        buffer.flip();

        Object assignmentObj = assignor.assignmentSchema().read(buffer);
        Assignment assignment = assignor.assignmentSchema().validate(assignmentObj);

        assertEquals(version, assignment.version());
        assertEquals(toSet(Arrays.asList(new TopicPartition("foo", 1))), toSet(assignment.partitions()));
    }

    private static <T> Set<T> toSet(Collection<T> collection) {
        return new HashSet<>(collection);
    }

    @Test
    public void testOneConsumerNoTopic() {
        String consumerId = "consumer";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic,
                Collections.singletonMap(consumerId, Collections.<String>emptyList()));

        assertEquals(Collections.singleton(consumerId), assignment.keySet());
        assertTrue(assignment.get(consumerId).isEmpty());
    }

    @Test
    public void testOneConsumerNonexistentTopic() {
        String topic = "topic";
        String consumerId = "consumer";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 0);

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic,
                Collections.singletonMap(consumerId, Arrays.asList(topic)));
        assertEquals(Collections.singleton(consumerId), assignment.keySet());
        assertTrue(assignment.get(consumerId).isEmpty());
    }

    @Test
    public void testOneConsumerOneTopic() {
        String topic = "topic";
        String consumerId = "consumer";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 3);

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic,
                Collections.singletonMap(consumerId, Arrays.asList(topic)));

        assertEquals(Collections.singleton(consumerId), assignment.keySet());
        assertAssignment(Arrays.asList(
                new TopicPartition(topic, 0),
                new TopicPartition(topic, 1),
                new TopicPartition(topic, 2)), assignment.get(consumerId));
    }

    @Test
    public void testOnlyAssignsPartitionsFromSubscribedTopics() {
        String topic = "topic";
        String otherTopic = "other";
        String consumerId = "consumer";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 3);
        partitionsPerTopic.put(otherTopic, 3);

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic,
                Collections.singletonMap(consumerId, Arrays.asList(topic)));
        assertEquals(Collections.singleton(consumerId), assignment.keySet());
        assertAssignment(Arrays.asList(
                new TopicPartition(topic, 0),
                new TopicPartition(topic, 1),
                new TopicPartition(topic, 2)), assignment.get(consumerId));
    }

    @Test
    public void testOneConsumerMultipleTopics() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        String consumerId = "consumer";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, 1);
        partitionsPerTopic.put(topic2, 2);

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic,
                Collections.singletonMap(consumerId, Arrays.asList(topic1, topic2)));

        assertEquals(Collections.singleton(consumerId), assignment.keySet());
        assertAssignment(Arrays.asList(
                new TopicPartition(topic1, 0),
                new TopicPartition(topic2, 0),
                new TopicPartition(topic2, 1)), assignment.get(consumerId));
    }

    @Test
    public void testTwoConsumersOneTopicOnePartition() {
        String topic = "topic";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 1);

        Map<String, List<String>> consumers = new HashMap<>();
        consumers.put(consumer1, Arrays.asList(topic));
        consumers.put(consumer2, Arrays.asList(topic));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        assertAssignment(Arrays.asList(new TopicPartition(topic, 0)), assignment.get(consumer1));
        assertAssignment(Collections.<TopicPartition>emptyList(), assignment.get(consumer2));
    }


    @Test
    public void testTwoConsumersOneTopicTwoPartitions() {
        String topic = "topic";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic, 2);

        Map<String, List<String>> consumers = new HashMap<>();
        consumers.put(consumer1, Arrays.asList(topic));
        consumers.put(consumer2, Arrays.asList(topic));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        assertAssignment(Arrays.asList(new TopicPartition(topic, 0)), assignment.get(consumer1));
        assertAssignment(Arrays.asList(new TopicPartition(topic, 1)), assignment.get(consumer2));
    }

    @Test
    public void testMultipleConsumersMixedTopics() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";
        String consumer3 = "consumer3";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, 3);
        partitionsPerTopic.put(topic2, 2);

        Map<String, List<String>> consumers = new HashMap<>();
        consumers.put(consumer1, Arrays.asList(topic1));
        consumers.put(consumer2, Arrays.asList(topic1, topic2));
        consumers.put(consumer3, Arrays.asList(topic1));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        assertAssignment(Arrays.asList(
                new TopicPartition(topic1, 0)), assignment.get(consumer1));
        assertAssignment(Arrays.asList(
                new TopicPartition(topic1, 1),
                new TopicPartition(topic2, 0),
                new TopicPartition(topic2, 1)), assignment.get(consumer2));
        assertAssignment(Arrays.asList(
                new TopicPartition(topic1, 2)), assignment.get(consumer3));
    }

    @Test
    public void testTwoConsumersTwoTopicsSixPartitions() {
        String topic1 = "topic1";
        String topic2 = "topic2";
        String consumer1 = "consumer1";
        String consumer2 = "consumer2";

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        partitionsPerTopic.put(topic1, 3);
        partitionsPerTopic.put(topic2, 3);

        Map<String, List<String>> consumers = new HashMap<>();
        consumers.put(consumer1, Arrays.asList(topic1, topic2));
        consumers.put(consumer2, Arrays.asList(topic1, topic2));

        Map<String, List<TopicPartition>> assignment = assignor.assign(partitionsPerTopic, consumers);
        assertAssignment(Arrays.asList(
                new TopicPartition(topic1, 0),
                new TopicPartition(topic1, 1),
                new TopicPartition(topic2, 0),
                new TopicPartition(topic2, 1)), assignment.get(consumer1));
        assertAssignment(Arrays.asList(
                new TopicPartition(topic1, 2),
                new TopicPartition(topic2, 2)), assignment.get(consumer2));
    }

    private void assertAssignment(List<TopicPartition> expected, List<TopicPartition> actual) {
        // order doesn't matter for assignment, so convert to a set
        assertEquals(new HashSet<>(expected), new HashSet<>(actual));
    }

}
