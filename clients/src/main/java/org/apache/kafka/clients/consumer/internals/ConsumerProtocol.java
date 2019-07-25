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

import java.util.Collections;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * ConsumerProtocol contains the schemas for consumer subscriptions and assignments for use with
 * Kafka's generalized group management protocol. Below is the version 1 format:
 *
 * <pre>
 * Subscription => Version Topics
 *   Version    => Int16
 *   Topics     => [String]
 *   UserData   => Bytes
 *   OwnedPartitions    => [Topic Partitions]
 *     Topic            => String
 *     Partitions       => [int32]
 *
 * Assignment => Version TopicPartitions
 *   Version            => int16
 *   AssignedPartitions => [Topic Partitions]
 *     Topic            => String
 *     Partitions       => [int32]
 *   UserData           => Bytes
 * </pre>
 *
 * Version 0 format:
 *
 * <pre>
 * Subscription => Version Topics
 *   Version    => Int16
 *   Topics     => [String]
 *   UserData   => Bytes
 *
 * Assignment => Version TopicPartitions
 *   Version            => int16
 *   AssignedPartitions => [Topic Partitions]
 *     Topic            => String
 *     Partitions       => [int32]
 *   UserData           => Bytes
 * </pre>
 *
 *
 * The current implementation assumes that future versions will not break compatibility. When
 * it encounters a newer version, it parses it using the current format. This basically means
 * that new versions cannot remove or reorder any of the existing fields.
 */
public class ConsumerProtocol {

    public static final String PROTOCOL_TYPE = "consumer";

    public static final String VERSION_KEY_NAME = "version";
    public static final String TOPICS_KEY_NAME = "topics";
    public static final String TOPIC_KEY_NAME = "topic";
    public static final String PARTITIONS_KEY_NAME = "partitions";
    public static final String OWNED_PARTITIONS_KEY_NAME = "owned_partitions";
    public static final String TOPIC_PARTITIONS_KEY_NAME = "topic_partitions";
    public static final String USER_DATA_KEY_NAME = "user_data";

    public static final short CONSUMER_PROTOCOL_V0 = 0;
    public static final short CONSUMER_PROTOCOL_V1 = 1;

    public static final short CONSUMER_PROTOCL_LATEST_VERSION = CONSUMER_PROTOCOL_V1;

    public static final Schema CONSUMER_PROTOCOL_HEADER_SCHEMA = new Schema(
            new Field(VERSION_KEY_NAME, Type.INT16));
    private static final Struct CONSUMER_PROTOCOL_HEADER_V0 = new Struct(CONSUMER_PROTOCOL_HEADER_SCHEMA)
            .set(VERSION_KEY_NAME, CONSUMER_PROTOCOL_V0);
    private static final Struct CONSUMER_PROTOCOL_HEADER_V1 = new Struct(CONSUMER_PROTOCOL_HEADER_SCHEMA)
            .set(VERSION_KEY_NAME, CONSUMER_PROTOCOL_V1);

    public static final Schema TOPIC_ASSIGNMENT_V0 = new Schema(
        new Field(TOPIC_KEY_NAME, Type.STRING),
        new Field(PARTITIONS_KEY_NAME, new ArrayOf(Type.INT32)));

    public static final Schema SUBSCRIPTION_V0 = new Schema(
            new Field(TOPICS_KEY_NAME, new ArrayOf(Type.STRING)),
            new Field(USER_DATA_KEY_NAME, Type.NULLABLE_BYTES));

    public static final Schema SUBSCRIPTION_V1 = new Schema(
        new Field(TOPICS_KEY_NAME, new ArrayOf(Type.STRING)),
        new Field(USER_DATA_KEY_NAME, Type.NULLABLE_BYTES),
        new Field(OWNED_PARTITIONS_KEY_NAME, new ArrayOf(TOPIC_ASSIGNMENT_V0)));

    public static final Schema ASSIGNMENT_V0 = new Schema(
            new Field(TOPIC_PARTITIONS_KEY_NAME, new ArrayOf(TOPIC_ASSIGNMENT_V0)),
            new Field(USER_DATA_KEY_NAME, Type.NULLABLE_BYTES));

    public static final Schema ASSIGNMENT_V1 = new Schema(
        new Field(TOPIC_PARTITIONS_KEY_NAME, new ArrayOf(TOPIC_ASSIGNMENT_V0)),
        new Field(USER_DATA_KEY_NAME, Type.NULLABLE_BYTES));

    public static ByteBuffer serializeSubscriptionV0(Subscription subscription) {
        Struct struct = new Struct(SUBSCRIPTION_V0);
        struct.set(USER_DATA_KEY_NAME, subscription.userData());
        struct.set(TOPICS_KEY_NAME, subscription.topics().toArray());

        ByteBuffer buffer = ByteBuffer.allocate(CONSUMER_PROTOCOL_HEADER_V0.sizeOf() + SUBSCRIPTION_V0.sizeOf(struct));
        CONSUMER_PROTOCOL_HEADER_V0.writeTo(buffer);
        SUBSCRIPTION_V0.write(buffer, struct);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer serializeSubscriptionV1(Subscription subscription) {
        Struct struct = new Struct(SUBSCRIPTION_V1);
        struct.set(USER_DATA_KEY_NAME, subscription.userData());
        struct.set(TOPICS_KEY_NAME, subscription.topics().toArray());
        List<Struct> topicAssignments = new ArrayList<>();
        Map<String, List<Integer>> partitionsByTopic = CollectionUtils.groupPartitionsByTopic(subscription.ownedPartitions());
        for (Map.Entry<String, List<Integer>> topicEntry : partitionsByTopic.entrySet()) {
            Struct topicAssignment = new Struct(TOPIC_ASSIGNMENT_V0);
            topicAssignment.set(TOPIC_KEY_NAME, topicEntry.getKey());
            topicAssignment.set(PARTITIONS_KEY_NAME, topicEntry.getValue().toArray());
            topicAssignments.add(topicAssignment);
        }
        struct.set(OWNED_PARTITIONS_KEY_NAME, topicAssignments.toArray());

        ByteBuffer buffer = ByteBuffer.allocate(CONSUMER_PROTOCOL_HEADER_V1.sizeOf() + SUBSCRIPTION_V1.sizeOf(struct));
        CONSUMER_PROTOCOL_HEADER_V1.writeTo(buffer);
        SUBSCRIPTION_V1.write(buffer, struct);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer serializeSubscription(Subscription subscription) {
        return serializeSubscription(subscription, CONSUMER_PROTOCL_LATEST_VERSION);
    }

    public static ByteBuffer serializeSubscription(Subscription subscription, short version) {
        switch (version) {
            case CONSUMER_PROTOCOL_V0:
                return serializeSubscriptionV0(subscription);

            case CONSUMER_PROTOCOL_V1:
                return serializeSubscriptionV1(subscription);

            default:
                // for any versions higher than known, try to serialize it as V1
                return serializeSubscriptionV1(subscription);
        }
    }

    public static Subscription deserializeSubscriptionV0(ByteBuffer buffer) {
        Struct struct = SUBSCRIPTION_V0.read(buffer);
        ByteBuffer userData = struct.getBytes(USER_DATA_KEY_NAME);
        List<String> topics = new ArrayList<>();
        for (Object topicObj : struct.getArray(TOPICS_KEY_NAME))
            topics.add((String) topicObj);

        return new Subscription(topics, userData, Collections.emptyList());
    }

    public static Subscription deserializeSubscriptionV1(ByteBuffer buffer) {
        Struct struct = SUBSCRIPTION_V1.read(buffer);
        ByteBuffer userData = struct.getBytes(USER_DATA_KEY_NAME);
        List<String> topics = new ArrayList<>();
        for (Object topicObj : struct.getArray(TOPICS_KEY_NAME))
            topics.add((String) topicObj);

        List<TopicPartition> ownedPartitions = new ArrayList<>();
        for (Object structObj : struct.getArray(OWNED_PARTITIONS_KEY_NAME)) {
            Struct assignment = (Struct) structObj;
            String topic = assignment.getString(TOPIC_KEY_NAME);
            for (Object partitionObj : assignment.getArray(PARTITIONS_KEY_NAME)) {
                ownedPartitions.add(new TopicPartition(topic, (Integer) partitionObj));
            }
        }

        return new Subscription(topics, userData, ownedPartitions);
    }

    public static Subscription deserializeSubscription(ByteBuffer buffer) {
        Struct header = CONSUMER_PROTOCOL_HEADER_SCHEMA.read(buffer);
        Short version = header.getShort(VERSION_KEY_NAME);

        if (version < CONSUMER_PROTOCOL_V0)
            throw new SchemaException("Unsupported subscription version: " + version);

        switch (version) {
            case CONSUMER_PROTOCOL_V0:
                return deserializeSubscriptionV0(buffer);

            case CONSUMER_PROTOCOL_V1:
                return deserializeSubscriptionV1(buffer);

            // assume all higher versions can be parsed as V1
            default:
                return deserializeSubscriptionV1(buffer);
        }
    }

    public static ByteBuffer serializeAssignmentV0(Assignment assignment) {
        Struct struct = new Struct(ASSIGNMENT_V0);
        struct.set(USER_DATA_KEY_NAME, assignment.userData());
        List<Struct> topicAssignments = new ArrayList<>();
        Map<String, List<Integer>> partitionsByTopic = CollectionUtils.groupPartitionsByTopic(assignment.partitions());
        for (Map.Entry<String, List<Integer>> topicEntry : partitionsByTopic.entrySet()) {
            Struct topicAssignment = new Struct(TOPIC_ASSIGNMENT_V0);
            topicAssignment.set(TOPIC_KEY_NAME, topicEntry.getKey());
            topicAssignment.set(PARTITIONS_KEY_NAME, topicEntry.getValue().toArray());
            topicAssignments.add(topicAssignment);
        }
        struct.set(TOPIC_PARTITIONS_KEY_NAME, topicAssignments.toArray());

        ByteBuffer buffer = ByteBuffer.allocate(CONSUMER_PROTOCOL_HEADER_V0.sizeOf() + ASSIGNMENT_V0.sizeOf(struct));
        CONSUMER_PROTOCOL_HEADER_V0.writeTo(buffer);
        ASSIGNMENT_V0.write(buffer, struct);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer serializeAssignmentV1(Assignment assignment) {
        Struct struct = new Struct(ASSIGNMENT_V1);
        struct.set(USER_DATA_KEY_NAME, assignment.userData());
        List<Struct> topicAssignments = new ArrayList<>();
        Map<String, List<Integer>> partitionsByTopic = CollectionUtils.groupPartitionsByTopic(assignment.partitions());
        for (Map.Entry<String, List<Integer>> topicEntry : partitionsByTopic.entrySet()) {
            Struct topicAssignment = new Struct(TOPIC_ASSIGNMENT_V0);
            topicAssignment.set(TOPIC_KEY_NAME, topicEntry.getKey());
            topicAssignment.set(PARTITIONS_KEY_NAME, topicEntry.getValue().toArray());
            topicAssignments.add(topicAssignment);
        }
        struct.set(TOPIC_PARTITIONS_KEY_NAME, topicAssignments.toArray());

        ByteBuffer buffer = ByteBuffer.allocate(CONSUMER_PROTOCOL_HEADER_V1.sizeOf() + ASSIGNMENT_V1.sizeOf(struct));
        CONSUMER_PROTOCOL_HEADER_V1.writeTo(buffer);
        ASSIGNMENT_V1.write(buffer, struct);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer serializeAssignment(Assignment assignment) {
        return serializeAssignment(assignment, CONSUMER_PROTOCL_LATEST_VERSION);
    }

    public static ByteBuffer serializeAssignment(Assignment assignment, short version) {
        switch (version) {
            case CONSUMER_PROTOCOL_V0:
                return serializeAssignmentV0(assignment);

            case CONSUMER_PROTOCOL_V1:
                return serializeAssignmentV1(assignment);

            default:
                // for any versions higher than known, try to serialize it as V1
                return serializeAssignmentV1(assignment);
        }
    }

    public static Assignment deserializeAssignmentV0(ByteBuffer buffer) {
        Struct struct = ASSIGNMENT_V0.read(buffer);
        ByteBuffer userData = struct.getBytes(USER_DATA_KEY_NAME);
        List<TopicPartition> partitions = new ArrayList<>();
        for (Object structObj : struct.getArray(TOPIC_PARTITIONS_KEY_NAME)) {
            Struct assignment = (Struct) structObj;
            String topic = assignment.getString(TOPIC_KEY_NAME);
            for (Object partitionObj : assignment.getArray(PARTITIONS_KEY_NAME)) {
                partitions.add(new TopicPartition(topic, (Integer) partitionObj));
            }
        }
        return new Assignment(partitions, userData);
    }

    public static Assignment deserializeAssignmentV1(ByteBuffer buffer) {
        return deserializeAssignmentV0(buffer);
    }

    public static Assignment deserializeAssignment(ByteBuffer buffer) {
        Struct header = CONSUMER_PROTOCOL_HEADER_SCHEMA.read(buffer);
        Short version = header.getShort(VERSION_KEY_NAME);

        if (version < CONSUMER_PROTOCOL_V0)
            throw new SchemaException("Unsupported assignment version: " + version);

        switch (version) {
            case CONSUMER_PROTOCOL_V0:
                return deserializeAssignmentV0(buffer);

            case CONSUMER_PROTOCOL_V1:
                return deserializeAssignmentV1(buffer);

            default:
                // assume all higher versions can be parsed as V1
                return deserializeAssignmentV1(buffer);
        }
    }
}
