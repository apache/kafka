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
 * Kafka's generalized group management protocol. Below is the version 0 format:
 *
 * <pre>
 * Subscription => Version Topics
 *   Version    => Int16
 *   Topics     => [String]
 *   UserData   => Bytes
 *
 * Assignment => Version TopicPartitions
 *   Version         => int16
 *   TopicPartitions => [Topic Partitions]
 *     Topic         => String
 *     Partitions    => [int32]
 * </pre>
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
    public static final String TOPIC_PARTITIONS_KEY_NAME = "topic_partitions";
    public static final String USER_DATA_KEY_NAME = "user_data";

    public static final short CONSUMER_PROTOCOL_V0 = 0;
    public static final Schema CONSUMER_PROTOCOL_HEADER_SCHEMA = new Schema(
            new Field(VERSION_KEY_NAME, Type.INT16));
    private static final Struct CONSUMER_PROTOCOL_HEADER_V0 = new Struct(CONSUMER_PROTOCOL_HEADER_SCHEMA)
            .set(VERSION_KEY_NAME, CONSUMER_PROTOCOL_V0);

    public static final Schema SUBSCRIPTION_V0 = new Schema(
            new Field(TOPICS_KEY_NAME, new ArrayOf(Type.STRING)),
            new Field(USER_DATA_KEY_NAME, Type.NULLABLE_BYTES));
    public static final Schema TOPIC_ASSIGNMENT_V0 = new Schema(
            new Field(TOPIC_KEY_NAME, Type.STRING),
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(Type.INT32)));
    public static final Schema ASSIGNMENT_V0 = new Schema(
            new Field(TOPIC_PARTITIONS_KEY_NAME, new ArrayOf(TOPIC_ASSIGNMENT_V0)),
            new Field(USER_DATA_KEY_NAME, Type.NULLABLE_BYTES));

    public static ByteBuffer serializeSubscription(PartitionAssignor.Subscription subscription) {
        Struct struct = new Struct(SUBSCRIPTION_V0);
        struct.set(USER_DATA_KEY_NAME, subscription.userData());
        struct.set(TOPICS_KEY_NAME, subscription.topics().toArray());
        ByteBuffer buffer = ByteBuffer.allocate(CONSUMER_PROTOCOL_HEADER_V0.sizeOf() + SUBSCRIPTION_V0.sizeOf(struct));
        CONSUMER_PROTOCOL_HEADER_V0.writeTo(buffer);
        SUBSCRIPTION_V0.write(buffer, struct);
        buffer.flip();
        return buffer;
    }

    public static PartitionAssignor.Subscription deserializeSubscription(ByteBuffer buffer) {
        Struct header = CONSUMER_PROTOCOL_HEADER_SCHEMA.read(buffer);
        Short version = header.getShort(VERSION_KEY_NAME);
        checkVersionCompatibility(version);
        Struct struct = SUBSCRIPTION_V0.read(buffer);
        ByteBuffer userData = struct.getBytes(USER_DATA_KEY_NAME);
        List<String> topics = new ArrayList<>();
        for (Object topicObj : struct.getArray(TOPICS_KEY_NAME))
            topics.add((String) topicObj);
        return new PartitionAssignor.Subscription(topics, userData);
    }

    public static PartitionAssignor.Assignment deserializeAssignment(ByteBuffer buffer) {
        Struct header = CONSUMER_PROTOCOL_HEADER_SCHEMA.read(buffer);
        Short version = header.getShort(VERSION_KEY_NAME);
        checkVersionCompatibility(version);
        Struct struct = ASSIGNMENT_V0.read(buffer);
        ByteBuffer userData = struct.getBytes(USER_DATA_KEY_NAME);
        return new PartitionAssignor.Assignment(CollectionUtils.getTopicPartitionFromStruct(struct, TOPIC_PARTITIONS_KEY_NAME), userData);
    }

    public static ByteBuffer serializeAssignment(PartitionAssignor.Assignment assignment) {
        Struct struct = new Struct(ASSIGNMENT_V0);
        struct.set(USER_DATA_KEY_NAME, assignment.userData());
        List<Struct> topicAssignments = new ArrayList<>();
        Map<String, List<Integer>> partitionsByTopic = CollectionUtils.groupDataByTopic(assignment.partitions());
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

    private static void checkVersionCompatibility(short version) {
        // check for invalid versions
        if (version < CONSUMER_PROTOCOL_V0)
            throw new SchemaException("Unsupported subscription version: " + version);

        // otherwise, assume versions can be parsed as V0
    }

}
