/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class implements the consumer's group protocol including the format of consumer subscription
 * and assignment data.
 */
public class ConsumerProtocol implements GroupProtocol<ConsumerProtocol.Subscription, ConsumerProtocol.Assignment> {

    public static final String VERSION_KEY_NAME = "version";
    public static final String SUBSCRIPTION_KEY_NAME = "subscription";
    public static final String TOPIC_KEY_NAME = "topic";
    public static final String PARTITIONS_KEY_NAME = "partitions";
    public static final String ASSIGNMENT_KEY_NAME = "assignment";

    public static final short CONSUMER_PROTOCOL_V0 = 0;
    public static final Schema SUBSCRIPTION_V0 = new Schema(
            new Field(VERSION_KEY_NAME, Type.INT16),
            new Field(SUBSCRIPTION_KEY_NAME, new ArrayOf(Type.STRING)));
    public static final Schema TOPIC_ASSIGNMENT_V0 = new Schema(
            new Field(TOPIC_KEY_NAME, Type.STRING),
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(Type.INT32)));
    public static final Schema ASSIGNMENT_V0 = new Schema(
            new Field(VERSION_KEY_NAME, Type.INT16),
            new Field(ASSIGNMENT_KEY_NAME, new ArrayOf(TOPIC_ASSIGNMENT_V0)));

    @Override
    public String name() {
        return "consumer";
    }

    @Override
    public GenType<Subscription> metadataSchema() {
        return new GenType<Subscription>() {
            @Override
            public Subscription validate(Object obj) {
                if (obj instanceof Subscription)
                    return (Subscription) obj;
                throw new SchemaException(obj + " is not a consumer subscription");
            }

            @Override
            public void write(ByteBuffer buffer, Object o) {
                Subscription subscription = (Subscription) o;
                subscription.struct.writeTo(buffer);
            }

            @Override
            public Object read(ByteBuffer buffer) {
                Struct struct = (Struct) SUBSCRIPTION_V0.read(buffer);
                return new Subscription(struct);
            }

            @Override
            public int sizeOf(Object o) {
                Subscription subscription = (Subscription) o;
                return subscription.struct.sizeOf();
            }
        };
    }

    @Override
    public GenType<Assignment> stateSchema() {
        return new GenType<Assignment>() {
            @Override
            public Assignment validate(Object obj) {
                if (obj instanceof Assignment)
                    return (Assignment) obj;
                throw new SchemaException(obj + "is not a consumer assignment");
            }

            @Override
            public void write(ByteBuffer buffer, Object o) {
                Assignment assignment = (Assignment) o;
                assignment.struct.writeTo(buffer);
            }

            @Override
            public Object read(ByteBuffer buffer) {
                Struct struct = (Struct) ASSIGNMENT_V0.read(buffer);
                return new Assignment(struct);
            }

            @Override
            public int sizeOf(Object o) {
                Assignment assignment = (Assignment) o;
                return assignment.struct.sizeOf();
            }
        };
    }

    public static class Assignment {
        private final Struct struct;
        private final short version;
        private final List<TopicPartition> partitions;

        private Map<String, List<Integer>> asMap(Collection<TopicPartition> partitions) {
            Map<String, List<Integer>> partitionMap = new HashMap<>();
            for (TopicPartition partition : partitions) {
                String topic = partition.topic();
                List<Integer> topicPartitions = partitionMap.get(topic);
                if (topicPartitions == null) {
                    topicPartitions = new ArrayList<>();
                    partitionMap.put(topic, topicPartitions);
                }
                topicPartitions.add(partition.partition());
            }
            return partitionMap;
        }

        public Assignment(List<TopicPartition> partitions) {
            this.struct = new Struct(ASSIGNMENT_V0);

            List<Struct> topicAssignments = new ArrayList<>();
            for (Map.Entry<String, List<Integer>> topicEntry : asMap(partitions).entrySet()) {
                Struct topicAssignment = new Struct(TOPIC_ASSIGNMENT_V0);
                topicAssignment.set(TOPIC_KEY_NAME, topicEntry.getKey());
                topicAssignment.set(PARTITIONS_KEY_NAME, topicEntry.getValue().toArray());
                topicAssignments.add(topicAssignment);
            }
            this.struct.set(ASSIGNMENT_KEY_NAME, topicAssignments.toArray());
            this.partitions = partitions;

            this.version = CONSUMER_PROTOCOL_V0;
            this.struct.set(VERSION_KEY_NAME, CONSUMER_PROTOCOL_V0);
        }

        public Assignment(Struct struct) {
            this.struct = struct;
            this.partitions = new ArrayList<>();
            for (Object structObj : struct.getArray(ASSIGNMENT_KEY_NAME)) {
                Struct assignment = (Struct) structObj;
                String topic = assignment.getString(TOPIC_KEY_NAME);
                for (Object partitionObj : assignment.getArray(PARTITIONS_KEY_NAME)) {
                    Integer partition = (Integer) partitionObj;
                    this.partitions.add(new TopicPartition(topic, partition));
                }
            }
            this.version = struct.getShort(VERSION_KEY_NAME);
        }

        public short version() {
            return version;
        }

        public Struct toStruct() {
            return struct;
        }

        public List<TopicPartition> partitions() {
            return partitions;
        }

        @Override
        public String toString() {
            return "Assignment{" +
                    "partitions=" + partitions +
                    '}';
        }
    }

    public static class Subscription {
        private final Struct struct;
        private final short version;
        private final List<String> topics;

        public Subscription(List<String> topics) {
            this.struct = new Struct(SUBSCRIPTION_V0);
            this.struct.set(SUBSCRIPTION_KEY_NAME, topics.toArray());
            this.struct.set(VERSION_KEY_NAME, CONSUMER_PROTOCOL_V0);
            this.version = CONSUMER_PROTOCOL_V0;
            this.topics = topics;
        }

        public Subscription(Struct struct) {
            this.struct = struct;
            this.version = struct.getShort(VERSION_KEY_NAME);
            this.topics = new ArrayList<>();
            for (Object topicObj : struct.getArray(SUBSCRIPTION_KEY_NAME))
                topics.add((String) topicObj);
        }

        public Struct toStruct() {
            return struct;
        }

        public short version() {
            return version;
        }

        public List<String> topics() {
            return topics;
        }

        @Override
        public String toString() {
            return "Subscription{" +
                    ", topics=" + topics +
                    '}';
        }
    }

}
