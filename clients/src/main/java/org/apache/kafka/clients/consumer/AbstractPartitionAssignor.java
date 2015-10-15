/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.GenericType;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Abstract assignor implementation used by default assignors shipped with Kafka. The embedded
 * format of assignments and subscriptions are as follows:
 *
 * <pre>
 * Subscription => Version Topics
 *   Version       => int16
 *   Topics        => [String]
 *
 * Assignment => Version TopicPartitions
 *   Version         => int16
 *   TopicPartitions => [Topic Partitions]
 *     Topic         => String
 *     Partitions    => [int32]
 * </pre>
 *
 * This assignor assumes that any changes to these formats will be compatible with this format.
 * In other words, newer versions can be parsed using the old format. This generally means that
 * fields can be added to the end of the structures, but no existing fields can be removed or reordered.
 */
public abstract class AbstractPartitionAssignor implements PartitionAssignor<AbstractPartitionAssignor.Subscription, AbstractPartitionAssignor.Assignment> {
    private static final Logger log = LoggerFactory.getLogger(AbstractPartitionAssignor.class);

    public static final String VERSION_KEY_NAME = "version";
    public static final String TOPICS_KEY_NAME = "topics";
    public static final String TOPIC_KEY_NAME = "topic";
    public static final String PARTITIONS_KEY_NAME = "partitions";
    public static final String TOPIC_PARTITIONS_KEY_NAME = "topic_partitions";

    public static final short CONSUMER_PROTOCOL_V0 = 0;
    public static final Schema CONSUMER_PROTOCOL_HEADER_SCHEMA = new Schema(
            new Field(VERSION_KEY_NAME, Type.INT16));
    private static final Struct CONSUMER_PROTOCOL_HEADER = new Struct(CONSUMER_PROTOCOL_HEADER_SCHEMA)
            .set(VERSION_KEY_NAME, CONSUMER_PROTOCOL_V0);

    public static final Schema SUBSCRIPTION_V0 = new Schema(
            new Field(TOPICS_KEY_NAME, new ArrayOf(Type.STRING)));
    public static final Schema TOPIC_ASSIGNMENT_V0 = new Schema(
            new Field(TOPIC_KEY_NAME, Type.STRING),
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(Type.INT32)));
    public static final Schema ASSIGNMENT_V0 = new Schema(
            new Field(TOPIC_PARTITIONS_KEY_NAME, new ArrayOf(TOPIC_ASSIGNMENT_V0)));

    public abstract Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                             Map<String, List<String>> subscriptions);

    @Override
    public Subscription subscription(Set<String> topics) {
        return new Subscription(new ArrayList<>(topics));
    }

    @Override
    public Map<String, Assignment> assign(Cluster metadata, Map<String, Subscription> subscriptions) {
        Set<String> allSubscribedTopics = new HashSet<>();
        for (Subscription subscription : subscriptions.values())
            allSubscribedTopics.addAll(subscription.topics);

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        for (String topic : allSubscribedTopics) {
            Integer numPartitions = metadata.partitionCountForTopic(topic);
            if (numPartitions != null)
                partitionsPerTopic.put(topic, numPartitions);
            else
                log.debug("Skipping assignment for topic {} since no metadata is available", topic);
        }

        Map<String, List<String>> unwrappedSubscription = Subscription.unwrap(subscriptions);
        return Assignment.wrap(assign(partitionsPerTopic, unwrappedSubscription));
    }

    protected static <K, V> void put(Map<K, List<V>> map, K key, V value) {
        List<V> list = map.get(key);
        if (list == null) {
            list = new ArrayList<>();
            map.put(key, list);
        }
        list.add(value);
    }

    private void checkVersionCompatibility(short version) {
        // check for invalid versions
        if (version < CONSUMER_PROTOCOL_V0)
            throw new SchemaException("Unsupported subscription version: " + version);

        // otherwise, assume versions can be parsed as V0
    }

    @Override
    public GenericType<Subscription> subscriptionSchema() {
        return new GenericType<Subscription>() {
            @Override
            public Subscription validate(Object obj) {
                if (obj instanceof Subscription)
                    return (Subscription) obj;
                throw new SchemaException(obj + " is not a consumer subscription");
            }

            @Override
            public void write(ByteBuffer buffer, Object o) {
                CONSUMER_PROTOCOL_HEADER.writeTo(buffer);
                Subscription subscription = (Subscription) o;
                subscription.struct.writeTo(buffer);
            }

            @Override
            public Object read(ByteBuffer buffer) {
                Struct header = (Struct) CONSUMER_PROTOCOL_HEADER_SCHEMA.read(buffer);
                Short version = header.getShort(VERSION_KEY_NAME);
                checkVersionCompatibility(version);
                Struct struct = (Struct) SUBSCRIPTION_V0.read(buffer);
                return new Subscription(version, struct);
            }

            @Override
            public int sizeOf(Object o) {
                Subscription subscription = (Subscription) o;
                return CONSUMER_PROTOCOL_HEADER.sizeOf() + subscription.struct.sizeOf();
            }
        };
    }

    @Override
    public GenericType<Assignment> assignmentSchema() {
        return new GenericType<Assignment>() {
            @Override
            public Assignment validate(Object obj) {
                if (obj instanceof Assignment)
                    return (Assignment) obj;
                throw new SchemaException(obj + "is not a consumer assignment");
            }

            @Override
            public void write(ByteBuffer buffer, Object o) {
                CONSUMER_PROTOCOL_HEADER.writeTo(buffer);
                Assignment assignment = (Assignment) o;
                assignment.struct.writeTo(buffer);
            }

            @Override
            public Object read(ByteBuffer buffer) {
                Struct header = (Struct) CONSUMER_PROTOCOL_HEADER_SCHEMA.read(buffer);
                Short version = header.getShort(VERSION_KEY_NAME);
                checkVersionCompatibility(version);
                Struct struct = (Struct) ASSIGNMENT_V0.read(buffer);
                return new Assignment(version, struct);
            }

            @Override
            public int sizeOf(Object o) {
                Assignment assignment = (Assignment) o;
                return CONSUMER_PROTOCOL_HEADER.sizeOf() + assignment.struct.sizeOf();
            }
        };
    }

    public static class Assignment implements PartitionAssignor.Assignment {
        private final Struct struct;
        private final short version;
        private final List<TopicPartition> partitions;

        public static Assignment empty() {
            return new Assignment(new ArrayList<TopicPartition>());
        }

        public static Map<String, Assignment> wrap(Map<String, List<TopicPartition>> assignmentMap) {
            Map<String, Assignment> assignment = new HashMap<>();
            for (Map.Entry<String, List<TopicPartition>> assignmentEntry : assignmentMap.entrySet())
                assignment.put(assignmentEntry.getKey(), new Assignment(assignmentEntry.getValue()));
            return assignment;
        }

        public static Map<String, List<TopicPartition>> unwrap(Map<String, Assignment> assignmentMap) {
            Map<String, List<TopicPartition>> assignment = new HashMap<>();
            for (Map.Entry<String, Assignment> assignmentEntry : assignmentMap.entrySet())
                assignment.put(assignmentEntry.getKey(), assignmentEntry.getValue().partitions);
            return assignment;
        }

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
            this.struct.set(TOPIC_PARTITIONS_KEY_NAME, topicAssignments.toArray());
            this.partitions = partitions;
            this.version = CONSUMER_PROTOCOL_V0;
        }

        public Assignment(short version, Struct struct) {
            this.version = version;
            this.struct = struct;
            this.partitions = new ArrayList<>();
            for (Object structObj : struct.getArray(TOPIC_PARTITIONS_KEY_NAME)) {
                Struct assignment = (Struct) structObj;
                String topic = assignment.getString(TOPIC_KEY_NAME);
                for (Object partitionObj : assignment.getArray(PARTITIONS_KEY_NAME)) {
                    Integer partition = (Integer) partitionObj;
                    this.partitions.add(new TopicPartition(topic, partition));
                }
            }

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

    public static class Subscription implements PartitionAssignor.Subscription {
        private final Struct struct;
        private final short version;
        private final List<String> topics;

        public static Map<String, Subscription> wrap(Map<String, List<String>> subscriptionMap) {
            Map<String, Subscription> subscription = new HashMap<>();
            for (Map.Entry<String, List<String>> subscriptionEntry : subscriptionMap.entrySet())
                subscription.put(subscriptionEntry.getKey(), new Subscription(subscriptionEntry.getValue()));
            return subscription;
        }

        public static Map<String, List<String>> unwrap(Map<String, Subscription> subscriptionMap) {
            Map<String, List<String>> subscription = new HashMap<>();
            for (Map.Entry<String, Subscription> subscriptionEntry : subscriptionMap.entrySet())
                subscription.put(subscriptionEntry.getKey(), subscriptionEntry.getValue().topics);
            return subscription;
        }

        public Subscription(List<String> topics) {
            this.struct = new Struct(SUBSCRIPTION_V0);
            this.struct.set(TOPICS_KEY_NAME, topics.toArray());
            this.version = CONSUMER_PROTOCOL_V0;
            this.topics = topics;
        }

        public Subscription(short version, Struct struct) {
            this.version = version;
            this.struct = struct;
            this.topics = new ArrayList<>();
            for (Object topicObj : struct.getArray(TOPICS_KEY_NAME))
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
