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
package org.apache.kafka.clients.consumer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Optional;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;

import static org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor.DEFAULT_GENERATION;

/**
 * This interface is used to define custom partition assignment for use in
 * {@link org.apache.kafka.clients.consumer.KafkaConsumer}. Members of the consumer group subscribe
 * to the topics they are interested in and forward their subscriptions to a Kafka broker serving
 * as the group coordinator. The coordinator selects one member to perform the group assignment and
 * propagates the subscriptions of all members to it. Then {@link #assign(Cluster, GroupSubscription)} is called
 * to perform the assignment and the results are forwarded back to each respective members
 * <p>
 * In some cases, it is useful to forward additional metadata to the assignor in order to make
 * assignment decisions. For this, you can override {@link #subscriptionUserData(Set)} and provide custom
 * userData in the returned Subscription. For example, to have a rack-aware assignor, an implementation
 * can use this user data to forward the rackId belonging to each member.
 * <p>
 * The implementation can extend {@link Configurable} to get configs from consumer.
 */
public interface ConsumerPartitionAssignor {

    /**
     * Return serialized data that will be included in the {@link Subscription} sent to the leader
     * and can be leveraged in {@link #assign(Cluster, GroupSubscription)} ((e.g. local host/rack information)
     *
     * @param topics Topics subscribed to through {@link org.apache.kafka.clients.consumer.KafkaConsumer#subscribe(java.util.Collection)}
     *               and variants
     * @return nullable subscription user data
     */
    default ByteBuffer subscriptionUserData(Set<String> topics) {
        return null;
    }

    /**
     * Perform the group assignment given the member subscriptions and current cluster metadata.
     * @param metadata Current topic/broker metadata known by consumer
     * @param groupSubscription Subscriptions from all members including metadata provided through {@link #subscriptionUserData(Set)}
     * @return A map from the members to their respective assignments. This should have one entry
     *         for each member in the input subscription map.
     */
    GroupAssignment assign(Cluster metadata, GroupSubscription groupSubscription);

    /**
     * Callback which is invoked when a group member receives its assignment from the leader.
     * @param assignment The local member's assignment as provided by the leader in {@link #assign(Cluster, GroupSubscription)}
     * @param metadata Additional metadata on the consumer (optional)
     */
    default void onAssignment(Assignment assignment, ConsumerGroupMetadata metadata) {
    }

    /**
     * Indicate which rebalance protocol this assignor works with;
     * By default it should always work with {@link RebalanceProtocol#EAGER}.
     */
    default List<RebalanceProtocol> supportedProtocols() {
        return Collections.singletonList(RebalanceProtocol.EAGER);
    }

    /**
     * Return the version of the assignor which indicates how the user metadata encodings
     * and the assignment algorithm gets evolved.
     */
    default short version() {
        return (short) 0;
    }

    /**
     * Unique name for this assignor (e.g. "range" or "roundrobin" or "sticky"). Note, this is not required
     * to be the same as the class name specified in {@link ConsumerConfig#PARTITION_ASSIGNMENT_STRATEGY_CONFIG}
     * @return non-null unique name
     */
    String name();

    final class Subscription {
        private final List<String> topics;
        private final ByteBuffer userData;
        private final List<TopicPartition> ownedPartitions;
        private final Optional<String> rackId;
        private Optional<String> groupInstanceId;
        private final Optional<Integer> generationId;

        public Subscription(List<String> topics, ByteBuffer userData, List<TopicPartition> ownedPartitions, int generationId, Optional<String> rackId) {
            this.topics = topics;
            this.userData = userData;
            this.ownedPartitions = ownedPartitions;
            this.groupInstanceId = Optional.empty();
            this.generationId = generationId < 0 ? Optional.empty() : Optional.of(generationId);
            this.rackId = rackId;
        }

        public Subscription(List<String> topics, ByteBuffer userData, List<TopicPartition> ownedPartitions) {
            this(topics, userData, ownedPartitions, DEFAULT_GENERATION, Optional.empty());
        }

        public Subscription(List<String> topics, ByteBuffer userData) {
            this(topics, userData, Collections.emptyList(), DEFAULT_GENERATION, Optional.empty());
        }

        public Subscription(List<String> topics) {
            this(topics, null, Collections.emptyList(), DEFAULT_GENERATION, Optional.empty());
        }

        public List<String> topics() {
            return topics;
        }

        public ByteBuffer userData() {
            return userData;
        }

        public List<TopicPartition> ownedPartitions() {
            return ownedPartitions;
        }

        public Optional<String> rackId() {
            return rackId;
        }

        public void setGroupInstanceId(Optional<String> groupInstanceId) {
            this.groupInstanceId = groupInstanceId;
        }

        public Optional<String> groupInstanceId() {
            return groupInstanceId;
        }

        public Optional<Integer> generationId() {
            return generationId;
        }

        @Override
        public String toString() {
            return "Subscription(" +
                "topics=" + topics +
                (userData == null ? "" : ", userDataSize=" + userData.remaining()) +
                ", ownedPartitions=" + ownedPartitions +
                ", groupInstanceId=" + groupInstanceId.map(String::toString).orElse("null") +
                ", generationId=" + generationId.orElse(-1) +
                ", rackId=" + (rackId.orElse("null")) +
                ")";
        }
    }

    final class Assignment {
        private final List<TopicPartition> partitions;
        private final ByteBuffer userData;

        public Assignment(List<TopicPartition> partitions, ByteBuffer userData) {
            this.partitions = partitions;
            this.userData = userData;
        }

        public Assignment(List<TopicPartition> partitions) {
            this(partitions, null);
        }

        public List<TopicPartition> partitions() {
            return partitions;
        }

        public ByteBuffer userData() {
            return userData;
        }

        @Override
        public String toString() {
            return "Assignment(" +
                "partitions=" + partitions +
                (userData == null ? "" : ", userDataSize=" + userData.remaining()) +
                ')';
        }
    }

    final class GroupSubscription {
        private final Map<String, Subscription> subscriptions;

        public GroupSubscription(Map<String, Subscription> subscriptions) {
            this.subscriptions = subscriptions;
        }

        public Map<String, Subscription> groupSubscription() {
            return subscriptions;
        }

        @Override
        public String toString() {
            return "GroupSubscription(" +
                "subscriptions=" + subscriptions +
                ")";
        }
    }

    final class GroupAssignment {
        private final Map<String, Assignment> assignments;

        public GroupAssignment(Map<String, Assignment> assignments) {
            this.assignments = assignments;
        }

        public Map<String, Assignment> groupAssignment() {
            return assignments;
        }

        @Override
        public String toString() {
            return "GroupAssignment(" +
                "assignments=" + assignments +
                ")";
        }
    }

    /**
     * The rebalance protocol defines partition assignment and revocation semantics. The purpose is to establish a
     * consistent set of rules that all consumers in a group follow in order to transfer ownership of a partition.
     * {@link ConsumerPartitionAssignor} implementors can claim supporting one or more rebalance protocols via the
     * {@link ConsumerPartitionAssignor#supportedProtocols()}, and it is their responsibility to respect the rules
     * of those protocols in their {@link ConsumerPartitionAssignor#assign(Cluster, GroupSubscription)} implementations.
     * Failures to follow the rules of the supported protocols would lead to runtime error or undefined behavior.
     *
     * The {@link RebalanceProtocol#EAGER} rebalance protocol requires a consumer to always revoke all its owned
     * partitions before participating in a rebalance event. It therefore allows a complete reshuffling of the assignment.
     *
     * {@link RebalanceProtocol#COOPERATIVE} rebalance protocol allows a consumer to retain its currently owned
     * partitions before participating in a rebalance event. The assignor should not reassign any owned partitions
     * immediately, but instead may indicate consumers the need for partition revocation so that the revoked
     * partitions can be reassigned to other consumers in the next rebalance event. This is designed for sticky assignment
     * logic which attempts to minimize partition reassignment with cooperative adjustments.
     */
    enum RebalanceProtocol {
        EAGER((byte) 0), COOPERATIVE((byte) 1);

        private final byte id;

        RebalanceProtocol(byte id) {
            this.id = id;
        }

        public byte id() {
            return id;
        }

        public static RebalanceProtocol forId(byte id) {
            switch (id) {
                case 0:
                    return EAGER;
                case 1:
                    return COOPERATIVE;
                default:
                    throw new IllegalArgumentException("Unknown rebalance protocol id: " + id);
            }
        }
    }

    /**
     * Get a list of configured instances of {@link org.apache.kafka.clients.consumer.ConsumerPartitionAssignor}
     * based on the class names/types specified by {@link org.apache.kafka.clients.consumer.ConsumerConfig#PARTITION_ASSIGNMENT_STRATEGY_CONFIG}
     */
    static List<ConsumerPartitionAssignor> getAssignorInstances(List<String> assignorClasses, Map<String, Object> configs) {
        List<ConsumerPartitionAssignor> assignors = new ArrayList<>();
        // a map to store assignor name -> assignor class name
        Map<String, String> assignorNameMap = new HashMap<>();

        if (assignorClasses == null)
            return assignors;

        for (Object klass : assignorClasses) {
            // first try to get the class if passed in as a string
            if (klass instanceof String) {
                try {
                    klass = Class.forName((String) klass, true, Utils.getContextOrKafkaClassLoader());
                } catch (ClassNotFoundException classNotFound) {
                    throw new KafkaException(klass + " ClassNotFoundException exception occurred", classNotFound);
                }
            }

            if (klass instanceof Class<?>) {
                Object assignor = Utils.newInstance((Class<?>) klass);
                if (assignor instanceof Configurable)
                    ((Configurable) assignor).configure(configs);

                if (assignor instanceof ConsumerPartitionAssignor) {
                    String assignorName = ((ConsumerPartitionAssignor) assignor).name();
                    if (assignorNameMap.containsKey(assignorName)) {
                        throw new KafkaException("The assignor name: '" + assignorName + "' is used in more than one assignor: " +
                            assignorNameMap.get(assignorName) + ", " + assignor.getClass().getName());
                    }
                    assignorNameMap.put(assignorName, assignor.getClass().getName());
                    assignors.add((ConsumerPartitionAssignor) assignor);
                } else {
                    throw new KafkaException(klass + " is not an instance of " + ConsumerPartitionAssignor.class.getName());
                }
            } else {
                throw new KafkaException("List contains element of type " + klass.getClass().getName() + ", expected String or Class");
            }
        }
        return assignors;
    }

}
