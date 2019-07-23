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
import java.util.Optional;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;

/**
 * This interface is used to define custom partition assignment for use in
 * {@link org.apache.kafka.clients.consumer.KafkaConsumer}. Members of the consumer group subscribe
 * to the topics they are interested in and forward their subscriptions to a Kafka broker serving
 * as the group coordinator. The coordinator selects one member to perform the group assignment and
 * propagates the subscriptions of all members to it. Then {@link #assign(Cluster, GroupSubscription)} is called
 * to perform the assignment and the results are forwarded back to each respective members
 *
 * In some cases, it is useful to forward additional metadata to the assignor in order to make
 * assignment decisions. For this, you can override {@link #subscriptionUserdata(Set)} and provide custom
 * userData in the returned Subscription. For example, to have a rack-aware assignor, an implementation
 * can use this user data to forward the rackId belonging to each member.
 */
public interface PartitionAssignor {

    /**
     * Return serialized data that will be included in the serializable subscription object sent in the
     * joinGroup and can be leveraged in {@link #assign(Cluster, GroupSubscription)} ((e.g. local host/rack information)
     *
     * @return Non-null optional join subscription user data
     */
    default ByteBuffer subscriptionUserdata(Set<String> topics) {
        return ByteBuffer.wrap(new byte[0]);
    }

    /**
     * Perform the group assignment given the member subscriptions and current cluster metadata.
     * @param metadata Current topic/broker metadata known by consumer
     * @param subscriptions Subscriptions from all members including metadata provided through {@link #subscriptionUserdata(Set)}
     * @return A map from the members to their respective assignment. This should have one entry
     *         for all members who in the input subscription map.
     */
    GroupAssignment assign(Cluster metadata, GroupSubscription subscriptions);

    /**
     * Callback which is invoked when a group member receives its assignment from the leader.
     * @param assignment The local member's assignment as provided by the leader in {@link #assign(Cluster, GroupSubscription)}
     */
    void onAssignment(Assignment assignment);

    /**
     * Callback which is invoked when a group member receives its assignment from the leader.
     * @param assignment The local member's assignment as provided by the leader in {@link #assign(Cluster, GroupSubscription)}
     * @param generation The consumer group generation associated with this partition assignment (optional)
     */
    default void onAssignment(Assignment assignment, int generation) {
        onAssignment(assignment);
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
     * Unique name for this assignor (e.g. "range" or "roundrobin" or "sticky")
     * @return non-null unique name
     */
    String name();

    final class GroupAssignment {
        private final Map<String, Assignment> assignments;

        public GroupAssignment(Map<String, Assignment> assignments) {
            this.assignments = assignments;
        }

        public Map<String, Assignment> groupAssignments() {
            return assignments;
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
    }

    final class Subscription {
        private final ConsumerSubscriptionData consumerData;
        private final ByteBuffer userData;

        public Subscription(ConsumerSubscriptionData consumerData, ByteBuffer userData) {
            this.consumerData = consumerData;
            this.userData = userData;
        }

        public Subscription(ConsumerSubscriptionData consumerData) {
            this(consumerData, ByteBuffer.wrap(new byte[0]));
        }

        public ConsumerSubscriptionData consumerData() {
            return consumerData;
        }

        public ByteBuffer userData() {
            return userData;
        }
    }

    final class Assignment {
        private final ConsumerAssignmentData consumerData;
        private final ByteBuffer userData;

        public Assignment(ConsumerAssignmentData consumerData, ByteBuffer userData) {
            this.consumerData = consumerData;
            this.userData = userData;
        }

        public Assignment(ConsumerAssignmentData consumerData) {
            this(consumerData, ByteBuffer.wrap(new byte[0]));
        }

        public ConsumerAssignmentData consumerData() {
            return consumerData;
        }

        public ByteBuffer userData() {
            return userData;
        }
    }

    final class ConsumerSubscriptionData {
        private final List<String> topics;
        private final List<TopicPartition> ownedPartitions;
        private Optional<String> groupInstanceId;

        public ConsumerSubscriptionData(List<String> topics, List<TopicPartition> ownedPartitions) {
            this.topics = topics;
            this.ownedPartitions = ownedPartitions;
            this.groupInstanceId = Optional.empty();
        }

        public ConsumerSubscriptionData(List<String> topics) {
            this(topics, Collections.emptyList());
        }

        public List<String> topics() {
            return topics;
        }

        public List<TopicPartition> ownedPartitions() {
            return ownedPartitions;
        }

        public void setGroupInstanceId(Optional<String> groupInstanceId) {
            this.groupInstanceId = groupInstanceId;
        }

        public Optional<String> groupInstanceId() {
            return groupInstanceId;
        }

        @Override
        public String toString() {
            return "Subscription(" +
                ", topics=" + topics +
                ", ownedPartitions=" + ownedPartitions +
                ", group.instance.id=" + groupInstanceId + ")";
        }
    }

    final class ConsumerAssignmentData {
        private final List<TopicPartition> partitions;

        public ConsumerAssignmentData(List<TopicPartition> partitions) {
            this.partitions = partitions;
        }

        public List<TopicPartition> partitions() {
            return partitions;
        }

        @Override
        public String toString() {
            return "Assignment(" +
                ", partitions=" + partitions +
                ')';
        }
    }

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

}
