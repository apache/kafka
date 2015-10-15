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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.consumer.PartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.PartitionAssignor.Subscription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.GenericType;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This interface is used to define custom partition assignment for use in {@link KafkaConsumer}.
 * Members of the consumer group subscribe to the topics they are interested in and forward their
 * subscriptions to a Kafka broker serving as the group coordinator. The coordinator
 * selects one member to perform the group assignment and propagates the subscriptions of all
 * members to it. Then {@link #assign(Cluster, Map)} is called to perform the assignment and the
 * results are forwarded back to each respective members
 *
 * Since subscriptions and assignments must be propagated to members through the consumer
 * coordinator, implementations must provide respective schemas for serialization (see
 * {@link #subscriptionSchema()} and {@link #assignmentSchema()}). It is
 * up to the implementor to ensure that subscription/assignment formats are compatible within
 * the group. In particular, special care must be taken when upgrading the format if rolling
 * upgrades are expected. In general, this requires formats to be forwards and backwards
 * compatible, at least within one version increment.
 *
 * Note that implementations should only implement this interface directly if they have a need
 * to propagate custom metadata in order to perform the assignment. For example, to have a
 * rack-aware assignor, an implementation will generally have to forward the rackId of each
 * consumer in the group, which will require a direct implementation of this interface. However,
 * for assignors which do not need custom metadata propagated, it would probably be preferred to
 * extend {@link AbstractPartitionAssignor}.
 */
public interface PartitionAssignor<S extends Subscription, A extends Assignment> {

    /**
     * Return a serializable object representing the local member's subscription. This can include
     * additional information as well (e.g. local host/rack information) which can be leveraged in
     * {@link #assign(Cluster, Map)}.
     * @param topics Topics subscribed to through {@link KafkaConsumer#subscribe(List)} and variants
     * @return Subscription object conforming to the schema in {@link #subscriptionSchema()}
     */
    S subscription(Set<String> topics);

    /**
     * Perform the group assignment given the member subscriptions and current cluster metadata.
     * @param metadata Current topic/broker metadata known by consumer
     * @param subscriptions Subscriptions from all members provided through {@link #subscription(Set)}
     * @return A map from the members to their respective assignment. This should have one entry
     *         for all members who in the input subscription map.
     */
    Map<String, A> assign(Cluster metadata, Map<String, S> subscriptions);

    /**
     * Unique name for this assignor (e.g. "consumer-range" or "consumer-roundrobin")
     * @return non-null unique name
     */
    String name();

    /**
     * Get the schema used to serialize/deserialize subscriptions.
     * @return Non-null schema object
     */
    GenericType<S> subscriptionSchema();

    /**
     * Get the schema used to serialize/deserialize assignments.
     * @return Non-null schema object
     */
    GenericType<A> assignmentSchema();

    /**
     * Minimum interface required for subscription implementations.
     */
    interface Subscription {
        /**
         * Get the topics subscribed to by the consumer
         * @return Non-null list of topics
         */
        List<String> topics();
    }

    /**
     * Minimum interface required for assignment implementations.
     */
    interface Assignment {
        /**
         * Get the list of partitions from this assignment.
         * @return Non-null list of partitions
         */
        List<TopicPartition> partitions();
    }

}
