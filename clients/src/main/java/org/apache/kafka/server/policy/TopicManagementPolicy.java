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

package org.apache.kafka.server.policy;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Represents the state of a topic either before, or as a result of,
 * an administrative request affecting a topic.
 */
interface TopicState {
    /**
     * The number of partitions of the topic.
     */
    int numPartitions();

    /**
     * The replication factor of the topic. More precisely, the number of assigned replicas for partition 0.
     * // TODO what about during reassignment
     */
    Short replicationFactor();

    /**
     * A map of the replica assignments of the topic, with partition ids as keys and
     * the assigned brokers as the corresponding values.
     * // TODO what about during reassignment
     */
    Map<Integer, List<Integer>> replicasAssignments();

    /**
     * The topic config.
     */
    Map<String,String> configs();

    /**
     * Returns whether the topic is marked for deletion.
     */
    boolean markedForDeletion();

    /**
     * Returns whether the topic is an internal topic.
     */
    boolean internal();

}


/** The current state of the topics in the cluster, before the request takes effect. */
interface ClusterState {
    /**
     * Returns the current state of the given topic, or null if the topic does not exist.
     */
    TopicState topicState(String topicName);

    /**
     * Returns all the topics in the cluster, including internal topics if
     * {@code includeInternal} is true, and including those marked for deletion
     * if {@code includeMarkedForDeletion} is true.
     */
    Set<String> topics(boolean includeInternal, boolean includeMarkedForDeletion);

    /**
     * The number of brokers in the cluster.
     */
    int clusterSize();
}


/**
 * A policy that is enforced on topic creation, alteration and deletion,
 * and for the deletion of messages from a topic.
 *
 * An implementation of this policy can be configured on a broker via the
 * {@code topic.management.policy.class.name} broker config.
 * When this is configured the named class will be instantiated reflectively
 * using its nullary constructor and will then pass the broker configs to
 * its <code>configure()</code> method. During broker shutdown, the
 * <code>close()</code> method will be invoked so that resources can be
 * released (if necessary).
 *
 */
interface TopicManagementPolicy  extends Configurable, AutoCloseable {

    static interface AbstractRequestMetadata {

        /**
         * The topic the action is being performed upon.
         */
        public String topic();

        /**
         * The authenticated principal making the request, or null if the session is not authenticated.
         */
        public KafkaPrincipal principal();
    }


    static interface CreateTopicRequest extends AbstractRequestMetadata {
        /**
         * The requested state of the topic to be created.
         */
        public TopicState requestedState();
    }

    /**
     * Validate the given request to create a topic
     * and throw a <code>PolicyViolationException</code> with a suitable error
     * message if the request does not satisfy this policy.
     *
     * Clients will receive the POLICY_VIOLATION error code along with the exception's message.
     * Note that validation failure only affects the relevant topic,
     * other topics in the request will still be processed.
     *
     * @param requestMetadata the request parameters for the provided topic.
     * @param clusterState the current state of the cluster
     * @throws PolicyViolationException if the request parameters do not satisfy this policy.
     */
    void validateCreateTopic(CreateTopicRequest requestMetadata, ClusterState clusterState) throws PolicyViolationException;

    static interface AlterTopicRequest extends AbstractRequestMetadata {
        /**
         * The state the topic will have after the alteration.
         */
        public TopicState requestedState();
    }

    /**
     * Validate the given request to alter an existing topic
     * and throw a <code>PolicyViolationException</code> with a suitable error
     * message if the request does not satisfy this policy.
     *
     * The given {@code clusterState} can be used to discover the current state of the topic to be modified.
     *
     * Clients will receive the POLICY_VIOLATION error code along with the exception's message.
     * Note that validation failure only affects the relevant topic,
     * other topics in the request will still be processed.
     *
     * @param requestMetadata the request parameters for the provided topic.
     * @param clusterState the current state of the cluster
     * @throws PolicyViolationException if the request parameters do not satisfy this policy.
     */
    void validateAlterTopic(AlterTopicRequest requestMetadata, ClusterState clusterState) throws PolicyViolationException;

    /**
     * Parameters for a request to delete the given topic.
     */
    static interface DeleteTopicRequest extends AbstractRequestMetadata {
    }

    /**
     * Validate the given request to delete a topic
     * and throw a <code>PolicyViolationException</code> with a suitable error
     * message if the request does not satisfy this policy.
     *
     * The given {@code clusterState} can be used to discover the current state of the topic to be deleted.
     *
     * Clients will receive the POLICY_VIOLATION error code along with the exception's message.
     * Note that validation failure only affects the relevant topic,
     * other topics in the request will still be processed.
     *
     * @param requestMetadata the request parameters for the provided topic.
     * @param clusterState the current state of the cluster
     * @throws PolicyViolationException if the request parameters do not satisfy this policy.
     */
    void validateDeleteTopic(DeleteTopicRequest requestMetadata, ClusterState clusterState) throws PolicyViolationException;

    /**
     * Parameters for a request to delete records from the topic.
     */
    static interface DeleteRecordsRequest extends AbstractRequestMetadata {

        /**
         * Returns a map of topic partitions and the corresponding offset of the last message
         * to be retained. Messages before this offset will be deleted.
         * Partitions which won't have messages deleted won't be present in the map.
         */
        Map<Integer, Long> deletedMessageOffsets();
    }

    /**
     * Validate the given request to delete records from a topic
     * and throw a <code>PolicyViolationException</code> with a suitable error
     * message if the request does not satisfy this policy.
     *
     * The given {@code clusterState} can be used to discover the current state of the topic to have records deleted.
     *
     * Clients will receive the POLICY_VIOLATION error code along with the exception's message.
     * Note that validation failure only affects the relevant topic,
     * other topics in the request will still be processed.
     *
     * @param requestMetadata the request parameters for the provided topic.
     * @param clusterState the current state of the cluster
     * @throws PolicyViolationException if the request parameters do not satisfy this policy.
     */
    void validateDeleteRecords(DeleteRecordsRequest requestMetadata, ClusterState clusterState) throws PolicyViolationException;
}


