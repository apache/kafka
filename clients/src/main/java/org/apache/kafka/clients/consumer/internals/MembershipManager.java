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

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;

/**
 * Manages group membership for a single member.
 * Responsible for:
 * <li>Keeping member state</li>
 * <li>Keeping assignment for the member</li>
 * <li>Computing assignment for the group if the member is required to do so<li/>
 */
public interface MembershipManager {

    String groupId();

    Optional<String> groupInstanceId();

    String memberId();

    int memberEpoch();

    MemberState state();

    /**
     * Update the current state of the member based on a heartbeat response
     */
    void updateState(ConsumerGroupHeartbeatResponseData response);

    /**
     * Returns the {@link AssignorSelection} for the member
     */
    AssignorSelection assignorSelection();

    /**
     * Returns the current assignment for the member
     */
    ConsumerGroupHeartbeatResponseData.Assignment assignment();

    /**
     * Update the assignment for the member, indicating that the provided assignment is the new
     * current assignment.
     */
    void updateAssignment(ConsumerGroupHeartbeatResponseData.Assignment assignment);

    /**
     * Transition the member to the FENCED state.  This is only invoked when the heartbeat returns a
     * FENCED_MEMBER_EPOCH or UNKNOWN_MEMBER_ID error code.
     */
    void transitionToFenced();

    /**
     * Transition the member to the FAILED state.  This is invoked when the heartbeat returns a non-retriable error.
     */
    void transitionToFailed();

    /**
     * Return true if the member should send heartbeat to the coordinator
     */
    boolean shouldSendHeartbeat();

    /**
     * This method should be invoked to signal the completion of a successful {@link TopicPartition partition}
     * assignment reconciliation. Specifically, it is to be executed on background thread <em>after</em> the
     * {@link ConsumerRebalanceListener#onPartitionsRevoked(Collection)} and
     * {@link ConsumerRebalanceListener#onPartitionsAssigned(Collection)} callbacks have completed execution on
     * the application thread. It should perform two tasks:
     *
     * <ol>
     *     <li>
     *        Update the set of {@link SubscriptionState#assignedPartitions() assigned partitions} based on the
     *        given partitions
     *     </li>
     *     <li>
     *        Update the necessary internal state to signal to the {@link HeartbeatRequestManager} that it
     *        should send an acknowledgement heartbeat request to the group coordinator
     *     </li>
     * </ol>
     *
     * Note: the partition assignment reconciliation process is started based on the receipt of a new
     * {@link ConsumerGroupHeartbeatResponseData.Assignment target assignment}.
     *
     * @param revokedPartitions Set of {@link TopicPartition partitions} that were revoked
     * @param assignedPartitions Set of {@link TopicPartition partitions} that were assigned
     * @param callbackError Optional {@link KafkaException error} if an exception was thrown during callbacks
     * @see AssignmentReconciler
     */
    void completeReconcile(Set<TopicPartition> revokedPartitions,
                           Set<TopicPartition> assignedPartitions,
                           Optional<KafkaException> callbackError);

    /**
     * This method should be invoked to signal the completion of the "{@link TopicPartition lost partition}"
     * process. Specifically, it is to be executed on background thread <em>after</em> the
     * {@link ConsumerRebalanceListener#onPartitionsLost(Collection)} callback was executed on the application
     * thread. It should perform two tasks:
     *
     * <ol>
     *     <li>
     *        Clear the set of {@link SubscriptionState#assignedPartitions() assigned partitions}, regardless of
     *        the set of "lost partitions"
     *     </li>
     *     <li>
     *        Update the necessary internal state to signal to the {@link HeartbeatRequestManager} that it
     *        should send an acknowledgement heartbeat request to the group coordinator
     *     </li>
     * </ol>
     *
     * @param lostPartitions Set of {@link TopicPartition partitions} that were lost
     * @param callbackError Optional {@link KafkaException error} if an exception was thrown during callback
     * @see AssignmentReconciler
     */
    void completeLost(Set<TopicPartition> lostPartitions, Optional<KafkaException> callbackError);
}
