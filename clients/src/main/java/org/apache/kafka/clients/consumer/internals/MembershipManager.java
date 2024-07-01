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
import org.apache.kafka.clients.consumer.internals.events.ConsumerRebalanceListenerCallbackCompletedEvent;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;

/**
 * A stateful object tracking the state of a single member in relationship to a consumer group:
 * <p/>
 * Responsible for:
 * <li>Keeping member state</li>
 * <li>Keeping assignment for the member</li>
 * <li>Computing assignment for the group if the member is required to do so<li/>
 */
public interface MembershipManager extends RequestManager {

    /**
     * @return Group ID of the consumer group the member is part of (or wants to be part of).
     */
    String groupId();

    /**
     * @return Instance ID used by the member when joining the group. If non-empty, it will indicate that
     * this is a static member.
     */
    Optional<String> groupInstanceId();

    /**
     * @return Member ID assigned by the server to this member when it joins the consumer group.
     */
    String memberId();

    /**
     * @return Current epoch of the member, maintained by the server.
     */
    int memberEpoch();

    /**
     * @return Current state of this member in relationship to a consumer group, as defined in
     * {@link MemberState}.
     */
    MemberState state();

    /**
     * Update member info and transition member state based on a successful heartbeat response.
     *
     * @param response Heartbeat response to extract member info and errors from.
     */
    void onHeartbeatSuccess(ConsumerGroupHeartbeatResponseData response);

    /**
     * Notify the member that an error heartbeat response was received.
     */
    void onHeartbeatFailure();

    /**
     * Update state when a heartbeat is sent out. This will transition out of the states that end
     * when a heartbeat request is sent, without waiting for a response (ex.
     * {@link MemberState#ACKNOWLEDGING} and {@link MemberState#LEAVING}).
     */
    void onHeartbeatRequestSent();

    /**
     * Transition out of the {@link MemberState#LEAVING} state even if the heartbeat was not sent
     * . This will ensure that the member is not blocked on {@link MemberState#LEAVING} (best
     * effort to send the request, without any response handling or retry logic)
     */
    void onHeartbeatRequestSkipped();

    /**
     * @return Server-side assignor implementation configured for the member, that will be sent
     * out to the server to be used. If empty, then the server will select the assignor.
     */
    Optional<String> serverAssignor();

    /**
     * @return Current assignment for the member as received from the broker (topic IDs and
     * partitions). This is the last assignment that the member has successfully reconciled.
     */
    LocalAssignment currentAssignment();

    /**
     * Transition the member to the FENCED state, where the member will release the assignment by
     * calling the onPartitionsLost callback, and when the callback completes, it will transition
     * to {@link MemberState#JOINING} to rejoin the group. This is expected to be invoked when
     * the heartbeat returns a FENCED_MEMBER_EPOCH or UNKNOWN_MEMBER_ID error.
     */
    void transitionToFenced();

    /**
     * Transition the member to the FAILED state and update the member info as required. This is
     * invoked when un-recoverable errors occur (ex. when the heartbeat returns a non-retriable
     * error)
     */
    void transitionToFatal();

    /**
     * Release assignment and transition to {@link MemberState#PREPARE_LEAVING} so that a heartbeat
     * request is sent indicating the broker that the member wants to leave the group. This is
     * expected to be invoked when the user calls the unsubscribe API.
     *
     * @return Future that will complete when the callback execution completes and the heartbeat
     * to leave the group has been sent out.
     */
    CompletableFuture<Void> leaveGroup();

    /**
     * @return True if the member should send heartbeat to the coordinator without waiting for
     * the interval.
     */
    boolean shouldHeartbeatNow();

    /**
     * @return True if the member should skip sending the heartbeat to the coordinator. This
     * could be the case then the member is not in a group, or when it failed with a fatal error.
     */
    boolean shouldSkipHeartbeat();

    /**
     * Join the group with the updated subscription, if the member is not part of it yet. If the
     * member is already part of the group, this will only ensure that the updated subscription
     * is included in the next heartbeat request.
     * <p/>
     * Note that list of topics of the subscription is taken from the shared subscription state.
     */
    void onSubscriptionUpdated();

    /**
     * Signals that a {@link ConsumerRebalanceListener} callback has completed. This is invoked when the
     * application thread has completed the callback and has submitted a
     * {@link ConsumerRebalanceListenerCallbackCompletedEvent} to the network I/O thread. At this point, we
     * notify the state machine that it's complete so that it can move to the next appropriate step of the
     * rebalance process.
     *
     * @param event Event with details about the callback that was executed
     */
    void consumerRebalanceListenerCallbackCompleted(ConsumerRebalanceListenerCallbackCompletedEvent event);

    /**
     * Transition to the {@link MemberState#JOINING} state to attempt joining a group.
     */
    void transitionToJoining();

    /**
     * Transition to the {@link MemberState#LEAVING} state to send a heartbeat to leave the group.
     */
    void transitionToSendingLeaveGroup(boolean dueToPollTimerExpired);

    /**
     * Register a listener that will be called whenever the member state changes due to
     * transitions of new data received from the server, as defined in {@link MemberStateListener}.
     */
    void registerStateListener(MemberStateListener listener);

    /**
     * @return True if the member is preparing to leave the group (waiting for callbacks), or
     * leaving (sending last heartbeat).
     */
    boolean isLeavingGroup();

    /**
     * Transition a {@link MemberState#STALE} member to {@link MemberState#JOINING} when it completes
     * releasing its assignment. This is expected to be used when the poll timer is reset.
     */
    void maybeRejoinStaleMember();

    /**
     * A data structure to represent the current assignment, and current target assignment of a member in a consumer group.
     *
     * Besides the assigned partitions, it contains a local epoch that is bumped whenever the assignment changes, to ensure
     * that two assignments with the same partitions but different local epochs are not considered equal.
     */
    final class LocalAssignment {

        public static final long NONE_EPOCH = -1;

        public static final LocalAssignment NONE = new LocalAssignment(NONE_EPOCH, Collections.emptyMap());

        public final long localEpoch;

        public final Map<Uuid, SortedSet<Integer>> partitions;

        public LocalAssignment(long localEpoch, Map<Uuid, SortedSet<Integer>> partitions) {
            this.localEpoch = localEpoch;
            this.partitions = partitions;
            if (localEpoch == NONE_EPOCH && !partitions.isEmpty()) {
                throw new IllegalArgumentException("Local epoch must be set if there are partitions");
            }
        }

        public LocalAssignment(long localEpoch, SortedSet<TopicIdPartition> topicIdPartitions) {
            this.localEpoch = localEpoch;
            this.partitions = new HashMap<>();
            if (localEpoch == NONE_EPOCH && !topicIdPartitions.isEmpty()) {
                throw new IllegalArgumentException("Local epoch must be set if there are partitions");
            }
            topicIdPartitions.forEach(topicIdPartition -> {
                Uuid topicId = topicIdPartition.topicId();
                partitions.computeIfAbsent(topicId, k -> new TreeSet<>()).add(topicIdPartition.partition());
            });
        }

        public String toString() {
            return "LocalAssignment{" +
                "localEpoch=" + localEpoch +
                ", partitions=" + partitions +
                '}';
        }

        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final LocalAssignment that = (LocalAssignment) o;
            return localEpoch == that.localEpoch && Objects.equals(partitions, that.partitions);
        }

        public int hashCode() {
            return Objects.hash(localEpoch, partitions);
        }

        public boolean isNone() {
            return localEpoch == NONE_EPOCH;
        }

        Optional<LocalAssignment> updateWith(ConsumerGroupHeartbeatResponseData.Assignment assignment) {
            // Return if we have an assignment, and it is the same as current assignment; comparison without creating a new collection
            if (localEpoch != NONE_EPOCH) {
                if (partitions.size() == assignment.topicPartitions().size() &&
                    assignment.topicPartitions().stream().allMatch(
                        tp -> partitions.containsKey(tp.topicId()) &&
                            partitions.get(tp.topicId()).size() == tp.partitions().size() &&
                            partitions.get(tp.topicId()).containsAll(tp.partitions()))) {
                    return Optional.empty();
                }
            }

            // Bump local epoch and replace assignment
            long nextLocalEpoch = localEpoch + 1;
            HashMap<Uuid, SortedSet<Integer>> partitions = new HashMap<>();
            assignment.topicPartitions().forEach(topicPartitions ->
                partitions.put(topicPartitions.topicId(), new TreeSet<>(topicPartitions.partitions())));
            return Optional.of(new LocalAssignment(nextLocalEpoch, partitions));

        }
    }
}
