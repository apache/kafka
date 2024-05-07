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

import org.apache.kafka.common.protocol.Errors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public enum MemberState {

    /**
     * Member has a group id, but it is not subscribed to any topic to receive automatic
     * assignments. This will be the state when the member has never subscribed, or when it has
     * unsubscribed from all topics. While in this state the member can commit offsets but won't
     * be an active member of the consumer group (no heartbeats sent).
     */
    UNSUBSCRIBED,

    /**
     * Member is attempting to join a consumer group. While in this state, the member will send
     * heartbeat requests on the interval, with epoch 0, until it gets a response with an epoch > 0
     * or a fatal failure. A member transitions to this state when it tries to join the group for
     * the first time with a call to subscribe, or when it has been fenced and tries to re-join.
     */
    JOINING,

    /**
     * Member has received a new target assignment (partitions could have been assigned or
     * revoked), and it is processing it. While in this state, the member will continue to send
     * heartbeat on the interval, and reconcile the assignment (it will commit offsets if
     * needed, invoke the user callbacks for onPartitionsAssigned or onPartitionsRevoked, and make
     * the new assignment effective). Note that while in this state the member may be trying to
     * resolve metadata for the target assignment, or triggering commits/callbacks if topic names
     * already resolved.
     */
    RECONCILING,

    /**
     * Member has completed reconciling an assignment received, and stays in this state only until
     * the next heartbeat request is sent out to acknowledge the assignment to the server. This
     * state indicates that the next heartbeat request must be sent without waiting for the
     * heartbeat interval to expire. Note that once the ack is sent, the member could go back to
     * {@link #RECONCILING} if it still has assignment waiting to be reconciled (assignments
     * waiting for metadata, assignments for which metadata was resolved, or new assignments
     * received from the broker)
     */
    ACKNOWLEDGING,

    /**
     * Member is active in a group and has processed all assignments received. While in this
     * state, the member will send heartbeats on the interval.
     */
    STABLE,

    /**
     * Member transitions to this state when it receives a {@link Errors#UNKNOWN_MEMBER_ID} or
     * {@link Errors#FENCED_MEMBER_EPOCH} error from the broker, indicating that it has been
     * left out of the group. While in this state, the member will stop sending heartbeats, it
     * will give up its partitions by invoking the user callbacks for onPartitionsLost, and then
     * transition to {@link #JOINING} to re-join the group as a new member.
     */
    FENCED,

    /**
     * The member transitions to this state before sending a heartbeat to leave the group,
     * While in this state, the member will continue sending heartbeats while it release its
     * assignment calling the user's callback. When callbacks complete, the member will transition
     * out of this state into {@link #LEAVING} to send a heartbeat to leave the group. Note that
     * if leaving due to expired poll timer, the member does not execute any callbacks while in
     * this state and just transitions to {@link #LEAVING} and then {@link #STALE}
     */
    PREPARE_LEAVING,

    /**
     * Member has committed offsets and releases its assignment, so it stays in this state until
     * the next heartbeat request is sent out with epoch -1 or -2 to effectively leave the group.
     * This state indicates that the next heartbeat request must be sent without waiting for the
     * heartbeat interval to expire.
     */
    LEAVING,

    /**
     * The member failed with an unrecoverable error received in a heartbeat response. This in an
     * unrecoverable state where the member won't send any requests to the broker and cannot
     * perform any other transition.
     */
    FATAL,

    /**
     * The member transitions to this state when the poll timer expires, indicating that there
     * hasn't been a call to consumer.poll within the <code>max.poll.interval.ms</code>. While in
     * this state, the member will send a heartbeat to leave the group, invoke the
     * onPartitionsLost callback, and clear its assignments. The member will only transition
     * out of this state on the next application poll event. The member will then transition
     * to JOINING, to rejoin the group.
     */
    STALE;

    // Valid state transitions
    static {

        STABLE.previousValidStates = Arrays.asList(JOINING, ACKNOWLEDGING, RECONCILING);

        RECONCILING.previousValidStates = Arrays.asList(STABLE, JOINING, ACKNOWLEDGING, RECONCILING);

        ACKNOWLEDGING.previousValidStates = Arrays.asList(RECONCILING);

        FATAL.previousValidStates = Arrays.asList(JOINING, STABLE, RECONCILING, ACKNOWLEDGING,
                PREPARE_LEAVING, LEAVING, UNSUBSCRIBED);

        FENCED.previousValidStates = Arrays.asList(JOINING, STABLE, RECONCILING, ACKNOWLEDGING,
                PREPARE_LEAVING, LEAVING);

        JOINING.previousValidStates = Arrays.asList(FENCED, UNSUBSCRIBED, STALE);

        PREPARE_LEAVING.previousValidStates = Arrays.asList(JOINING, STABLE, RECONCILING,
                ACKNOWLEDGING, UNSUBSCRIBED);

        LEAVING.previousValidStates = Arrays.asList(PREPARE_LEAVING);

        UNSUBSCRIBED.previousValidStates = Arrays.asList(PREPARE_LEAVING, LEAVING, FENCED);

        STALE.previousValidStates = Arrays.asList(LEAVING);
    }

    private List<MemberState> previousValidStates;

    MemberState() {
        this.previousValidStates = new ArrayList<>();
    }

    public List<MemberState> getPreviousValidStates() {
        return this.previousValidStates;
    }

    /**
     * @return True if the member is in a state where it should reconcile the new assignment.
     * Expected to be true whenever the member is part of the group and intends of staying in it
     * (ex. false when the member is preparing to leave the group).
     */
    public boolean canHandleNewAssignment() {
        return MemberState.RECONCILING.getPreviousValidStates().contains(this);
    }
}
