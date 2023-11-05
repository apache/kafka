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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public enum MemberState {

    /**
     * Member has a group id, but it is not subscribed to any topic to receive automatic
     * assignments. This will be the state when the member has never subscribed, or when it has
     * unsubscribed from all topics. While in this state the member can commit offsets but won't
     * be an active member ofRe the consumer group (no heartbeats).
     */
    UNSUBSCRIBED,

    /**
     * Member is attempting to join a consumer group. This could be the case when joining for the
     * first time, or when it has been fenced and tries to re-join.
     */
    JOINING,

    /**
     * Member has received a new target assignment (partitions could have been assigned or
     * revoked), and it is processing it. While in this state, the member will
     * invoke the user callbacks for onPartitionsAssigned or onPartitionsRevoked, and then make
     * the new assignment effective.
     */
    RECONCILING,

    /**
     * Member has completed reconciling an assignment received, and stays in this state until the
     * next heartbeat request is sent out to acknowledge the assignment to the server.
     */
    ACKNOWLEDGING,

    /**
     * Member is active in a group, sending heartbeats, and has processed all assignments received.
     */
    STABLE,

    /**
     * Member transitions to this state when it receives a
     * {@link org.apache.kafka.common.protocol.Errors#UNKNOWN_MEMBER_ID} or
     * {@link org.apache.kafka.common.protocol.Errors#FENCED_MEMBER_EPOCH} error from the
     * broker. This is a recoverable state, where the member
     * gives up its partitions by invoking the user callbacks for onPartitionsLost, and then
     * transitions to {@link #JOINING} to rejoin the group as a new member.
     */
    FENCED,

    /**
     * The member transitions to this state after a call to unsubscribe, to start taking actions
     * before actually leaving the group. These actions involve committing offsets if needed and
     * releasing its assignment (calling user's callback for partitions revoked or lost).
     */
    PREPARE_LEAVING,

    /**
     * Member has completed releasing its assignment, and stays in this state until the next
     * heartbeat request is sent out to effectively leave the group.
     */
    LEAVING,

    /**
     * The member failed with an unrecoverable error.
     */
    FATAL;

    /**
     * Valid state transitions
     */
    static {

        STABLE.previousValidStates = Arrays.asList(JOINING, ACKNOWLEDGING);

        RECONCILING.previousValidStates = Arrays.asList(STABLE, JOINING);

        ACKNOWLEDGING.previousValidStates = Arrays.asList(RECONCILING);

        FATAL.previousValidStates = Arrays.asList(JOINING, STABLE, RECONCILING, ACKNOWLEDGING);

        FENCED.previousValidStates = Arrays.asList(JOINING, STABLE, RECONCILING, ACKNOWLEDGING);

        JOINING.previousValidStates = Arrays.asList(FENCED, UNSUBSCRIBED);

        PREPARE_LEAVING.previousValidStates = Arrays.asList(JOINING, STABLE, RECONCILING, ACKNOWLEDGING, UNSUBSCRIBED);

        LEAVING.previousValidStates = Arrays.asList(PREPARE_LEAVING);
    }

    private List<MemberState> previousValidStates;

    MemberState() {
        this.previousValidStates = new ArrayList<>();
    }

    public List<MemberState> getPreviousValidStates() {
        return this.previousValidStates;
    }
}
