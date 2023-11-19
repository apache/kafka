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
     * Member has not joined a consumer group yet, or has been fenced and needs to re-join.
     */
    UNJOINED,

    /**
     * Member has received a new target assignment (partitions could have been assigned or
     * revoked), and it is processing it. While in this state, the member will
     * invoke the user callbacks for onPartitionsAssigned or onPartitionsRevoked, and then make
     * the new assignment effective.
     */
    // TODO: determine if separate state will be needed for assign/revoke (not for now)
    RECONCILING,

    /**
     * Member is active in a group (heartbeating) and has processed all assignments received.
     */
    STABLE,

    /**
     * Member transitions to this state when it receives a
     * {@link org.apache.kafka.common.protocol.Errors#UNKNOWN_MEMBER_ID} or
     * {@link org.apache.kafka.common.protocol.Errors#FENCED_MEMBER_EPOCH} error from the
     * broker. This is a recoverable state, where the member
     * gives up its partitions by invoking the user callbacks for onPartitionsLost, and then
     * transitions to {@link #UNJOINED} to rejoin the group as a new member.
     */
    FENCED,

    /**
     * The member failed with an unrecoverable error
     */
    FAILED;

    static {
        // Valid state transitions
        STABLE.previousValidStates = Arrays.asList(UNJOINED, RECONCILING);

        RECONCILING.previousValidStates = Arrays.asList(STABLE, UNJOINED);

        FAILED.previousValidStates = Arrays.asList(UNJOINED, STABLE, RECONCILING);

        FENCED.previousValidStates = Arrays.asList(STABLE, RECONCILING);

        UNJOINED.previousValidStates = Arrays.asList(FENCED);
    }

    private List<MemberState> previousValidStates;

    MemberState() {
        this.previousValidStates = new ArrayList<>();
    }

    public List<MemberState> getPreviousValidStates() {
        return this.previousValidStates;
    }
}
