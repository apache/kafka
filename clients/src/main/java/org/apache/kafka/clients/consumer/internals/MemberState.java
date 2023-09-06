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
     * Member has not joined a consumer group yet
     */
    UNJOINED,

    /**
     * Member has received a new assignment (partitions assigned or revoked), and it is applying
     * it. While in this state, the member will invoke the user callbacks for
     * onPartitionsAssigned or onPartitionsRevoked, and then make the new assignment effective.
     */
    // TODO: determine if separate state will be needed for assign/revoke (not for now)
    PROCESSING_ASSIGNMENT,

    /**
     * Member is active in a group (heartbeating) and has processed all assignments received.
     */
    STABLE,

    /**
     * The member failed with an unrecoverable error
     */
    FAILED;

    static {
        // Valid state transitions
        STABLE.previousValidStates = Arrays.asList(UNJOINED, PROCESSING_ASSIGNMENT);

        PROCESSING_ASSIGNMENT.previousValidStates = Arrays.asList(STABLE, UNJOINED);

        FAILED.previousValidStates = Arrays.asList(STABLE, PROCESSING_ASSIGNMENT);

        // TODO: consider FAILED->STABLE?
    }

    private List<MemberState> previousValidStates;

    MemberState() {
        this.previousValidStates = new ArrayList<>();
    }

    public List<MemberState> getPreviousValidStates() {
        return this.previousValidStates;
    }
}
