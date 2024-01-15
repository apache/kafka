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

package org.apache.kafka.coordinator.group.generic;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Represents all states that a generic group can be in, as well as the states that a group must
 * be in to transition to a particular state.
 */
public enum GenericGroupState {

    /**
     * Group has no more members, but lingers until all offsets have expired. This state
     * also represents groups which use Kafka only for offset commits and have no members.
     *
     * action: respond normally to join group from new members
     *         respond to sync group with UNKNOWN_MEMBER_ID
     *         respond to heartbeat with UNKNOWN_MEMBER_ID
     *         respond to leave group with UNKNOWN_MEMBER_ID
     *         respond to offset commit with UNKNOWN_MEMBER_ID
     *         allow offset fetch requests
     * transition: last offsets removed in periodic expiration task => DEAD
     *             join group from a new member => PREPARING_REBALANCE
     *             group is removed by partition emigration => DEAD
     *             group is removed by expiration => DEAD
     */
    EMPTY("Empty"),

    /**
     * Group is preparing to rebalance.
     *
     * action: respond to heartbeats with REBALANCE_IN_PROGRESS
     *         respond to sync group with REBALANCE_IN_PROGRESS
     *         remove member on leave group request
     *         park join group requests from new or existing members until all expected members have joined
     *         allow offset commits from previous generation
     *         allow offset fetch requests
     * transition: some members have joined by the timeout => COMPLETING_REBALANCE
     *             all members have left the group => EMPTY
     *             group is removed by partition emigration => DEAD
     */
    PREPARING_REBALANCE("PreparingRebalance"),

    /**
     * Group is awaiting state assignment from the leader.
     *
     * action: respond to heartbeats with REBALANCE_IN_PROGRESS
     *         respond to offset commits with REBALANCE_IN_PROGRESS
     *         park sync group requests from followers until transition to STABLE
     *         allow offset fetch requests
     * transition: sync group with state assignment received from leader => STABLE
     *             join group from new member or existing member with updated metadata => PREPARING_REBALANCE
     *             leave group from existing member => PREPARING_REBALANCE
     *             member failure detected => PREPARING_REBALANCE
     *             group is removed by partition emigration => DEAD
     */
    COMPLETING_REBALANCE("CompletingRebalance"),

    /**
     * Group is stable.
     *
     * action: respond to member heartbeats normally
     *         respond to sync group from any member with current assignment
     *         respond to join group from followers with matching metadata with current group metadata
     *         allow offset commits from member of current generation
     *         allow offset fetch requests
     * transition: member failure detected via heartbeat => PREPARING_REBALANCE
     *             leave group from existing member => PREPARING_REBALANCE
     *             leader join-group received => PREPARING_REBALANCE
     *             follower join-group with new metadata => PREPARING_REBALANCE
     *             group is removed by partition emigration => DEAD
     */
    STABLE("Stable"),

    /**
     * Group has no more members and its metadata is being removed.
     *
     * action: respond to join group with UNKNOWN_MEMBER_ID
     *         respond to sync group with UNKNOWN_MEMBER_ID
     *         respond to heartbeat with UNKNOWN_MEMBER_ID
     *         respond to leave group with UNKNOWN_MEMBER_ID
     *         respond to offset commit with UNKNOWN_MEMBER_ID
     *         allow offset fetch requests
     * transition: DEAD is a final state before group metadata is cleaned up, so there are no transitions
     */
    DEAD("Dead");

    private final String name;
    private Set<GenericGroupState> validPreviousStates;

    static {
        EMPTY.addValidPreviousStates(PREPARING_REBALANCE);
        PREPARING_REBALANCE.addValidPreviousStates(STABLE, COMPLETING_REBALANCE, EMPTY);
        COMPLETING_REBALANCE.addValidPreviousStates(PREPARING_REBALANCE);
        STABLE.addValidPreviousStates(COMPLETING_REBALANCE);
        DEAD.addValidPreviousStates(STABLE, PREPARING_REBALANCE, COMPLETING_REBALANCE, EMPTY, DEAD);
    }

    GenericGroupState(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    private void addValidPreviousStates(GenericGroupState... validPreviousStates) {
        this.validPreviousStates = new HashSet<>(Arrays.asList(validPreviousStates));
    }

    /**
     * @return valid previous states a group must be in to transition to this state.
     */
    public Set<GenericGroupState> validPreviousStates() {
        return this.validPreviousStates;
    }
}
