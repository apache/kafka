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
package org.apache.kafka.raft;

import org.apache.kafka.common.record.Records;

/**
 * First try at an interface for state machines on top of Raft. At any time, the state
 * machine is either following or leading the Raft quorum. If leading, then all new
 * record appends will be routed to the leader, which will have an opportunity to either
 * accept or reject the append attempt.
 */
public interface ReplicatedStateMachine extends AutoCloseable {


    /**
     * Initialize the state machine.
     *
     * @param recordAppender handler for appending records.
     */
    void initialize(RecordAppender recordAppender);

    /**
     * Become a leader. This is invoked after a new election in the quorum if this
     * node was elected as the leader.
     *
     * @param epoch The latest quorum epoch
     */
    void becomeLeader(int epoch);

    /**
     * Become a follower. This is invoked after a new election finishes if this
     * node was not elected as the leader.
     *
     * @param epoch The latest quorum epoch
     */
    void becomeFollower(int epoch);

    /**
     * The next expected offset that will be appended to the log. This should be
     * updated after every call to {@link #apply(Records)}.
     *
     * @return The offset and epoch that was last consumed
     */
    OffsetAndEpoch position();

    /**
     * Apply committed records to the state machine. Committed records should not
     * be lost by the quorum.
     *
     * @param records The records to apply to the state machine
     */
    void apply(Records records);

    /**
     * This is only invoked by leaders. The leader is guaranteed to have the full committed
     * state before this method is invoked in a new leader epoch.
     *
     * Note that acceptance does not guarantee that the records will become committed
     * since that depends on replication to the quorum. For example, if there is a leader
     * change before the record can be committed, then accepted records may be lost.
     *
     * @param records The records appended to the leader
     * @return true if the records should be appended to the log
     */
    boolean accept(Records records);

    /**
     * Close the state machine.
     */
    @Override
    default void close() {
    }
}
