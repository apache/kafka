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

import org.apache.kafka.raft.internals.EpochElection;

interface NomineeState extends EpochState {
    EpochElection epochElection();

    /**
     * Record a granted vote from one of the voters.
     *
     * @param remoteNodeId The id of the voter
     * @return true if the voter had not been previously recorded
     * @throws IllegalArgumentException
     */
    boolean recordGrantedVote(int remoteNodeId);

    /**
     * Record a rejected vote from one of the voters.
     *
     * @param remoteNodeId The id of the voter
     * @return true if the voter had not been previously recorded
     * @throws IllegalArgumentException
     */
    boolean recordRejectedVote(int remoteNodeId);

    /**
     * Returns true if the election timeout has expired, false otherwise.
     * @param currentTimeMs The current time in milliseconds
     */
    boolean hasElectionTimeoutExpired(long currentTimeMs);

    /**
     * Returns the remaining time in milliseconds until the election timeout expires.
     * @param currentTimeMs The current time in milliseconds
     */
    long remainingElectionTimeMs(long currentTimeMs);
}
