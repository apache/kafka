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

import java.io.Closeable;
import java.util.Optional;
import org.apache.kafka.common.Uuid;

public interface EpochState extends Closeable {

    default Optional<LogOffsetMetadata> highWatermark() {
        return Optional.empty();
    }

    /**
     * Decide whether to grant a vote to a candidate.
     *
     * It is the responsibility of the caller to invoke
     * {@link QuorumState#transitionToVoted(int, int)} if vote is granted.
     *
     * @param candidateId the id of the candidate attempting to become leader
     * @param candidateDirectoryId the directory id of the candidate attempting to become leader
     * @param isLogUpToDate whether the candidate’s log is at least as up-to-date as receiver’s log
     * @return true if it can grant the vote, false otherwise
     */
    boolean canGrantVote(int candidateId, Optional<Uuid> candidateDirectoryId, boolean isLogUpToDate);

    /**
     * Get the current election state, which is guaranteed to be immutable.
     */
    ElectionState election();

    /**
     * Get the current (immutable) epoch.
     */
    int epoch();

    /**
     * User-friendly description of the state
     */
    String name();
}
