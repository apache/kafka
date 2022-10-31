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

/**
 *  Maintain the save and retrieval of quorum state information, so far only supports
 *  read and write of election states.
 */
public interface QuorumStateStore {

    int UNKNOWN_LEADER_ID = -1;
    int NOT_VOTED = -1;

    /**
     * Read the latest election state.
     *
     * @return The latest written election state or `null` if there is none
     */
    ElectionState readElectionState();

    /**
     * Persist the updated election state. This must be atomic, both writing the full updated state
     * and replacing the old state.
     * @param latest The latest election state
     */
    void writeElectionState(ElectionState latest);

    /**
     * Clear any state associated to the store for a fresh start
     */
    void clear();
}
