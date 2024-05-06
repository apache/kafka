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

import java.nio.file.Path;
import java.util.Optional;

/**
 *  Maintain the save and retrieval of quorum state information, so far only supports
 *  read and write of election states.
 */
public interface QuorumStateStore {
    /**
     * Read the latest election state.
     *
     * @return the latest written election state or {@code Optional.empty()} if there is none
     */
    Optional<ElectionState> readElectionState();

    /**
     * Persist the updated election state.
     *
     * This must be atomic, both writing the full updated state and replacing the old state.
     *
     * @param latest the latest election state
     * @param kraftVersion the finalized kraft.version
     */
    void writeElectionState(ElectionState latest, short kraftVersion);

    /**
     * Path to the quorum state store
     */
    Path path();

    /**
     * Clear any state associated to the store for a fresh start
     */
    void clear();
}
