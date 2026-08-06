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

package org.apache.kafka.controller;

import org.apache.kafka.raft.RaftClient;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Supplies the IDs of the nodes which are part of the voter set according to the raft client.
 *
 * When the kraft.version is 0, the voter set comes from the {@code controller.quorum.voters}
 * configuration and it never changes, so the latest voter set describes the quorum completely.
 *
 * When the kraft.version is 1, the voter set comes from the {@code VotersRecord}s in the metadata
 * log. The latest voter set may not have been committed yet, and the latest committed voter set may
 * get replaced by an uncommitted one, so a node is considered a voter if it is in either of them.
 */
public final class RaftClientVotersSupplier implements Supplier<Set<Integer>> {
    private final RaftClient<?> raftClient;

    public RaftClientVotersSupplier(RaftClient<?> raftClient) {
        this.raftClient = raftClient;
    }

    @Override
    public Set<Integer> get() {
        Set<Integer> voterIds = new HashSet<>(raftClient.latestVoterSet().voterIds());
        if (raftClient.kraftVersion().isReconfigSupported()) {
            raftClient.latestCommittedVoterSet().ifPresent(voters -> voterIds.addAll(voters.voterIds()));
        }
        return voterIds;
    }
}
