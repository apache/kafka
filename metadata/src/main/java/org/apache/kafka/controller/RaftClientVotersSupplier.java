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

import java.util.Set;
import java.util.function.Supplier;

/**
 * Checks if the provided node id is a voter according to the raft client.
 *
 * When the kraft.version == 0, the voter set comes from the {@code controller.quorum.voters}
 * configuration and never changes.
 *
 * When the kraft.version > 0, the voter set comes from the {@code VotersRecord}s in the metadata
 * log. The latest voter set may not have been committed yet, but if it is present, we should base
 * voter set membership based only on that. This is because any records whose writing is conditioned
 * on voter set membership, such as feature upgrades or unregistrations, will be written at a later
 * offset than the uncommitted voter set. Committing these records assumes the voter set record
 * also is committed.
 */
public final class RaftClientVotersSupplier implements Supplier<Set<Integer>> {
    private final RaftClient<?> raftClient;

    public RaftClientVotersSupplier(RaftClient<?> raftClient) {
        this.raftClient = raftClient;
    }

    @Override
    public Set<Integer> get() {
        return raftClient.latestVoterSet().voterIds();
    }
}
