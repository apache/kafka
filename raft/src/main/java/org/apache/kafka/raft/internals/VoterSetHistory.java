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
package org.apache.kafka.raft.internals;

import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.Map;

// TODO: write unittest for VoterSetHistory 
// TODO: Write documentation
final public class VoterSetHistory {
    Optional<VoterSet> staticVoterSet;
    TreeMap<Long, VoterSet> votersHistory = new TreeMap<>();

    public VoterSetHistory(Optional<VoterSet> staticVoterSet) {
        this.staticVoterSet = staticVoterSet;
    }

    public void nextVoterSet(long offset, VoterSet voters) {
        Optional<Map.Entry<Long, VoterSet>> lastEntry = Optional.ofNullable(votersHistory.lastEntry());
        Optional<Long> currentOffset = lastEntry.map(Map.Entry::getKey);
        if (currentOffset.isPresent() && offset <= currentOffset.get()) {
            throw new IllegalArgumentException(
                String.format("Next offset %d must be greater than the last offset %d", offset, currentOffset.get())
            );
        }

        if (lastEntry.isPresent() && lastEntry.get().getKey() >= 0) {
            // If the last voter set comes from the replicated log then the majorities must overlap. This ignores
            // the static voter set and the bootstrapped voter set since they come from the configuration and the KRaft
            // leader never guaranteed that they are the same across all replicas.
            VoterSet lastVoterSet = lastEntry.get().getValue();
            if (!lastVoterSet.hasOverlappingMajority(voters)) {
                throw new IllegalArgumentException(
                    String.format(
                        "Last voter set %s doesn't have an overlapping majority with the new voter set %s",
                        lastVoterSet,
                        voters
                    )
                );
            }
        }

        votersHistory.compute(
            offset,
            (key, value) -> {
                if (value != null) {
                    throw new IllegalArgumentException(
                        String.format("Rejected %s sincea voter set already exist at %d: %s", voters, offset, value)
                    );
                }

                return voters;
            }
        );
    }

    // TODO: document that this doesn't include the static configuration
    public Optional<VoterSet> voterSetAt(long offset) {
        return Optional.ofNullable(votersHistory.floorEntry(offset)).map(Entry::getValue);
    }

    public VoterSet latestVoterSet() {
        return Optional
            .ofNullable(votersHistory.lastEntry())
            .map(Entry::getValue)
            .orElseGet(() -> staticVoterSet.orElseThrow(() -> new IllegalStateException("No voter set found")));
    }

    void truncateTo(long endOffset) {
        votersHistory.tailMap(endOffset, false).clear();
    }

    void trimPrefixTo(long startOffset) {
        NavigableMap<Long, VoterSet> lesserVoters = votersHistory.headMap(startOffset, true);
        while (lesserVoters.size() > 1) {
            // Poll and ignore the entry to remove the first entry
            lesserVoters.pollFirstEntry();
        }
    }
}
