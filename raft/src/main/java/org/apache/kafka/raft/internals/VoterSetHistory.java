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

import java.util.Optional;

// TODO: write unittest for VoterSetHistory 
// TODO: Write documentation
final public class VoterSetHistory implements History<VoterSet> {
    private final Optional<VoterSet> staticVoterSet;
    private final History<VoterSet> votersHistory = new TreeMapHistory<>();

    VoterSetHistory(Optional<VoterSet> staticVoterSet) {
        this.staticVoterSet = staticVoterSet;
    }

    @Override
    public void addAt(long offset, VoterSet voters) {
        Optional<History.Entry<VoterSet>> lastEntry = votersHistory.lastEntry();
        if (lastEntry.isPresent() && lastEntry.get().offset() >= 0) {
            // If the last voter set comes from the replicated log then the majorities must overlap. This ignores
            // the static voter set and the bootstrapped voter set since they come from the configuration and the KRaft
            // leader never guaranteed that they are the same across all replicas.
            VoterSet lastVoterSet = lastEntry.get().value();
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

        votersHistory.addAt(offset, voters);
    }

    // TODO: document that this doesn't include the static configuration
    @Override
    public Optional<VoterSet> valueAt(long offset) {
        return votersHistory.valueAt(offset);
    }

    @Override
    public Optional<History.Entry<VoterSet>> lastEntry() {
        Optional<History.Entry<VoterSet>> result = votersHistory.lastEntry();
        if (result.isPresent()) return result;

        return staticVoterSet.map(value -> new History.Entry<>(-1, value));
    }

    public VoterSet lastValue() {
        return lastEntry().orElseThrow(() -> new IllegalStateException("No voter set found")).value();
    }

    @Override
    public void truncateTo(long endOffset) {
        votersHistory.truncateTo(endOffset);
    }

    @Override
    public void trimPrefixTo(long startOffset) {
        votersHistory.trimPrefixTo(startOffset);
    }

    @Override
    public void clear() {
        votersHistory.clear();
    }
}
