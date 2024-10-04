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


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * The textual representation of a set of KIP-853 voters.
 *
 * Since this is used in command-line tools, format changes to the parsing logic require a KIP,
 * and should be backwards compatible.
 */
public final class DynamicVoters {
    /**
     * Create a DynamicVoters object by parsing an input string.
     *
     * @param input                         The input string.
     *
     * @return                              The DynamicVoters object.
     *
     * @throws IllegalArgumentException     If parsing fails.
     */
    public static DynamicVoters parse(String input) {
        input = input.trim();
        List<DynamicVoter> voters = new ArrayList<>();
        for (String voterString : input.split(",")) {
            if (!voterString.isEmpty()) {
                voters.add(DynamicVoter.parse(voterString));
            }
        }
        return new DynamicVoters(voters);
    }

    /**
     * Maps node ids to dynamic voters.
     */
    private final NavigableMap<Integer, DynamicVoter> voters;

    /**
     * Create a new DynamicVoters object.
     *
     * @param voters        The voters.
     */
    public DynamicVoters(Collection<DynamicVoter> voters) {
        if (voters.isEmpty()) {
            throw new IllegalArgumentException("No voters given.");
        }
        TreeMap<Integer, DynamicVoter> votersMap = new TreeMap<>();
        for (DynamicVoter voter : voters) {
            if (votersMap.put(voter.nodeId(), voter) != null) {
                throw new IllegalArgumentException("Node id " + voter.nodeId() +
                        " was specified more than once.");
            }
        }
        this.voters = Collections.unmodifiableNavigableMap(votersMap);
    }

    public NavigableMap<Integer, DynamicVoter> voters() {
        return voters;
    }

    public VoterSet toVoterSet(String controllerListenerName) {
        Map<Integer, VoterSet.VoterNode> voterSetMap = new HashMap<>();
        for (DynamicVoter voter : voters.values()) {
            voterSetMap.put(voter.nodeId(), voter.toVoterNode(controllerListenerName));
        }
        return VoterSet.fromMap(voterSetMap);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || (!(o.getClass().equals(DynamicVoters.class)))) return false;
        DynamicVoters other = (DynamicVoters) o;
        return voters.equals(other.voters);
    }

    @Override
    public int hashCode() {
        return voters.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        String prefix = "";
        for (DynamicVoter voter : voters.values()) {
            builder.append(prefix);
            prefix = ",";
            builder.append(voter.toString());
        }
        return builder.toString();
    }
}
