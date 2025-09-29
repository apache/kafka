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

package org.apache.kafka.common;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The group state.
 * <p>
 * The following table shows the correspondence between the group states and types.
 * <table>
 *     <thead>
 *         <tr><th>State</th><th>Classic group</th><th>Consumer group</th><th>Share group</th></tr>
 *     </thead>
 *     <tbody>
 *         <tr><td>UNKNOWN</td><td>Yes</td><td>Yes</td><td>Yes</td></tr>
 *         <tr><td>PREPARING_REBALANCE</td><td>Yes</td><td>Yes</td><td></td></tr>
 *         <tr><td>COMPLETING_REBALANCE</td><td>Yes</td><td>Yes</td><td></td></tr>
 *         <tr><td>STABLE</td><td>Yes</td><td>Yes</td><td>Yes</td></tr>
 *         <tr><td>DEAD</td><td>Yes</td><td>Yes</td><td>Yes</td></tr>
 *         <tr><td>EMPTY</td><td>Yes</td><td>Yes</td><td>Yes</td></tr>
 *         <tr><td>ASSIGNING</td><td></td><td>Yes</td><td></td></tr>
 *         <tr><td>RECONCILING</td><td></td><td>Yes</td><td></td></tr>
 *     </tbody>
 * </table>
 */
@InterfaceStability.Evolving
public enum GroupState {
    UNKNOWN("Unknown"),
    PREPARING_REBALANCE("PreparingRebalance"),
    COMPLETING_REBALANCE("CompletingRebalance"),
    STABLE("Stable"),
    DEAD("Dead"),
    EMPTY("Empty"),
    ASSIGNING("Assigning"),
    RECONCILING("Reconciling");

    private static final Map<String, GroupState> NAME_TO_ENUM = Arrays.stream(values())
            .collect(Collectors.toMap(state -> state.name.toUpperCase(Locale.ROOT), Function.identity()));

    private final String name;

    GroupState(String name) {
        this.name = name;
    }

    /**
     * Case-insensitive group state lookup by string name.
     */
    public static GroupState parse(String name) {
        GroupState state = NAME_TO_ENUM.get(name.toUpperCase(Locale.ROOT));
        return state == null ? UNKNOWN : state;
    }

    public static Set<GroupState> groupStatesForType(GroupType type) {
        if (type == GroupType.CLASSIC) {
            return Set.of(PREPARING_REBALANCE, COMPLETING_REBALANCE, STABLE, DEAD, EMPTY);
        } else if (type == GroupType.CONSUMER) {
            return Set.of(PREPARING_REBALANCE, COMPLETING_REBALANCE, STABLE, DEAD, EMPTY, ASSIGNING, RECONCILING);
        } else if (type == GroupType.SHARE) {
            return Set.of(STABLE, DEAD, EMPTY);
        } else {
            throw new IllegalArgumentException("Group type not known");
        }
    }

    @Override
    public String toString() {
        return name;
    }
}
