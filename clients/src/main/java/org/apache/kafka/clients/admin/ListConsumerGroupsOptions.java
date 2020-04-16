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

package org.apache.kafka.clients.admin;

import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * Options for {@link Admin#listConsumerGroups()}.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class ListConsumerGroupsOptions extends AbstractOptions<ListConsumerGroupsOptions> {

    private Optional<Set<ConsumerGroupState>> states = Optional.empty();

    /**
     * Only groups in these states will be returned by listConsumerGroups()
     * If not set, all groups are returned without their states
     * throw IllegalArgumentException if states is empty
     */
    public ListConsumerGroupsOptions inStates(Set<ConsumerGroupState> states) {
        if (states == null || states.isEmpty()) {
            throw new IllegalArgumentException("states should not be null or empty");
        }
        this.states = Optional.of(states);
        return this;
    }

    /**
     * All groups with their states will be returned by listConsumerGroups()
     */
    public ListConsumerGroupsOptions inAnyState() {
        this.states = Optional.of(EnumSet.allOf(ConsumerGroupState.class));
        return this;
    }

    /**
     * Returns the list of States that are requested
     */
    public Optional<Set<ConsumerGroupState>> states() {
        return states;
    }
}
