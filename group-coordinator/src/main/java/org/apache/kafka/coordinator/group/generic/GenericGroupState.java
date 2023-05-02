/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.coordinator.group.generic;

import java.util.Set;

/**
 * Java rewrite of {@link kafka.coordinator.group.GroupState} that is used
 * by the new group coordinator (KIP-848).
 */
public enum GenericGroupState {
    Empty(),
    PreparingRebalance(),
    CompletingRebalance(),
    Stable(),
    Dead();

    private Set<GenericGroupState> validPreviousStates;

    static {
        Empty.addValidPreviousStates(PreparingRebalance);
        PreparingRebalance.addValidPreviousStates(Stable, CompletingRebalance, Empty);
        CompletingRebalance.addValidPreviousStates(PreparingRebalance);
        Stable.addValidPreviousStates(CompletingRebalance);
        Dead.addValidPreviousStates(Stable, PreparingRebalance, CompletingRebalance, Empty, Dead);
    }

    private void addValidPreviousStates(GenericGroupState... validPreviousStates) {
        this.validPreviousStates = Set.of(validPreviousStates);
    }

    public Set<GenericGroupState> validPreviousStates() {
        return this.validPreviousStates;
    }
}
