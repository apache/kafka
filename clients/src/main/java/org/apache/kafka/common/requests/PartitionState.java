/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.kafka.common.requests;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class PartitionState {
    public final int controllerEpoch;
    public final int leader;
    public final int leaderEpoch;
    public final List<Integer> isr;
    public final int zkVersion;
    public final Set<Integer> replicas;

    public PartitionState(int controllerEpoch, int leader, int leaderEpoch, List<Integer> isr, int zkVersion, Set<Integer> replicas) {
        this.controllerEpoch = controllerEpoch;
        this.leader = leader;
        this.leaderEpoch = leaderEpoch;
        this.isr = isr;
        this.zkVersion = zkVersion;
        this.replicas = replicas;
    }

    @Override
    public String toString() {
        return "PartitionState(controllerEpoch=" + controllerEpoch +
                ", leader=" + leader +
                ", leaderEpoch=" + leaderEpoch +
                ", isr=" + Arrays.toString(isr.toArray()) +
                ", zkVersion=" + zkVersion +
                ", replicas=" + Arrays.toString(replicas.toArray()) + ")";
    }
}
