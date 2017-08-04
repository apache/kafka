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
package org.apache.kafka.common.requests;

import java.util.List;

// This class contains the common fields shared between LeaderAndIsrRequest.PartitionState and UpdateMetadataRequest.PartitionState
public class BasePartitionState {

    public final int controllerEpoch;
    public final int leader;
    public final int leaderEpoch;
    public final List<Integer> isr;
    public final int zkVersion;
    public final List<Integer> replicas;

    BasePartitionState(int controllerEpoch, int leader, int leaderEpoch, List<Integer> isr, int zkVersion, List<Integer> replicas) {
        this.controllerEpoch = controllerEpoch;
        this.leader = leader;
        this.leaderEpoch = leaderEpoch;
        this.isr = isr;
        this.zkVersion = zkVersion;
        this.replicas = replicas;
    }

}
