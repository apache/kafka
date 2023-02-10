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

import java.util.Objects;
import java.util.OptionalInt;

public class LeaderAndEpoch {
    private final OptionalInt leaderId;
    private final int epoch;
    public static final LeaderAndEpoch UNKNOWN = new LeaderAndEpoch(OptionalInt.empty(), 0);

    public LeaderAndEpoch(OptionalInt leaderId, int epoch) {
        this.leaderId = Objects.requireNonNull(leaderId);
        this.epoch = epoch;
    }

    public OptionalInt leaderId() {
        return leaderId;
    }

    public int epoch() {
        return epoch;
    }

    public boolean isLeader(int nodeId) {
        return leaderId.isPresent() && leaderId.getAsInt() == nodeId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LeaderAndEpoch that = (LeaderAndEpoch) o;
        return epoch == that.epoch &&
            leaderId.equals(that.leaderId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(leaderId, epoch);
    }

    @Override
    public String toString() {
        return "LeaderAndEpoch(" +
            "leaderId=" + leaderId +
            ", epoch=" + epoch +
            ')';
    }
}
