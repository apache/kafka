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

package org.apache.kafka.metadata;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class LeaderAndIsrTest {
    @Test
    public void testRecoveringLeaderAndIsr() {
        LeaderAndIsr leaderAndIsr = new LeaderAndIsr(1, Arrays.asList(1, 2));
        LeaderAndIsr recoveringLeaderAndIsr = leaderAndIsr.newRecoveringLeaderAndIsr(3, Collections.singletonList(3));

        assertEquals(3, recoveringLeaderAndIsr.leader());
        assertEquals(Collections.singletonList(3), recoveringLeaderAndIsr.isr());
        assertEquals(LeaderRecoveryState.RECOVERING, recoveringLeaderAndIsr.leaderRecoveryState());
    }

    @Test
    public void testNewLeaderAndIsr() {
        LeaderAndIsr leaderAndIsr = new LeaderAndIsr(1, Arrays.asList(1, 2));
        LeaderAndIsr newLeaderAndIsr = leaderAndIsr.newLeaderAndIsr(2, Arrays.asList(1, 2));

        assertEquals(2, newLeaderAndIsr.leader());
        assertEquals(Arrays.asList(1, 2), newLeaderAndIsr.isr());
        assertEquals(LeaderRecoveryState.RECOVERED, newLeaderAndIsr.leaderRecoveryState());
    }

    @Test
    public void testNewLeader() {
        LeaderAndIsr leaderAndIsr = new LeaderAndIsr(2, Arrays.asList(1, 2, 3));

        assertEquals(2, leaderAndIsr.leader());
        assertEquals(Arrays.asList(1, 2, 3), leaderAndIsr.isr());

        LeaderAndIsr newLeaderAndIsr = leaderAndIsr.newLeader(3);

        assertEquals(3, newLeaderAndIsr.leader());
        assertEquals(Arrays.asList(1, 2, 3), newLeaderAndIsr.isr());
    }

    @Test
    public void testNewEpoch() {
        LeaderAndIsr leaderAndIsr = new LeaderAndIsr(3, Arrays.asList(1, 2, 3));

        assertEquals(0, leaderAndIsr.leaderEpoch());

        LeaderAndIsr leaderWithNewEpoch = leaderAndIsr.newEpoch();

        assertEquals(1, leaderWithNewEpoch.leaderEpoch());
    }

    @Test
    public void testLeaderOpt() {
        LeaderAndIsr leaderAndIsr = new LeaderAndIsr(2, Arrays.asList(1, 2, 3));

        assertEquals(2, leaderAndIsr.leaderOpt().orElse(0));
    }
}
