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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LeaderAndEpochTest {

    @Test
    void testConstructorWithValidLeaderId() {
        LeaderAndEpoch leaderAndEpoch = new LeaderAndEpoch(OptionalInt.of(1), 5);
        
        assertEquals(OptionalInt.of(1), leaderAndEpoch.leaderId());
        assertEquals(5, leaderAndEpoch.epoch());
    }

    @Test
    void testConstructorWithEmptyLeaderId() {
        LeaderAndEpoch leaderAndEpoch = new LeaderAndEpoch(OptionalInt.empty(), 3);
        
        assertEquals(OptionalInt.empty(), leaderAndEpoch.leaderId());
        assertEquals(3, leaderAndEpoch.epoch());
    }

    @Test
    void testConstructorThrowsExceptionWhenLeaderIdIsNull() {
        Executable executable = () -> new LeaderAndEpoch(null, 1);
        
        assertThrows(NullPointerException.class, executable);
    }
}
