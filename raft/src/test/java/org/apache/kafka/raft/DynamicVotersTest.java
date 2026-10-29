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

import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DynamicVotersTest {
    @Test
    public void testParsingEmptyStringFails() {
        assertEquals("No voters given.",
            assertThrows(IllegalArgumentException.class,
                () -> DynamicVoters.parse("")).
                    getMessage());
    }

    @Test
    public void testParsingSingleDynamicVoter() {
        assertEquals(new DynamicVoters(List.of(
            new DynamicVoter(
                Uuid.fromString("K90IZ-0DRNazJ49kCZ1EMQ"),
                2,
                "localhost",
                (short) 8020))),
            DynamicVoters.parse("2@localhost:8020:K90IZ-0DRNazJ49kCZ1EMQ"));
    }

    @Test
    public void testParsingThreeDynamicVoters() {
        assertEquals(new DynamicVoters(List.of(
            new DynamicVoter(
                Uuid.fromString("K90IZ-0DRNazJ49kCZ1EMQ"),
                0,
                "localhost",
                (short) 8020),
            new DynamicVoter(
                Uuid.fromString("aUARLskQTCW4qCZDtS_cwA"),
                1,
                "localhost",
                (short) 8030),
            new DynamicVoter(
                Uuid.fromString("2ggvsS4kQb-fSJ_-zC_Ang"),
                2,
                "localhost",
                (short) 8040))),
            DynamicVoters.parse(
                "0@localhost:8020:K90IZ-0DRNazJ49kCZ1EMQ," +
                "1@localhost:8030:aUARLskQTCW4qCZDtS_cwA," +
                "2@localhost:8040:2ggvsS4kQb-fSJ_-zC_Ang"));
    }

    @Test
    public void testParsingInvalidStringWithDuplicateNodeIds() {
        assertEquals("Node id 1 was specified more than once.",
            assertThrows(IllegalArgumentException.class,
                () -> DynamicVoters.parse(
                    "0@localhost:8020:K90IZ-0DRNazJ49kCZ1EMQ," +
                    "1@localhost:8030:aUARLskQTCW4qCZDtS_cwA," +
                    "1@localhost:8040:2ggvsS4kQb-fSJ_-zC_Ang")).
                        getMessage());
    }

    private static void testRoundTrip(String input) {
        DynamicVoters voters = DynamicVoters.parse(input);
        assertEquals(input, voters.toString());
    }

    @Test
    public void testRoundTripSingleVoter() {
        testRoundTrip("2@localhost:8020:K90IZ-0DRNazJ49kCZ1EMQ");
    }

    @Test
    public void testRoundTripThreeVoters() {
        testRoundTrip(
            "0@localhost:8020:K90IZ-0DRNazJ49kCZ1EMQ," +
            "1@localhost:8030:aUARLskQTCW4qCZDtS_cwA," +
            "2@localhost:8040:2ggvsS4kQb-fSJ_-zC_Ang");
    }

    @Test
    public void testToVoterSet() {
        Map<Integer, VoterSet.VoterNode> voterMap = new HashMap<>();
        voterMap.put(0, DynamicVoter.parse(
            "0@localhost:8020:K90IZ-0DRNazJ49kCZ1EMQ").toVoterNode("CONTROLLER2"));
        voterMap.put(1, DynamicVoter.parse(
            "1@localhost:8030:aUARLskQTCW4qCZDtS_cwA").toVoterNode("CONTROLLER2"));
        voterMap.put(2, DynamicVoter.parse(
            "2@localhost:8040:2ggvsS4kQb-fSJ_-zC_Ang").toVoterNode("CONTROLLER2"));
        assertEquals(VoterSet.fromMap(voterMap),
            DynamicVoters.parse(
                "0@localhost:8020:K90IZ-0DRNazJ49kCZ1EMQ," +
                "1@localhost:8030:aUARLskQTCW4qCZDtS_cwA," +
                "2@localhost:8040:2ggvsS4kQb-fSJ_-zC_Ang").toVoterSet("CONTROLLER2"));
    }
}
