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
package org.apache.kafka.server.mirror;

import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MirrorPartitionTest {
    private static final Uuid TOPIC_ID = Uuid.randomUuid();
    private static final String MIRROR_NAME = "my-mirror";
    private static final int PARTITION = 5;

    // -- Construction --

    @Test
    void testConstruction() {
        MirrorPartition mp = new MirrorPartition(MIRROR_NAME, TOPIC_ID, PARTITION);
        assertEquals(MIRROR_NAME, mp.mirrorName());
        assertEquals(TOPIC_ID, mp.topicId());
        assertEquals(PARTITION, mp.partition());
    }

    @Test
    void testOfFactory() {
        MirrorPartition mp = MirrorPartition.of(MIRROR_NAME, TOPIC_ID, PARTITION);
        assertEquals(new MirrorPartition(MIRROR_NAME, TOPIC_ID, PARTITION), mp);
    }

    @Test
    void testNullMirrorName() {
        assertThrows(NullPointerException.class,
            () -> new MirrorPartition(null, TOPIC_ID, PARTITION));
    }

    @Test
    void testNullTopicId() {
        assertThrows(NullPointerException.class,
            () -> new MirrorPartition(MIRROR_NAME, null, PARTITION));
    }

    // -- Mirror name validation --

    @ParameterizedTest
    @MethodSource("invalidMirrorNames")
    void testMirrorNameRejected(String name) {
        assertThrows(IllegalArgumentException.class,
            () -> new MirrorPartition(name, TOPIC_ID, PARTITION));
    }

    private static Stream<String> invalidMirrorNames() {
        return Stream.of(
            "",
            ".",
            "..",
            "a".repeat(250),
            "has space",
            "no@sign",
            "no!bang",
            "no:colon",
            "no/slash",
            "tab\there"
        );
    }

    @Test
    void testMirrorNameAtMaxLength() {
        String maxName = "a".repeat(249);
        MirrorPartition mp = new MirrorPartition(maxName, TOPIC_ID, PARTITION);
        assertEquals(maxName, mp.mirrorName());
    }

    @ParameterizedTest
    @ValueSource(strings = {"simple", "with.dot", "with_underscore", "with-dash", "MiXeD.CaSe_01-23", "a", "Z"})
    void testMirrorNameAccepted(String name) {
        MirrorPartition mp = new MirrorPartition(name, TOPIC_ID, PARTITION);
        assertEquals(name, mp.mirrorName());
    }

    // -- Record equality and hashCode --

    @Test
    void testEqualInstances() {
        MirrorPartition a = new MirrorPartition(MIRROR_NAME, TOPIC_ID, PARTITION);
        MirrorPartition b = new MirrorPartition(MIRROR_NAME, TOPIC_ID, PARTITION);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void testDifferentMirrorName() {
        MirrorPartition a = new MirrorPartition("mirror-a", TOPIC_ID, PARTITION);
        MirrorPartition b = new MirrorPartition("mirror-b", TOPIC_ID, PARTITION);
        assertNotEquals(a, b);
    }

    @Test
    void testDifferentTopicId() {
        MirrorPartition a = new MirrorPartition(MIRROR_NAME, TOPIC_ID, PARTITION);
        MirrorPartition b = new MirrorPartition(MIRROR_NAME, Uuid.randomUuid(), PARTITION);
        assertNotEquals(a, b);
    }

    @Test
    void testDifferentPartition() {
        MirrorPartition a = new MirrorPartition(MIRROR_NAME, TOPIC_ID, 0);
        MirrorPartition b = new MirrorPartition(MIRROR_NAME, TOPIC_ID, 1);
        assertNotEquals(a, b);
    }

    // -- toString and fromString --

    @Test
    void testToString() {
        MirrorPartition mp = new MirrorPartition(MIRROR_NAME, TOPIC_ID, PARTITION);
        assertEquals(MIRROR_NAME + ":" + TOPIC_ID + ":" + PARTITION, mp.toString());
    }

    @Test
    void testFromStringRoundTrip() {
        MirrorPartition original = new MirrorPartition(MIRROR_NAME, TOPIC_ID, PARTITION);
        assertEquals(original, MirrorPartition.fromString(original.toString()));
    }

    @Test
    void testFromStringNullKey() {
        assertThrows(NullPointerException.class,
            () -> MirrorPartition.fromString(null));
    }

    @ParameterizedTest
    @MethodSource("invalidFromStringKeys")
    void testFromStringRejected(String key) {
        assertThrows(IllegalArgumentException.class,
            () -> MirrorPartition.fromString(key));
    }

    private static Stream<String> invalidFromStringKeys() {
        return Stream.of(
            "",
            "single",
            "only:two",
            ":" + TOPIC_ID + ":0",
            "mirror:not-a-uuid:0",
            "mirror:" + TOPIC_ID + ":abc",
            "bad name:" + TOPIC_ID + ":0"
        );
    }

    @Test
    void testNegativePartition() {
        assertThrows(IllegalArgumentException.class,
            () -> new MirrorPartition(MIRROR_NAME, TOPIC_ID, -1));
    }

    @Test
    void testFromStringNegativePartition() {
        assertThrows(IllegalArgumentException.class,
            () -> MirrorPartition.fromString("mirror:" + TOPIC_ID + ":-1"));
    }

    // -- coordinatingPartition --

    @Test
    void testCoordinatingPartitionInRange() {
        MirrorPartition mp = new MirrorPartition(MIRROR_NAME, TOPIC_ID, PARTITION);
        for (int count = 1; count <= 64; count++) {
            int result = mp.coordinatingPartition(count);
            assertTrue(result >= 0 && result < count,
                "Expected [0, " + count + ") but got " + result);
        }
    }

    @Test
    void testCoordinatingPartitionDeterministic() {
        MirrorPartition mp = new MirrorPartition(MIRROR_NAME, TOPIC_ID, PARTITION);
        assertEquals(mp.coordinatingPartition(50), mp.coordinatingPartition(50));
    }
}
