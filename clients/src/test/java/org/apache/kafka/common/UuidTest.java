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

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class UuidTest {

    @Test
    public void testSignificantBits() {
        Uuid id = new Uuid(34L, 98L);

        assertEquals(id.getMostSignificantBits(), 34L);
        assertEquals(id.getLeastSignificantBits(), 98L);
    }

    @Test
    public void testUuidEquality() {
        Uuid id1 = new Uuid(12L, 13L);
        Uuid id2 = new Uuid(12L, 13L);
        Uuid id3 = new Uuid(24L, 38L);

        assertEquals(Uuid.ZERO_UUID, Uuid.ZERO_UUID);
        assertEquals(id1, id2);
        assertNotEquals(id1, id3);

        assertEquals(Uuid.ZERO_UUID.hashCode(), Uuid.ZERO_UUID.hashCode());
        assertEquals(id1.hashCode(), id2.hashCode());
        assertNotEquals(id1.hashCode(), id3.hashCode());
    }
    
    @Test
    public void testHashCode() {
        Uuid id1 = new Uuid(16L, 7L);
        Uuid id2 = new Uuid(1043L, 20075L);
        Uuid id3 = new Uuid(104312423523523L, 200732425676585L);
        
        assertEquals(23, id1.hashCode());
        assertEquals(19064, id2.hashCode());
        assertEquals(-2011255899, id3.hashCode());
    }

    @Test
    public void testStringConversion() {
        Uuid id = Uuid.randomUuid();
        String idString = id.toString();

        assertEquals(Uuid.fromString(idString), id);

        String zeroIdString = Uuid.ZERO_UUID.toString();

        assertEquals(Uuid.fromString(zeroIdString), Uuid.ZERO_UUID);
    }

    @RepeatedTest(100)
    public void testRandomUuid() {
        Uuid randomID = Uuid.randomUuid();

        assertNotEquals(randomID, Uuid.ZERO_UUID);
        assertNotEquals(randomID, Uuid.METADATA_TOPIC_ID);
        assertFalse(randomID.toString().startsWith("-"));
    }

    @Test
    public void testCompareUuids() {
        Uuid id00 = new Uuid(0L, 0L);
        Uuid id01 = new Uuid(0L, 1L);
        Uuid id10 = new Uuid(1L, 0L);
        assertEquals(0, id00.compareTo(id00));
        assertEquals(0, id01.compareTo(id01));
        assertEquals(0, id10.compareTo(id10));
        assertEquals(-1, id00.compareTo(id01));
        assertEquals(-1, id00.compareTo(id10));
        assertEquals(1, id01.compareTo(id00));
        assertEquals(1, id10.compareTo(id00));
        assertEquals(-1, id01.compareTo(id10));
        assertEquals(1, id10.compareTo(id01));
    }

    @Test
    public void testFromStringWithInvalidInput() {
        String oversizeString = Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[32]);
        assertThrows(IllegalArgumentException.class, () -> Uuid.fromString(oversizeString));

        String undersizeString = Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[4]);
        assertThrows(IllegalArgumentException.class, () -> Uuid.fromString(undersizeString));
    }

    @Test
    void testToArray() {
        assertNull(Uuid.toArray(null));
        assertArrayEquals(
                new Uuid[]{
                    Uuid.ZERO_UUID, Uuid.fromString("UXyU9i5ARn6W00ON2taeWA")
                },
                Uuid.toArray(Arrays.asList(
                    Uuid.ZERO_UUID, Uuid.fromString("UXyU9i5ARn6W00ON2taeWA")
                ))
        );
    }

    @Test
    void testToList() {
        assertNull(Uuid.toList(null));
        assertEquals(
                Arrays.asList(
                    Uuid.ZERO_UUID, Uuid.fromString("UXyU9i5ARn6W00ON2taeWA")
                ),
                Uuid.toList(new Uuid[]{
                    Uuid.ZERO_UUID, Uuid.fromString("UXyU9i5ARn6W00ON2taeWA")
                })
        );
    }
}
