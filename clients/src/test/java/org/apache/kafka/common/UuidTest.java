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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

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

    @Test
    public void testRandomUuid() {
        Uuid randomID = Uuid.randomUuid();
        // reservedSentinel is based on the value of SENTINEL_ID_INTERNAL in Uuid.
        Uuid reservedSentinel = new Uuid(0L, 1L);

        assertNotEquals(randomID, Uuid.ZERO_UUID);
        assertNotEquals(randomID, reservedSentinel);
    }
}
