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

public class UUIDTest {

    @Test
    public void testSignificantBits() {
        UUID id = new UUID(34L, 98L);

        assertEquals(id.getMostSignificantBits(), 34L);
        assertEquals(id.getLeastSignificantBits(), 98L);
    }

    @Test
    public void testUUIDEquality() {
        UUID id1 = new UUID(12L, 13L);
        UUID id2 = new UUID(12L, 13L);
        UUID id3 = new UUID(24L, 38L);

        assertEquals(UUID.ZERO_UUID, UUID.ZERO_UUID);
        assertEquals(id1, id2);
        assertNotEquals(id1, id3);

        assertEquals(UUID.ZERO_UUID.hashCode(), UUID.ZERO_UUID.hashCode());
        assertEquals(id1.hashCode(), id2.hashCode());
        assertNotEquals(id1.hashCode(), id3.hashCode());
    }

    @Test
    public void testStringConversion() {
        UUID id = UUID.randomUUID();
        String idString = id.toString();

        assertEquals(UUID.fromString(idString), id);

        String zeroIdString = UUID.ZERO_UUID.toString();

        assertEquals(UUID.fromString(zeroIdString), UUID.ZERO_UUID);
    }

    @Test
    public void testRandomUUID() {
        UUID randomID = UUID.randomUUID();
        // reservedSentinel is based on the value of SENTINEL_ID_INTERNAL in UUID.
        UUID reservedSentinel = new UUID(0L, 1L);

        assertNotEquals(randomID, UUID.ZERO_UUID);
        assertNotEquals(randomID, reservedSentinel);
    }
}
