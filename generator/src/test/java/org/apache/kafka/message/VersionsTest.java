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

package org.apache.kafka.message;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

@Timeout(120)
public class VersionsTest {

    private static Versions newVersions(int lower, int higher) {
        if ((lower < Short.MIN_VALUE) || (lower > Short.MAX_VALUE)) {
            throw new RuntimeException("lower bound out of range.");
        }
        if ((higher < Short.MIN_VALUE) || (higher > Short.MAX_VALUE)) {
            throw new RuntimeException("higher bound out of range.");
        }
        return new Versions((short) lower, (short) higher);
    }

    @Test
    public void testVersionsParse() {
        assertEquals(Versions.NONE, Versions.parse(null, Versions.NONE));
        assertEquals(Versions.ALL, Versions.parse(" ", Versions.ALL));
        assertEquals(Versions.ALL, Versions.parse("", Versions.ALL));
        assertEquals(newVersions(4, 5), Versions.parse(" 4-5 ", null));
    }

    @Test
    public void testRoundTrips() {
        testRoundTrip(Versions.ALL, "0+");
        testRoundTrip(newVersions(1, 3), "1-3");
        testRoundTrip(newVersions(2, 2), "2");
        testRoundTrip(newVersions(3, Short.MAX_VALUE), "3+");
        testRoundTrip(Versions.NONE, "none");
    }

    private void testRoundTrip(Versions versions, String string) {
        assertEquals(string, versions.toString());
        assertEquals(versions, Versions.parse(versions.toString(), null));
    }

    @Test
    public void testIntersections() {
        assertEquals(newVersions(2, 3), newVersions(1, 3).intersect(
                newVersions(2, 4)));
        assertEquals(newVersions(3, 3), newVersions(0, Short.MAX_VALUE).intersect(
                newVersions(3, 3)));
        assertEquals(Versions.NONE, newVersions(9, Short.MAX_VALUE).intersect(
                newVersions(2, 8)));
        assertEquals(Versions.NONE, Versions.NONE.intersect(Versions.NONE));
    }

    @Test
    public void testContains() {
        assertTrue(newVersions(2, 3).contains((short) 3));
        assertTrue(newVersions(2, 3).contains((short) 2));
        assertFalse(newVersions(0, 1).contains((short) 2));
        assertTrue(newVersions(0, Short.MAX_VALUE).contains((short) 100));
        assertFalse(newVersions(2, Short.MAX_VALUE).contains((short) 0));
        assertTrue(newVersions(2, 3).contains(newVersions(2, 3)));
        assertTrue(newVersions(2, 3).contains(newVersions(2, 2)));
        assertFalse(newVersions(2, 3).contains(newVersions(2, 4)));
        assertTrue(newVersions(2, 3).contains(Versions.NONE));
        assertTrue(Versions.ALL.contains(newVersions(1, 2)));
    }

    @Test
    public void testSubtract() {
        assertEquals(Versions.NONE, Versions.NONE.subtract(Versions.NONE));
        assertEquals(newVersions(0, 0),
            newVersions(0, 0).subtract(Versions.NONE));
        assertEquals(newVersions(1, 1),
            newVersions(1, 2).subtract(newVersions(2, 2)));
        assertEquals(newVersions(2, 2),
            newVersions(1, 2).subtract(newVersions(1, 1)));
        assertNull(newVersions(0, Short.MAX_VALUE).subtract(newVersions(1, 100)));
        assertEquals(newVersions(10, 10),
            newVersions(1, 10).subtract(newVersions(1, 9)));
        assertEquals(newVersions(1, 1),
            newVersions(1, 10).subtract(newVersions(2, 10)));
        assertEquals(newVersions(2, 4),
            newVersions(2, Short.MAX_VALUE).subtract(newVersions(5, Short.MAX_VALUE)));
        assertEquals(newVersions(5, Short.MAX_VALUE),
            newVersions(0, Short.MAX_VALUE).subtract(newVersions(0, 4)));
    }
}
