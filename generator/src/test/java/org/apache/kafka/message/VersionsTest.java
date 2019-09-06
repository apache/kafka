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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class VersionsTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

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
        assertEquals(newVersions(2, 3),
            newVersions(1, 3).intersect(
                newVersions(2, 4)));
        assertEquals(newVersions(3, 3),
            newVersions(0, Short.MAX_VALUE).intersect(
                newVersions(3, 3)));
        assertEquals(Versions.NONE,
            newVersions(9, Short.MAX_VALUE).intersect(
                newVersions(2, 8)));
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
}
