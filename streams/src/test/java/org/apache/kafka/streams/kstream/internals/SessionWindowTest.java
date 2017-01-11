/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.kafka.streams.kstream.internals;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SessionWindowTest {

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfEndSmallerThanStart() {
        new SessionWindow(1, 0);
    }

    @Test
    public void testOverlap() {
        final SessionWindow window = new SessionWindow(50, 100);

        assertFalse(window.overlap(new SessionWindow(0, 25)));
        assertFalse(window.overlap(new SessionWindow(0, 49)));
        assertTrue(window.overlap(new SessionWindow(0, 50)));
        assertTrue(window.overlap(new SessionWindow(0, 75)));
        assertTrue(window.overlap(new SessionWindow(0, 99)));
        assertTrue(window.overlap(new SessionWindow(0, 100)));
        assertTrue(window.overlap(new SessionWindow(0, 101)));
        assertTrue(window.overlap(new SessionWindow(0, 150)));

        assertFalse(window.overlap(new SessionWindow(49, 49)));
        assertTrue(window.overlap(new SessionWindow(49, 50)));
        assertTrue(window.overlap(new SessionWindow(49, 75)));
        assertTrue(window.overlap(new SessionWindow(49, 99)));
        assertTrue(window.overlap(new SessionWindow(49, 100)));
        assertTrue(window.overlap(new SessionWindow(49, 101)));
        assertTrue(window.overlap(new SessionWindow(49, 150)));

        assertTrue(window.overlap(new SessionWindow(50, 50)));
        assertTrue(window.overlap(new SessionWindow(50, 75)));
        assertTrue(window.overlap(new SessionWindow(50, 99)));
        assertTrue(window.overlap(new SessionWindow(50, 100)));
        assertTrue(window.overlap(new SessionWindow(50, 101)));
        assertTrue(window.overlap(new SessionWindow(50, 150)));

        assertTrue(window.overlap(new SessionWindow(75, 75)));
        assertTrue(window.overlap(new SessionWindow(75, 99)));
        assertTrue(window.overlap(new SessionWindow(75, 100)));
        assertTrue(window.overlap(new SessionWindow(75, 101)));
        assertTrue(window.overlap(new SessionWindow(75, 150)));

        assertTrue(window.overlap(new SessionWindow(100, 100)));
        assertTrue(window.overlap(new SessionWindow(100, 101)));
        assertTrue(window.overlap(new SessionWindow(100, 150)));

        assertFalse(window.overlap(new SessionWindow(101, 101)));
        assertFalse(window.overlap(new SessionWindow(101, 150)));

        assertFalse(window.overlap(new SessionWindow(125, 150)));
    }

    @Test
    public void testEquals() {
        assertTrue(new SessionWindow(5, 10).equals(new SessionWindow(5, 10)));
        assertFalse(new SessionWindow(5, 10).equals(new SessionWindow(0, 10)));
        assertFalse(new SessionWindow(7, 10).equals(new SessionWindow(0, 10)));
        assertFalse(new SessionWindow(5, 10).equals(new SessionWindow(5, 8)));
        assertFalse(new SessionWindow(5, 10).equals(new SessionWindow(5, 15)));
        assertFalse(new SessionWindow(5, 10).equals(new SessionWindow(0, 15)));
        assertFalse(new SessionWindow(5, 10).equals(new SessionWindow(7, 8)));
    }
}