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
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 40)
public class VersionRangeTest {
    @SuppressWarnings("unchecked")
    private static VersionRange v(int a, int b) {
        assertTrue(a <= Short.MAX_VALUE);
        assertTrue(a >= Short.MIN_VALUE);
        assertTrue(b <= Short.MAX_VALUE);
        assertTrue(b >= Short.MIN_VALUE);
        return new VersionRange((short) a, (short) b);
    }

    @Test
    public void testEquality() {
        assertEquals(v(1, 1), v(1, 1));
        assertFalse(v(1, 1).equals(v(1, 2)));
        assertFalse(v(2, 1).equals(v(1, 2)));
        assertFalse(v(2, 1).equals(v(2, 2)));
    }

    @Test
    public void testContains() {
        assertTrue(v(1, 1).contains(v(1, 1)));
        assertFalse(v(1, 1).contains(v(1, 2)));
        assertTrue(v(1, 2).contains(v(1, 1)));
        assertFalse(v(4, 10).contains(v(3, 8)));
        assertTrue(v(2, 12).contains(v(3, 11)));
    }

    @Test
    public void testToString() {
        assertEquals("1-2", v(1, 2).toString());
        assertEquals("1", v(1, 1).toString());
        assertEquals("1+", v(1, Short.MAX_VALUE).toString());
        assertEquals("100-200", v(100, 200).toString());
    }
}
