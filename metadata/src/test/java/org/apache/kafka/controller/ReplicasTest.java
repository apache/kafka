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

package org.apache.kafka.controller;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(40)
public class ReplicasTest {
    @Test
    public void testToList() {
        assertEquals(Arrays.asList(1, 2, 3, 4), Replicas.toList(new int[] {1, 2, 3, 4}));
        assertEquals(Arrays.asList(), Replicas.toList(Replicas.NONE));
        assertEquals(Arrays.asList(2), Replicas.toList(new int[] {2}));
    }

    @Test
    public void testToArray() {
        assertArrayEquals(new int[] {3, 2, 1}, Replicas.toArray(Arrays.asList(3, 2, 1)));
        assertArrayEquals(new int[] {}, Replicas.toArray(Arrays.asList()));
        assertArrayEquals(new int[] {2}, Replicas.toArray(Arrays.asList(2)));
    }

    @Test
    public void testClone() {
        assertArrayEquals(new int[]{3, 2, 1}, Replicas.clone(new int[]{3, 2, 1}));
        assertArrayEquals(new int[]{}, Replicas.clone(new int[]{}));
        assertArrayEquals(new int[]{2}, Replicas.clone(new int[]{2}));
    }

    @Test
    public void testValidate() {
        assertTrue(Replicas.validate(new int[] {}));
        assertTrue(Replicas.validate(new int[] {3}));
        assertTrue(Replicas.validate(new int[] {3, 1, 2, 6}));
        assertFalse(Replicas.validate(new int[] {3, 3}));
        assertFalse(Replicas.validate(new int[] {4, -1, 3}));
        assertFalse(Replicas.validate(new int[] {-1}));
        assertFalse(Replicas.validate(new int[] {3, 1, 2, 6, 1}));
        assertTrue(Replicas.validate(new int[] {1, 100}));
    }

    @Test
    public void testValidateIsr() {
        assertTrue(Replicas.validateIsr(new int[] {}, new int[] {}));
        assertTrue(Replicas.validateIsr(new int[] {1, 2, 3}, new int[] {}));
        assertTrue(Replicas.validateIsr(new int[] {1, 2, 3}, new int[] {1, 2, 3}));
        assertTrue(Replicas.validateIsr(new int[] {3, 1, 2}, new int[] {2, 1}));
        assertFalse(Replicas.validateIsr(new int[] {3, 1, 2}, new int[] {4, 1}));
        assertFalse(Replicas.validateIsr(new int[] {1, 2, 4}, new int[] {4, 4}));
    }

    @Test
    public void testContains() {
        assertTrue(Replicas.contains(new int[] {3, 0, 1}, 0));
        assertFalse(Replicas.contains(new int[] {}, 0));
        assertTrue(Replicas.contains(new int[] {1}, 1));
    }

    @Test
    public void testCopyWithout() {
        assertArrayEquals(new int[] {}, Replicas.copyWithout(new int[] {}, 0));
        assertArrayEquals(new int[] {}, Replicas.copyWithout(new int[] {1}, 1));
        assertArrayEquals(new int[] {1, 3}, Replicas.copyWithout(new int[] {1, 2, 3}, 2));
        assertArrayEquals(new int[] {4, 1}, Replicas.copyWithout(new int[] {4, 2, 2, 1}, 2));
    }

    @Test
    public void testCopyWith() {
        assertArrayEquals(new int[] {-1}, Replicas.copyWith(new int[] {}, -1));
        assertArrayEquals(new int[] {1, 2, 3, 4}, Replicas.copyWith(new int[] {1, 2, 3}, 4));
    }
}
