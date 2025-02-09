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
package org.apache.kafka.coordinator.group.modern;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UnionSetTest {
    @Test
    public void testSetsCannotBeNull() {
        assertThrows(NullPointerException.class, () -> new UnionSet<String>(Collections.emptySet(), null));
        assertThrows(NullPointerException.class, () -> new UnionSet<String>(null, Collections.emptySet()));
    }

    @Test
    public void testUnion() {
        UnionSet<Integer> union = new UnionSet<>(
            Set.of(1, 2, 3),
            Set.of(2, 3, 4, 5)
        );

        List<Integer> result = new ArrayList<>(union);
        result.sort(Integer::compareTo);

        assertEquals(List.of(1, 2, 3, 4, 5), result);
    }

    @Test
    public void testSize() {
        UnionSet<Integer> union = new UnionSet<>(
            Set.of(1, 2, 3),
            Set.of(2, 3, 4, 5)
        );

        assertEquals(5, union.size());
    }

    @Test
    public void testIsEmpty() {
        UnionSet<Integer> union = new UnionSet<>(
            Set.of(1, 2, 3),
            Set.of(2, 3, 4, 5)
        );

        assertFalse(union.isEmpty());

        union = new UnionSet<>(
            Set.of(1, 2, 3),
            Collections.emptySet()
        );

        assertFalse(union.isEmpty());

        union = new UnionSet<>(
            Collections.emptySet(),
            Set.of(2, 3, 4, 5)
        );

        assertFalse(union.isEmpty());

        union = new UnionSet<>(
            Collections.emptySet(),
            Collections.emptySet()
        );
        assertTrue(union.isEmpty());
    }

    @Test
    public void testContains() {
        UnionSet<Integer> union = new UnionSet<>(
            Set.of(1, 2, 3),
            Set.of(2, 3, 4, 5)
        );

        IntStream.range(1, 6).forEach(item -> assertTrue(union.contains(item)));

        assertFalse(union.contains(0));
        assertFalse(union.contains(6));
    }

    @Test
    public void testToArray() {
        UnionSet<Integer> union = new UnionSet<>(
            Set.of(1, 2, 3),
            Set.of(2, 3, 4, 5)
        );

        Object[] expected = {1, 2, 3, 4, 5};
        Object[] actual = union.toArray();
        Arrays.sort(actual);
        assertArrayEquals(expected, actual);
    }

    @Test
    public void testToArrayWithArrayParameter() {
        UnionSet<Integer> union = new UnionSet<>(
            Set.of(1, 2, 3),
            Set.of(2, 3, 4, 5)
        );

        Integer[] input = new Integer[5];
        Integer[] expected = {1, 2, 3, 4, 5};
        union.toArray(input);
        Arrays.sort(input);
        assertArrayEquals(expected, input);
    }

    @Test
    public void testEquals() {
        UnionSet<Integer> union = new UnionSet<>(
            Set.of(1, 2, 3),
            Set.of(2, 3, 4, 5)
        );

        assertEquals(Set.of(1, 2, 3, 4, 5), union);
    }
}
