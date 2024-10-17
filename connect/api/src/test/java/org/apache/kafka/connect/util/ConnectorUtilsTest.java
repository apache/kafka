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
package org.apache.kafka.connect.util;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConnectorUtilsTest {

    private static final List<Integer> FIVE_ELEMENTS = Arrays.asList(1, 2, 3, 4, 5);

    @Test
    public void testGroupPartitions() {

        List<List<Integer>> grouped = ConnectorUtils.groupPartitions(FIVE_ELEMENTS, 1);
        assertEquals(Collections.singletonList(FIVE_ELEMENTS), grouped);

        grouped = ConnectorUtils.groupPartitions(FIVE_ELEMENTS, 2);
        assertEquals(Arrays.asList(Arrays.asList(1, 2, 3), Arrays.asList(4, 5)), grouped);

        grouped = ConnectorUtils.groupPartitions(FIVE_ELEMENTS, 3);
        assertEquals(Arrays.asList(Arrays.asList(1, 2),
                Arrays.asList(3, 4),
                Collections.singletonList(5)), grouped);

        grouped = ConnectorUtils.groupPartitions(FIVE_ELEMENTS, 5);
        assertEquals(Arrays.asList(Collections.singletonList(1),
                Collections.singletonList(2),
                Collections.singletonList(3),
                Collections.singletonList(4),
                Collections.singletonList(5)), grouped);

        grouped = ConnectorUtils.groupPartitions(FIVE_ELEMENTS, 7);
        assertEquals(Arrays.asList(Collections.singletonList(1),
                Collections.singletonList(2),
                Collections.singletonList(3),
                Collections.singletonList(4),
                Collections.singletonList(5),
                Collections.emptyList(),
                Collections.emptyList()), grouped);
    }

    @Test
    public void testGroupPartitionsInvalidCount() {
        assertThrows(IllegalArgumentException.class,
                () -> ConnectorUtils.groupPartitions(FIVE_ELEMENTS, 0));
    }

    @Test
    public void testGroupElementsRoundRobin() {
        List<List<Integer>> grouped = ConnectorUtils.groupElementsRoundRobin(FIVE_ELEMENTS, 1);
        assertEquals(Collections.singletonList(FIVE_ELEMENTS), grouped);

        grouped = ConnectorUtils.groupElementsRoundRobin(FIVE_ELEMENTS, 2);
        assertEquals(Arrays.asList(Arrays.asList(1, 3, 5), Arrays.asList(2, 4)), grouped);

        grouped = ConnectorUtils.groupElementsRoundRobin(FIVE_ELEMENTS, 3);
        assertEquals(Arrays.asList(Arrays.asList(1, 4), Arrays.asList(2, 5), Collections.singletonList(3)), grouped);

        grouped = ConnectorUtils.groupElementsRoundRobin(FIVE_ELEMENTS, 5);
        assertEquals(Arrays.asList(Collections.singletonList(1),
                Collections.singletonList(2),
                Collections.singletonList(3),
                Collections.singletonList(4),
                Collections.singletonList(5)), grouped);

        grouped = ConnectorUtils.groupElementsRoundRobin(FIVE_ELEMENTS, 7);
        assertEquals(Arrays.asList(Collections.singletonList(1),
                Collections.singletonList(2),
                Collections.singletonList(3),
                Collections.singletonList(4),
                Collections.singletonList(5),
                Collections.emptyList(),
                Collections.emptyList()), grouped);
    }

    @Test
    public void testGroupElementsRoundRobinInvalidArgument() {
        assertThrows(IllegalArgumentException.class,
                () -> ConnectorUtils.groupElementsRoundRobin(FIVE_ELEMENTS, 0));
        assertThrows(IllegalArgumentException.class,
                () -> ConnectorUtils.groupElementsRoundRobin(null, 1));
    }
}
