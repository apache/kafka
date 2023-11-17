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
package org.apache.kafka.common.internals;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PartitionStatesTest {

    @Test
    public void testSet() {
        PartitionStates<String> states = new PartitionStates<>();
        LinkedHashMap<TopicPartition, String> map = createMap();
        states.set(map);
        LinkedHashMap<TopicPartition, String> expected = new LinkedHashMap<>();
        expected.put(new TopicPartition("foo", 2), "foo 2");
        expected.put(new TopicPartition("foo", 0), "foo 0");
        expected.put(new TopicPartition("blah", 2), "blah 2");
        expected.put(new TopicPartition("blah", 1), "blah 1");
        expected.put(new TopicPartition("baz", 2), "baz 2");
        expected.put(new TopicPartition("baz", 3), "baz 3");
        checkState(states, expected);

        states.set(new LinkedHashMap<>());
        checkState(states, new LinkedHashMap<>());
    }

    private LinkedHashMap<TopicPartition, String> createMap() {
        LinkedHashMap<TopicPartition, String> map = new LinkedHashMap<>();
        map.put(new TopicPartition("foo", 2), "foo 2");
        map.put(new TopicPartition("blah", 2), "blah 2");
        map.put(new TopicPartition("blah", 1), "blah 1");
        map.put(new TopicPartition("baz", 2), "baz 2");
        map.put(new TopicPartition("foo", 0), "foo 0");
        map.put(new TopicPartition("baz", 3), "baz 3");
        return map;
    }

    private void checkState(PartitionStates<String> states, LinkedHashMap<TopicPartition, String> expected) {
        assertEquals(expected.keySet(), states.partitionSet());
        assertEquals(expected.size(), states.size());
        assertEquals(expected, states.partitionStateMap());
    }

    @Test
    public void testMoveToEnd() {
        PartitionStates<String> states = new PartitionStates<>();
        LinkedHashMap<TopicPartition, String> map = createMap();
        states.set(map);

        states.moveToEnd(new TopicPartition("baz", 2));
        LinkedHashMap<TopicPartition, String> expected = new LinkedHashMap<>();
        expected.put(new TopicPartition("foo", 2), "foo 2");
        expected.put(new TopicPartition("foo", 0), "foo 0");
        expected.put(new TopicPartition("blah", 2), "blah 2");
        expected.put(new TopicPartition("blah", 1), "blah 1");
        expected.put(new TopicPartition("baz", 3), "baz 3");
        expected.put(new TopicPartition("baz", 2), "baz 2");
        checkState(states, expected);

        states.moveToEnd(new TopicPartition("foo", 2));
        expected = new LinkedHashMap<>();
        expected.put(new TopicPartition("foo", 0), "foo 0");
        expected.put(new TopicPartition("blah", 2), "blah 2");
        expected.put(new TopicPartition("blah", 1), "blah 1");
        expected.put(new TopicPartition("baz", 3), "baz 3");
        expected.put(new TopicPartition("baz", 2), "baz 2");
        expected.put(new TopicPartition("foo", 2), "foo 2");
        checkState(states, expected);

        // no-op
        states.moveToEnd(new TopicPartition("foo", 2));
        checkState(states, expected);

        // partition doesn't exist
        states.moveToEnd(new TopicPartition("baz", 5));
        checkState(states, expected);

        // topic doesn't exist
        states.moveToEnd(new TopicPartition("aaa", 2));
        checkState(states, expected);
    }

    @Test
    public void testUpdateAndMoveToEnd() {
        PartitionStates<String> states = new PartitionStates<>();
        LinkedHashMap<TopicPartition, String> map = createMap();
        states.set(map);

        states.updateAndMoveToEnd(new TopicPartition("foo", 0), "foo 0 updated");
        LinkedHashMap<TopicPartition, String> expected = new LinkedHashMap<>();
        expected.put(new TopicPartition("foo", 2), "foo 2");
        expected.put(new TopicPartition("blah", 2), "blah 2");
        expected.put(new TopicPartition("blah", 1), "blah 1");
        expected.put(new TopicPartition("baz", 2), "baz 2");
        expected.put(new TopicPartition("baz", 3), "baz 3");
        expected.put(new TopicPartition("foo", 0), "foo 0 updated");
        checkState(states, expected);

        states.updateAndMoveToEnd(new TopicPartition("baz", 2), "baz 2 updated");
        expected = new LinkedHashMap<>();
        expected.put(new TopicPartition("foo", 2), "foo 2");
        expected.put(new TopicPartition("blah", 2), "blah 2");
        expected.put(new TopicPartition("blah", 1), "blah 1");
        expected.put(new TopicPartition("baz", 3), "baz 3");
        expected.put(new TopicPartition("foo", 0), "foo 0 updated");
        expected.put(new TopicPartition("baz", 2), "baz 2 updated");
        checkState(states, expected);

        // partition doesn't exist
        states.updateAndMoveToEnd(new TopicPartition("baz", 5), "baz 5 new");
        expected = new LinkedHashMap<>();
        expected.put(new TopicPartition("foo", 2), "foo 2");
        expected.put(new TopicPartition("blah", 2), "blah 2");
        expected.put(new TopicPartition("blah", 1), "blah 1");
        expected.put(new TopicPartition("baz", 3), "baz 3");
        expected.put(new TopicPartition("foo", 0), "foo 0 updated");
        expected.put(new TopicPartition("baz", 2), "baz 2 updated");
        expected.put(new TopicPartition("baz", 5), "baz 5 new");
        checkState(states, expected);

        // topic doesn't exist
        states.updateAndMoveToEnd(new TopicPartition("aaa", 2), "aaa 2 new");
        expected = new LinkedHashMap<>();
        expected.put(new TopicPartition("foo", 2), "foo 2");
        expected.put(new TopicPartition("blah", 2), "blah 2");
        expected.put(new TopicPartition("blah", 1), "blah 1");
        expected.put(new TopicPartition("baz", 3), "baz 3");
        expected.put(new TopicPartition("foo", 0), "foo 0 updated");
        expected.put(new TopicPartition("baz", 2), "baz 2 updated");
        expected.put(new TopicPartition("baz", 5), "baz 5 new");
        expected.put(new TopicPartition("aaa", 2), "aaa 2 new");
        checkState(states, expected);
    }

    @Test
    public void testPartitionValues() {
        PartitionStates<String> states = new PartitionStates<>();
        LinkedHashMap<TopicPartition, String> map = createMap();
        states.set(map);
        List<String> expected = new ArrayList<>();
        expected.add("foo 2");
        expected.add("foo 0");
        expected.add("blah 2");
        expected.add("blah 1");
        expected.add("baz 2");
        expected.add("baz 3");
        assertEquals(expected, states.partitionStateValues());
    }

    @Test
    public void testClear() {
        PartitionStates<String> states = new PartitionStates<>();
        LinkedHashMap<TopicPartition, String> map = createMap();
        states.set(map);
        states.clear();
        checkState(states, new LinkedHashMap<>());
    }

    @Test
    public void testRemove() {
        PartitionStates<String> states = new PartitionStates<>();
        LinkedHashMap<TopicPartition, String> map = createMap();
        states.set(map);

        states.remove(new TopicPartition("foo", 2));
        LinkedHashMap<TopicPartition, String> expected = new LinkedHashMap<>();
        expected.put(new TopicPartition("foo", 0), "foo 0");
        expected.put(new TopicPartition("blah", 2), "blah 2");
        expected.put(new TopicPartition("blah", 1), "blah 1");
        expected.put(new TopicPartition("baz", 2), "baz 2");
        expected.put(new TopicPartition("baz", 3), "baz 3");
        checkState(states, expected);

        states.remove(new TopicPartition("blah", 1));
        expected = new LinkedHashMap<>();
        expected.put(new TopicPartition("foo", 0), "foo 0");
        expected.put(new TopicPartition("blah", 2), "blah 2");
        expected.put(new TopicPartition("baz", 2), "baz 2");
        expected.put(new TopicPartition("baz", 3), "baz 3");
        checkState(states, expected);

        states.remove(new TopicPartition("baz", 3));
        expected = new LinkedHashMap<>();
        expected.put(new TopicPartition("foo", 0), "foo 0");
        expected.put(new TopicPartition("blah", 2), "blah 2");
        expected.put(new TopicPartition("baz", 2), "baz 2");
        checkState(states, expected);
    }

}
