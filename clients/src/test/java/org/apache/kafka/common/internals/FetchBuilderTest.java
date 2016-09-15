/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FetchBuilderTest {

    @Test
    public void testSet() {
        FetchBuilder<String> builder = new FetchBuilder<>();
        LinkedHashMap<TopicPartition, String> map = createMap();
        builder.set(map);
        LinkedHashMap<TopicPartition, String> expected = new LinkedHashMap<>();
        expected.put(new TopicPartition("foo", 2), "foo 2");
        expected.put(new TopicPartition("foo", 0), "foo 0");
        expected.put(new TopicPartition("blah", 2), "blah 2");
        expected.put(new TopicPartition("blah", 1), "blah 1");
        expected.put(new TopicPartition("baz", 2), "baz 2");
        expected.put(new TopicPartition("baz", 3), "baz 3");
        checkState(builder, expected);

        builder.set(new LinkedHashMap<TopicPartition, String>());
        checkState(builder, new LinkedHashMap<TopicPartition, String>());
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

    private void checkState(FetchBuilder<String> builder, LinkedHashMap<TopicPartition, String> expected) {
        assertEquals(expected.keySet(), builder.partitionSet());
        assertEquals(expected.size(), builder.size());
        List<FetchBuilder.PartitionState<String>> states = new ArrayList<>();
        for (Map.Entry<TopicPartition, String> entry : expected.entrySet()) {
            states.add(new FetchBuilder.PartitionState<>(entry.getKey(), entry.getValue()));
            assertTrue(builder.contains(entry.getKey()));
        }
        assertEquals(states, builder.partitionStates());
    }

    @Test
    public void testMoveToEnd() {
        FetchBuilder<String> builder = new FetchBuilder<>();
        LinkedHashMap<TopicPartition, String> map = createMap();
        builder.set(map);

        builder.moveToEnd(new TopicPartition("baz", 2));
        LinkedHashMap<TopicPartition, String> expected = new LinkedHashMap<>();
        expected.put(new TopicPartition("foo", 2), "foo 2");
        expected.put(new TopicPartition("foo", 0), "foo 0");
        expected.put(new TopicPartition("blah", 2), "blah 2");
        expected.put(new TopicPartition("blah", 1), "blah 1");
        expected.put(new TopicPartition("baz", 3), "baz 3");
        expected.put(new TopicPartition("baz", 2), "baz 2");
        checkState(builder, expected);

        builder.moveToEnd(new TopicPartition("foo", 2));
        expected = new LinkedHashMap<>();
        expected.put(new TopicPartition("foo", 0), "foo 0");
        expected.put(new TopicPartition("blah", 2), "blah 2");
        expected.put(new TopicPartition("blah", 1), "blah 1");
        expected.put(new TopicPartition("baz", 3), "baz 3");
        expected.put(new TopicPartition("baz", 2), "baz 2");
        expected.put(new TopicPartition("foo", 2), "foo 2");
        checkState(builder, expected);

        // no-op
        builder.moveToEnd(new TopicPartition("foo", 2));
        checkState(builder, expected);

        // partition doesn't exist
        builder.moveToEnd(new TopicPartition("baz", 5));
        checkState(builder, expected);

        // topic doesn't exist
        builder.moveToEnd(new TopicPartition("aaa", 2));
        checkState(builder, expected);
    }

    @Test
    public void testUpdateAndMoveToEnd() {
        FetchBuilder<String> builder = new FetchBuilder<>();
        LinkedHashMap<TopicPartition, String> map = createMap();
        builder.set(map);

        builder.updateAndMoveToEnd(new TopicPartition("foo", 0), "foo 0 updated");
        LinkedHashMap<TopicPartition, String> expected = new LinkedHashMap<>();
        expected.put(new TopicPartition("foo", 2), "foo 2");
        expected.put(new TopicPartition("blah", 2), "blah 2");
        expected.put(new TopicPartition("blah", 1), "blah 1");
        expected.put(new TopicPartition("baz", 2), "baz 2");
        expected.put(new TopicPartition("baz", 3), "baz 3");
        expected.put(new TopicPartition("foo", 0), "foo 0 updated");
        checkState(builder, expected);

        builder.updateAndMoveToEnd(new TopicPartition("baz", 2), "baz 2 updated");
        expected = new LinkedHashMap<>();
        expected.put(new TopicPartition("foo", 2), "foo 2");
        expected.put(new TopicPartition("blah", 2), "blah 2");
        expected.put(new TopicPartition("blah", 1), "blah 1");
        expected.put(new TopicPartition("baz", 3), "baz 3");
        expected.put(new TopicPartition("foo", 0), "foo 0 updated");
        expected.put(new TopicPartition("baz", 2), "baz 2 updated");
        checkState(builder, expected);

        // partition doesn't exist
        builder.updateAndMoveToEnd(new TopicPartition("baz", 5), "baz 5 new");
        expected = new LinkedHashMap<>();
        expected.put(new TopicPartition("foo", 2), "foo 2");
        expected.put(new TopicPartition("blah", 2), "blah 2");
        expected.put(new TopicPartition("blah", 1), "blah 1");
        expected.put(new TopicPartition("baz", 3), "baz 3");
        expected.put(new TopicPartition("foo", 0), "foo 0 updated");
        expected.put(new TopicPartition("baz", 2), "baz 2 updated");
        expected.put(new TopicPartition("baz", 5), "baz 5 new");
        checkState(builder, expected);

        // topic doesn't exist
        builder.updateAndMoveToEnd(new TopicPartition("aaa", 2), "aaa 2 new");
        expected = new LinkedHashMap<>();
        expected.put(new TopicPartition("foo", 2), "foo 2");
        expected.put(new TopicPartition("blah", 2), "blah 2");
        expected.put(new TopicPartition("blah", 1), "blah 1");
        expected.put(new TopicPartition("baz", 3), "baz 3");
        expected.put(new TopicPartition("foo", 0), "foo 0 updated");
        expected.put(new TopicPartition("baz", 2), "baz 2 updated");
        expected.put(new TopicPartition("baz", 5), "baz 5 new");
        expected.put(new TopicPartition("aaa", 2), "aaa 2 new");
        checkState(builder, expected);
    }

    @Test
    public void testPartitionValues() {
        FetchBuilder<String> builder = new FetchBuilder<>();
        LinkedHashMap<TopicPartition, String> map = createMap();
        builder.set(map);
        List<String> expected = new ArrayList<>();
        expected.add("foo 2");
        expected.add("foo 0");
        expected.add("blah 2");
        expected.add("blah 1");
        expected.add("baz 2");
        expected.add("baz 3");
        assertEquals(expected, builder.partitionStateValues());
    }

    @Test
    public void testClear() {
        FetchBuilder<String> builder = new FetchBuilder<>();
        LinkedHashMap<TopicPartition, String> map = createMap();
        builder.set(map);
        builder.clear();
        checkState(builder, new LinkedHashMap<TopicPartition, String>());
    }

    @Test
    public void testRemove() {
        FetchBuilder<String> builder = new FetchBuilder<>();
        LinkedHashMap<TopicPartition, String> map = createMap();
        builder.set(map);

        builder.remove(new TopicPartition("foo", 2));
        LinkedHashMap<TopicPartition, String> expected = new LinkedHashMap<>();
        expected.put(new TopicPartition("foo", 0), "foo 0");
        expected.put(new TopicPartition("blah", 2), "blah 2");
        expected.put(new TopicPartition("blah", 1), "blah 1");
        expected.put(new TopicPartition("baz", 2), "baz 2");
        expected.put(new TopicPartition("baz", 3), "baz 3");
        checkState(builder, expected);

        builder.remove(new TopicPartition("blah", 1));
        expected = new LinkedHashMap<>();
        expected.put(new TopicPartition("foo", 0), "foo 0");
        expected.put(new TopicPartition("blah", 2), "blah 2");
        expected.put(new TopicPartition("baz", 2), "baz 2");
        expected.put(new TopicPartition("baz", 3), "baz 3");
        checkState(builder, expected);

        builder.remove(new TopicPartition("baz", 3));
        expected = new LinkedHashMap<>();
        expected.put(new TopicPartition("foo", 0), "foo 0");
        expected.put(new TopicPartition("blah", 2), "blah 2");
        expected.put(new TopicPartition("baz", 2), "baz 2");
        checkState(builder, expected);
    }

}
