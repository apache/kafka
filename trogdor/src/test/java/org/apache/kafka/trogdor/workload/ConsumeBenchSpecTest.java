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

package org.apache.kafka.trogdor.workload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ConsumeBenchSpecTest {

    @Test
    public void testMaterializeTopicsWithNoPartitions() {
        Map<String, List<TopicPartition>> materializedTopics = consumeBenchSpec(Arrays.asList("topic[1-3]", "secondTopic")).materializeTopics();
        Map<String, List<TopicPartition>> expected = new HashMap<>();
        expected.put("topic1", new ArrayList<>());
        expected.put("topic2", new ArrayList<>());
        expected.put("topic3", new ArrayList<>());
        expected.put("secondTopic", new ArrayList<>());

        assertEquals(expected, materializedTopics);
    }

    @Test
    public void testMaterializeTopicsWithSomePartitions() {
        Map<String, List<TopicPartition>> materializedTopics = consumeBenchSpec(Arrays.asList("topic[1-3]:[1-5]", "secondTopic", "thirdTopic:1")).materializeTopics();
        Map<String, List<TopicPartition>> expected = new HashMap<>();
        expected.put("topic1", IntStream.range(1, 6).asLongStream().mapToObj(i -> new TopicPartition("topic1", (int) i)).collect(Collectors.toList()));
        expected.put("topic2", IntStream.range(1, 6).asLongStream().mapToObj(i -> new TopicPartition("topic2", (int) i)).collect(Collectors.toList()));
        expected.put("topic3", IntStream.range(1, 6).asLongStream().mapToObj(i -> new TopicPartition("topic3", (int) i)).collect(Collectors.toList()));
        expected.put("secondTopic", new ArrayList<>());
        expected.put("thirdTopic", Collections.singletonList(new TopicPartition("thirdTopic", 1)));

        assertEquals(expected, materializedTopics);
    }

    @Test
    public void testInvalidTopicNameRaisesExceptionInMaterialize() {
        for (String invalidName : Arrays.asList("In:valid", "invalid:", ":invalid", "in:valid:1", "invalid:2:2", "invalid::1", "invalid[1-3]:")) {
            assertThrows(IllegalArgumentException.class, () -> consumeBenchSpec(Collections.singletonList(invalidName)).materializeTopics());
        }
    }

    private ConsumeBenchSpec consumeBenchSpec(List<String> activeTopics) {
        return new ConsumeBenchSpec(0, 0, "node", "localhost",
            123, 1234, "cg-1",
            Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), 1,
            Optional.empty(), activeTopics);
    }
}
