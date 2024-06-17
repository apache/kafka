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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.trogdor.common.JsonUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 120)
public class TopicsSpecTest {

    private final static TopicsSpec FOO;
    private final static PartitionsSpec PARTSA;
    private final static PartitionsSpec PARTSB;

    static {
        FOO = new TopicsSpec();

        PARTSA = new PartitionsSpec(3, (short) 3, null, null);
        FOO.set("topicA[0-2]", PARTSA);

        Map<Integer, List<Integer>> assignmentsB = new HashMap<>();
        assignmentsB.put(0, Arrays.asList(0, 1, 2));
        assignmentsB.put(1, Arrays.asList(2, 3, 4));
        PARTSB = new PartitionsSpec(0, (short) 0, assignmentsB, null);
        FOO.set("topicB", PARTSB);
    }

    @Test
    public void testMaterialize() {
        Map<String, PartitionsSpec> parts = FOO.materialize();
        assertTrue(parts.containsKey("topicA0"));
        assertTrue(parts.containsKey("topicA1"));
        assertTrue(parts.containsKey("topicA2"));
        assertTrue(parts.containsKey("topicB"));
        assertEquals(4, parts.keySet().size());
        assertEquals(PARTSA, parts.get("topicA0"));
        assertEquals(PARTSA, parts.get("topicA1"));
        assertEquals(PARTSA, parts.get("topicA2"));
        assertEquals(PARTSB, parts.get("topicB"));
    }

    @Test
    public void testPartitionNumbers() {
        List<Integer> partsANumbers = PARTSA.partitionNumbers();
        assertEquals(Integer.valueOf(0), partsANumbers.get(0));
        assertEquals(Integer.valueOf(1), partsANumbers.get(1));
        assertEquals(Integer.valueOf(2), partsANumbers.get(2));
        assertEquals(3, partsANumbers.size());

        List<Integer> partsBNumbers = PARTSB.partitionNumbers();
        assertEquals(Integer.valueOf(0), partsBNumbers.get(0));
        assertEquals(Integer.valueOf(1), partsBNumbers.get(1));
        assertEquals(2, partsBNumbers.size());
    }

    @Test
    public void testPartitionsSpec() throws Exception {
        String text = "{\"numPartitions\": 5, \"configs\": {\"foo\": \"bar\"}}";
        PartitionsSpec spec = JsonUtil.JSON_SERDE.readValue(text, PartitionsSpec.class);
        assertEquals(5, spec.numPartitions());
        assertEquals("bar", spec.configs().get("foo"));
        assertEquals(1, spec.configs().size());
    }
}
