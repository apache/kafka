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

package org.apache.kafka.server.config;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KafkaConfigTest {
    @Test
    public void testPopulateSynonymsOnEmptyMap() {
        assertEquals(Collections.emptyMap(), KafkaConfig.populateSynonyms(Collections.emptyMap()));
    }

    @Test
    public void testPopulateSynonymsOnMapWithoutNodeId() {
        Map<String, String> input =  new HashMap<>();
        input.put(KafkaConfig.BROKER_ID_PROP, "4");
        Map<String, String>  expectedOutput = new HashMap<>();
        expectedOutput.put(KafkaConfig.BROKER_ID_PROP, "4");
        expectedOutput.put(KafkaConfig.NODE_ID_PROP, "4");
        assertEquals(expectedOutput, KafkaConfig.populateSynonyms(input));
    }

    @Test
    public void testPopulateSynonymsOnMapWithoutBrokerId() {
        Map<String, String> input =  new HashMap<>();
        input.put(KafkaConfig.NODE_ID_PROP, "4");
        Map<String, String> expectedOutput = new HashMap<>();
        expectedOutput.put(KafkaConfig.BROKER_ID_PROP, "4");
        expectedOutput.put(KafkaConfig.NODE_ID_PROP, "4");
        assertEquals(expectedOutput, KafkaConfig.populateSynonyms(input));
    }

}
