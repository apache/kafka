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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ShareConsumeBenchSpecTest {

    @Test
    public void testExpandTopicNames() {
        ShareConsumeBenchSpec shareConsumeBenchSpec = shareConsumeBenchSpec(Arrays.asList("foo[1-3]", "bar"));
        Set<String> expectedNames = new HashSet<>();

        expectedNames.add("foo1");
        expectedNames.add("foo2");
        expectedNames.add("foo3");
        expectedNames.add("bar");

        assertEquals(expectedNames, shareConsumeBenchSpec.expandTopicNames());
    }

    @Test
    public void testInvalidNameRaisesException() {
        for (String invalidName : Arrays.asList("In:valid", "invalid:", ":invalid[]", "in:valid:", "invalid[1-3]:")) {
            assertThrows(IllegalArgumentException.class, () -> shareConsumeBenchSpec(Collections.singletonList(invalidName)).expandTopicNames());
        }
    }

    private ShareConsumeBenchSpec shareConsumeBenchSpec(List<String> activeTopics) {
        return new ShareConsumeBenchSpec(0, 0, "node", "localhost",
                123, 1234, "sg-1",
                Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), 1,
                Optional.empty(), activeTopics);
    }

}