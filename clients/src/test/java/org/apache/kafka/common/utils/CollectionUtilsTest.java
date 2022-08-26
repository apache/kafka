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
package org.apache.kafka.common.utils;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.utils.CollectionUtils.subtractMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotSame;

public class CollectionUtilsTest {

    @Test
    public void testSubtractMapRemovesSecondMapsKeys() {
        Map<String, String> mainMap = new HashMap<>();
        mainMap.put("one", "1");
        mainMap.put("two", "2");
        mainMap.put("three", "3");
        Map<String, String> secondaryMap = new HashMap<>();
        secondaryMap.put("one", "4");
        secondaryMap.put("two", "5");

        Map<String, String> newMap = subtractMap(mainMap, secondaryMap);

        assertEquals(3, mainMap.size());  // original map should not be modified
        assertEquals(1, newMap.size());
        assertTrue(newMap.containsKey("three"));
        assertEquals("3", newMap.get("three"));
    }

    @Test
    public void testSubtractMapDoesntRemoveAnythingWhenEmptyMap() {
        Map<String, String> mainMap = new HashMap<>();
        mainMap.put("one", "1");
        mainMap.put("two", "2");
        mainMap.put("three", "3");
        Map<String, String> secondaryMap = new HashMap<>();

        Map<String, String> newMap = subtractMap(mainMap, secondaryMap);

        assertEquals(3, newMap.size());
        assertEquals("1", newMap.get("one"));
        assertEquals("2", newMap.get("two"));
        assertEquals("3", newMap.get("three"));
        assertNotSame(newMap, mainMap);
    }
}
