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
package org.apache.kafka.utils;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class CoreUtilsTest {
    @Test
    public void testCsvMap() {
        String emptyString = "";
        Map<String, String> emptyMap = CoreUtils.parseCsvMap(emptyString);
        Map<String, String> emptyStringMap = new HashMap<>();
        assertTrue(emptyMap != null);
        assertTrue(emptyStringMap.equals(emptyStringMap));

        String kvPairsIpV6 = "a:b:c:v,a:b:c:v";
        Map<String, String> ipv6Map = CoreUtils.parseCsvMap(kvPairsIpV6);
        ipv6Map.forEach((k, v) -> {
            assertTrue(k.equals("a:b:c"));
            assertTrue(v.equals("v"));
        });

        String singleEntry = "key:value";
        Map<String, String> singleMap = CoreUtils.parseCsvMap(singleEntry);
        String value = singleMap.getOrDefault("key", "0");
        assertTrue(value.equals("value"));

        String kvPairsIpV4 = "192.168.2.1/30:allow, 192.168.2.1/30:allow";
        Map<String, String> ipv4Map = CoreUtils.parseCsvMap(kvPairsIpV4);

        ipv4Map.forEach((k, v) -> {
            assertTrue(k.equals("192.168.2.1/30"));
            assertTrue(v.equals("allow"));
        });

        String kvPairsSpaces = "key:value      , key:   value";
        Map<String, String> spaceMap = CoreUtils.parseCsvMap(kvPairsSpaces);
        spaceMap.forEach((k, v) -> {
            assertTrue(k.equals("key"));
            assertTrue(v.equals("value"));
        });
    }

}
