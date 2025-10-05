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
package org.apache.kafka.server.util;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CsvTest {

    @Test
    public void testCsvMap() {
        Map<String, String> emptyMap = Csv.parseCsvMap("");
        assertEquals(Map.of(), emptyMap);

        String kvPairsIpV6 = "a:b:c:v,a:b:c:v";
        Map<String, String> ipv6Map = Csv.parseCsvMap(kvPairsIpV6);
        for (Map.Entry<String, String> entry : ipv6Map.entrySet()) {
            assertEquals("a:b:c", entry.getKey());
            assertEquals("v", entry.getValue());
        }

        String singleEntry = "key:value";
        Map<String, String> singleMap = Csv.parseCsvMap(singleEntry);
        String value = singleMap.get("key");
        assertEquals("value", value);

        String kvPairsIpV4 = "192.168.2.1/30:allow, 192.168.2.1/30:allow";
        Map<String, String> ipv4Map = Csv.parseCsvMap(kvPairsIpV4);
        for (Map.Entry<String, String> entry : ipv4Map.entrySet()) {
            assertEquals("192.168.2.1/30", entry.getKey());
            assertEquals("allow", entry.getValue());
        }

        String kvPairsSpaces = "key:value      , key:   value";
        Map<String, String> spaceMap = Csv.parseCsvMap(kvPairsSpaces);
        for (Map.Entry<String, String> entry : spaceMap.entrySet()) {
            assertEquals("key", entry.getKey());
            assertEquals("value", entry.getValue());
        }
    }

    @Test
    public void testCsvList() {
        List<String> emptyList = Csv.parseCsvList("");
        assertEquals(List.of(), emptyList);

        List<String> emptyListFromNullString = Csv.parseCsvList(null);
        assertEquals(List.of(), emptyListFromNullString);

        List<String> csvList = Csv.parseCsvList("a,b ,c, d,,e,");
        assertEquals(List.of("a", "b", "c", "d", "e"), csvList);
    }
}
