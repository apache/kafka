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

package org.apache.kafka.trogdor.common;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Test;

import java.time.ZoneOffset;
import java.util.Arrays;

import static org.apache.kafka.trogdor.common.StringFormatter.durationString;
import static org.apache.kafka.trogdor.common.StringFormatter.dateString;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(value = 120)
public class StringFormatterTest {

    @Test
    public void testDateString() {
        assertEquals("2019-01-08T20:59:29.85Z", dateString(1546981169850L, ZoneOffset.UTC));
    }

    @Test
    public void testDurationString() {
        assertEquals("1m", durationString(60000));
        assertEquals("1m1s", durationString(61000));
        assertEquals("1m1s", durationString(61200));
        assertEquals("5s", durationString(5000));
        assertEquals("2h", durationString(7200000));
        assertEquals("2h1s", durationString(7201000));
        assertEquals("2h5m3s", durationString(7503000));
    }

    @Test
    public void testPrettyPrintGrid() {
        assertEquals(String.format(
                "ANIMAL  NUMBER INDEX %n" +
                "lion    1      12345 %n" +
                "manatee 50     1     %n"),
            StringFormatter.prettyPrintGrid(
                Arrays.asList(Arrays.asList("ANIMAL", "NUMBER", "INDEX"),
                    Arrays.asList("lion", "1", "12345"),
                    Arrays.asList("manatee", "50", "1"))));
    }
}
