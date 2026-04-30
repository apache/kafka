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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(value = 120)
public class StringExpanderTest {

    @Test
    public void testNoExpansionNeeded() {
        assertEquals(Set.of("foo"), StringExpander.expand("foo"));
        assertEquals(Set.of("bar"), StringExpander.expand("bar"));
        assertEquals(Set.of(""), StringExpander.expand(""));
    }

    @Test
    public void testExpansions() {
        Set<String> expected1 = Set.of(
            "foo1",
            "foo2",
            "foo3"
        );
        assertEquals(expected1, StringExpander.expand("foo[1-3]"));

        Set<String> expected2 = Set.of(
            "foo bar baz 0"
        );
        assertEquals(expected2, StringExpander.expand("foo bar baz [0-0]"));

        Set<String> expected3 = Set.of(
            "[[ wow50 ]]",
            "[[ wow51 ]]",
            "[[ wow52 ]]"
        );
        assertEquals(expected3, StringExpander.expand("[[ wow[50-52] ]]"));

        Set<String> expected4 = Set.of(
            "foo1bar",
            "foo2bar",
            "foo3bar"
        );
        assertEquals(expected4, StringExpander.expand("foo[1-3]bar"));

        // should expand latest range first
        Set<String> expected5 = Set.of(
            "start[1-3]middle1epilogue",
            "start[1-3]middle2epilogue",
            "start[1-3]middle3epilogue"
        );
        assertEquals(expected5, StringExpander.expand("start[1-3]middle[1-3]epilogue"));
    }
}
