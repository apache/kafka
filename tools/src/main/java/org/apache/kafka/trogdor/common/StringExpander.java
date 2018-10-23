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

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utilities for expanding strings that have range expressions in them.
 */
public class StringExpander {
    private final static Pattern NUMERIC_RANGE_PATTERN =
        Pattern.compile("(.*?)\\[([0-9]*)\\-([0-9]*)\\](.*?)");

    public static class ExpandedResult {
        private String parsedResult;
        private Set<String> expandedResult;
        private List<Integer> range;

        /**
         * The result of an expansion on a string
         *
         * Given that the string "foo[5-7]" was expanded, its result would be:
         * @param parsedResult - the parsed string - "foo"
         * @param expandedResult - the result of the expansion - {"foo5", "foo6", "foo7"}
         * @param range - the range of the expansions - [5, 6, 7]
         */
        ExpandedResult(String parsedResult, Set<String> expandedResult, List<Integer> range) {
            this.parsedResult = parsedResult;
            this.expandedResult = expandedResult;
            this.range = range;
        }

        public Set<String> expandedResult() {
            return expandedResult;
        }

        public String parsedResult() {
            return parsedResult;
        }

        public List<Integer> range() {
            return range;
        }
    }

    /**
     * Returns a boolean indicating whether this string can be expanded
     * via the methods #{@link StringExpander#expand(String)} and #{@link StringExpander#expandIntoMap(String)}
     */
    public static boolean canExpand(String val) {
        return NUMERIC_RANGE_PATTERN.matcher(val).matches();
    }

    /**
     * Expands a string with a range in it.
     *
     * It is important to check if the passed string can be expanded via @{{@link #canExpand(String)}}
     *
     * 'foo[1-3]' would be expanded to foo1, foo2, foo3.
     *
     * @throws IllegalArgumentException if the string cannot be expanded.
     */
    public static ExpandedResult expand(String val) throws IllegalArgumentException  {
        Matcher matcher = NUMERIC_RANGE_PATTERN.matcher(val);
        if (!matcher.matches())
            throw new IllegalArgumentException(String.format("Cannot expand string %s", val));
        HashSet<String> set = new HashSet<>();
        List<Integer> range = new ArrayList<>();

        String prequel = matcher.group(1);
        String rangeStart = matcher.group(2);
        String rangeEnd = matcher.group(3);
        String epilog = matcher.group(4);
        int rangeStartInt = Integer.parseInt(rangeStart);
        int rangeEndInt = Integer.parseInt(rangeEnd);
        if (rangeEndInt < rangeStartInt) {
            throw new RuntimeException("Invalid range: start " + rangeStartInt +
                    " is higher than end " + rangeEndInt);
        }
        for (int i = rangeStartInt; i <= rangeEndInt; i++) {
            range.add(i);
            set.add(String.format("%s%d%s", prequel, i, epilog));
        }
        return new ExpandedResult(prequel + epilog, set, range);
    }

    /**
     * Expands a string with two ranges in it into a map.
     *
     * 'foo[1-3][1-3]` => { foo1: [1,2,3], foo2: [1,2,3], foo3: [1,2,3] }
     * `foo[1-3]` => { foo1: [], foo2: [], foo3: [] }
     *
     * It is important to check if the passed string can be expanded via @{{@link #canExpand(String)}}
     *
     * @throws IllegalArgumentException if the string cannot be expanded.
     */
    public static Map<String, List<Integer>> expandIntoMap(String val) throws IllegalArgumentException {
        Map<String, List<Integer>> expandedMap = new HashMap<>();
        for (String range: expand(val).expandedResult()) {
            if (canExpand(range)) {
                ExpandedResult expandedRange = expand(range);
                expandedMap.put(expandedRange.parsedResult(), expandedRange.range());
            } else
                expandedMap.put(range, new ArrayList<>());
        }
        return expandedMap;
    }
}
