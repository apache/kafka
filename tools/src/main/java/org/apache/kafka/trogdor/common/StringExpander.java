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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Utilities for expanding strings that have range expressions in them.
 */
public class StringExpander {
    private final static Pattern NUMERIC_RANGE_PATTERN =
        Pattern.compile("(.*?)\\[([0-9]*)\\-([0-9]*)\\](.*?)");
    private final static Pattern NUMERIC_RANGE_VALUE_PATTERN =
        Pattern.compile("(.*?):(\\[([0-9]*)\\-([0-9]*)\\]|[0-9]+)$");

    /**
     * Returns a boolean indicating whether this string can be expanded
     * via the method #{@link StringExpander#expand(String)} and #{@link StringExpander#expandIntoMap(String)}
     */
    public static boolean canExpand(String val) {
        return NUMERIC_RANGE_PATTERN.matcher(val).matches();
    }

    /**
     * Returns a boolean indicating whether this string can be expanded
     * via the method #{@link StringExpander#expandIntoMap(String)}
     */
    public static boolean canExpandIntoMap(String val) {
        return NUMERIC_RANGE_PATTERN.matcher(val).matches() || NUMERIC_RANGE_VALUE_PATTERN.matcher(val).matches();
    }

    /**
     * Expands a string with a range in it.
     *
     * It is important to check if the passed string can be expanded via #{@link StringExpander#canExpand(String)}
     *
     * 'foo[1-3]' would be expanded to foo1, foo2, foo3.
     *
     * @throws IllegalArgumentException if the string cannot be expanded.
     */
    public static Set<String> expand(String val) throws IllegalArgumentException  {
        Matcher matcher = NUMERIC_RANGE_PATTERN.matcher(val);
        if (!matcher.matches())
            throw new IllegalArgumentException(String.format("Cannot expand string %s", val));
        HashSet<String> set = new HashSet<>();

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
            set.add(String.format("%s%d%s", prequel, i, epilog));
        }

        return set;
    }

    /**
     * Expands a key-value string into a map of the string and its integers.
     *
     * This method supports three types of notations:
     *  string[1-3] - denotes multiple strings, each with an unique suffix from the range
     *  string:[1-3] - denotes one string that corresponds to a list of integers in that range
     *  string:1 - denotes one string that corresponds to a list of one integer
     *
     * 'foo[1-3]' => { foo1: [], foo2: [], foo3: [] }
     * 'foo:1' => { foo: [1] }
     * 'foo:[1-3]' => { foo: [1, 2, 3] }
     * 'foo[1-3]:3' => { foo1: [3], foo2: [3], foo3: [3] }
     * 'foo[1-3]:[1-3]' => { foo1: [1,2,3], foo2: [1,2,3], foo3: [1,2,3] }
     *
     * It is important to check if the passed string can be expanded via #{@link StringExpander#canExpandIntoMap(String)}
     *
     * @throws IllegalArgumentException if the string cannot be expanded.
     */
    public static Map<String, List<Integer>> expandIntoMap(String val) throws IllegalArgumentException {
        if (!canExpandIntoMap(val))
            throw new IllegalArgumentException(String.format("Cannot expand string %s into a map", val));
        Map<String, List<Integer>> expandedMap = new HashMap<>();

        Set<String> keys = new HashSet<>();
        List<Integer> valueList;

        Matcher valueMatcher = NUMERIC_RANGE_VALUE_PATTERN.matcher(val);
        if (valueMatcher.matches()) {
            val = valueMatcher.group(1);
            String range = valueMatcher.group(2);
            if (!range.contains("-"))
                valueList = Collections.singletonList(Integer.parseInt(range));
            else {
                String rangeStart = valueMatcher.group(3);
                String rangeEnd = valueMatcher.group(4);
                int rangeStartInt = Integer.parseInt(rangeStart);
                int rangeEndInt = Integer.parseInt(rangeEnd);
                if (rangeEndInt < rangeStartInt) {
                    throw new RuntimeException("Invalid range for a value: start " + rangeStartInt +
                        " is higher than end " + rangeEndInt);
                }
                valueList = IntStream.range(rangeStartInt, rangeEndInt + 1).boxed().collect(Collectors.toList());
            }
        } else
            valueList = new ArrayList<>();

        if (canExpand(val)) {
            keys.addAll(expand(val));
        } else {
            keys.add(val);
        }

        for (String key : keys) {
            expandedMap.put(key, Collections.unmodifiableList(valueList));
        }

        return expandedMap;
    }
}
