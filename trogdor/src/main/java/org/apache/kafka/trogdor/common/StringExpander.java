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

import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utilities for expanding strings that have range expressions in them.
 *
 * For example, 'foo[1-3]' would be expaneded to foo1, foo2, foo3.
 * Strings that have no range expressions will not be expanded.
 */
public class StringExpander {
    private final static Pattern NUMERIC_RANGE_PATTERN =
        Pattern.compile("(.*)\\[([0-9]*)\\-([0-9]*)\\](.*)");

    public static HashSet<String> expand(String val) {
        HashSet<String> set = new HashSet<>();
        Matcher matcher = NUMERIC_RANGE_PATTERN.matcher(val);
        if (!matcher.matches()) {
            set.add(val);
            return set;
        }
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
}
