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

import com.google.re2j.Pattern;

import java.util.List;
import java.util.stream.Collectors;

public final class MirrorUtils {
    private MirrorUtils() {}

    /**
     * Compiles a single regex pattern string into a {@link Pattern}.
     *
     * @param pattern the regex pattern string
     * @return a compiled Pattern, or null if the input is null or blank
     */
    public static Pattern compilePattern(String pattern) {
        if (pattern == null) return null;
        String trimmed = pattern.trim();
        return trimmed.isEmpty() ? null : Pattern.compile(trimmed);
    }

    /**
     * Compiles a list of regex pattern strings into a single {@link Pattern} by joining them with {@code |}.
     *
     * @param patterns the list of regex pattern strings
     * @return a compiled Pattern that matches any of the given patterns, or null if no non-empty patterns
     */
    public static Pattern compilePatternList(List<String> patterns) {
        String combined = patterns.stream()
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.joining("|"));
        return compilePattern(combined.isEmpty() ? null : "^(" + combined + ")$");
    }
}
