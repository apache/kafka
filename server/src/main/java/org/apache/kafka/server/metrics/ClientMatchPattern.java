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
package org.apache.kafka.server.metrics;

/**
 * A compiled client-metrics match pattern, backed by RE2/J. java.util.regex is supported only as
 * a deprecated, temporary fallback for patterns that predate RE2/J validation. See
 * {@link #ofLegacy}.
 * <p>
 * RE2/J's {@code Pattern} and {@code java.util.regex.Pattern} share no common supertype, so this
 * interface gives both a uniform shape wherever a compiled match pattern is stored or evaluated.
 */
public interface ClientMatchPattern {

    boolean matches(String input);

    String pattern();

    static ClientMatchPattern ofRe2(com.google.re2j.Pattern pattern) {
        return new ClientMatchPattern() {
            @Override
            public boolean matches(String input) {
                return pattern.matcher(input).matches();
            }

            @Override
            public String pattern() {
                return pattern.pattern();
            }
        };
    }

    /**
     * Wraps a pattern compiled with the legacy java.util.regex engine, for a match pattern that
     * predates RE2/J validation and would otherwise be rejected (e.g. one relying on backreferences
     * or lookaround). This fallback support is temporary and will be removed in Apache Kafka 5.0.
     */
    @Deprecated(since = "4.5", forRemoval = true)
    static ClientMatchPattern ofLegacy(java.util.regex.Pattern pattern) {
        return new ClientMatchPattern() {
            @Override
            public boolean matches(String input) {
                return pattern.matcher(input).matches();
            }

            @Override
            public String pattern() {
                return pattern.pattern();
            }
        };
    }
}
