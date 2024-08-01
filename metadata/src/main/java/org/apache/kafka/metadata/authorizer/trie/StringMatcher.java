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
package org.apache.kafka.metadata.authorizer.trie;

import java.util.function.Predicate;

/**
 * A {@link Matcher} for String patterns
 * @param <T> the data type stored in the Trie
 */
public class StringMatcher<T> implements Matcher<T> {
    /** The base of this string matcher */
    private final StringMatcherBase<T> base;

    /** The position within the pattern that this matcher is matching */
    private final int position;

    /**
     * Constructs a matcher from the pattern and a predicate to detect early exit conditions.
     * @param pattern The pattern to match.
     * @param exit the Node Predicate that when {@code  true} causes the match to terminate.
     */
    public StringMatcher(String pattern, Predicate<Node<T>> exit) {
        this(new StringMatcherBase<>(pattern, exit), 0);
    }

    /**
     * Constructs a matcher from a StringMatcherBase and a new position
     * @param base the base for the matcher.
     * @param position the new position within the base.
     */
    private StringMatcher(StringMatcherBase<T> base, int position) {
        this.base = base;
        this.position = position;
    }

    @Override
    public String getFragment() {
        return this.base.pattern.substring(position);
    }

    @Override
    public StringMatcher<T> advance(int advance) {
        return new StringMatcher<>(base, position + advance);
    }

    @Override
    public boolean test(Node<T> node) {
        return base.exit.test(node);
    }

    /**
     * The base for a string matcher.  This class maintains the pattern and exit condition across all matchers built on
     * the same pattern.
     * @param <T> the data type stored in the trie.
     */
    private static class StringMatcherBase<T> {
        /** The pattern */
        private final String pattern;
        /** The Node Predicate that determines an early exit */
        private final Predicate<Node<T>> exit;

        private StringMatcherBase(String pattern, Predicate<Node<T>> exit) {
            this.pattern = pattern;
            this.exit = exit;
        }
    }
}
