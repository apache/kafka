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
 * The standard matching strategy.
 * <ol>
 *     <li>While descending the tree each node is checked for the early exit match.</li>
 *     <li>Descent stops at <ul>
 *         <li>an exact match for the pattern</li>
 *         <li>a leaf node</li>
 *         </ul></li>
 * </ol>
 * @param <T> the data type stored in the Trie
 */
public abstract class AbstractMatcher<T> implements Matcher<T> {
    /** The base of this string matcher */
    private final StringMatcherBase<T> base;

    /** The position within the pattern that this matcher is matching */
    private final int position;
    /**
     * Constructs a matcher from the pattern and a predicate to detect early exit conditions.
     * @param pattern The pattern to match.
     */
    protected AbstractMatcher(String pattern) {
        this(new StringMatcherBase<T>(pattern, Matcher.noExit()), 0);
    }


    /**
     * Constructs a matcher from the pattern and a predicate to detect early exit conditions.
     * @param pattern The pattern to match.
     * @param exit the Node Predicate that when {@code  true} causes the match to terminate.
     */
    protected AbstractMatcher(String pattern, Predicate<NodeData<T>> exit) {
        this(new StringMatcherBase<>(pattern, exit), 0);
    }

    /**
     * Constructs a matcher from the current matcher and the
     * @param advance the number of characters to advance the new matcher.
     */
    protected AbstractMatcher(AbstractMatcher<T> existing, int advance) {
        this(existing.base, existing.position + advance);
    }


    /**
     * Constructs a matcher from a StringMatcherBase and a new position
     * @param base the base for the matcher.
     * @param position the new position within the base.
     */
    private AbstractMatcher(StringMatcherBase<T> base, int position) {
        this.base = base;
        this.position = position;
    }

    @Override
    public final String getFragment() {
        return this.base.getPattern().substring(position);
    }

    @Override
    public final boolean test(Node<T> node) {
        return base.shouldExit(node);
    }

    @Override
    public String toString() {
        return getFragment();
    }
}
