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
public class StandardMatcher<T> extends AbstractMatcher<T> {

    /**
     * Constructs a matcher from the pattern and a predicate to detect early exit conditions.
     *
     * @param pattern The pattern to match.
     */
    public StandardMatcher(String pattern) {
        super(pattern);
    }

    /**
     * Constructs a matcher from the pattern and a predicate to detect early exit conditions.
     *
     * @param pattern The pattern to match.
     * @param exit    the Node Predicate that when {@code  true} causes the match to terminate.
     */
    public StandardMatcher(String pattern, Predicate<NodeData<T>> exit) {
        super(pattern, exit);
    }

    private StandardMatcher(StandardMatcher<T> existing, int advance) {
        super(existing, advance);
    }

    @Override
    public StandardMatcher<T> advance(int advance) {
        return new StandardMatcher<>(this, advance);
    }

    /**
     * Find a Node based on a Matcher.
     *
     * @param node the node from which to start searching.
     * @return The Node on which the find stopped, will be the "root" node if no match is found.
     */
    @Override
    public SearchResult<T> searchIn(final Node<T> node) {
        // this node is a match return.
        if (!test(node) && node.getChildren() != null) {
            // find exact(ish) match first.  Will also find tail wildcard.
            Node<T> candidate = eq(node);
            if (candidate != null) {
                return searchResult(candidate);
            }
            // find nodes lt matcher.  Navigate down the trie if there is a partial match.
            candidate = lt(node);
            if (candidate != null && getFragment().startsWith(candidate.getFragment())) {
                SearchResult<T> result = advance(candidate.getFragment().length()).searchIn(candidate);
                if (result.hasContents())
                    return result;
            }
        }
        // nothing below this node so return this node.
        return searchResult(node);
    }
}