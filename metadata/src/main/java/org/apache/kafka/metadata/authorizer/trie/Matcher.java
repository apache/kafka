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

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * An object that contains a fragment of an underlying pattern and is used to match a Node in a Trie.
 * @param <T> the data held on the nodes.
 */
public interface Matcher<T> extends Predicate<Node<T>>, FragmentHolder {

    /** A predicate to just execute a find without processing intermediate steps */
    static <T> Predicate<NodeData<T>> noExit() {
        return x -> false;
    }


    /**
     * Advance to the next matcher position in the underlying pattern.
     * @param advance The number of pattern elements to advance.  (e.g. for Strings this is the number of chars to advance in the pattern)
     * @return a new Matcher.  May <emp>NOT</emp> be the original instance modified.
     */
    Matcher<T> advance(int advance);

    default SearchResult<T> searchResult(Node<T> node) {
        return new SearchResult<>(node);
    }

    /**
     * Find the node in the trie.
     * @param node the node to start searching from.
     * @return the Node or @{code null} if no node was found.
     */
    SearchResult<T> searchIn(Node<T> node);

    class SearchResult<T> {
        /** The found node */
        private final Node<T> node;
        /** {@code true} if the node is an exact match. */

        SearchResult(Node<T> node) {
            this.node = node;
        }

        /**
         * Retrieve the internal Node representation.
         * NOTE: Visible for testing.
         * @return the internal Node, may be {@code null}
         */
        Node<T> getNode() {
            return node;
        }

        /**
         * Determines if the match result is a node that contains data.
         *
         * @return {@code true} if the {@code node} contains data..
         */
        public boolean hasContents() {
            return node != null && node.getContents() != null;
        }

        public T getContents() {
            return node != null ? node.getContents() : null;
        }

        public void removeContents(Function<T, T> remappingFunction) {
            if (node != null) {
                node.removeContents(remappingFunction);
            }
        }

        public String getName() {
            if (node == null) {
                return null;
            }
            StringBuilder sb = new StringBuilder();
            for (Node<T> node : node.pathTo()) {
                sb.append(node.getFragment());
            }
            return sb.toString();
        }

        public ReadOnlyNode<T> asReadOnlyNode() {
            return ReadOnlyNode.create(node);
        }
    }

    /**
     * The base for a string matcher.  This class maintains the pattern and exit condition across all matchers built on
     * the same pattern.
     * @param <T> the data type stored in the trie.
     */
    class StringMatcherBase<T> {
        /** The pattern */
        private final String pattern;
        /** The Node Predicate that determines an early exit */
        private final Predicate<NodeData<T>> exit;

        public StringMatcherBase(String pattern, Predicate<NodeData<T>> exit) {
            this.pattern = pattern;
            this.exit = exit;
        }

        public String getPattern() {
            return pattern;
        }

        public boolean shouldExit(NodeData<T> node) {
            return exit.test(node);
        }
    }
}
