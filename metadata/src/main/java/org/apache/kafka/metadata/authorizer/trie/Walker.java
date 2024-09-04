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
 * An object that walks the Trie in specific order applying a predicate.
 * <p>
 *     The predicates are intended to perform some action on the node and return true if the walk should continue, false
 *     otherwise.
 *
 *     As an example a predicate that counted nodes in the trie could be implemented as follows:
 *
 *     <pre>
 *          final int[] count = new int[] { 0 };
 *          Predicate<Node<T>> pred = n -> {
 *              count[0]++;
 *              return true;
 *         }
 *     </pre>
 * </p>
 *
 * @see <a href='https://en.wikipedia.org/wiki/Tree_traversal'>Tree Traversal [Wikipedia]</a>
 * @param <T> the data type for the Nodes.
 */
public class Walker<T> {

    /**
     * Applies the predicate to each node in a depth-first fashion.
     * If the predicate returns true the walker will stop and return the node.
     * @param predicate the Predicate to apply.
     * @param data the Node to start at.
     * @return The node on which the predicate returned {@code true} or null if that did not occur.
     * @param <T> the data type for the Nodes.
     */
    static <T> Node<T> depthFirst(Predicate<Node<T>> predicate, Node<T> data) {
        if (data == null) {
            return null;
        }
        if (data.getChildren() != null) {
            for (Node<T> child : data.getChildren()) {
                Node<T> candidate = depthFirst(predicate, child);
                if (candidate != null)
                    return candidate;
            }
        }
        return predicate.test(data) ? data : null;
    }

    /**
     * Applies the predicate to each node in a depth-first fashion.
     * If the predicate returns true the walker will stop and return the node.
     * @param predicate the Predicate to apply.
     * @param data the Trie to search
     * @return The node on which the predicate returned {@code true} or null if that did not occur.
     * @param <T> the data type for the Nodes.
     */
    public static <T> Matcher.SearchResult<T>  depthFirst(Predicate<Node<T>> predicate, Trie<T> data) {
        return new Matcher.SearchResult<>(data == null ? null : depthFirst(predicate, data.getRoot()));
    }

    /**
     * Applies the predicate to each node in a pre-order fashion.
     * If the predicate returns true the walker will stop and return the node.
     * @param predicate the Predicate to apply.
     * @param data the Node to start at.
     * @return The node on which the predicate returned {@code true} or null if that did not occur.
     * @param <T> the data type for the Nodes.
     */
    static <T> Node<T> preOrder(Predicate<Node<T>> predicate, Node<T> data) {
        if (data != null) {
            if (predicate.test(data)) {
                return data;
            }
            if (data.getChildren() != null) {
                for (Node<T> child : data.getChildren()) {
                    Node<T> candidate = preOrder(predicate, child);
                    if (candidate != null) {
                        return candidate;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Applies the predicate to each node in a pre-order fashion.
     * If the predicate returns true the walker will stop and return the node.
     * @param predicate the Predicate to apply.
     * @param data the trie to search.
     * @return The node on which the predicate returned {@code true} or null if that did not occur.
     * @param <T> the data type for the Nodes.
     */
    public static <T> Matcher.SearchResult<T> preOrder(Predicate<Node<T>> predicate, Trie<T> data) {
        return new Matcher.SearchResult<>(data == null ? null : preOrder(predicate, data.getRoot()));
    }

    /**
     * Executes a Traverser on the specified Trie.
     * @param trie the trie to execute on.
     * @param traverser the Traverser to use.
     * @return the traverser.
     * @param <T> the data type for the Nodes.
     * @param <W> the implementation of the Traverser
     */
    public static <T, W extends Traverser<T>> W traverse(Trie<T> trie, W traverser) {
        traverser.traverse(trie.getRoot());
        return traverser;
    }

    public Walker() {
    }

    /**
     * create a standard inserter for the node data type.
     * @param pattern the pattern to insert.
     * @return the Inserter that will insert the pattern.
     */
    public Inserter inserter(String pattern) {
        return new StandardInserter(pattern);
    }

    /**
     * Constructs a Matcher to find the pattern.
     * @param pattern the pattern to locate
     * @return A matcher that will perform the matching.
     */
    public Matcher<T> matcher(String pattern) {
        return new StandardMatcher<>(pattern);
    }

    /**
     * Constructs a matcher to find the pattern with an early exist check.
     * @param pattern the pattern to locate
     * @param exit the early exit check.
     * @return A matcher that will perform the matching.
     */
    public Matcher<T> matcher(String pattern, Predicate<NodeData<T>> exit) {
        return new StandardMatcher<>(pattern, exit);
    }
}
