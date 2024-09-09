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

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * The Trie is a Radix Trie structure indexed by strings.
 * @param <T> the type of object held in the trie.
 */
public class Trie<T>  {
    /** the root node */
    private Node<T> root;

    /**
     * Constructor.
     * Creates an empty tree.
     */
    public Trie() {
        root = Node.makeRoot();
    }

    /**
     * Clear the contents of the trie.
     */
    public void clear() {
        root = Node.makeRoot();
    }

    /**
     * Gets the root.
     * @return Returns root node of the trie
     */
    Node<T> getRoot() {
        return root;
    }

    /**
     * Gets the object from the trie.
     * @param matcher the matcher to search with..
     * @return The stored object or {@code null} if not found.
     */
    public ReadOnlyNode<T> search(Matcher<T> matcher) {
        return matcher.searchIn(root).asReadOnlyNode();
    }

    /**
     * Inserts data into the trie, merging existing data as necessary.
     *
     * @param inserter the Inserter
     * @param value the value to place on the node.
     * @param remappingFunction Function to merge value into existing value.  Existing value will be the first parameter.
     */
    public void insert(Inserter inserter, T value, BiFunction<T, T, T> remappingFunction) {
        inserter.insertIn(root).mergeContents(value, remappingFunction);
    }

    /**
     * Inserts data into the trie, overwriting any existing value.
     *
     * @param inserter the Inserter
     * @param value the value to place on the node.
     */
    public void insert(Inserter inserter, T value) {
        inserter.insertIn(root).mergeContents(value, (x, y) -> y);
    }


    /**
     * Remove an object from the trie.
     * @param matcher that matches the node to be removed.
     * @param remappingFunction If the function passing the current value as the argument.  Result is the value for
     *                          the node.
     */
    public void remove(Matcher<T> matcher, Function<T, T> remappingFunction) {
        matcher.searchIn(root).removeContents(remappingFunction);
    }
}
