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

import java.util.SortedSet;

/**
 * An object that holds a fragment of the resource name.
 *
 */
@FunctionalInterface
interface FragmentHolder extends Comparable<FragmentHolder> {
    /**
     * Retrieves the fragment that this holder is holding.
     * @return the fragment this holder is holding.
     */
    String getFragment();

    default int compareTo(FragmentHolder other) {
        return getFragment().compareTo(other.getFragment());
    }

    /**
     * Finds the child node who's fragment matches the fragmentHolder.
     * @param node The node to searhc.
     * @return matching child node or {@code null} if none match.
     */
    default <T> Node<T> eq(Node<T> node) {
        SortedSet<Node<T>> children = node.getChildren();
        if (children != null) {
            Node<T> test = Node.makeRoot(getFragment());
            SortedSet<Node<T>> set = children.tailSet(test);
            if (!set.isEmpty()) {
                return compareTo(set.first()) == 0 ? set.first() : null;
            }
        }
        return null;
    }

    /**
     * Finds the child node that is less than but closest to the fragmentHolder.
     * @param node the node to search.
     * @return the nearest child less than the fragment or {@code null} if not found.
     */
    default <T> Node<T> lt(Node<T> node) {
        SortedSet<Node<T>> children = node.getChildren();
        if (children != null) {
            Node<T> test = Node.makeRoot(getFragment());
            SortedSet<Node<T>> set = children.headSet(test);
            return set.isEmpty() ? null : set.last();
        }
        return null;
    }
}
