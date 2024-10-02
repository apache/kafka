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

/**
 * An object that extends {@link FragmentHolder} to represent a fragment from an underlying pattern.  This effectively
 * removes leading fragments from the value while descending the tree.
 */
public interface Inserter extends FragmentHolder {

    /**
     * Returns {@code true} if the fragment is empty.
     *
     * @return {@code true} if the fragment is empty.
     */
    boolean isEmpty();

    /**
     * Advance to the next inserter position in the underlying pattern.  If there is no next inserter position
     * an inserter that return {@code true} for {@link #isEmpty()} is returned.
     *
     * @param advance The number of pattern elements to advance.  (e.g. for Strings this is the number of chars to advance in the pattern)
     * @return an inserter starts at the new position.  May be the original instance modified or may  be anew instance of Inserter.
     */
    Inserter advance(int advance);

    /**
     * Insert the fragment into the trie and return the new node.
     * @param node the node to start insert from.
     * @return the new Node.
     * @param <T> The object type stored in the trie.
     */
    <T> Node<T> insertIn(Node<T> node);
}
