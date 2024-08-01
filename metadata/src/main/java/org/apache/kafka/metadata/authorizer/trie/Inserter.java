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
 * @param <T>  The type of object that comprises the pattern.  Normally a String.
 */
public interface Inserter<T extends Comparable<T>> extends FragmentHolder<T> {

    /**
     * Returns {@code true} if the fragment represents a wildcard.
     *
     * @return {@code true} if the fragment represents a wildcard.
     */
    boolean isWildcard();

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
    Inserter<T> advance(int advance);
}
