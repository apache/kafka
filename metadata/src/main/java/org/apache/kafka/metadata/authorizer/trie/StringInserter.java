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

import java.util.TreeSet;
import java.util.function.IntConsumer;

/**
 * An {@link Inserter} for String patterns.
 * <p>
 *     Wildcard patterns are inserted on their own child node and not combined with any other characters.  So inserting
 *     {@code AB*C?D} will yield the trie:
 *     <pre>
 *     AB
 *      *
 *      C
 *      ?
 *      D
 *      </pre>
 * </p>
 */
public class StringInserter implements Inserter<String> {

    /** The pattern to insert */
    private final String pattern;

    /** The starting position of the fragment within the pattern */
    private int position;

    /** {@code true} if this is a wild card fragment */
    private boolean wildcardFlag;

    /**
     * Crreatesa StringInserter starting at the beginning of the pattern.
     * @param pattern
     */
    public StringInserter(String pattern) {
        this.pattern = pattern;
        this.position = 0;
        this.wildcardFlag = WildcardRegistry.isWildcard(pattern.charAt(position));
    }

    @Override
    public boolean isWildcard() {
        return wildcardFlag;
    }

    @Override
    public String getFragment() {
        return WildcardRegistry.getSegment(pattern.substring(position));
    }

    @Override
    public boolean isEmpty() {
        return position >= pattern.length();
    }

    @Override
    public StringInserter advance(int advance) {
        position += advance;
        if (!isEmpty()) {
            wildcardFlag = WildcardRegistry.isWildcard(pattern.charAt(position));
        }
        return this;
    }
}
