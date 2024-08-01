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

import static java.lang.String.format;

/**
 * The registry of all wildcard patterns.
 */
public class WildcardRegistry {
    /* list of wildcard strings.  Order is important.  Most specific match first */
    public static final String[] PATTERNS = {"?", "*"};

    /**
     * Returns {@code true} if the string is a wildcard.
     * @param str the String to check.
     * @return {@code true} if the string is a wildcard.
     */
    public static boolean isWildcard(String str) {
        return str.length() == 1 && isWildcard(str.charAt(0));
    }

    /**
     * Returns {@code true} if the char is a wildcard.
     * @param ch the char to check.
     * @return {@code true} if the char is a wildcard.
     */
    public static boolean isWildcard(char ch) {
        return "*?".indexOf(ch) > -1;
    }

    /**
     * Returns a wildcard adjusted pattern as follows:
     * <ul>
     *     <li>If the pattern starts with a wildcard - return the wild card.</li>
     *     <li>If the pattern does not contain a wildcard - return the pattern</li>
     *     <li>else return the fragment of the patten from the start but not including the wildcard.</li>
     * </ul>
     * @param pattern the pattern to get the segment from.
     * @return The wildcard adjusted pattern.
     */
    public static String getSegment(String pattern) {
        int splat = pattern.indexOf('*');
        int quest = pattern.indexOf('?');

        splat = splat == -1 ? quest : splat;
        quest = quest == -1 ? splat : quest;
        int pos = Math.min(splat, quest);
        if (pos == 0) {
            return pattern.substring(0, 1);
        }
        return pos == -1 ? pattern : pattern.substring(0, pos);
    }

    /**
     * Apply any wildcards in the matcher to a child search on the node.
     * @param parent the node containing the children.
     * @param matcher the matcher to process.
     * @return matching node or {@code null} if there is no match.
     * @param <T> the type of data in the Trie.
     */
    public static <T> Node<T> processWildcards(Node<T> parent, Matcher<T> matcher) {
        // create a searcher on the parent node.
        Node<T>.Search searcher = parent.new Search();
        Node<T> match = null;

        /** Check the wildcards from most specific to least specific */
        for (String wildcard : PATTERNS) {
            // search for and exact match for the wildcard.
            Node<T> child = searcher.eq(() -> wildcard);
            if (child != null) {
                switch (wildcard) {
                    case "?":
                        // with a single character wildcard just skip on position in the matcher and
                        // look in the children of the wildcard node.
                        match = child.findNodeFor(matcher.advance(1));
                        if (Matcher.validMatch(match)) {
                            return match;
                        }
                        break;
                    case "*":
                        // for multi character wildcards we have to skip 1 to n-1 characters looking for
                        // the match.
                        for (int advance = 1; advance < matcher.getFragment().length(); advance++) {
                            match = child.findNodeFor(matcher.advance(advance));
                            if (Matcher.validMatch(match)) {
                                return match;
                            }
                        }
                        break;
                    default:
                        throw new IllegalArgumentException(format("'%s' is not a valid wildcard", wildcard));
                }
            }
        }
        return null;
    }
}
