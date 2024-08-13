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
package org.apache.kafka.metadata.authorizer;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.function.BiPredicate;
import java.util.stream.Stream;

import static org.apache.kafka.common.resource.ResourcePattern.WILDCARD_RESOURCE;

/**
 * An Authorizer that extends the standard Authorizer by reimplementing the
 * {@link #authorizeByResourceType(AuthorizableRequestContext, AclOperation, ResourceType)}
 * method and providing a Radix Tree implementation for the {@link AuthorizerData}.
 * <p>
 *     All implementation details are described in the {@link NameTrieAuthorizerData} javadoc.
 * </p>
 * @see <a href="https://en.wikipedia.org/wiki/Radix_tree">Radix Tree (Wikipedia)</a>
 */
public class NameTrieAuthorizer extends StandardAuthorizer {

    /**
     * Constructor.
     */
    public NameTrieAuthorizer() {
        super(NameTrieAuthorizerData.createEmpty());
    }


    @Override
    public BiPredicate<ResourcePattern, String> patternNameMatcher() {
        return getPatternNameMatcher();
    }

    /**
     * Gets the pattern name matcher for the Trie implementaiton.  This is exposes like this for testing
     * the ResourcePatternFilter.
     * @return
     */
    public static BiPredicate<ResourcePattern, String> getPatternNameMatcher() {
        return (pattern, name) -> {
            switch (pattern.patternType()) {
                case LITERAL:
                    return name.equals(pattern.name()) || pattern.name().equals(WILDCARD_RESOURCE);

                case PREFIXED:
                    return  wildcardMatch(name, pattern.name() + "*", IOCase.SENSITIVE);

                default:
                    throw new IllegalArgumentException("Unsupported PatternType: " + pattern.patternType());
            }
        };
    }


    /**
     * Checks a fileName to see if it matches the specified wildcard matcher
     * allowing control over case-sensitivity.
     * <p>
     * The wildcard matcher uses the characters '?' and '*' to represent a
     * single or multiple (zero or more) wildcard characters.
     * N.B. the sequence "*?" does not work properly at present in match strings.
     *
     * This code was lifted from Apache commons-io FileNameUtils v 2.16.1
     *
     * @param fileName  the file name to match on
     * @param wildcardMatcher  the wildcard string to match against
     * @param ioCase  what case sensitivity rule to use, null means case-sensitive
     * @return true if the file name matches the wildcard string

     */
    private static boolean wildcardMatch(final String fileName, final String wildcardMatcher, IOCase ioCase) {
        if (fileName == null && wildcardMatcher == null) {
            return true;
        }
        if (fileName == null || wildcardMatcher == null) {
            return false;
        }
        ioCase = IOCase.value(ioCase, IOCase.SENSITIVE);
        final String[] wcs = splitOnTokens(wildcardMatcher);
        boolean anyChars = false;
        int textIdx = 0;
        int wcsIdx = 0;
        final Deque<int[]> backtrack = new ArrayDeque<>(wcs.length);

        // loop around a backtrack stack, to handle complex * matching
        do {
            if (!backtrack.isEmpty()) {
                final int[] array = backtrack.pop();
                wcsIdx = array[0];
                textIdx = array[1];
                anyChars = true;
            }

            // loop whilst tokens and text left to process
            while (wcsIdx < wcs.length) {

                if (wcs[wcsIdx].equals("?")) {
                    // ? so move to next text char
                    textIdx++;
                    if (textIdx > fileName.length()) {
                        break;
                    }
                    anyChars = false;

                } else if (wcs[wcsIdx].equals("*")) {
                    // set any chars status
                    anyChars = true;
                    if (wcsIdx == wcs.length - 1) {
                        textIdx = fileName.length();
                    }

                } else {
                    // matching text token
                    if (anyChars) {
                        // any chars then try to locate text token
                        textIdx = ioCase.checkIndexOf(fileName, textIdx, wcs[wcsIdx]);
                        if (textIdx == NOT_FOUND) {
                            // token not found
                            break;
                        }
                        final int repeat = ioCase.checkIndexOf(fileName, textIdx + 1, wcs[wcsIdx]);
                        if (repeat >= 0) {
                            backtrack.push(new int[] {wcsIdx, repeat});
                        }
                    } else if (!ioCase.checkRegionMatches(fileName, textIdx, wcs[wcsIdx])) {
                        // matching from current position
                        // couldn't match token
                        break;
                    }

                    // matched text token, move text index to end of matched token
                    textIdx += wcs[wcsIdx].length();
                    anyChars = false;
                }

                wcsIdx++;
            }

            // full match
            if (wcsIdx == wcs.length && textIdx == fileName.length()) {
                return true;
            }

        } while (!backtrack.isEmpty());

        return false;
    }

    private static final int NOT_FOUND = -1;
    private static final String[] EMPTY_STRING_ARRAY = {};

    /**
     * Splits a string into a number of tokens.
     * The text is split by '?' and '*'.
     * Where multiple '*' occur consecutively they are collapsed into a single '*'.
     *
     * This code was lifted from Apache commons-io FileNameUtils v 2.16.1
     **
     * @param text  the text to split
     * @return the array of tokens, never null
     */
    private static String[] splitOnTokens(final String text) {
        // used by wildcardMatch
        // package level so a unit test may run on this

        if (text.indexOf('?') == NOT_FOUND && text.indexOf('*') == NOT_FOUND) {
            return new String[] {text};
        }

        final char[] array = text.toCharArray();
        final ArrayList<String> list = new ArrayList<>();
        final StringBuilder buffer = new StringBuilder();
        char prevChar = 0;
        for (final char ch : array) {
            if (ch == '?' || ch == '*') {
                if (buffer.length() != 0) {
                    list.add(buffer.toString());
                    buffer.setLength(0);
                }
                if (ch == '?') {
                    list.add("?");
                } else if (prevChar != '*') {
                    // ch == '*' here; check if previous char was '*'
                    list.add("*");
                }
            } else {
                buffer.append(ch);
            }
            prevChar = ch;
        }
        if (buffer.length() != 0) {
            list.add(buffer.toString());
        }

        return list.toArray(EMPTY_STRING_ARRAY);
    }


    /**
     * Enumeration of IO case sensitivity.
     * <p>
     * Different filing systems have different rules for case-sensitivity.
     * Windows is case-insensitive, UNIX is case-sensitive.
     * </p>
     * <p>
     * This class captures that difference, providing an enumeration to
     * control how file name comparisons should be performed. It also provides
     * methods that use the enumeration to perform comparisons.
     * </p>
     * <p>
     * Wherever possible, you should use the {@code check} methods in this
     * class to compare file names.
     * </p>
     **
     * This code was lifted and reduced from Apache commons-io FileNameUtils v 2.16.1
     */
    public enum IOCase {

        /**
         * The constant for case-sensitive regardless of operating system.
         */
        SENSITIVE("Sensitive", true),

        /**
         * The constant for case-insensitive regardless of operating system.
         */
        INSENSITIVE("Insensitive", false);

        /** Serialization version. */
        private static final long serialVersionUID = -6343169151696340687L;

        /**
         * Looks up an IOCase by name.
         *
         * @param name  the name to find
         * @return the IOCase object
         * @throws IllegalArgumentException if the name is invalid
         */
        public static IOCase forName(final String name) {
            return Stream.of(values()).filter(ioCase -> ioCase.getName().equals(name)).findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Illegal IOCase name: " + name));
        }

        /**
         * Returns the given value if not-null, the defaultValue if null.
         *
         * @param value the value to test.
         * @param defaultValue the default value.
         * @return the given value if not-null, the defaultValue if null.
         * @since 2.12.0
         */
        public static IOCase value(final IOCase value, final IOCase defaultValue) {
            return value != null ? value : defaultValue;
        }

        /** The enumeration name. */
        private final String name;

        /** The sensitivity flag. */
        private final transient boolean sensitive;

        /**
         * Constructs a new instance.
         *
         * @param name  the name.
         * @param sensitive  the sensitivity.
         */
        IOCase(final String name, final boolean sensitive) {
            this.name = name;
            this.sensitive = sensitive;
        }

        /**
         * Checks if one string contains another starting at a specific index using the
         * case-sensitivity rule.
         * <p>
         * This method mimics parts of {@link String#indexOf(String, int)}
         * but takes case-sensitivity into account.
         * </p>
         *
         * @param str  the string to check.
         * @param strStartIndex  the index to start at in str.
         * @param search  the start to search for.
         * @return the first index of the search String,
         *  -1 if no match or {@code null} string input.
         * @since 2.0
         */
        public int checkIndexOf(final String str, final int strStartIndex, final String search) {
            if (str != null && search != null) {
                final int endIndex = str.length() - search.length();
                if (endIndex >= strStartIndex) {
                    for (int i = strStartIndex; i <= endIndex; i++) {
                        if (checkRegionMatches(str, i, search)) {
                            return i;
                        }
                    }
                }
            }
            return -1;
        }

        /**
         * Checks if one string contains another at a specific index using the case-sensitivity rule.
         * <p>
         * This method mimics parts of {@link String#regionMatches(boolean, int, String, int, int)}
         * but takes case-sensitivity into account.
         * </p>
         *
         * This code was lifted from Apache commons-io FileNameUtils v 2.17.0
         *
         * @param str  the string to check.
         * @param strStartIndex  the index to start at in str.
         * @param search  the start to search for,.
         * @return true if equal using the case rules.
         */
        public boolean checkRegionMatches(final String str, final int strStartIndex, final String search) {
            return str != null && search != null && str.regionMatches(!sensitive, strStartIndex, search, 0, search.length());
        }

        /**
         * Gets the name of the constant.
         *
         * @return the name of the constant
         */
        public String getName() {
            return name;
        }

        /**
         * Replaces the enumeration from the stream with a real one.
         * This ensures that the correct flag is set for SYSTEM.
         *
         * @return the resolved object.
         */
        private Object readResolve() {
            return forName(name);
        }

        /**
         * Gets a string describing the sensitivity.
         *
         * @return a string describing the sensitivity.
         */
        @Override
        public String toString() {
            return name;
        }

    }
}
