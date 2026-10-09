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

package org.apache.kafka.shell.glob;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

/**
 * Implements a per-path-component glob.
 */
public final class GlobComponent {
    private static final Logger log = LoggerFactory.getLogger(GlobComponent.class);

    /**
     * Returns true if the character is a special character for regular expressions.
     */
    private static boolean isRegularExpressionSpecialCharacter(char ch) {
        return switch (ch) {
            case '$', '(', ')', '+', '.', '[', ']', '^', '{', '|' -> true;
            default -> false;
        };
    }

    /**
     * Returns true if the character is a special character for globs.
     */
    private static boolean isGlobSpecialCharacter(char ch) {
        return switch (ch) {
            case '*', '?', '\\', '{', '}', '[', ']' -> true;
            default -> false;
        };
    }

    /**
     * Converts a glob string to a regular expression string.
     * Returns null if the glob should be handled as a literal (can only match one string).
     * Throws an exception if the glob is malformed.
     */
    static String toRegularExpression(String glob) {
        StringBuilder output = new StringBuilder("^");
        boolean literal = true;
        boolean processingGroup = false;

        for (int i = 0; i < glob.length(); ) {
            char c = glob.charAt(i++);
            switch (c) {
                case '?':
                    literal = false;
                    output.append(".");
                    break;
                case '*':
                    literal = false;
                    output.append(".*");
                    break;
                case '\\':
                    if (i == glob.length()) {
                        output.append(c);
                    } else {
                        char next = glob.charAt(i);
                        i++;
                        if (isGlobSpecialCharacter(next) ||
                                isRegularExpressionSpecialCharacter(next)) {
                            output.append('\\');
                        }
                        output.append(next);
                    }
                    break;
                case '{':
                    if (processingGroup) {
                        throw new RuntimeException("Can't nest glob groups.");
                    }
                    literal = false;
                    output.append("(?:(?:");
                    processingGroup = true;
                    break;
                case ',':
                    if (processingGroup) {
                        literal = false;
                        output.append(")|(?:");
                    } else {
                        output.append(c);
                    }
                    break;
                case '}':
                    if (processingGroup) {
                        literal = false;
                        output.append("))");
                        processingGroup = false;
                    } else {
                        output.append(c);
                    }
                    break;
                case '[':
                    literal = false;
                    i = handleCharacterClass(glob, i, output);
                    break;

                default:
                    if (isRegularExpressionSpecialCharacter(c)) {
                        output.append('\\');
                    }
                    output.append(c);
            }
        }
        if (processingGroup) {
            throw new RuntimeException("Unterminated glob group.");
        }
        if (literal) {
            return null;
        }
        output.append('$');
        return output.toString();
    }

    /**
     * Parses a glob character class starting at the given index and appends the
     * equivalent regular expression representation to the provided {@code output}.
     *
     * <p>The {@code startIdx} must point to the first character inside the
     * character class (i.e., immediately after the opening '[').</p>
     *
     * <p>This method supports:
     * <ul>
     *   <li>Leading ']' as a literal</li>
     *   <li>Negation using '!' or '^'</li>
     *   <li>Escaped characters using backslash</li>
     * </ul>
     *
     * @param glob the full glob pattern
     * @param startIdx index of the first character inside the character class
     * @param output the {@link StringBuilder} to append the regex representation to
     * @return the index immediately after the closing ']'
     * @throws RuntimeException if the character class is empty or doesn't close properly
     */
    private static int handleCharacterClass(String glob, int startIdx, StringBuilder output) {
        int idx = startIdx;

        if (idx >= glob.length()) {
            throw new RuntimeException("Glob has '[' but no ']'.");
        }
        output.append("[");

        boolean hasContent = false;
        char first = glob.charAt(idx);
        if (first == ']') {
            output.append(']');
            hasContent = true;
            idx++;
        } else if (first == '!' || first == '^') {
            output.append('^');
            idx++;
        }

        while (idx < glob.length() && glob.charAt(idx) != ']') {
            char ch = glob.charAt(idx);
            if (ch == '\\') {
                idx++;
                if (idx >= glob.length()) {
                    throw new RuntimeException("Trailing backslash in character class.");
                }
                char escaped = glob.charAt(idx);

                // Escape for regex safety only if needed
                if (escaped == '\\' || escaped == '[' || escaped == ']') {
                    output.append('\\');
                }
                output.append(escaped);
                hasContent = true;
            } else {
                // Converting [[] to [\[] for regex safety
                if (ch == '[') {
                    output.append('\\');
                }
                output.append(ch);
                hasContent = true;
            }

            idx++;
        }
        if (idx >= glob.length()) {
            throw new RuntimeException("Unterminated character class opened '[' but never closed.");
        }

        if (!hasContent) {
            throw new RuntimeException("Empty glob character class.");
        }

        output.append(']');
        return idx + 1;
    }

    private final String component;
    private final Pattern pattern;

    public GlobComponent(String component) {
        this.component = component;
        Pattern newPattern = null;
        try {
            String regularExpression = toRegularExpression(component);
            if (regularExpression != null) {
                newPattern = Pattern.compile(regularExpression);
            }
        } catch (RuntimeException e) {
            log.debug("Invalid glob pattern: " + e.getMessage());
        }
        this.pattern = newPattern;
    }

    public String component() {
        return component;
    }

    public boolean literal() {
        return pattern == null;
    }

    public boolean matches(String nodeName) {
        if (pattern == null) {
            return component.equals(nodeName);
        } else {
            return pattern.matcher(nodeName).matches();
        }
    }
}
