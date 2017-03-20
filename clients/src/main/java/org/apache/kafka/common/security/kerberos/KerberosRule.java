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
package org.apache.kafka.common.security.kerberos;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An encoding of a rule for translating kerberos names.
 */
class KerberosRule {

    /**
     * A pattern that matches a string without '$' and then a single
     * parameter with $n.
     */
    private static final Pattern PARAMETER_PATTERN = Pattern.compile("([^$]*)(\\$(\\d*))?");

    /**
     * A pattern that recognizes simple/non-simple names.
     */
    private static final Pattern NON_SIMPLE_PATTERN = Pattern.compile("[/@]");

    private final String defaultRealm;
    private final boolean isDefault;
    private final int numOfComponents;
    private final String format;
    private final Pattern match;
    private final Pattern fromPattern;
    private final String toPattern;
    private final boolean repeat;

    KerberosRule(String defaultRealm) {
        this.defaultRealm = defaultRealm;
        isDefault = true;
        numOfComponents = 0;
        format = null;
        match = null;
        fromPattern = null;
        toPattern = null;
        repeat = false;
    }

    KerberosRule(String defaultRealm, int numOfComponents, String format, String match, String fromPattern,
                 String toPattern, boolean repeat) {
        this.defaultRealm = defaultRealm;
        isDefault = false;
        this.numOfComponents = numOfComponents;
        this.format = format;
        this.match = match == null ? null : Pattern.compile(match);
        this.fromPattern =
                fromPattern == null ? null : Pattern.compile(fromPattern);
        this.toPattern = toPattern;
        this.repeat = repeat;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        if (isDefault) {
            buf.append("DEFAULT");
        } else {
            buf.append("RULE:[");
            buf.append(numOfComponents);
            buf.append(':');
            buf.append(format);
            buf.append(']');
            if (match != null) {
                buf.append('(');
                buf.append(match);
                buf.append(')');
            }
            if (fromPattern != null) {
                buf.append("s/");
                buf.append(fromPattern);
                buf.append('/');
                buf.append(toPattern);
                buf.append('/');
                if (repeat) {
                    buf.append('g');
                }
            }
        }
        return buf.toString();
    }

    /**
     * Replace the numbered parameters of the form $n where n is from 1 to
     * the length of params. Normal text is copied directly and $n is replaced
     * by the corresponding parameter.
     * @param format the string to replace parameters again
     * @param params the list of parameters
     * @return the generated string with the parameter references replaced.
     * @throws BadFormatString
     */
    static String replaceParameters(String format,
                                    String[] params) throws BadFormatString {
        Matcher match = PARAMETER_PATTERN.matcher(format);
        int start = 0;
        StringBuilder result = new StringBuilder();
        while (start < format.length() && match.find(start)) {
            result.append(match.group(1));
            String paramNum = match.group(3);
            if (paramNum != null) {
                try {
                    int num = Integer.parseInt(paramNum);
                    if (num < 0 || num > params.length) {
                        throw new BadFormatString("index " + num + " from " + format +
                                " is outside of the valid range 0 to " +
                                (params.length - 1));
                    }
                    result.append(params[num]);
                } catch (NumberFormatException nfe) {
                    throw new BadFormatString("bad format in username mapping in " +
                            paramNum, nfe);
                }

            }
            start = match.end();
        }
        return result.toString();
    }

    /**
     * Replace the matches of the from pattern in the base string with the value
     * of the to string.
     * @param base the string to transform
     * @param from the pattern to look for in the base string
     * @param to the string to replace matches of the pattern with
     * @param repeat whether the substitution should be repeated
     * @return
     */
    static String replaceSubstitution(String base, Pattern from, String to,
                                      boolean repeat) {
        Matcher match = from.matcher(base);
        if (repeat) {
            return match.replaceAll(to);
        } else {
            return match.replaceFirst(to);
        }
    }

    /**
     * Try to apply this rule to the given name represented as a parameter
     * array.
     * @param params first element is the realm, second and later elements are
     *        are the components of the name "a/b@FOO" -> {"FOO", "a", "b"}
     * @return the short name if this rule applies or null
     * @throws IOException throws if something is wrong with the rules
     */
    String apply(String[] params) throws IOException {
        String result = null;
        if (isDefault) {
            if (defaultRealm.equals(params[0])) {
                result = params[1];
            }
        } else if (params.length - 1 == numOfComponents) {
            String base = replaceParameters(format, params);
            if (match == null || match.matcher(base).matches()) {
                if (fromPattern == null) {
                    result = base;
                } else {
                    result = replaceSubstitution(base, fromPattern, toPattern,  repeat);
                }
            }
        }
        if (result != null && NON_SIMPLE_PATTERN.matcher(result).find()) {
            throw new NoMatchingRule("Non-simple name " + result + " after auth_to_local rule " + this);
        }
        return result;
    }
}
