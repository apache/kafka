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
package org.apache.kafka.common.security.ssl;

import java.io.IOException;
import java.util.List;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SslPrincipalMapper {

    private static final Pattern RULE_PARSER = Pattern.compile("((DEFAULT)|(RULE:(([^/]*)/([^/]*))/([LU])?))");

    private final List<Rule> rules;

    public SslPrincipalMapper(List<Rule> sslPrincipalMappingRules) {
        this.rules = sslPrincipalMappingRules;
    }

    public static SslPrincipalMapper fromRules(List<String> sslPrincipalMappingRules) {
        List<String> rules = sslPrincipalMappingRules == null ? Collections.singletonList("DEFAULT") : sslPrincipalMappingRules;
        return new SslPrincipalMapper(parseRules(rules));
    }

    private static List<String> joinSplitRules(List<String> rules) {
        String rule = "RULE:";
        String defaultRule = "DEFAULT";
        List<String> retVal = new ArrayList<>();
        StringBuilder currentRule = new StringBuilder();
        for (String r : rules) {
            if (currentRule.length() > 0) {
                if (r.startsWith(rule) || r.equals(defaultRule)) {
                    retVal.add(currentRule.toString());
                    currentRule.setLength(0);
                    currentRule.append(r);
                } else {
                    currentRule.append(String.format(",%s", r));
                }
            } else {
                currentRule.append(r);
            }
        }
        if (currentRule.length() > 0) {
            retVal.add(currentRule.toString());
        }
        return retVal;
    }

    private static List<Rule> parseRules(List<String> rules) {
        rules = joinSplitRules(rules);
        List<Rule> result = new ArrayList<>();
        for (String rule : rules) {
            Matcher matcher = RULE_PARSER.matcher(rule);
            if (!matcher.lookingAt()) {
                throw new IllegalArgumentException("Invalid rule: " + rule);
            }
            if (rule.length() != matcher.end()) {
                throw new IllegalArgumentException("Invalid rule: `" + rule + "`, unmatched substring: `" + rule.substring(matcher.end()) + "`");
            }
            if (matcher.group(2) != null) {
                result.add(new Rule());
            } else {
                result.add(new Rule(matcher.group(5),
                                    matcher.group(6),
                                    "L".equals(matcher.group(7)),
                                    "U".equals(matcher.group(7))));
            }
        }
        return result;
    }

    public String getName(String distinguishedName) throws IOException {
        for (Rule r : rules) {
            String principalName = r.apply(distinguishedName);
            if (principalName != null) {
                return principalName;
            }
        }
        throw new NoMatchingRule("No rules apply to " + distinguishedName + ", rules " + rules);
    }

    @Override
    public String toString() {
        return "SslPrincipalMapper(rules = " + rules + ")";
    }

    public static class NoMatchingRule extends IOException {
        NoMatchingRule(String msg) {
            super(msg);
        }
    }

    private static class Rule {
        private static final Pattern BACK_REFERENCE_PATTERN = Pattern.compile("\\$(\\d+)");

        private final boolean isDefault;
        private final Pattern pattern;
        private final String replacement;
        private final boolean toLowerCase;
        private final boolean toUpperCase;

        Rule() {
            isDefault = true;
            pattern = null;
            replacement = null;
            toLowerCase = false;
            toUpperCase = false;
        }

        Rule(String pattern, String replacement, boolean toLowerCase, boolean toUpperCase) {
            isDefault = false;
            this.pattern = pattern == null ? null : Pattern.compile(pattern);
            this.replacement = replacement;
            this.toLowerCase = toLowerCase;
            this.toUpperCase = toUpperCase;
        }

        String apply(String distinguishedName) {
            if (isDefault) {
                return distinguishedName;
            }

            String result = null;
            final Matcher m = pattern.matcher(distinguishedName);

            if (m.matches()) {
                result = distinguishedName.replaceAll(pattern.pattern(), escapeLiteralBackReferences(replacement, m.groupCount()));
            }

            if (toLowerCase && result != null) {
                result = result.toLowerCase(Locale.ENGLISH);
            } else if (toUpperCase & result != null) {
                result = result.toUpperCase(Locale.ENGLISH);
            }

            return result;
        }

        //If we find a back reference that is not valid, then we will treat it as a literal string. For example, if we have 3 capturing
        //groups and the Replacement Value has the value is "$1@$4", then we want to treat the $4 as a literal "$4", rather
        //than attempting to use it as a back reference.
        //This method was taken from Apache Nifi project : org.apache.nifi.authorization.util.IdentityMappingUtil
        private String escapeLiteralBackReferences(final String unescaped, final int numCapturingGroups) {
            if (numCapturingGroups == 0) {
                return unescaped;
            }

            String value = unescaped;
            final Matcher backRefMatcher = BACK_REFERENCE_PATTERN.matcher(value);
            while (backRefMatcher.find()) {
                final String backRefNum = backRefMatcher.group(1);
                if (backRefNum.startsWith("0")) {
                    continue;
                }
                final int originalBackRefIndex = Integer.parseInt(backRefNum);
                int backRefIndex = originalBackRefIndex;


                // if we have a replacement value like $123, and we have less than 123 capturing groups, then
                // we want to truncate the 3 and use capturing group 12; if we have less than 12 capturing groups,
                // then we want to truncate the 2 and use capturing group 1; if we don't have a capturing group then
                // we want to truncate the 1 and get 0.
                while (backRefIndex > numCapturingGroups && backRefIndex >= 10) {
                    backRefIndex /= 10;
                }

                if (backRefIndex > numCapturingGroups) {
                    final StringBuilder sb = new StringBuilder(value.length() + 1);
                    final int groupStart = backRefMatcher.start(1);

                    sb.append(value.substring(0, groupStart - 1));
                    sb.append("\\");
                    sb.append(value.substring(groupStart - 1));
                    value = sb.toString();
                }
            }

            return value;
        }

        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder();
            if (isDefault) {
                buf.append("DEFAULT");
            } else {
                buf.append("RULE:");
                if (pattern != null) {
                    buf.append(pattern);
                }
                if (replacement != null) {
                    buf.append("/");
                    buf.append(replacement);
                }
                if (toLowerCase) {
                    buf.append("/L");
                } else if (toUpperCase) {
                    buf.append("/U");
                }
            }
            return buf.toString();
        }

    }
}
