/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.security.kerberos;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class implements parsing and handling of Kerberos principal names. In
 * particular, it splits them apart and translates them down into local
 * operating system names.
 */
public class KerberosNameParser {

    /**
     * A pattern that matches a Kerberos name with at most 3 components.
     */
    private static final Pattern NAME_PARSER = Pattern.compile("([^/@]*)(/([^/@]*))?@([^/@]*)");

    /**
     * A pattern for parsing a auth_to_local rule.
     */
    private static final Pattern RULE_PARSER = Pattern.compile("((DEFAULT)|(RULE:\\[(\\d*):([^\\]]*)](\\(([^)]*)\\))?(s/([^/]*)/([^/]*)/(g)?)?))");

    /**
     * The list of translation rules.
     */
    private final List<KerberosRule> authToLocalRules;

    public KerberosNameParser(String defaultRealm, List<String> authToLocalRules) {
        this.authToLocalRules = parseRules(defaultRealm, authToLocalRules);
    }

    /**
     * Create a name from the full Kerberos principal name.
     */
    public KerberosName parse(String principalName) {
        Matcher match = NAME_PARSER.matcher(principalName);
        if (!match.matches()) {
            if (principalName.contains("@")) {
                throw new IllegalArgumentException("Malformed Kerberos name: " + principalName);
            } else {
                return new KerberosName(principalName, null, null, authToLocalRules);
            }
        } else {
            return new KerberosName(match.group(1), match.group(3), match.group(4), authToLocalRules);
        }
    }

    private static List<KerberosRule> parseRules(String defaultRealm, List<String> rules) {
        List<KerberosRule> result = new ArrayList<>();
        for (String rule : rules) {
            Matcher matcher = RULE_PARSER.matcher(rule);
            if (!matcher.lookingAt()) {
                throw new IllegalArgumentException("Invalid rule: " + rule);
            }
            if (rule.length() != matcher.end())
                throw new IllegalArgumentException("Invalid rule: `" + rule + "`, unmatched substring: `" + rule.substring(matcher.end()) + "`");
            if (matcher.group(2) != null) {
                result.add(new KerberosRule(defaultRealm));
            } else {
                result.add(new KerberosRule(defaultRealm,
                        Integer.parseInt(matcher.group(4)),
                        matcher.group(5),
                        matcher.group(7),
                        matcher.group(9),
                        matcher.group(10),
                        "g".equals(matcher.group(11))));

            }
        }
        return result;
    }

    public static class BadFormatString extends IOException {
        BadFormatString(String msg) {
            super(msg);
        }
        BadFormatString(String msg, Throwable err) {
            super(msg, err);
        }
    }

}
