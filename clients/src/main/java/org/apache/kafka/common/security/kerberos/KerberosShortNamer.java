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

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.PrincipalToLocal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.kafka.common.security.JaasUtils.defaultRealm;

/**
 * This class implements parsing and handling of Kerberos principal names. In
 * particular, it splits them apart and translates them down into local
 * operating system names.
 */
public class KerberosShortNamer implements PrincipalToLocal {

    /**
     * A pattern for parsing a auth_to_local rule.
     */
    private static final Pattern RULE_PARSER = Pattern.compile("((DEFAULT)|(RULE:\\[(\\d*):([^\\]]*)](\\(([^)]*)\\))?(s/([^/]*)/([^/]*)/(g)?)?))");

    /* Rules for the translation of the principal name into an operating system name */
    private List<KerberosRule> principalToLocalRules;

    public static KerberosShortNamer fromUnparsedRules(String defaultRealm, List<String> principalToLocalRules) {
        List<String> rules = principalToLocalRules == null ? Collections.singletonList("DEFAULT") : principalToLocalRules;
        KerberosShortNamer shortNamer = new KerberosShortNamer();
        shortNamer.principalToLocalRules = parseRules(defaultRealm, rules);
        return shortNamer;
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

    /**
     * Get the translation of the principal name into an operating system
     * user name.
     * @return the short name
     * @throws IOException
     */

    @Override
    public KafkaPrincipal toLocal(KafkaPrincipal principal) throws IOException {
        String[] params;
        KerberosName kerberosName = KerberosName.parse(principal.getName());
        if (kerberosName.hostName() == null) {
            // if it is already simple, just return it
            if (kerberosName.realm() == null)
                return new KafkaPrincipal(KafkaPrincipal.USER_TYPE, kerberosName.serviceName());
            params = new String[]{kerberosName.realm(), kerberosName.serviceName()};
        } else {
            params = new String[]{kerberosName.realm(), kerberosName.serviceName(), kerberosName.hostName()};
        }
        for (KerberosRule r : principalToLocalRules) {
            String result = r.apply(params);
            if (result != null)
                return new KafkaPrincipal(KafkaPrincipal.USER_TYPE, result);
        }
        throw new NoMatchingRule("No rules applied to " + toString());
    }

    @Override
    public void configure(Map<String, ?> configs) {
        String defaultRealm = "";
        try {
            defaultRealm = defaultRealm();
        } catch (Exception e) {
            //ignore exception.
        }
        List<String> rules = configs.get(SaslConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES) == null ?
                Collections.singletonList("DEFAULT") : (List<String>) configs.get(SaslConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES);

        this.principalToLocalRules = parseRules(defaultRealm, rules);
    }

}
