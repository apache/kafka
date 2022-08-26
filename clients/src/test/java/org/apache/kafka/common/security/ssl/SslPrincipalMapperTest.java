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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class SslPrincipalMapperTest {

    @Test
    public void testValidRules() {
        testValidRule("DEFAULT");
        testValidRule("RULE:^CN=(.*?),OU=ServiceUsers.*$/$1/");
        testValidRule("RULE:^CN=(.*?),OU=ServiceUsers.*$/$1/L, DEFAULT");
        testValidRule("RULE:^CN=(.*?),OU=(.*?),O=(.*?),L=(.*?),ST=(.*?),C=(.*?)$/$1@$2/");
        testValidRule("RULE:^.*[Cc][Nn]=([a-zA-Z0-9.]*).*$/$1/L");
        testValidRule("RULE:^cn=(.?),ou=(.?),dc=(.?),dc=(.?)$/$1@$2/U");

        testValidRule("RULE:^CN=([^,ADEFLTU,]+)(,.*|$)/$1/");
        testValidRule("RULE:^CN=([^,DEFAULT,]+)(,.*|$)/$1/");
    }

    private void testValidRule(String rules) {
        SslPrincipalMapper.fromRules(rules);
    }

    @Test
    public void testInvalidRules() {
        testInvalidRule("default");
        testInvalidRule("DEFAUL");
        testInvalidRule("DEFAULT/L");
        testInvalidRule("DEFAULT/U");

        testInvalidRule("RULE:CN=(.*?),OU=ServiceUsers.*/$1");
        testInvalidRule("rule:^CN=(.*?),OU=ServiceUsers.*$/$1/");
        testInvalidRule("RULE:^CN=(.*?),OU=ServiceUsers.*$/$1/L/U");
        testInvalidRule("RULE:^CN=(.*?),OU=ServiceUsers.*$/L");
        testInvalidRule("RULE:^CN=(.*?),OU=ServiceUsers.*$/U");
        testInvalidRule("RULE:^CN=(.*?),OU=ServiceUsers.*$/LU");
    }

    private void testInvalidRule(String rules) {
        try {
            System.out.println(SslPrincipalMapper.fromRules(rules));
            fail("should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testSslPrincipalMapper() throws Exception {
        String rules = String.join(", ",
            "RULE:^CN=(.*?),OU=ServiceUsers.*$/$1/L",
            "RULE:^CN=(.*?),OU=(.*?),O=(.*?),L=(.*?),ST=(.*?),C=(.*?)$/$1@$2/L",
            "RULE:^cn=(.*?),ou=(.*?),dc=(.*?),dc=(.*?)$/$1@$2/U",
            "RULE:^.*[Cc][Nn]=([a-zA-Z0-9.]*).*$/$1/U",
            "DEFAULT"
        );

        SslPrincipalMapper mapper = SslPrincipalMapper.fromRules(rules);

        assertEquals("duke", mapper.getName("CN=Duke,OU=ServiceUsers,O=Org,C=US"));
        assertEquals("duke@sme", mapper.getName("CN=Duke,OU=SME,O=mycp,L=Fulton,ST=MD,C=US"));
        assertEquals("DUKE@SME", mapper.getName("cn=duke,ou=sme,dc=mycp,dc=com"));
        assertEquals("DUKE", mapper.getName("cN=duke,OU=JavaSoft,O=Sun Microsystems"));
        assertEquals("OU=JavaSoft,O=Sun Microsystems,C=US", mapper.getName("OU=JavaSoft,O=Sun Microsystems,C=US"));
    }

    private void testRulesSplitting(String expected, String rules) {
        SslPrincipalMapper mapper = SslPrincipalMapper.fromRules(rules);
        assertEquals(String.format("SslPrincipalMapper(rules = %s)", expected), mapper.toString());
    }

    @Test
    public void testRulesSplitting() {
        // seeing is believing
        testRulesSplitting("[]", "");
        testRulesSplitting("[DEFAULT]", "DEFAULT");
        testRulesSplitting("[RULE:/]", "RULE://");
        testRulesSplitting("[RULE:/.*]", "RULE:/.*/");
        testRulesSplitting("[RULE:/.*/L]", "RULE:/.*/L");
        testRulesSplitting("[RULE:/, DEFAULT]", "RULE://,DEFAULT");
        testRulesSplitting("[RULE:/, DEFAULT]", "  RULE:// ,  DEFAULT  ");
        testRulesSplitting("[RULE:   /     , DEFAULT]", "  RULE:   /     / ,  DEFAULT  ");
        testRulesSplitting("[RULE:  /     /U, DEFAULT]", "  RULE:  /     /U   ,DEFAULT  ");
        testRulesSplitting("[RULE:([A-Z]*)/$1/U, RULE:([a-z]+)/$1, DEFAULT]", "  RULE:([A-Z]*)/$1/U   ,RULE:([a-z]+)/$1/,   DEFAULT  ");

        // empty rules are ignored
        testRulesSplitting("[]", ",   , , ,      , , ,   ");
        testRulesSplitting("[RULE:/, DEFAULT]", ",,RULE://,,,DEFAULT,,");
        testRulesSplitting("[RULE: /   , DEFAULT]", ",  , RULE: /   /    ,,,   DEFAULT, ,   ");
        testRulesSplitting("[RULE:   /  /U, DEFAULT]", "     ,  , RULE:   /  /U    ,,  ,DEFAULT, ,");

        // escape sequences
        testRulesSplitting("[RULE:\\/\\\\\\(\\)\\n\\t/\\/\\/]", "RULE:\\/\\\\\\(\\)\\n\\t/\\/\\//");
        testRulesSplitting("[RULE:\\**\\/+/*/L, RULE:\\/*\\**/**]", "RULE:\\**\\/+/*/L,RULE:\\/*\\**/**/");

        // rules rule
        testRulesSplitting(
            "[RULE:,RULE:,/,RULE:,\\//U, RULE:,/RULE:,, RULE:,RULE:,/L,RULE:,/L, RULE:, DEFAULT, /DEFAULT, DEFAULT]",
            "RULE:,RULE:,/,RULE:,\\//U,RULE:,/RULE:,/,RULE:,RULE:,/L,RULE:,/L,RULE:, DEFAULT, /DEFAULT/,DEFAULT"
        );
    }

    @Test
    public void testCommaWithWhitespace() throws Exception {
        String rules = "RULE:^CN=((\\\\, *|\\w)+)(,.*|$)/$1/,DEFAULT";

        SslPrincipalMapper mapper = SslPrincipalMapper.fromRules(rules);
        assertEquals("Tkac\\, Adam", mapper.getName("CN=Tkac\\, Adam,OU=ITZ,DC=geodis,DC=cz"));
    }
}
