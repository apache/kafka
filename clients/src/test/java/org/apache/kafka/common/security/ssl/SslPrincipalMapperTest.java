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

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SslPrincipalMapperTest {

    @Test
    public void testValidRules() {
        testValidRule(Arrays.asList("DEFAULT"));
        testValidRule(Arrays.asList("RULE:^CN=(.*?),OU=ServiceUsers.*$/$1/"));
        testValidRule(Arrays.asList("RULE:^CN=(.*?),OU=ServiceUsers.*$/$1/L", "DEFAULT"));
        testValidRule(Arrays.asList("RULE:^CN=(.*?),OU=(.*?),O=(.*?),L=(.*?),ST=(.*?),C=(.*?)$/$1@$2/"));
        testValidRule(Arrays.asList("RULE:^.*[Cc][Nn]=([a-zA-Z0-9.]*).*$/$1/L"));
        testValidRule(Arrays.asList("RULE:^cn=(.?),ou=(.?),dc=(.?),dc=(.?)$/$1@$2/U"));
    }

    @Test
    public void testValidSplitRules() {
        testValidRule(Arrays.asList("DEFAULT"));
        testValidRule(Arrays.asList("RULE:^CN=(.*?)", "OU=ServiceUsers.*$/$1/"));
        testValidRule(Arrays.asList("RULE:^CN=(.*?)", "OU=ServiceUsers.*$/$1/L", "DEFAULT"));
        testValidRule(Arrays.asList("RULE:^CN=(.*?)", "OU=(.*?),O=(.*?),L=(.*?)", "ST=(.*?)", "C=(.*?)$/$1@$2/"));
        testValidRule(Arrays.asList("RULE:^.*[Cc][Nn]=([a-zA-Z0-9.]*).*$/$1/L"));
        testValidRule(Arrays.asList("RULE:^cn=(.?)", "ou=(.?)", "dc=(.?)", "dc=(.?)$/$1@$2/U"));
    }

    private void testValidRule(List<String> rules) {
        SslPrincipalMapper.fromRules(rules);
    }

    @Test
    public void testInvalidRules() {
        testInvalidRule(Arrays.asList("default"));
        testInvalidRule(Arrays.asList("DEFAUL"));
        testInvalidRule(Arrays.asList("DEFAULT/L"));
        testInvalidRule(Arrays.asList("DEFAULT/U"));

        testInvalidRule(Arrays.asList("RULE:CN=(.*?),OU=ServiceUsers.*/$1"));
        testInvalidRule(Arrays.asList("rule:^CN=(.*?),OU=ServiceUsers.*$/$1/"));
        testInvalidRule(Arrays.asList("RULE:^CN=(.*?),OU=ServiceUsers.*$/$1/L/U"));
        testInvalidRule(Arrays.asList("RULE:^CN=(.*?),OU=ServiceUsers.*$/L"));
        testInvalidRule(Arrays.asList("RULE:^CN=(.*?),OU=ServiceUsers.*$/U"));
        testInvalidRule(Arrays.asList("RULE:^CN=(.*?),OU=ServiceUsers.*$/LU"));
    }

    @Test
    public void testInvalidSplitRules() {
        testInvalidRule(Arrays.asList("default"));
        testInvalidRule(Arrays.asList("DEFAUL"));
        testInvalidRule(Arrays.asList("DEFAULT/L"));
        testInvalidRule(Arrays.asList("DEFAULT/U"));

        testInvalidRule(Arrays.asList("RULE:CN=(.*?)", "OU=ServiceUsers.*/$1"));
        testInvalidRule(Arrays.asList("rule:^CN=(.*?)", "OU=ServiceUsers.*$/$1/"));
        testInvalidRule(Arrays.asList("RULE:^CN=(.*?)", "OU=ServiceUsers.*$/$1/L/U"));
        testInvalidRule(Arrays.asList("RULE:^CN=(.*?)", "OU=ServiceUsers.*$/L"));
        testInvalidRule(Arrays.asList("RULE:^CN=(.*?)", "OU=ServiceUsers.*$/U"));
        testInvalidRule(Arrays.asList("RULE:^CN=(.*?)", "OU=ServiceUsers.*$/LU"));
    }

    private void testInvalidRule(List<String> rules) {
        try {
            System.out.println(SslPrincipalMapper.fromRules(rules));
            fail("should have thrown IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testSslPrincipalMapper() throws Exception {
        List<String> rules = Arrays.asList(
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

}
