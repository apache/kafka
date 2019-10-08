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
package org.apache.kafka.common.security.ldap.internals;

import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifs;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.integ.AbstractLdapTestUnit;
import org.apache.directory.server.core.integ.FrameworkRunner;
import org.apache.directory.server.ldap.LdapServer;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.authenticator.TestJaasConfig;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.security.plain.internals.PlainSaslServer;
import org.apache.kafka.common.security.plain.internals.PlainServerCallbackHandler;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@RunWith(FrameworkRunner.class)
@CreateDS()
@CreateLdapServer(transports =  @CreateTransport(protocol = "LDAP"))
@ApplyLdifs
    ({
        "dn: uid=kafkaUser,ou=users,ou=system",
        "objectClass: uidObject",
        "objectClass: person",
        "objectClass: top",
        "uid: kafkauUser",
        "cn: Kafka User",
        "sn: Kafka",
        "userPassword: ldappassword"
    })
public class LdapAuthenticationTest extends AbstractLdapTestUnit {

    private PlainSaslServer saslServer;
    private static final String USER_ID = "kafkaUser";
    private static final String USER_PASSWORD = "ldappassword";

    @Before
    public void setUp() {
        LdapServer ldapServer = getLdapServer();

        TestJaasConfig jaasConfig = new TestJaasConfig();
        Map<String, Object> options = new HashMap<>();
        options.put("ldap_url", "ldap://localhost:" + ldapServer.getPort());
        options.put("user_dn_template", "uid={0},ou=users,ou=system");
        jaasConfig.addEntry("jaasContext", PlainLoginModule.class.getName(), options);
        JaasContext jaasContext = new JaasContext("jaasContext", JaasContext.Type.SERVER, jaasConfig, null);
        PlainServerCallbackHandler callbackHandler = new LdapPlainServerCallbackHandler();
        callbackHandler.configure(null, "PLAIN", jaasContext.configurationEntries());
        saslServer = new PlainSaslServer(callbackHandler);
    }

    @Test
    public void testSuccessfulAuthentication() {
        saslServer.evaluateResponse(saslMessage(USER_PASSWORD));
    }

    @Test(expected = SaslAuthenticationException.class)
    public void testFailingAuthentication() {
        saslServer.evaluateResponse(saslMessage("incorrect_password"));
    }

    private byte[] saslMessage(String password) {
        String nul = "\u0000";
        String message = String.format("%s%s%s%s%s", LdapAuthenticationTest.USER_ID, nul, LdapAuthenticationTest.USER_ID, nul, password);
        return message.getBytes(StandardCharsets.UTF_8);
    }

}
