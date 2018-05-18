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
package org.apache.kafka.common.security.plain.internal;

import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.authenticator.TestJaasConfig;

public class PlainSaslServerTest {

    private static final String USER_A = "userA";
    private static final String PASSWORD_A = "passwordA";
    private static final String USER_B = "userB";
    private static final String PASSWORD_B = "passwordB";

    private PlainSaslServer saslServer;

    @Before
    public void setUp() throws Exception {
        TestJaasConfig jaasConfig = new TestJaasConfig();
        Map<String, Object> options = new HashMap<>();
        options.put("user_" + USER_A, PASSWORD_A);
        options.put("user_" + USER_B, PASSWORD_B);
        jaasConfig.addEntry("jaasContext", PlainLoginModule.class.getName(), options);
        JaasContext jaasContext = new JaasContext("jaasContext", JaasContext.Type.SERVER, jaasConfig, null);
        PlainServerCallbackHandler callbackHandler = new PlainServerCallbackHandler();
        callbackHandler.configure(null, "PLAIN", jaasContext.configurationEntries());
        saslServer = new PlainSaslServer(callbackHandler);
    }

    @Test
    public void noAuthorizationIdSpecified() throws Exception {
        byte[] nextChallenge = saslServer.evaluateResponse(saslMessage("", USER_A, PASSWORD_A));
        assertEquals(0, nextChallenge.length);
    }

    @Test
    public void authorizatonIdEqualsAuthenticationId() throws Exception {
        byte[] nextChallenge = saslServer.evaluateResponse(saslMessage(USER_A, USER_A, PASSWORD_A));
        assertEquals(0, nextChallenge.length);
    }

    @Test(expected = SaslAuthenticationException.class)
    public void authorizatonIdNotEqualsAuthenticationId() throws Exception {
        saslServer.evaluateResponse(saslMessage(USER_B, USER_A, PASSWORD_A));
    }

    private byte[] saslMessage(String authorizationId, String userName, String password) {
        String nul = "\u0000";
        String message = String.format("%s%s%s%s%s", authorizationId, nul, userName, nul, password);
        return message.getBytes(StandardCharsets.UTF_8);
    }
}
