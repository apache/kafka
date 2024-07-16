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
package org.apache.kafka.common.security.plain.internals;

import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.authenticator.TestJaasConfig;
import org.apache.kafka.common.security.plain.PlainLoginModule;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PlainSaslServerTest {

    private static final String USER_A = "userA";
    private static final String PASSWORD_A = "passwordA";
    private static final String USER_B = "userB";
    private static final String PASSWORD_B = "passwordB";

    private PlainSaslServer saslServer;

    @BeforeEach
    public void setUp() {
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
    public void noAuthorizationIdSpecified() {
        byte[] nextChallenge = saslServer.evaluateResponse(saslMessage("", USER_A, PASSWORD_A));
        assertEquals(0, nextChallenge.length);
    }

    @Test
    public void authorizationIdEqualsAuthenticationId() {
        byte[] nextChallenge = saslServer.evaluateResponse(saslMessage(USER_A, USER_A, PASSWORD_A));
        assertEquals(0, nextChallenge.length);
    }

    @Test
    public void authorizationIdNotEqualsAuthenticationId() {
        assertThrows(SaslAuthenticationException.class, () -> saslServer.evaluateResponse(saslMessage(USER_B, USER_A, PASSWORD_A)));
    }

    @Test
    public void emptyTokens() {
        Exception e = assertThrows(SaslAuthenticationException.class, () ->
            saslServer.evaluateResponse(saslMessage("", "", "")));
        assertEquals("Authentication failed: username not specified", e.getMessage());

        e = assertThrows(SaslAuthenticationException.class, () ->
            saslServer.evaluateResponse(saslMessage("", "", "p")));
        assertEquals("Authentication failed: username not specified", e.getMessage());

        e = assertThrows(SaslAuthenticationException.class, () ->
            saslServer.evaluateResponse(saslMessage("", "u", "")));
        assertEquals("Authentication failed: password not specified for user u", e.getMessage());

        e = assertThrows(SaslAuthenticationException.class, () ->
            saslServer.evaluateResponse(saslMessage("a", "", "")));
        assertEquals("Authentication failed: username not specified", e.getMessage());

        e = assertThrows(SaslAuthenticationException.class, () ->
            saslServer.evaluateResponse(saslMessage("a", "", "p")));
        assertEquals("Authentication failed: username not specified", e.getMessage());

        e = assertThrows(SaslAuthenticationException.class, () ->
            saslServer.evaluateResponse(saslMessage("a", "u", "")));
        assertEquals("Authentication failed: password not specified for user u", e.getMessage());

        String nul = "\u0000";

        e = assertThrows(SaslAuthenticationException.class, () ->
            saslServer.evaluateResponse(
                String.format("%s%s%s%s%s%s", "a", nul, "u", nul, "p", nul).getBytes(StandardCharsets.UTF_8)));
        assertEquals("Invalid SASL/PLAIN response: expected 3 tokens, got 4", e.getMessage());

        e = assertThrows(SaslAuthenticationException.class, () ->
            saslServer.evaluateResponse(
                String.format("%s%s%s", "", nul, "u").getBytes(StandardCharsets.UTF_8)));
        assertEquals("Invalid SASL/PLAIN response: expected 3 tokens, got 2", e.getMessage());
    }

    private byte[] saslMessage(String authorizationId, String userName, String password) {
        String nul = "\u0000";
        String message = String.format("%s%s%s%s%s", authorizationId, nul, userName, nul, password);
        return message.getBytes(StandardCharsets.UTF_8);
    }
}
