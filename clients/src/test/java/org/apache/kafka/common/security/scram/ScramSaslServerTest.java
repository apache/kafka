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
package org.apache.kafka.common.security.scram;

import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import javax.security.sasl.SaslException;

import org.apache.kafka.common.security.authenticator.CredentialCache;

public class ScramSaslServerTest {

    private static final String USER_A = "userA";
    private static final String USER_B = "userB";

    private ScramMechanism mechanism;
    private ScramFormatter formatter;
    private ScramSaslServer saslServer;

    @Before
    public void setUp() throws Exception {
        mechanism = ScramMechanism.SCRAM_SHA_256;
        formatter  = new ScramFormatter(mechanism);
        CredentialCache.Cache<ScramCredential> credentialCache = new CredentialCache().createCache(mechanism.mechanismName(), ScramCredential.class);
        credentialCache.put(USER_A, formatter.generateCredential("passwordA", 4096));
        credentialCache.put(USER_B, formatter.generateCredential("passwordB", 4096));
        ScramServerCallbackHandler callbackHandler = new ScramServerCallbackHandler(credentialCache);
        saslServer = new ScramSaslServer(mechanism, new HashMap<String, Object>(), callbackHandler);
    }

    @Test
    public void noAuthorizationIdSpecified() throws Exception {
        String nonce = formatter.secureRandomString();
        String firstMessage = String.format("n,,n=%s,r=%s", USER_A, nonce);
        saslServer.evaluateResponse(firstMessage.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    public void authorizatonIdEqualsAuthenticationId() throws Exception {
        String nonce = formatter.secureRandomString();
        String firstMessage = String.format("n,a=%s,n=%s,r=%s", USER_A, USER_A, nonce);
        saslServer.evaluateResponse(firstMessage.getBytes(StandardCharsets.UTF_8));
    }

    @Test(expected = SaslException.class)
    public void authorizatonIdNotEqualsAuthenticationId() throws Exception {
        String nonce = formatter.secureRandomString();
        String firstMessage = String.format("n,a=%s,n=%s,r=%s", USER_A, USER_B, nonce);
        saslServer.evaluateResponse(firstMessage.getBytes(StandardCharsets.UTF_8));
    }
}
