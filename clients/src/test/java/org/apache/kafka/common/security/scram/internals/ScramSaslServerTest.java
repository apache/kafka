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
package org.apache.kafka.common.security.scram.internals;


import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.authenticator.CredentialCache;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

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
        ScramServerCallbackHandler callbackHandler = new ScramServerCallbackHandler(credentialCache, new DelegationTokenCache(ScramMechanism.mechanismNames()));
        saslServer = new ScramSaslServer(mechanism, new HashMap<String, Object>(), callbackHandler);
    }

    @Test
    public void noAuthorizationIdSpecified() throws Exception {
        byte[] nextChallenge = saslServer.evaluateResponse(clientFirstMessage(USER_A, null));
        assertTrue("Next challenge is empty", nextChallenge.length > 0);
    }

    @Test
    public void authorizatonIdEqualsAuthenticationId() throws Exception {
        byte[] nextChallenge = saslServer.evaluateResponse(clientFirstMessage(USER_A, USER_A));
        assertTrue("Next challenge is empty", nextChallenge.length > 0);
    }

    @Test(expected = SaslAuthenticationException.class)
    public void authorizatonIdNotEqualsAuthenticationId() throws Exception {
        saslServer.evaluateResponse(clientFirstMessage(USER_A, USER_B));
    }

    private byte[] clientFirstMessage(String userName, String authorizationId) {
        String nonce = formatter.secureRandomString();
        String authorizationField = authorizationId != null ? "a=" + authorizationId : "";
        String firstMessage = String.format("n,%s,n=%s,r=%s", authorizationField, userName, nonce);
        return firstMessage.getBytes(StandardCharsets.UTF_8);
    }
}
