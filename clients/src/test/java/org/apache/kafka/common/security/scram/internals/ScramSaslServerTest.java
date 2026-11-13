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


import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.authenticator.CredentialCache;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.internals.ScramMessages.ClientFinalMessage;
import org.apache.kafka.common.security.scram.internals.ScramMessages.ClientFirstMessage;
import org.apache.kafka.common.security.scram.internals.ScramMessages.ServerFinalMessage;
import org.apache.kafka.common.security.scram.internals.ScramMessages.ServerFirstMessage;
import org.apache.kafka.common.security.token.delegation.DelegationToken;
import org.apache.kafka.common.security.token.delegation.TokenInformation;
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.security.sasl.SaslException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ScramSaslServerTest {

    private static final String USER_A = "userA";
    private static final String USER_B = "userB";

    private ScramFormatter formatter;
    private ScramServerCallbackHandler callbackHandler;
    private ScramSaslServer saslServer;

    @BeforeEach
    public void setUp() throws Exception {
        ScramMechanism mechanism = ScramMechanism.SCRAM_SHA_256;
        formatter  = new ScramFormatter(mechanism);
        CredentialCache.Cache<ScramCredential> credentialCache = new CredentialCache().createCache(mechanism.mechanismName(), ScramCredential.class);
        credentialCache.put(USER_A, formatter.generateCredential("passwordA", 4096));
        credentialCache.put(USER_B, formatter.generateCredential("passwordB", 4096));
        callbackHandler = new ScramServerCallbackHandler(credentialCache, new DelegationTokenCache(ScramMechanism.mechanismNames()));
        callbackHandler.configure(new HashMap<>(), mechanism.mechanismName(), List.of());
        saslServer = new ScramSaslServer(mechanism, new HashMap<>(), callbackHandler);
    }

    @Test
    public void noAuthorizationIdSpecified() throws Exception {
        byte[] nextChallenge = saslServer.evaluateResponse(clientFirstMessage(USER_A, null));
        assertTrue(nextChallenge.length > 0, "Next challenge is empty");
    }

    @Test
    public void authorizationIdEqualsAuthenticationId() throws Exception {
        byte[] nextChallenge = saslServer.evaluateResponse(clientFirstMessage(USER_A, USER_A));
        assertTrue(nextChallenge.length > 0, "Next challenge is empty");
    }

    @Test
    public void authorizationIdNotEqualsAuthenticationId() {
        assertThrows(SaslAuthenticationException.class, () -> saslServer.evaluateResponse(clientFirstMessage(USER_A, USER_B)));
    }

    @Test
    public void unknownUserAuthorizationIdNotEqualsAuthenticationId() {
        assertThrows(SaslAuthenticationException.class, () -> saslServer.evaluateResponse(clientFirstMessage("unknownUser", USER_B)));
    }

    @Test
    public void unknownUserFailsOnClientFinalMessage() throws Exception {
        ServerFirstMessage serverFirstMessage = new ServerFirstMessage(saslServer.evaluateResponse(clientFirstMessage("unknownUser", null)));
        assertEquals(4096, serverFirstMessage.iterations());

        ScramSaslServer otherSaslServer = new ScramSaslServer(ScramMechanism.SCRAM_SHA_256, new HashMap<>(), callbackHandler);
        ServerFirstMessage otherServerFirstMessage = new ServerFirstMessage(otherSaslServer.evaluateResponse(clientFirstMessage("unknownUser", null)));
        assertArrayEquals(serverFirstMessage.salt(), otherServerFirstMessage.salt());

        ScramSaslServer tokenSaslServer = new ScramSaslServer(ScramMechanism.SCRAM_SHA_256, new HashMap<>(), callbackHandler);
        String tokenClientFirstMessage = "n,,n=unknownTokenId,r=" + formatter.secureRandomString() + ",tokenauth=true";
        assertEquals(4096, new ServerFirstMessage(tokenSaslServer.evaluateResponse(tokenClientFirstMessage.getBytes(StandardCharsets.UTF_8))).iterations());

        SaslException e = assertThrows(SaslException.class, () -> saslServer.evaluateResponse(clientFinalMessage(serverFirstMessage.nonce())));
        assertEquals("Authentication failed: Invalid user credentials", e.getMessage());

        ScramSaslServer knownUserSaslServer = new ScramSaslServer(ScramMechanism.SCRAM_SHA_256, new HashMap<>(), callbackHandler);
        ServerFirstMessage knownUserServerFirstMessage = new ServerFirstMessage(knownUserSaslServer.evaluateResponse(clientFirstMessage(USER_A, null)));
        e = assertThrows(SaslException.class, () -> knownUserSaslServer.evaluateResponse(clientFinalMessage(knownUserServerFirstMessage.nonce())));
        assertEquals("Invalid client credentials", e.getMessage());
    }

    @Test
    public void tokenWithoutOwnerFailsOnClientFinalMessage() throws Exception {
        String tokenHmac = "tokenHmac";
        DelegationTokenCache tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames());
        TokenInformation tokenInfo = new TokenInformation("tokenId", new KafkaPrincipal(KafkaPrincipal.USER_TYPE, USER_A), List.of(), 0L, Long.MAX_VALUE, Long.MAX_VALUE);
        tokenCache.updateCache(new DelegationToken(tokenInfo, ScramFormatter.toBytes(tokenHmac)), Map.of(ScramMechanism.SCRAM_SHA_256.mechanismName(), formatter.generateCredential(tokenHmac, 4096)));
        tokenCache.removeToken("tokenId");
        ScramServerCallbackHandler tokenCallbackHandler = new ScramServerCallbackHandler(new CredentialCache().createCache(ScramMechanism.SCRAM_SHA_256.mechanismName(), ScramCredential.class), tokenCache);
        tokenCallbackHandler.configure(Map.of(), ScramMechanism.SCRAM_SHA_256.mechanismName(), List.of());
        ScramSaslServer tokenSaslServer = new ScramSaslServer(ScramMechanism.SCRAM_SHA_256, new HashMap<>(), tokenCallbackHandler);

        ClientFirstMessage clientFirstMessage = new ClientFirstMessage(ScramFormatter.toBytes("n,,n=tokenId,r=" + formatter.secureRandomString() + ",tokenauth=true"));
        ServerFirstMessage serverFirstMessage = new ServerFirstMessage(tokenSaslServer.evaluateResponse(clientFirstMessage.toBytes()));
        ClientFinalMessage clientFinalMessage = new ClientFinalMessage(ScramFormatter.toBytes("n,,"), serverFirstMessage.nonce());
        byte[] saltedPassword = formatter.saltedPassword(tokenHmac, serverFirstMessage.salt(), serverFirstMessage.iterations());
        clientFinalMessage.proof(formatter.clientProof(saltedPassword, clientFirstMessage, serverFirstMessage, clientFinalMessage));

        SaslException e = assertThrows(SaslException.class, () -> tokenSaslServer.evaluateResponse(clientFinalMessage.toBytes()));
        assertEquals("Authentication failed: Invalid user credentials", e.getMessage());
        assertFalse(tokenSaslServer.isComplete());
    }

    /**
     * Validate that server responds with client's nonce as prefix of its nonce in the
     * server first message.
     * <br>
     * In addition, it checks that the client final message has nonce that it sent in its
     * first message.
     */
    @Test
    public void validateNonceExchange() throws SaslException {
        ScramSaslServer spySaslServer = Mockito.spy(saslServer);
        byte[] clientFirstMsgBytes = clientFirstMessage(USER_A, USER_A);
        ClientFirstMessage clientFirstMessage = new ClientFirstMessage(clientFirstMsgBytes);

        byte[] serverFirstMsgBytes = spySaslServer.evaluateResponse(clientFirstMsgBytes);
        ServerFirstMessage serverFirstMessage = new ServerFirstMessage(serverFirstMsgBytes);
        assertTrue(serverFirstMessage.nonce().startsWith(clientFirstMessage.nonce()),
            "Nonce in server message should start with client first message's nonce");

        byte[] clientFinalMessage = clientFinalMessage(serverFirstMessage.nonce());
        Mockito.doNothing()
            .when(spySaslServer).verifyClientProof(Mockito.any(ScramMessages.ClientFinalMessage.class));
        byte[] serverFinalMsgBytes = spySaslServer.evaluateResponse(clientFinalMessage);
        ServerFinalMessage serverFinalMessage = new ServerFinalMessage(serverFinalMsgBytes);
        assertNull(serverFinalMessage.error(), "Server final message should not contain error");
    }

    @Test
    public void validateFailedNonceExchange() throws SaslException {
        ScramSaslServer spySaslServer = Mockito.spy(saslServer);
        byte[] clientFirstMsgBytes = clientFirstMessage(USER_A, USER_A);
        ClientFirstMessage clientFirstMessage = new ClientFirstMessage(clientFirstMsgBytes);

        byte[] serverFirstMsgBytes = spySaslServer.evaluateResponse(clientFirstMsgBytes);
        ServerFirstMessage serverFirstMessage = new ServerFirstMessage(serverFirstMsgBytes);
        assertTrue(serverFirstMessage.nonce().startsWith(clientFirstMessage.nonce()),
            "Nonce in server message should start with client first message's nonce");

        byte[] clientFinalMessage = clientFinalMessage(formatter.secureRandomString());
        Mockito.doNothing()
            .when(spySaslServer).verifyClientProof(Mockito.any(ScramMessages.ClientFinalMessage.class));
        SaslException saslException = assertThrows(SaslException.class,
            () -> spySaslServer.evaluateResponse(clientFinalMessage));
        assertEquals("Invalid client nonce in the final client message.",
            saslException.getMessage(),
            "Failure message: " + saslException.getMessage());
    }

    @Test
    public void validateFailedNonceExchangeWithPrependingClientNonce() throws SaslException {
        ScramSaslServer spySaslServer = Mockito.spy(saslServer);
        byte[] clientFirstMsgBytes = clientFirstMessage(USER_A, USER_A);
        ClientFirstMessage clientFirstMessage = new ClientFirstMessage(clientFirstMsgBytes);

        byte[] serverFirstMsgBytes = spySaslServer.evaluateResponse(clientFirstMsgBytes);
        ServerFirstMessage serverFirstMessage = new ServerFirstMessage(serverFirstMsgBytes);
        assertTrue(serverFirstMessage.nonce().startsWith(clientFirstMessage.nonce()),
                "Nonce in server message should start with client first message's nonce");

        //send client final message with nonce prepended with clientFirstMessage's nonce
        byte[] clientFinalMessage = clientFinalMessage(clientFirstMessage.nonce() + serverFirstMessage.nonce());
        Mockito.doNothing()
                .when(spySaslServer).verifyClientProof(Mockito.any(ScramMessages.ClientFinalMessage.class));
        SaslException saslException = assertThrows(SaslException.class,
                () -> spySaslServer.evaluateResponse(clientFinalMessage));
        assertEquals("Invalid client nonce in the final client message.",
                saslException.getMessage(),
                "Failure message: " + saslException.getMessage());
    }

    private byte[] clientFirstMessage(String userName, String authorizationId) {
        String nonce = formatter.secureRandomString();
        String authorizationField = authorizationId != null ? "a=" + authorizationId : "";
        String firstMessage = String.format("n,%s,n=%s,r=%s", authorizationField, userName, nonce);
        return firstMessage.getBytes(StandardCharsets.UTF_8);
    }

    private byte[] clientFinalMessage(String nonce) {
        String channelBinding = randomBytesAsString();
        String proof = randomBytesAsString();

        String message = String.format("c=%s,r=%s,p=%s", channelBinding, nonce, proof);
        return message.getBytes(StandardCharsets.UTF_8);
    }

    private String randomBytesAsString() {
        return Base64.getEncoder().encodeToString(formatter.secureRandomBytes());
    }
}
