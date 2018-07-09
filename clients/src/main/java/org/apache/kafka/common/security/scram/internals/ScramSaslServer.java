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

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.IllegalSaslStateException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.ScramCredentialCallback;
import org.apache.kafka.common.security.scram.ScramLoginModule;
import org.apache.kafka.common.security.scram.internals.ScramMessages.ClientFinalMessage;
import org.apache.kafka.common.security.scram.internals.ScramMessages.ClientFirstMessage;
import org.apache.kafka.common.security.scram.internals.ScramMessages.ServerFinalMessage;
import org.apache.kafka.common.security.scram.internals.ScramMessages.ServerFirstMessage;
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCredentialCallback;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SaslServer implementation for SASL/SCRAM. This server is configured with a callback
 * handler for integration with a credential manager. Kafka brokers provide callbacks
 * based on a Zookeeper-based password store.
 *
 * @see <a href="https://tools.ietf.org/html/rfc5802">RFC 5802</a>
 */
public class ScramSaslServer implements SaslServer {

    private static final Logger log = LoggerFactory.getLogger(ScramSaslServer.class);
    private static final Set<String> SUPPORTED_EXTENSIONS = Utils.mkSet(ScramLoginModule.TOKEN_AUTH_CONFIG);

    enum State {
        RECEIVE_CLIENT_FIRST_MESSAGE,
        RECEIVE_CLIENT_FINAL_MESSAGE,
        COMPLETE,
        FAILED
    };

    private final ScramMechanism mechanism;
    private final ScramFormatter formatter;
    private final CallbackHandler callbackHandler;
    private State state;
    private String username;
    private ClientFirstMessage clientFirstMessage;
    private ServerFirstMessage serverFirstMessage;
    private ScramExtensions scramExtensions;
    private ScramCredential scramCredential;
    private String authorizationId;

    public ScramSaslServer(ScramMechanism mechanism, Map<String, ?> props, CallbackHandler callbackHandler) throws NoSuchAlgorithmException {
        this.mechanism = mechanism;
        this.formatter = new ScramFormatter(mechanism);
        this.callbackHandler = callbackHandler;
        setState(State.RECEIVE_CLIENT_FIRST_MESSAGE);
    }

    /**
     * @throws SaslAuthenticationException if the requested authorization id is not the same as username.
     * <p>
     * <b>Note:</b> This method may throw {@link SaslAuthenticationException} to provide custom error messages
     * to clients. But care should be taken to avoid including any information in the exception message that
     * should not be leaked to unauthenticated clients. It may be safer to throw {@link SaslException} in
     * most cases so that a standard error message is returned to clients.
     * </p>
     */
    @Override
    public byte[] evaluateResponse(byte[] response) throws SaslException, SaslAuthenticationException {
        try {
            switch (state) {
                case RECEIVE_CLIENT_FIRST_MESSAGE:
                    this.clientFirstMessage = new ClientFirstMessage(response);
                    this.scramExtensions = clientFirstMessage.extensions();
                    if (!SUPPORTED_EXTENSIONS.containsAll(scramExtensions.extensionNames())) {
                        log.debug("Unsupported extensions will be ignored, supported {}, provided {}",
                                SUPPORTED_EXTENSIONS, scramExtensions.extensionNames());
                    }
                    String serverNonce = formatter.secureRandomString();
                    try {
                        String saslName = clientFirstMessage.saslName();
                        this.username = formatter.username(saslName);
                        NameCallback nameCallback = new NameCallback("username", username);
                        ScramCredentialCallback credentialCallback;
                        if (scramExtensions.tokenAuthenticated()) {
                            DelegationTokenCredentialCallback tokenCallback = new DelegationTokenCredentialCallback();
                            credentialCallback = tokenCallback;
                            callbackHandler.handle(new Callback[]{nameCallback, tokenCallback});
                            if (tokenCallback.tokenOwner() == null)
                                throw new SaslException("Token Authentication failed: Invalid tokenId : " + username);
                            this.authorizationId = tokenCallback.tokenOwner();
                        } else {
                            credentialCallback = new ScramCredentialCallback();
                            callbackHandler.handle(new Callback[]{nameCallback, credentialCallback});
                            this.authorizationId = username;
                        }
                        this.scramCredential = credentialCallback.scramCredential();
                        if (scramCredential == null)
                            throw new SaslException("Authentication failed: Invalid user credentials");
                        String authorizationIdFromClient = clientFirstMessage.authorizationId();
                        if (!authorizationIdFromClient.isEmpty() && !authorizationIdFromClient.equals(username))
                            throw new SaslAuthenticationException("Authentication failed: Client requested an authorization id that is different from username");

                        if (scramCredential.iterations() < mechanism.minIterations())
                            throw new SaslException("Iterations " + scramCredential.iterations() +  " is less than the minimum " + mechanism.minIterations() + " for " + mechanism);
                        this.serverFirstMessage = new ServerFirstMessage(clientFirstMessage.nonce(),
                                serverNonce,
                                scramCredential.salt(),
                                scramCredential.iterations());
                        setState(State.RECEIVE_CLIENT_FINAL_MESSAGE);
                        return serverFirstMessage.toBytes();
                    } catch (SaslException | AuthenticationException e) {
                        throw e;
                    } catch (Throwable e) {
                        throw new SaslException("Authentication failed: Credentials could not be obtained", e);
                    }

                case RECEIVE_CLIENT_FINAL_MESSAGE:
                    try {
                        ClientFinalMessage clientFinalMessage = new ClientFinalMessage(response);
                        verifyClientProof(clientFinalMessage);
                        byte[] serverKey = scramCredential.serverKey();
                        byte[] serverSignature = formatter.serverSignature(serverKey, clientFirstMessage, serverFirstMessage, clientFinalMessage);
                        ServerFinalMessage serverFinalMessage = new ServerFinalMessage(null, serverSignature);
                        clearCredentials();
                        setState(State.COMPLETE);
                        return serverFinalMessage.toBytes();
                    } catch (InvalidKeyException e) {
                        throw new SaslException("Authentication failed: Invalid client final message", e);
                    }

                default:
                    throw new IllegalSaslStateException("Unexpected challenge in Sasl server state " + state);
            }
        } catch (SaslException | AuthenticationException e) {
            clearCredentials();
            setState(State.FAILED);
            throw e;
        }
    }

    @Override
    public String getAuthorizationID() {
        if (!isComplete())
            throw new IllegalStateException("Authentication exchange has not completed");
        return authorizationId;
    }

    @Override
    public String getMechanismName() {
        return mechanism.mechanismName();
    }

    @Override
    public Object getNegotiatedProperty(String propName) {
        if (!isComplete())
            throw new IllegalStateException("Authentication exchange has not completed");

        if (SUPPORTED_EXTENSIONS.contains(propName))
            return scramExtensions.extensionValue(propName);
        else
            return null;
    }

    @Override
    public boolean isComplete() {
        return state == State.COMPLETE;
    }

    @Override
    public byte[] unwrap(byte[] incoming, int offset, int len) throws SaslException {
        if (!isComplete())
            throw new IllegalStateException("Authentication exchange has not completed");
        return Arrays.copyOfRange(incoming, offset, offset + len);
    }

    @Override
    public byte[] wrap(byte[] outgoing, int offset, int len) throws SaslException {
        if (!isComplete())
            throw new IllegalStateException("Authentication exchange has not completed");
        return Arrays.copyOfRange(outgoing, offset, offset + len);
    }

    @Override
    public void dispose() throws SaslException {
    }

    private void setState(State state) {
        log.debug("Setting SASL/{} server state to {}", mechanism, state);
        this.state = state;
    }

    private void verifyClientProof(ClientFinalMessage clientFinalMessage) throws SaslException {
        try {
            byte[] expectedStoredKey = scramCredential.storedKey();
            byte[] clientSignature = formatter.clientSignature(expectedStoredKey, clientFirstMessage, serverFirstMessage, clientFinalMessage);
            byte[] computedStoredKey = formatter.storedKey(clientSignature, clientFinalMessage.proof());
            if (!Arrays.equals(computedStoredKey, expectedStoredKey))
                throw new SaslException("Invalid client credentials");
        } catch (InvalidKeyException e) {
            throw new SaslException("Sasl client verification failed", e);
        }
    }

    private void clearCredentials() {
        scramCredential = null;
        clientFirstMessage = null;
        serverFirstMessage = null;
    }

    public static class ScramSaslServerFactory implements SaslServerFactory {

        @Override
        public SaslServer createSaslServer(String mechanism, String protocol, String serverName, Map<String, ?> props, CallbackHandler cbh)
            throws SaslException {

            if (!ScramMechanism.isScram(mechanism)) {
                throw new SaslException(String.format("Requested mechanism '%s' is not supported. Supported mechanisms are '%s'.",
                        mechanism, ScramMechanism.mechanismNames()));
            }
            try {
                return new ScramSaslServer(ScramMechanism.forMechanismName(mechanism), props, cbh);
            } catch (NoSuchAlgorithmException e) {
                throw new SaslException("Hash algorithm not supported for mechanism " + mechanism, e);
            }
        }

        @Override
        public String[] getMechanismNames(Map<String, ?> props) {
            Collection<String> mechanisms = ScramMechanism.mechanismNames();
            return mechanisms.toArray(new String[mechanisms.size()]);
        }
    }
}
