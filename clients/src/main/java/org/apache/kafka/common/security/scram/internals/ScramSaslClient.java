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
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslClientFactory;
import javax.security.sasl.SaslException;

import org.apache.kafka.common.errors.IllegalSaslStateException;
import org.apache.kafka.common.security.scram.ScramExtensionsCallback;
import org.apache.kafka.common.security.scram.internals.ScramMessages.ClientFinalMessage;
import org.apache.kafka.common.security.scram.internals.ScramMessages.ServerFinalMessage;
import org.apache.kafka.common.security.scram.internals.ScramMessages.ServerFirstMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SaslClient implementation for SASL/SCRAM.
 * <p>
 * This implementation expects a login module that populates username as
 * the Subject's public credential and password as the private credential.
 *
 * @see <a href="https://tools.ietf.org/html/rfc5802">RFC 5802</a>
 *
 */
public class ScramSaslClient implements SaslClient {

    private static final Logger log = LoggerFactory.getLogger(ScramSaslClient.class);

    enum State {
        SEND_CLIENT_FIRST_MESSAGE,
        RECEIVE_SERVER_FIRST_MESSAGE,
        RECEIVE_SERVER_FINAL_MESSAGE,
        COMPLETE,
        FAILED
    }

    private final ScramMechanism mechanism;
    private final CallbackHandler callbackHandler;
    private final ScramFormatter formatter;
    private String clientNonce;
    private State state;
    private byte[] saltedPassword;
    private ScramMessages.ClientFirstMessage clientFirstMessage;
    private ScramMessages.ServerFirstMessage serverFirstMessage;
    private ScramMessages.ClientFinalMessage clientFinalMessage;

    public ScramSaslClient(ScramMechanism mechanism, CallbackHandler cbh) throws NoSuchAlgorithmException {
        this.mechanism = mechanism;
        this.callbackHandler = cbh;
        this.formatter = new ScramFormatter(mechanism);
        setState(State.SEND_CLIENT_FIRST_MESSAGE);
    }

    @Override
    public String getMechanismName() {
        return mechanism.mechanismName();
    }

    @Override
    public boolean hasInitialResponse() {
        return true;
    }

    @Override
    public byte[] evaluateChallenge(byte[] challenge) throws SaslException {
        try {
            switch (state) {
                case SEND_CLIENT_FIRST_MESSAGE:
                    if (challenge != null && challenge.length != 0)
                        throw new SaslException("Expected empty challenge");
                    clientNonce = formatter.secureRandomString();
                    NameCallback nameCallback = new NameCallback("Name:");
                    ScramExtensionsCallback extensionsCallback = new ScramExtensionsCallback();

                    try {
                        callbackHandler.handle(new Callback[]{nameCallback});
                        try {
                            callbackHandler.handle(new Callback[]{extensionsCallback});
                        } catch (UnsupportedCallbackException e) {
                            log.debug("Extensions callback is not supported by client callback handler {}, no extensions will be added",
                                    callbackHandler);
                        }
                    } catch (Throwable e) {
                        throw new SaslException("User name or extensions could not be obtained", e);
                    }

                    String username = nameCallback.getName();
                    String saslName = ScramFormatter.saslName(username);
                    Map<String, String> extensions = extensionsCallback.extensions();
                    this.clientFirstMessage = new ScramMessages.ClientFirstMessage(saslName, clientNonce, extensions);
                    setState(State.RECEIVE_SERVER_FIRST_MESSAGE);
                    return clientFirstMessage.toBytes();

                case RECEIVE_SERVER_FIRST_MESSAGE:
                    this.serverFirstMessage = new ServerFirstMessage(challenge);
                    if (!serverFirstMessage.nonce().startsWith(clientNonce))
                        throw new SaslException("Invalid server nonce: does not start with client nonce");
                    if (serverFirstMessage.iterations() < mechanism.minIterations())
                        throw new SaslException("Requested iterations " + serverFirstMessage.iterations() +  " is less than the minimum " + mechanism.minIterations() + " for " + mechanism);
                    PasswordCallback passwordCallback = new PasswordCallback("Password:", false);
                    try {
                        callbackHandler.handle(new Callback[]{passwordCallback});
                    } catch (Throwable e) {
                        throw new SaslException("User name could not be obtained", e);
                    }
                    this.clientFinalMessage = handleServerFirstMessage(passwordCallback.getPassword());
                    setState(State.RECEIVE_SERVER_FINAL_MESSAGE);
                    return clientFinalMessage.toBytes();

                case RECEIVE_SERVER_FINAL_MESSAGE:
                    ServerFinalMessage serverFinalMessage = new ServerFinalMessage(challenge);
                    if (serverFinalMessage.error() != null)
                        throw new SaslException("Sasl authentication using " + mechanism + " failed with error: " + serverFinalMessage.error());
                    handleServerFinalMessage(serverFinalMessage.serverSignature());
                    setState(State.COMPLETE);
                    return null;

                default:
                    throw new IllegalSaslStateException("Unexpected challenge in Sasl client state " + state);
            }
        } catch (SaslException e) {
            setState(State.FAILED);
            throw e;
        }
    }

    @Override
    public boolean isComplete() {
        return state == State.COMPLETE;
    }

    @Override
    public byte[] unwrap(byte[] incoming, int offset, int len) {
        if (!isComplete())
            throw new IllegalStateException("Authentication exchange has not completed");
        return Arrays.copyOfRange(incoming, offset, offset + len);
    }

    @Override
    public byte[] wrap(byte[] outgoing, int offset, int len) {
        if (!isComplete())
            throw new IllegalStateException("Authentication exchange has not completed");
        return Arrays.copyOfRange(outgoing, offset, offset + len);
    }

    @Override
    public Object getNegotiatedProperty(String propName) {
        if (!isComplete())
            throw new IllegalStateException("Authentication exchange has not completed");
        return null;
    }

    @Override
    public void dispose() {
    }

    private void setState(State state) {
        log.debug("Setting SASL/{} client state to {}", mechanism, state);
        this.state = state;
    }

    private ClientFinalMessage handleServerFirstMessage(char[] password) throws SaslException {
        try {
            byte[] passwordBytes = ScramFormatter.normalize(new String(password));
            this.saltedPassword = formatter.hi(passwordBytes, serverFirstMessage.salt(), serverFirstMessage.iterations());

            ClientFinalMessage clientFinalMessage = new ClientFinalMessage("n,,".getBytes(StandardCharsets.UTF_8), serverFirstMessage.nonce());
            byte[] clientProof = formatter.clientProof(saltedPassword, clientFirstMessage, serverFirstMessage, clientFinalMessage);
            clientFinalMessage.proof(clientProof);
            return clientFinalMessage;
        } catch (InvalidKeyException e) {
            throw new SaslException("Client final message could not be created", e);
        }
    }

    private void handleServerFinalMessage(byte[] signature) throws SaslException {
        try {
            byte[] serverKey = formatter.serverKey(saltedPassword);
            byte[] serverSignature = formatter.serverSignature(serverKey, clientFirstMessage, serverFirstMessage, clientFinalMessage);
            if (!Arrays.equals(signature, serverSignature))
                throw new SaslException("Invalid server signature in server final message");
        } catch (InvalidKeyException e) {
            throw new SaslException("Sasl server signature verification failed", e);
        }
    }

    public static class ScramSaslClientFactory implements SaslClientFactory {

        @Override
        public SaslClient createSaslClient(String[] mechanisms,
                String authorizationId,
                String protocol,
                String serverName,
                Map<String, ?> props,
                CallbackHandler cbh) throws SaslException {

            ScramMechanism mechanism = null;
            for (String mech : mechanisms) {
                mechanism = ScramMechanism.forMechanismName(mech);
                if (mechanism != null)
                    break;
            }
            if (mechanism == null)
                throw new SaslException(String.format("Requested mechanisms '%s' not supported. Supported mechanisms are '%s'.",
                        Arrays.asList(mechanisms), ScramMechanism.mechanismNames()));

            try {
                return new ScramSaslClient(mechanism, cbh);
            } catch (NoSuchAlgorithmException e) {
                throw new SaslException("Hash algorithm not supported for mechanism " + mechanism, e);
            }
        }

        @Override
        public String[] getMechanismNames(Map<String, ?> props) {
            Collection<String> mechanisms = ScramMechanism.mechanismNames();
            return mechanisms.toArray(new String[0]);
        }
    }
}
