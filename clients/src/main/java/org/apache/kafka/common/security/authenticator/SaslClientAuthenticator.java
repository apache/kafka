/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.security.authenticator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.Arrays;
import java.util.Map;

import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import org.apache.kafka.common.network.Authenticator;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.TransportLayer;
import org.apache.kafka.common.security.auth.PrincipalBuilder;
import org.apache.kafka.common.KafkaException;

import org.apache.kafka.common.security.auth.SimplePrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SaslClientAuthenticator implements Authenticator {

    public enum SaslState {
        INITIAL, INTERMEDIATE, COMPLETE, FAILED
    }

    private static final Logger LOG = LoggerFactory.getLogger(SaslClientAuthenticator.class);

    private final Subject subject;
    private final String servicePrincipal;
    private final String host;
    private final String node;

    // assigned in `configure`
    private SaslClient saslClient;
    private String clientPrincipalName;
    private TransportLayer transportLayer;

    // buffers used in `authenticate`
    private NetworkReceive netInBuffer;
    private NetworkSend netOutBuffer;

    private SaslState saslState = SaslState.INITIAL;

    public SaslClientAuthenticator(String node, Subject subject, String servicePrincipal, String host) throws IOException {
        this.node = node;
        this.subject = subject;
        this.host = host;
        this.servicePrincipal = servicePrincipal;
    }

    public void configure(TransportLayer transportLayer, PrincipalBuilder principalBuilder, Map<String, ?> configs) throws KafkaException {
        try {
            this.transportLayer = transportLayer;

            // determine client principal from subject.
            Principal clientPrincipal = subject.getPrincipals().iterator().next();
            this.clientPrincipalName = clientPrincipal.getName();
            this.saslClient = createSaslClient();
        } catch (Exception e) {
            throw new KafkaException("Failed to configure SaslClientAuthenticator", e);
        }
    }

    private SaslClient createSaslClient() {
        try {
            return Subject.doAs(subject, new PrivilegedExceptionAction<SaslClient>() {
                public SaslClient run() throws SaslException {
                    String[] mechs = {"GSSAPI"};
                    LOG.debug("Creating SaslClient: client={};service={};serviceHostname={};mechs={}",
                        clientPrincipalName, servicePrincipal, host, Arrays.toString(mechs));
                    return Sasl.createSaslClient(mechs, clientPrincipalName, servicePrincipal, host, null,
                            new ClientCallbackHandler());
                }
            });
        } catch (PrivilegedActionException e) {
            throw new KafkaException("Failed to create SaslClient", e.getCause());
        }
    }

    /**
     * Sends an empty message to the server to initiate the authentication process. It then evaluates server challenges
     * via `SaslClient.evaluateChallenge` and returns client responses until authentication succeeds or fails.
     *
     * The messages are sent and received as size delimited bytes that consists of a 4 byte network-ordered size N
     * followed by N bytes representing the opaque payload.
     */
    public void authenticate() throws IOException {
        if (netOutBuffer != null && !flushNetOutBufferAndUpdateInterestOps())
            return;

        switch (saslState) {
            case INITIAL:
                sendSaslToken(new byte[0]);
                saslState = SaslState.INTERMEDIATE;
                break;
            case INTERMEDIATE:
                if (netInBuffer == null) netInBuffer = new NetworkReceive(node);
                netInBuffer.readFrom(transportLayer);
                if (netInBuffer.complete()) {
                    netInBuffer.payload().rewind();
                    byte[] serverToken = new byte[netInBuffer.payload().remaining()];
                    netInBuffer.payload().get(serverToken, 0, serverToken.length);
                    netInBuffer = null; // reset the networkReceive as we read all the data.
                    sendSaslToken(serverToken);
                }
                if (saslClient.isComplete()) {
                    saslState = SaslState.COMPLETE;
                    transportLayer.removeInterestOps(SelectionKey.OP_WRITE);
                }
                break;
            case COMPLETE:
                break;
            case FAILED:
                throw new IOException("SASL handshake failed");
        }
    }

    private void sendSaslToken(byte[] serverToken) throws IOException {
        if (!saslClient.isComplete()) {
            try {
                byte[] saslToken = createSaslToken(serverToken);
                if (saslToken != null) {
                    netOutBuffer = new NetworkSend(node, ByteBuffer.wrap(saslToken));
                    flushNetOutBufferAndUpdateInterestOps();
                }
            } catch (IOException e) {
                saslState = SaslState.FAILED;
                throw e;
            }
        }
    }

    private boolean flushNetOutBufferAndUpdateInterestOps() throws IOException {
        boolean flushedCompletely = flushNetOutBuffer();
        if (flushedCompletely)
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE);
        else
            transportLayer.addInterestOps(SelectionKey.OP_WRITE);
        return flushedCompletely;
    }

    public Principal principal() {
        return new SimplePrincipal(clientPrincipalName);
    }

    public boolean complete() {
        return saslState == SaslState.COMPLETE;
    }

    public void close() throws IOException {
        saslClient.dispose();
    }

    private byte[] createSaslToken(final byte[] saslToken) throws SaslException {
        if (saslToken == null)
            throw new SaslException("Error authenticating with the Kafka Broker: received a `null` saslToken.");

        try {
            return Subject.doAs(subject, new PrivilegedExceptionAction<byte[]>() {
                public byte[] run() throws SaslException {
                    return saslClient.evaluateChallenge(saslToken);
                }
            });
        } catch (PrivilegedActionException e) {
            String error = "An error: (" + e + ") occurred when evaluating SASL token received from the Kafka Broker.";
            // Try to provide hints to use about what went wrong so they can fix their configuration.
            // TODO: introspect about e: look for GSS information.
            final String unknownServerErrorText =
                "(Mechanism level: Server not found in Kerberos database (7) - UNKNOWN_SERVER)";
            if (e.toString().indexOf(unknownServerErrorText) > -1) {
                error += " This may be caused by Java's being unable to resolve the Kafka Broker's" +
                    " hostname correctly. You may want to try to adding" +
                    " '-Dsun.net.spi.nameservice.provider.1=dns,sun' to your client's JVMFLAGS environment." +
                    " Users must configure FQDN of kafka brokers when authenticating using SASL and" +
                    " `socketChannel.socket().getInetAddress().getHostName()` must match the hostname in `principal/hostname@realm`";
            }
            error += " Kafka Client will go to AUTH_FAILED state.";
            //Unwrap the SaslException inside `PrivilegedActionException`
            throw new SaslException(error, e.getCause());
        }
    }

    private boolean flushNetOutBuffer() throws IOException {
        if (!netOutBuffer.completed()) {
            netOutBuffer.writeTo(transportLayer);
        }
        return netOutBuffer.completed();
    }

    public static class ClientCallbackHandler implements CallbackHandler {

        public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback) {
                    NameCallback nc = (NameCallback) callback;
                    nc.setName(nc.getDefaultName());
                } else if (callback instanceof PasswordCallback) {
                    // Call `setPassword` once we support obtaining a password from the user and update message below
                    throw new UnsupportedCallbackException(callback, "Could not login: the client is being asked for a password, but the Kafka" +
                             " client code does not currently support obtaining a password from the user." +
                             " Make sure -Djava.security.auth.login.config property passed to JVM and" +
                             " the client is configured to use a ticket cache (using" +
                             " the JAAS configuration setting 'useTicketCache=true)'. Make sure you are using" +
                             " FQDN of the Kafka broker you are trying to connect to.");
                } else if (callback instanceof RealmCallback) {
                    RealmCallback rc = (RealmCallback) callback;
                    rc.setText(rc.getDefaultText());
                } else if (callback instanceof AuthorizeCallback) {
                    AuthorizeCallback ac = (AuthorizeCallback) callback;
                    String authId = ac.getAuthenticationID();
                    String authzId = ac.getAuthorizationID();
                    ac.setAuthorized(authId.equals(authzId));
                    if (ac.isAuthorized())
                        ac.setAuthorizedID(authzId);
                } else {
                    throw new UnsupportedCallbackException(callback, "Unrecognized SASL ClientCallback");
                }
            }
        }
    }
}
