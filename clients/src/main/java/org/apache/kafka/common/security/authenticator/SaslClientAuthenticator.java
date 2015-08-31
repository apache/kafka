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
import org.apache.kafka.common.security.auth.PrincipalBuilder;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.kerberos.KerberosName;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.TransportLayer;
import org.apache.kafka.common.KafkaException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SaslClientAuthenticator implements Authenticator {

    private static final Logger LOG = LoggerFactory.getLogger(SaslClientAuthenticator.class);
    private SaslClient saslClient;
    private Subject subject;
    private String servicePrincipal;
    private PrincipalBuilder principalBuilder;
    private String host;
    private String node = "0";
    private TransportLayer transportLayer;
    private NetworkReceive netInBuffer;
    private NetworkSend netOutBuffer;
    private byte[] saslToken = new byte[0];

    public enum SaslState {
        INITIAL, INTERMEDIATE, COMPLETE, FAILED
    }

    private SaslState saslState = SaslState.INITIAL;

    public SaslClientAuthenticator(Subject subject, String servicePrincipal, String host) throws IOException {
        this.subject = subject;
        this.host = host;
        this.servicePrincipal = servicePrincipal;
        this.saslClient = createSaslClient();
    }

    public void configure(TransportLayer transportLayer, PrincipalBuilder principalBuilder) throws KafkaException {
        this.transportLayer = transportLayer;
        this.principalBuilder = principalBuilder;
        this.saslClient = createSaslClient();
    }

    private SaslClient createSaslClient() {
        final Object[] principals = subject.getPrincipals().toArray();
        // determine client principal from subject.
        final Principal clientPrincipal = (Principal) principals[0];
        final KerberosName clientKerberosName = new KerberosName(clientPrincipal.getName());
        // assume that server and client are in the same realm (by default; unless the config
        // "kafka.server.realm" is set).
        String serverRealm = System.getProperty("kafka.server.realm", clientKerberosName.realm());
        KerberosName serviceKerberosName = new KerberosName(servicePrincipal + "@" + serverRealm);
        final String clientPrincipalName = clientKerberosName.toString();
        try {
            saslClient = Subject.doAs(subject, new PrivilegedExceptionAction<SaslClient>() {
                    public SaslClient run() throws SaslException {
                        LOG.debug("Client will use GSSAPI as SASL mechanism.");
                        String[] mechs = {"GSSAPI"};
                        LOG.debug("creating sasl client: client=" + clientPrincipalName + ";service=" + servicePrincipal + ";serviceHostname=" + host);
                        SaslClient saslClient = Sasl.createSaslClient(mechs, clientPrincipalName, servicePrincipal, host, null, new ClientCallbackHandler(null));
                        return saslClient;
                    }
                });
            return saslClient;
        } catch (Exception e) {
            LOG.error("Exception while trying to create SASL client", e);
            throw new KafkaException("Failed to create SASL client", e);
        }
    }

    public void authenticate() throws IOException {
        if (netOutBuffer != null && !flushNetOutBuffer()) {
            transportLayer.addInterestOps(SelectionKey.OP_WRITE);
            return;
        }

        if (saslClient.isComplete()) {
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE);
            saslState = SaslState.COMPLETE;
            return;
        }

        byte[] serverToken = new byte[0];

        if (saslState == SaslState.INTERMEDIATE) {
            if (netInBuffer == null) netInBuffer = new NetworkReceive(node);
            long readLen = netInBuffer.readFrom(transportLayer);
            if (readLen != 0 && !netInBuffer.complete()) {
                netInBuffer.payload().rewind();
                serverToken = new byte[netInBuffer.payload().remaining()];
                netInBuffer.payload().get(serverToken, 0, serverToken.length);
                netInBuffer = null; // reset the networkReceive as we read all the data.
            }
        } else if (saslState == SaslState.INITIAL) {
            saslState = SaslState.INTERMEDIATE;
        }

        if (!(saslClient.isComplete())) {
            try {
                saslToken = createSaslToken(serverToken);
                if (saslToken != null) {
                    netOutBuffer = new NetworkSend(node, ByteBuffer.wrap(saslToken));
                    if (!flushNetOutBuffer())
                        transportLayer.addInterestOps(SelectionKey.OP_WRITE);
                }
            } catch (SaslException se) {
                saslState = SaslState.FAILED;
                throw new IOException("Unable to authenticate using SASL " + se);
            }
        }
    }

    public Principal principal() {
        return new KafkaPrincipal("ANONYMOUS");
    }

    public boolean complete() {
        return saslClient.isComplete() && saslState == SaslState.COMPLETE;
    }

    public void close() throws IOException {
        saslClient.dispose();
    }

    private byte[] createSaslToken(final byte[] saslToken) throws SaslException {
        if (saslToken == null) {
            throw new SaslException("Error in authenticating with a Kafka Broker: the kafka broker saslToken is null.");
        }

        try {
            final byte[] retval =
                Subject.doAs(subject, new PrivilegedExceptionAction<byte[]>() {
                        public byte[] run() throws SaslException {
                            return saslClient.evaluateChallenge(saslToken);
                        }
                    });
            return retval;
        } catch (PrivilegedActionException e) {
            String error = "An error: (" + e + ") occurred when evaluating Kafka Brokers " +
                      " received SASL token.";
            // Try to provide hints to use about what went wrong so they can fix their configuration.
            // TODO: introspect about e: look for GSS information.
            final String unknownServerErrorText =
                "(Mechanism level: Server not found in Kerberos database (7) - UNKNOWN_SERVER)";
            if (e.toString().indexOf(unknownServerErrorText) > -1) {
                error += " This may be caused by Java's being unable to resolve the Kafka Broker's" +
                    " hostname correctly. You may want to try to adding" +
                    " '-Dsun.net.spi.nameservice.provider.1=dns,sun' to your client's JVMFLAGS environment." +
                    " Users must configure FQDN of kafka brokers when authenticating using SASL.";
            }
            error += " Kafka Client will go to AUTH_FAILED state.";
            LOG.error(error);
            throw new SaslException(error);
        }
    }

    private boolean flushNetOutBuffer() throws IOException {
        if (!netOutBuffer.completed()) {
            netOutBuffer.writeTo(transportLayer);
        }
        return netOutBuffer.completed();
    }

    public static class ClientCallbackHandler implements CallbackHandler {
        private String password = null;

        public ClientCallbackHandler(String password) {
            this.password = password;
        }

        public void handle(Callback[] callbacks) throws
          UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback) {
                    NameCallback nc = (NameCallback) callback;
                    nc.setName(nc.getDefaultName());
                } else {
                    if (callback instanceof PasswordCallback) {
                        PasswordCallback pc = (PasswordCallback) callback;
                        if (password != null) {
                            pc.setPassword(this.password.toCharArray());
                        } else {
                            LOG.warn("Could not login: the client is being asked for a password, but the Kafka" +
                                     " client code does not currently support obtaining a password from the user." +
                                     " Make sure -Djava.security.auth.login.config property passed to JVM and " +
                                     " the client is configured to use a ticket cache (using" +
                                     " the JAAS configuration setting 'useTicketCache=true)'. Make sure you are using" +
                                     " FQDN of the Kafka broker you are trying to connect to. ");
                        }
                    } else {
                        if (callback instanceof RealmCallback) {
                            RealmCallback rc = (RealmCallback) callback;
                            rc.setText(rc.getDefaultText());
                        } else {
                            if (callback instanceof AuthorizeCallback) {
                                AuthorizeCallback ac = (AuthorizeCallback) callback;
                                String authid = ac.getAuthenticationID();
                                String authzid = ac.getAuthorizationID();

                                if (authid.equals(authzid))
                                    ac.setAuthorized(true);
                                else
                                    ac.setAuthorized(false);


                                if (ac.isAuthorized())
                                    ac.setAuthorizedID(authzid);
                            } else {
                                throw new UnsupportedCallbackException(callback, "Unrecognized SASL ClientCallback");
                            }
                        }
                    }
                }
            }
        }
    }

}
