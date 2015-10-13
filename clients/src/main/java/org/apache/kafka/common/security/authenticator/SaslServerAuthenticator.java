/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.security.authenticator;

import java.io.IOException;
import java.util.Map;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.login.Configuration;
import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslException;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.network.Authenticator;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.TransportLayer;
import org.apache.kafka.common.security.auth.PrincipalBuilder;

public class SaslServerAuthenticator implements Authenticator {

    private static final Logger LOG = LoggerFactory.getLogger(SaslServerAuthenticator.class);

    private final SaslServer saslServer;
    private final Subject subject;
    private final String node;

    // assigned in `configure`
    private TransportLayer transportLayer;

    // buffers used in `authenticate`
    private NetworkReceive netInBuffer;
    private NetworkSend netOutBuffer;

    public SaslServerAuthenticator(String node, final Subject subject) throws IOException {
        this.node = node;
        this.subject = subject;
        saslServer = createSaslServer();
    }

    public void configure(TransportLayer transportLayer, PrincipalBuilder principalBuilder, Map<String, ?> configs) {
        this.transportLayer = transportLayer;
    }

    private SaslServer createSaslServer() throws IOException {
        if (subject != null) {
            // server is using a JAAS-authenticated subject: determine service principal name and hostname from kafka server's subject.
            if (subject.getPrincipals().size() > 0) {
                try {
                    final SaslServerCallbackHandler saslServerCallbackHandler = new SaslServerCallbackHandler(Configuration.getConfiguration());
                    final Principal servicePrincipal = subject.getPrincipals().iterator().next();

                    final String servicePrincipalNameAndHostname = servicePrincipal.getName();
                    int indexOf = servicePrincipalNameAndHostname.indexOf("/");
                    final String servicePrincipalName = servicePrincipalNameAndHostname.substring(0, indexOf);
                    final String serviceHostnameAndKerbDomain = servicePrincipalNameAndHostname.substring(indexOf + 1, servicePrincipalNameAndHostname.length());
                    indexOf = serviceHostnameAndKerbDomain.indexOf("@");
                    final String serviceHostname = serviceHostnameAndKerbDomain.substring(0, indexOf);
                    final String mech = "GSSAPI";

                    LOG.debug("serviceHostname is '" + serviceHostname + "'");
                    LOG.debug("servicePrincipalName is '" + servicePrincipalName + "'");
                    LOG.debug("SASL mechanism is '" + mech + "'");
                    boolean usingNativeJgss = Boolean.getBoolean("sun.security.jgss.native");
                    if (usingNativeJgss) {
                        try {
                            GSSManager manager = GSSManager.getInstance();
                            Oid krb5Mechanism = new Oid("1.2.840.113554.1.2.2");
                            GSSName gssName = manager.createName(servicePrincipalName + "@" + serviceHostname, GSSName.NT_HOSTBASED_SERVICE);
                            GSSCredential cred = manager.createCredential(gssName, GSSContext.INDEFINITE_LIFETIME, krb5Mechanism, GSSCredential.ACCEPT_ONLY);
                            subject.getPrivateCredentials().add(cred);
                        } catch (GSSException ex) {
                            LOG.warn("Cannot add private credential to subject; clients authentication may fail", ex);
                        }
                    }

                    try {
                        return Subject.doAs(subject, new PrivilegedExceptionAction<SaslServer>() {
                            public SaslServer run() {
                                try {
                                    return Sasl.createSaslServer(mech, servicePrincipalName, serviceHostname, null, saslServerCallbackHandler);
                                } catch (SaslException e) {
                                    LOG.error("Kafka Server failed to create a SaslServer to interact with a client during session authentication: " + e);
                                    return null;
                                }
                            }
                        });
                    } catch (PrivilegedActionException e) {
                        LOG.error("KafkaBroker experienced a PrivilegedActionException exception while creating a SaslServer using a JAAS principal context:" + e);
                    }
                } catch (IndexOutOfBoundsException e) {
                    LOG.error("Kafka Server principal name/hostname determination error: ", e);
                }
            }
        }
        return null;
    }

    public void authenticate() throws IOException {
        if (netOutBuffer != null && !flushNetOutBuffer()) {
            transportLayer.addInterestOps(SelectionKey.OP_WRITE);
            return;
        }

        if (saslServer.isComplete()) {
            transportLayer.addInterestOps(SelectionKey.OP_READ);
            return;
        }

        if (netInBuffer == null) netInBuffer = new NetworkReceive(node);

        netInBuffer.readFrom(transportLayer);

        if (netInBuffer.complete()) {
            netInBuffer.payload().rewind();
            byte[] clientToken = new byte[netInBuffer.payload().remaining()];
            netInBuffer.payload().get(clientToken, 0, clientToken.length);
            netInBuffer = null; // reset the networkReceive as we read all the data.
            try {
                byte[] response = saslServer.evaluateResponse(clientToken);
                if (response != null) {
                    netOutBuffer = new NetworkSend(node, ByteBuffer.wrap(response));
                    if (!flushNetOutBuffer()) {
                        transportLayer.addInterestOps(SelectionKey.OP_WRITE);
                        return;
                    }
                }
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    public Principal principal() {
        return new KafkaPrincipal(KafkaPrincipal.USER_TYPE, saslServer.getAuthorizationID());
    }

    public boolean complete() {
        return saslServer.isComplete();
    }

    public void close() throws IOException {
        saslServer.dispose();
    }

    private boolean flushNetOutBuffer() throws IOException {
        if (!netOutBuffer.completed())
            netOutBuffer.writeTo(transportLayer);
        return netOutBuffer.completed();
    }
}
