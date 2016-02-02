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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.security.kerberos.KerberosName;
import org.apache.kafka.common.security.kerberos.KerberosShortNamer;
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
    private final KerberosShortNamer kerberosNamer;
    private final int maxReceiveSize;

    // assigned in `configure`
    private TransportLayer transportLayer;

    // buffers used in `authenticate`
    private NetworkReceive netInBuffer;
    private NetworkSend netOutBuffer;

    public SaslServerAuthenticator(String node, final Subject subject, KerberosShortNamer kerberosNameParser, int maxReceiveSize) throws IOException {
        if (subject == null)
            throw new IllegalArgumentException("subject cannot be null");
        if (subject.getPrincipals().isEmpty())
            throw new IllegalArgumentException("subject must have at least one principal");
        this.node = node;
        this.subject = subject;
        this.kerberosNamer = kerberosNameParser;
        this.maxReceiveSize = maxReceiveSize;
        saslServer = createSaslServer();
    }

    public void configure(TransportLayer transportLayer, PrincipalBuilder principalBuilder, Map<String, ?> configs) {
        this.transportLayer = transportLayer;
    }

    private SaslServer createSaslServer() throws IOException {
        // server is using a JAAS-authenticated subject: determine service principal name and hostname from kafka server's subject.
        final SaslServerCallbackHandler saslServerCallbackHandler = new SaslServerCallbackHandler(
                Configuration.getConfiguration(), kerberosNamer);
        final Principal servicePrincipal = subject.getPrincipals().iterator().next();
        KerberosName kerberosName;
        try {
            kerberosName = KerberosName.parse(servicePrincipal.getName());
        } catch (IllegalArgumentException e) {
            throw new KafkaException("Principal has name with unexpected format " + servicePrincipal);
        }
        final String servicePrincipalName = kerberosName.serviceName();
        final String serviceHostname = kerberosName.hostName();

        final String mech = "GSSAPI";

        LOG.debug("Creating SaslServer for {} with mechanism {}", kerberosName, mech);

        // As described in http://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/jgss-features.html:
        // "To enable Java GSS to delegate to the native GSS library and its list of native mechanisms,
        // set the system property "sun.security.jgss.native" to true"
        // "In addition, when performing operations as a particular Subject, for example, Subject.doAs(...)
        // or Subject.doAsPrivileged(...), the to-be-used GSSCredential should be added to Subject's
        // private credential set. Otherwise, the GSS operations will fail since no credential is found."
        boolean usingNativeJgss = Boolean.getBoolean("sun.security.jgss.native");
        if (usingNativeJgss) {
            try {
                GSSManager manager = GSSManager.getInstance();
                // This Oid is used to represent the Kerberos version 5 GSS-API mechanism. It is defined in
                // RFC 1964.
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
                public SaslServer run() throws SaslException {
                    return Sasl.createSaslServer(mech, servicePrincipalName, serviceHostname, null, saslServerCallbackHandler);
                }
            });
        } catch (PrivilegedActionException e) {
            throw new SaslException("Kafka Server failed to create a SaslServer to interact with a client during session authentication", e.getCause());
        }
    }

    /**
     * Evaluates client responses via `SaslServer.evaluateResponse` and returns the issued challenge to the client until
     * authentication succeeds or fails.
     *
     * The messages are sent and received as size delimited bytes that consists of a 4 byte network-ordered size N
     * followed by N bytes representing the opaque payload.
     */
    public void authenticate() throws IOException {
        if (netOutBuffer != null && !flushNetOutBufferAndUpdateInterestOps())
            return;

        if (saslServer.isComplete()) {
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE);
            return;
        }

        if (netInBuffer == null) netInBuffer = new NetworkReceive(maxReceiveSize, node);

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
                    flushNetOutBufferAndUpdateInterestOps();
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

    private boolean flushNetOutBufferAndUpdateInterestOps() throws IOException {
        boolean flushedCompletely = flushNetOutBuffer();
        if (flushedCompletely)
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE);
        else
            transportLayer.addInterestOps(SelectionKey.OP_WRITE);
        return flushedCompletely;
    }

    private boolean flushNetOutBuffer() throws IOException {
        if (!netOutBuffer.completed())
            netOutBuffer.writeTo(transportLayer);
        return netOutBuffer.completed();
    }
}
