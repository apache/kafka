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
package org.apache.kafka.common.security.authenticator;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.IllegalSaslStateException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.UnsupportedSaslMechanismException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.network.Authenticator;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.network.TransportLayer;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.RequestAndSize;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslAuthenticateResponse;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.auth.SaslAuthenticationContext;
import org.apache.kafka.common.security.kerberos.KerberosName;
import org.apache.kafka.common.security.kerberos.KerberosShortNamer;
import org.apache.kafka.common.security.scram.ScramLoginModule;
import org.apache.kafka.common.security.scram.internal.ScramMechanism;
import org.apache.kafka.common.utils.Utils;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SaslServerAuthenticator implements Authenticator {

    // GSSAPI limits requests to 64K, but we allow a bit extra for custom SASL mechanisms
    static final int MAX_RECEIVE_SIZE = 524288;
    private static final Logger LOG = LoggerFactory.getLogger(SaslServerAuthenticator.class);
    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    private enum SaslState {
        INITIAL_REQUEST,               // May be GSSAPI token, SaslHandshake or ApiVersions
        HANDSHAKE_OR_VERSIONS_REQUEST, // May be SaslHandshake or ApiVersions
        HANDSHAKE_REQUEST,             // After an ApiVersions request, next request must be SaslHandshake
        AUTHENTICATE,                  // Authentication tokens (SaslHandshake v1 and above indicate SaslAuthenticate headers)
        COMPLETE,                      // Authentication completed successfully
        FAILED                         // Authentication failed
    }

    private final SecurityProtocol securityProtocol;
    private final ListenerName listenerName;
    private final String connectionId;
    private final Map<String, Subject> subjects;
    private final TransportLayer transportLayer;
    private final Set<String> enabledMechanisms;
    private final Map<String, ?> configs;
    private final KafkaPrincipalBuilder principalBuilder;
    private final Map<String, AuthenticateCallbackHandler> callbackHandlers;

    // Current SASL state
    private SaslState saslState = SaslState.INITIAL_REQUEST;
    // Next SASL state to be set when outgoing writes associated with the current SASL state complete
    private SaslState pendingSaslState = null;
    // Exception that will be thrown by `authenticate()` when SaslState is set to FAILED after outbound writes complete
    private AuthenticationException pendingException = null;
    private SaslServer saslServer;
    private String saslMechanism;

    // buffers used in `authenticate`
    private NetworkReceive netInBuffer;
    private Send netOutBuffer;
    // flag indicating if sasl tokens are sent as Kafka SaslAuthenticate request/responses
    private boolean enableKafkaSaslAuthenticateHeaders;

    public SaslServerAuthenticator(Map<String, ?> configs,
                                   Map<String, AuthenticateCallbackHandler> callbackHandlers,
                                   String connectionId,
                                   Map<String, Subject> subjects,
                                   KerberosShortNamer kerberosNameParser,
                                   ListenerName listenerName,
                                   SecurityProtocol securityProtocol,
                                   TransportLayer transportLayer) throws IOException {
        this.callbackHandlers = callbackHandlers;
        this.connectionId = connectionId;
        this.subjects = subjects;
        this.listenerName = listenerName;
        this.securityProtocol = securityProtocol;
        this.enableKafkaSaslAuthenticateHeaders = false;
        this.transportLayer = transportLayer;

        this.configs = configs;
        @SuppressWarnings("unchecked")
        List<String> enabledMechanisms = (List<String>) this.configs.get(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG);
        if (enabledMechanisms == null || enabledMechanisms.isEmpty())
            throw new IllegalArgumentException("No SASL mechanisms are enabled");
        this.enabledMechanisms = new HashSet<>(enabledMechanisms);
        for (String mechanism : enabledMechanisms) {
            if (!callbackHandlers.containsKey(mechanism))
                throw new IllegalArgumentException("Callback handler not specified for SASL mechanism " + mechanism);
            if (!subjects.containsKey(mechanism))
                throw new IllegalArgumentException("Subject cannot be null for SASL mechanism " + mechanism);
        }

        // Note that the old principal builder does not support SASL, so we do not need to pass the
        // authenticator or the transport layer
        this.principalBuilder = ChannelBuilders.createPrincipalBuilder(configs, null, null, kerberosNameParser);
    }

    private void createSaslServer(String mechanism) throws IOException {
        this.saslMechanism = mechanism;
        Subject subject = subjects.get(mechanism);
        final AuthenticateCallbackHandler callbackHandler = callbackHandlers.get(mechanism);
        if (mechanism.equals(SaslConfigs.GSSAPI_MECHANISM)) {
            saslServer = createSaslKerberosServer(callbackHandler, configs, subject);
        } else {
            try {
                saslServer = Subject.doAs(subject, new PrivilegedExceptionAction<SaslServer>() {
                    public SaslServer run() throws SaslException {
                        return Sasl.createSaslServer(saslMechanism, "kafka", serverAddress().getHostName(),
                                configs, callbackHandler);
                    }
                });
            } catch (PrivilegedActionException e) {
                throw new SaslException("Kafka Server failed to create a SaslServer to interact with a client during session authentication", e.getCause());
            }
        }
    }

    private SaslServer createSaslKerberosServer(final AuthenticateCallbackHandler saslServerCallbackHandler, final Map<String, ?> configs, Subject subject) throws IOException {
        // server is using a JAAS-authenticated subject: determine service principal name and hostname from kafka server's subject.
        final String servicePrincipal = SaslClientAuthenticator.firstPrincipal(subject);
        KerberosName kerberosName;
        try {
            kerberosName = KerberosName.parse(servicePrincipal);
        } catch (IllegalArgumentException e) {
            throw new KafkaException("Principal has name with unexpected format " + servicePrincipal);
        }
        final String servicePrincipalName = kerberosName.serviceName();
        final String serviceHostname = kerberosName.hostName();

        LOG.debug("Creating SaslServer for {} with mechanism {}", kerberosName, saslMechanism);

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
                    return Sasl.createSaslServer(saslMechanism, servicePrincipalName, serviceHostname, configs, saslServerCallbackHandler);
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
    @Override
    public void authenticate() throws IOException {
        if (netOutBuffer != null && !flushNetOutBufferAndUpdateInterestOps())
            return;

        if (saslServer != null && saslServer.isComplete()) {
            setSaslState(SaslState.COMPLETE);
            return;
        }

        if (netInBuffer == null) netInBuffer = new NetworkReceive(MAX_RECEIVE_SIZE, connectionId);

        netInBuffer.readFrom(transportLayer);

        if (netInBuffer.complete()) {
            netInBuffer.payload().rewind();
            byte[] clientToken = new byte[netInBuffer.payload().remaining()];
            netInBuffer.payload().get(clientToken, 0, clientToken.length);
            netInBuffer = null; // reset the networkReceive as we read all the data.
            try {
                switch (saslState) {
                    case HANDSHAKE_OR_VERSIONS_REQUEST:
                    case HANDSHAKE_REQUEST:
                        handleKafkaRequest(clientToken);
                        break;
                    case INITIAL_REQUEST:
                        if (handleKafkaRequest(clientToken))
                            break;
                        // For default GSSAPI, fall through to authenticate using the client token as the first GSSAPI packet.
                        // This is required for interoperability with 0.9.0.x clients which do not send handshake request
                    case AUTHENTICATE:
                        handleSaslToken(clientToken);
                        // When the authentication exchange is complete and no more tokens are expected from the client,
                        // update SASL state. Current SASL state will be updated when outgoing writes to the client complete.
                        if (saslServer.isComplete())
                            setSaslState(SaslState.COMPLETE);
                        break;
                    default:
                        break;
                }
            } catch (SaslException | AuthenticationException e) {
                // Exception will be propagated after response is sent to client
                AuthenticationException authException = (e instanceof AuthenticationException) ?
                        (AuthenticationException) e : new AuthenticationException("SASL authentication failed", e);
                setSaslState(SaslState.FAILED, authException);
            } catch (Exception e) {
                // In the case of IOExceptions and other unexpected exceptions, fail immediately
                saslState = SaslState.FAILED;
                throw e;
            }
        }
    }

    @Override
    public KafkaPrincipal principal() {
        SaslAuthenticationContext context = new SaslAuthenticationContext(saslServer, securityProtocol, clientAddress(), listenerName.value());
        KafkaPrincipal principal = principalBuilder.build(context);
        if (ScramMechanism.isScram(saslMechanism) && Boolean.parseBoolean((String) saslServer.getNegotiatedProperty(ScramLoginModule.TOKEN_AUTH_CONFIG))) {
            principal.tokenAuthenticated(true);
        }
        return principal;
    }

    @Override
    public boolean complete() {
        return saslState == SaslState.COMPLETE;
    }

    @Override
    public void close() throws IOException {
        if (principalBuilder instanceof Closeable)
            Utils.closeQuietly((Closeable) principalBuilder, "principal builder");
        if (saslServer != null)
            saslServer.dispose();
    }

    private void setSaslState(SaslState saslState) throws IOException {
        setSaslState(saslState, null);
    }

    private void setSaslState(SaslState saslState, AuthenticationException exception) throws IOException {
        if (netOutBuffer != null && !netOutBuffer.completed()) {
            pendingSaslState = saslState;
            pendingException = exception;
        } else {
            this.saslState = saslState;
            LOG.debug("Set SASL server state to {}", saslState);
            this.pendingSaslState = null;
            this.pendingException = null;
            if (exception != null)
                throw exception;
        }
    }

    private boolean flushNetOutBufferAndUpdateInterestOps() throws IOException {
        boolean flushedCompletely = flushNetOutBuffer();
        if (flushedCompletely) {
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE);
            if (pendingSaslState != null)
                setSaslState(pendingSaslState, pendingException);
        } else
            transportLayer.addInterestOps(SelectionKey.OP_WRITE);
        return flushedCompletely;
    }

    private boolean flushNetOutBuffer() throws IOException {
        if (!netOutBuffer.completed())
            netOutBuffer.writeTo(transportLayer);
        return netOutBuffer.completed();
    }

    private InetAddress serverAddress() {
        return transportLayer.socketChannel().socket().getLocalAddress();
    }

    private InetAddress clientAddress() {
        return transportLayer.socketChannel().socket().getInetAddress();
    }

    private void handleSaslToken(byte[] clientToken) throws IOException {
        if (!enableKafkaSaslAuthenticateHeaders) {
            byte[] response = saslServer.evaluateResponse(clientToken);
            if (response != null) {
                netOutBuffer = new NetworkSend(connectionId, ByteBuffer.wrap(response));
                flushNetOutBufferAndUpdateInterestOps();
            }
        } else {
            ByteBuffer requestBuffer = ByteBuffer.wrap(clientToken);
            RequestHeader header = RequestHeader.parse(requestBuffer);
            ApiKeys apiKey = header.apiKey();
            short version = header.apiVersion();
            RequestContext requestContext = new RequestContext(header, connectionId, clientAddress(),
                    KafkaPrincipal.ANONYMOUS, listenerName, securityProtocol);
            RequestAndSize requestAndSize = requestContext.parseRequest(requestBuffer);
            if (apiKey != ApiKeys.SASL_AUTHENTICATE) {
                IllegalSaslStateException e = new IllegalSaslStateException("Unexpected Kafka request of type " + apiKey + " during SASL authentication.");
                sendKafkaResponse(requestContext, requestAndSize.request.getErrorResponse(e));
                throw e;
            }
            if (!apiKey.isVersionSupported(version)) {
                // We cannot create an error response if the request version of SaslAuthenticate is not supported
                // This should not normally occur since clients typically check supported versions using ApiVersionsRequest
                throw new UnsupportedVersionException("Version " + version + " is not supported for apiKey " + apiKey);
            }
            SaslAuthenticateRequest saslAuthenticateRequest = (SaslAuthenticateRequest) requestAndSize.request;

            try {
                byte[] responseToken = saslServer.evaluateResponse(Utils.readBytes(saslAuthenticateRequest.saslAuthBytes()));
                // For versions with SASL_AUTHENTICATE header, send a response to SASL_AUTHENTICATE request even if token is empty.
                ByteBuffer responseBuf = responseToken == null ? EMPTY_BUFFER : ByteBuffer.wrap(responseToken);
                sendKafkaResponse(requestContext, new SaslAuthenticateResponse(Errors.NONE, null, responseBuf));
            } catch (SaslAuthenticationException | SaslException e) {
                String errorMessage = e instanceof SaslAuthenticationException ? e.getMessage() :
                    "Authentication failed due to invalid credentials with SASL mechanism " + saslMechanism;
                sendKafkaResponse(requestContext, new SaslAuthenticateResponse(Errors.SASL_AUTHENTICATION_FAILED,
                        errorMessage));
                throw e;
            }
        }
    }

    private boolean handleKafkaRequest(byte[] requestBytes) throws IOException, AuthenticationException {
        boolean isKafkaRequest = false;
        String clientMechanism = null;
        try {
            ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);
            RequestHeader header = RequestHeader.parse(requestBuffer);
            ApiKeys apiKey = header.apiKey();

            // A valid Kafka request header was received. SASL authentication tokens are now expected only
            // following a SaslHandshakeRequest since this is not a GSSAPI client token from a Kafka 0.9.0.x client.
            if (saslState == SaslState.INITIAL_REQUEST)
                setSaslState(SaslState.HANDSHAKE_OR_VERSIONS_REQUEST);
            isKafkaRequest = true;

            // Raise an error prior to parsing if the api cannot be handled at this layer. This avoids
            // unnecessary exposure to some of the more complex schema types.
            if (apiKey != ApiKeys.API_VERSIONS && apiKey != ApiKeys.SASL_HANDSHAKE)
                throw new IllegalSaslStateException("Unexpected Kafka request of type " + apiKey + " during SASL handshake.");

            LOG.debug("Handling Kafka request {}", apiKey);


            RequestContext requestContext = new RequestContext(header, connectionId, clientAddress(),
                    KafkaPrincipal.ANONYMOUS, listenerName, securityProtocol);
            RequestAndSize requestAndSize = requestContext.parseRequest(requestBuffer);
            if (apiKey == ApiKeys.API_VERSIONS)
                handleApiVersionsRequest(requestContext, (ApiVersionsRequest) requestAndSize.request);
            else
                clientMechanism = handleHandshakeRequest(requestContext, (SaslHandshakeRequest) requestAndSize.request);
        } catch (InvalidRequestException e) {
            if (saslState == SaslState.INITIAL_REQUEST) {
                // InvalidRequestException is thrown if the request is not in Kafka format or if the API key
                // is invalid. For compatibility with 0.9.0.x where the first packet is a GSSAPI token
                // starting with 0x60, revert to GSSAPI for both these exceptions.
                if (LOG.isDebugEnabled()) {
                    StringBuilder tokenBuilder = new StringBuilder();
                    for (byte b : requestBytes) {
                        tokenBuilder.append(String.format("%02x", b));
                        if (tokenBuilder.length() >= 20)
                             break;
                    }
                    LOG.debug("Received client packet of length {} starting with bytes 0x{}, process as GSSAPI packet", requestBytes.length, tokenBuilder);
                }
                if (enabledMechanisms.contains(SaslConfigs.GSSAPI_MECHANISM)) {
                    LOG.debug("First client packet is not a SASL mechanism request, using default mechanism GSSAPI");
                    clientMechanism = SaslConfigs.GSSAPI_MECHANISM;
                } else
                    throw new UnsupportedSaslMechanismException("Exception handling first SASL packet from client, GSSAPI is not supported by server", e);
            } else
                throw e;
        }
        if (clientMechanism != null) {
            createSaslServer(clientMechanism);
            setSaslState(SaslState.AUTHENTICATE);
        }
        return isKafkaRequest;
    }

    private String handleHandshakeRequest(RequestContext context, SaslHandshakeRequest handshakeRequest) throws IOException, UnsupportedSaslMechanismException {
        String clientMechanism = handshakeRequest.mechanism();
        short version = context.header.apiVersion();
        if (version >= 1)
            this.enableKafkaSaslAuthenticateHeaders(true);
        if (enabledMechanisms.contains(clientMechanism)) {
            LOG.debug("Using SASL mechanism '{}' provided by client", clientMechanism);
            sendKafkaResponse(context, new SaslHandshakeResponse(Errors.NONE, enabledMechanisms));
            return clientMechanism;
        } else {
            LOG.debug("SASL mechanism '{}' requested by client is not supported", clientMechanism);
            sendKafkaResponse(context, new SaslHandshakeResponse(Errors.UNSUPPORTED_SASL_MECHANISM, enabledMechanisms));
            throw new UnsupportedSaslMechanismException("Unsupported SASL mechanism " + clientMechanism);
        }
    }

    // Visible to override for testing
    protected ApiVersionsResponse apiVersionsResponse() {
        return ApiVersionsResponse.defaultApiVersionsResponse();
    }

    // Visible to override for testing
    protected void enableKafkaSaslAuthenticateHeaders(boolean flag) {
        this.enableKafkaSaslAuthenticateHeaders = flag;
    }

    private void handleApiVersionsRequest(RequestContext context, ApiVersionsRequest apiVersionsRequest) throws IOException {
        if (saslState != SaslState.HANDSHAKE_OR_VERSIONS_REQUEST)
            throw new IllegalStateException("Unexpected ApiVersions request received during SASL authentication state " + saslState);

        if (apiVersionsRequest.hasUnsupportedRequestVersion())
            sendKafkaResponse(context, apiVersionsRequest.getErrorResponse(0, Errors.UNSUPPORTED_VERSION.exception()));
        else {
            sendKafkaResponse(context, apiVersionsResponse());
            setSaslState(SaslState.HANDSHAKE_REQUEST);
        }
    }

    private void sendKafkaResponse(RequestContext context, AbstractResponse response) throws IOException {
        sendKafkaResponse(context.buildResponse(response));
    }

    private void sendKafkaResponse(Send send) throws IOException {
        netOutBuffer = send;
        flushNetOutBufferAndUpdateInterestOps();
    }
}
