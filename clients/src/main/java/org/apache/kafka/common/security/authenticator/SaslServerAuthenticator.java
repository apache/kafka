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
import org.apache.kafka.common.internals.SecurityManagerCompatibility;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.network.Authenticator;
import org.apache.kafka.common.network.ByteBufferSend;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.ChannelMetadataRegistry;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.InvalidReceiveException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.ReauthenticationContext;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.network.SslTransportLayer;
import org.apache.kafka.common.network.TransportLayer;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
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
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde;
import org.apache.kafka.common.security.auth.SaslAuthenticationContext;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.kerberos.KerberosError;
import org.apache.kafka.common.security.kerberos.KerberosName;
import org.apache.kafka.common.security.kerberos.KerberosShortNamer;
import org.apache.kafka.common.security.scram.ScramLoginModule;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import javax.net.ssl.SSLSession;
import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

public class SaslServerAuthenticator implements Authenticator {
    private static final Logger LOG = LoggerFactory.getLogger(SaslServerAuthenticator.class);

    /**
     * The internal state transitions for initial authentication of a channel on the
     * server side are declared in order, starting with {@link #INITIAL_REQUEST} and
     * ending in either {@link #COMPLETE} or {@link #FAILED}.
     * <p>
     * Re-authentication of a channel on the server side starts with the state
     * {@link #REAUTH_PROCESS_HANDSHAKE}. It may then flow to
     * {@link #REAUTH_BAD_MECHANISM} before a transition to {@link #FAILED} if
     * re-authentication is attempted with a mechanism different than the original
     * one; otherwise it joins the authentication flow at the {@link #AUTHENTICATE}
     * state and likewise ends at either {@link #COMPLETE} or {@link #FAILED}.
     */
    private enum SaslState {
        INITIAL_REQUEST,               // May be GSSAPI token, SaslHandshake or ApiVersions for authentication
        HANDSHAKE_OR_VERSIONS_REQUEST, // May be SaslHandshake or ApiVersions
        HANDSHAKE_REQUEST,             // After an ApiVersions request, next request must be SaslHandshake
        AUTHENTICATE,                  // Authentication tokens (SaslHandshake v1 and above indicate SaslAuthenticate headers)
        COMPLETE,                      // Authentication completed successfully
        FAILED,                        // Authentication failed
        REAUTH_PROCESS_HANDSHAKE,      // Initial state for re-authentication, processes SASL handshake request
        REAUTH_BAD_MECHANISM,          // When re-authentication requested with wrong mechanism, generate exception
    }

    private final SecurityProtocol securityProtocol;
    private final ListenerName listenerName;
    private final String connectionId;
    private final Map<String, Subject> subjects;
    private final TransportLayer transportLayer;
    private final List<String> enabledMechanisms;
    private final Map<String, ?> configs;
    private final KafkaPrincipalBuilder principalBuilder;
    private final Map<String, AuthenticateCallbackHandler> callbackHandlers;
    private final Map<String, Long> connectionsMaxReauthMsByMechanism;
    private final Time time;
    private final ReauthInfo reauthInfo;
    private final ChannelMetadataRegistry metadataRegistry;
    private final Function<Short, ApiVersionsResponse> apiVersionSupplier;

    // Current SASL state
    private SaslState saslState = SaslState.INITIAL_REQUEST;
    // Next SASL state to be set when outgoing writes associated with the current SASL state complete
    private SaslState pendingSaslState = null;
    // Exception that will be thrown by `authenticate()` when SaslState is set to FAILED after outbound writes complete
    private AuthenticationException pendingException = null;
    private SaslServer saslServer;
    private String saslMechanism;

    // buffers used in `authenticate`
    private Integer saslAuthRequestMaxReceiveSize;
    private NetworkReceive netInBuffer;
    private Send netOutBuffer;
    private Send authenticationFailureSend = null;
    // flag indicating if sasl tokens are sent as Kafka SaslAuthenticate request/responses
    private boolean enableKafkaSaslAuthenticateHeaders;

    public SaslServerAuthenticator(Map<String, ?> configs,
                                   Map<String, AuthenticateCallbackHandler> callbackHandlers,
                                   String connectionId,
                                   Map<String, Subject> subjects,
                                   KerberosShortNamer kerberosNameParser,
                                   ListenerName listenerName,
                                   SecurityProtocol securityProtocol,
                                   TransportLayer transportLayer,
                                   Map<String, Long> connectionsMaxReauthMsByMechanism,
                                   ChannelMetadataRegistry metadataRegistry,
                                   Time time,
                                   Function<Short, ApiVersionsResponse> apiVersionSupplier) {
        this.callbackHandlers = callbackHandlers;
        this.connectionId = connectionId;
        this.subjects = subjects;
        this.listenerName = listenerName;
        this.securityProtocol = securityProtocol;
        this.enableKafkaSaslAuthenticateHeaders = false;
        this.transportLayer = transportLayer;
        this.connectionsMaxReauthMsByMechanism = connectionsMaxReauthMsByMechanism;
        this.time = time;
        this.reauthInfo = new ReauthInfo();
        this.metadataRegistry = metadataRegistry;
        this.apiVersionSupplier = apiVersionSupplier;

        this.configs = configs;
        @SuppressWarnings("unchecked")
        List<String> enabledMechanisms = (List<String>) this.configs.get(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG);
        if (enabledMechanisms == null || enabledMechanisms.isEmpty())
            throw new IllegalArgumentException("No SASL mechanisms are enabled");
        this.enabledMechanisms = new ArrayList<>(new HashSet<>(enabledMechanisms));
        for (String mechanism : this.enabledMechanisms) {
            if (!callbackHandlers.containsKey(mechanism))
                throw new IllegalArgumentException("Callback handler not specified for SASL mechanism " + mechanism);
            if (!subjects.containsKey(mechanism))
                throw new IllegalArgumentException("Subject cannot be null for SASL mechanism " + mechanism);
            LOG.trace("{} for mechanism={}: {}", BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS_CONFIG, mechanism,
                    connectionsMaxReauthMsByMechanism.get(mechanism));
        }

        // Note that the old principal builder does not support SASL, so we do not need to pass the
        // authenticator or the transport layer
        this.principalBuilder = ChannelBuilders.createPrincipalBuilder(configs, kerberosNameParser, null);

        saslAuthRequestMaxReceiveSize = (Integer) configs.get(BrokerSecurityConfigs.SASL_SERVER_MAX_RECEIVE_SIZE_CONFIG);
        if (saslAuthRequestMaxReceiveSize == null)
            saslAuthRequestMaxReceiveSize = BrokerSecurityConfigs.DEFAULT_SASL_SERVER_MAX_RECEIVE_SIZE;
    }

    private void createSaslServer(String mechanism) throws IOException {
        this.saslMechanism = mechanism;
        Subject subject = subjects.get(mechanism);
        final AuthenticateCallbackHandler callbackHandler = callbackHandlers.get(mechanism);
        if (mechanism.equals(SaslConfigs.GSSAPI_MECHANISM)) {
            saslServer = createSaslKerberosServer(callbackHandler, configs, subject);
        } else {
            try {
                saslServer = SecurityManagerCompatibility.get().callAs(subject, () ->
                    Sasl.createSaslServer(saslMechanism, "kafka", serverAddress().getHostName(), configs, callbackHandler));
                if (saslServer == null) {
                    throw new SaslException("Kafka Server failed to create a SaslServer to interact with a client during session authentication with server mechanism " + saslMechanism);
                }
            } catch (CompletionException e) {
                throw new SaslException("Kafka Server failed to create a SaslServer to interact with a client during session authentication with server mechanism " + saslMechanism, e.getCause());
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

        try {
            return SecurityManagerCompatibility.get().callAs(subject, () ->
                    Sasl.createSaslServer(saslMechanism, servicePrincipalName, serviceHostname, configs, saslServerCallbackHandler));
        } catch (CompletionException e) {
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
    @SuppressWarnings("fallthrough")
    @Override
    public void authenticate() throws IOException {
        if (saslState != SaslState.REAUTH_PROCESS_HANDSHAKE) {
            if (netOutBuffer != null && !flushNetOutBufferAndUpdateInterestOps())
                return;

            if (saslServer != null && saslServer.isComplete()) {
                setSaslState(SaslState.COMPLETE);
                return;
            }

            // allocate on heap (as opposed to any socket server memory pool)
            if (netInBuffer == null) netInBuffer = new NetworkReceive(saslAuthRequestMaxReceiveSize, connectionId);

            try {
                netInBuffer.readFrom(transportLayer);
            } catch (InvalidReceiveException e) {
                throw new SaslAuthenticationException("Failing SASL authentication due to invalid receive size", e);
            }
            if (!netInBuffer.complete())
                return;
            netInBuffer.payload().rewind();
        }
        byte[] clientToken = new byte[netInBuffer.payload().remaining()];
        netInBuffer.payload().get(clientToken, 0, clientToken.length);
        netInBuffer = null; // reset the networkReceive as we read all the data.
        try {
            switch (saslState) {
                case REAUTH_PROCESS_HANDSHAKE:
                case HANDSHAKE_OR_VERSIONS_REQUEST:
                case HANDSHAKE_REQUEST:
                    handleKafkaRequest(clientToken);
                    break;
                case REAUTH_BAD_MECHANISM:
                    throw new SaslAuthenticationException(reauthInfo.badMechanismErrorMessage);
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
        } catch (AuthenticationException e) {
            // Exception will be propagated after response is sent to client
            setSaslState(SaslState.FAILED, e);
        } catch (Exception e) {
            // In the case of IOExceptions and other unexpected exceptions, fail immediately
            saslState = SaslState.FAILED;
            LOG.debug("Failed during {}: {}", reauthInfo.authenticationOrReauthenticationText(), e.getMessage());
            throw e;
        }
    }

    @Override
    public KafkaPrincipal principal() {
        Optional<SSLSession> sslSession = transportLayer instanceof SslTransportLayer ?
                Optional.of(((SslTransportLayer) transportLayer).sslSession()) : Optional.empty();
        SaslAuthenticationContext context = new SaslAuthenticationContext(saslServer, securityProtocol,
                clientAddress(), listenerName.value(), sslSession);
        KafkaPrincipal principal = principalBuilder.build(context);
        if (ScramMechanism.isScram(saslMechanism) && Boolean.parseBoolean((String) saslServer.getNegotiatedProperty(ScramLoginModule.TOKEN_AUTH_CONFIG))) {
            principal.tokenAuthenticated(true);
        }
        return principal;
    }

    @Override
    public Optional<KafkaPrincipalSerde> principalSerde() {
        return principalBuilder instanceof KafkaPrincipalSerde ? Optional.of((KafkaPrincipalSerde) principalBuilder) : Optional.empty();
    }

    @Override
    public boolean complete() {
        return saslState == SaslState.COMPLETE;
    }

    @Override
    public void handleAuthenticationFailure() throws IOException {
        sendAuthenticationFailureResponse();
    }

    @Override
    public void close() throws IOException {
        if (principalBuilder instanceof Closeable)
            Utils.closeQuietly((Closeable) principalBuilder, "principal builder");
        if (saslServer != null)
            saslServer.dispose();
    }

    @Override
    public void reauthenticate(ReauthenticationContext reauthenticationContext) throws IOException {
        NetworkReceive saslHandshakeReceive = reauthenticationContext.networkReceive();
        if (saslHandshakeReceive == null)
            throw new IllegalArgumentException(
                    "Invalid saslHandshakeReceive in server-side re-authentication context: null");
        SaslServerAuthenticator previousSaslServerAuthenticator = (SaslServerAuthenticator) reauthenticationContext.previousAuthenticator();
        reauthInfo.reauthenticating(previousSaslServerAuthenticator.saslMechanism,
                previousSaslServerAuthenticator.principal(), reauthenticationContext.reauthenticationBeginNanos());
        previousSaslServerAuthenticator.close();
        netInBuffer = saslHandshakeReceive;
        LOG.debug("Beginning re-authentication: {}", this);
        netInBuffer.payload().rewind();
        setSaslState(SaslState.REAUTH_PROCESS_HANDSHAKE);
        authenticate();
    }

    @Override
    public Long serverSessionExpirationTimeNanos() {
        return reauthInfo.sessionExpirationTimeNanos;
    }

    @Override
    public Long reauthenticationLatencyMs() {
        return reauthInfo.reauthenticationLatencyMs();
    }

    @Override
    public boolean connectedClientSupportsReauthentication() {
        return reauthInfo.connectedClientSupportsReauthentication;
    }

    private void setSaslState(SaslState saslState) {
        setSaslState(saslState, null);
    }

    private void setSaslState(SaslState saslState, AuthenticationException exception) {
        if (netOutBuffer != null && !netOutBuffer.completed()) {
            pendingSaslState = saslState;
            pendingException = exception;
        } else {
            this.saslState = saslState;
            LOG.debug("Set SASL server state to {} during {}", saslState, reauthInfo.authenticationOrReauthenticationText());
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

    private int clientPort() {
        return transportLayer.socketChannel().socket().getPort();
    }

    private void handleSaslToken(byte[] clientToken) throws IOException {
        if (!enableKafkaSaslAuthenticateHeaders) {
            byte[] response = saslServer.evaluateResponse(clientToken);
            if (saslServer.isComplete()) {
                reauthInfo.calcCompletionTimesAndReturnSessionLifetimeMs();
                if (reauthInfo.reauthenticating())
                    reauthInfo.ensurePrincipalUnchanged(principal());
            }
            if (response != null) {
                netOutBuffer = ByteBufferSend.sizePrefixed(ByteBuffer.wrap(response));
                flushNetOutBufferAndUpdateInterestOps();
            }
        } else {
            ByteBuffer requestBuffer = ByteBuffer.wrap(clientToken);
            RequestHeader header = RequestHeader.parse(requestBuffer);
            ApiKeys apiKey = header.apiKey();
            short version = header.apiVersion();
            RequestContext requestContext = new RequestContext(header, connectionId, clientAddress(), Optional.of(clientPort()),
                    KafkaPrincipal.ANONYMOUS, listenerName, securityProtocol, ClientInformation.EMPTY, false);
            RequestAndSize requestAndSize = requestContext.parseRequest(requestBuffer);
            if (apiKey != ApiKeys.SASL_AUTHENTICATE) {
                IllegalSaslStateException e = new IllegalSaslStateException("Unexpected Kafka request of type " + apiKey + " during SASL authentication.");
                buildResponseOnAuthenticateFailure(requestContext, requestAndSize.request.getErrorResponse(e));
                throw e;
            }
            if (!apiKey.isVersionSupported(version)) {
                // We cannot create an error response if the request version of SaslAuthenticate is not supported
                // This should not normally occur since clients typically check supported versions using ApiVersionsRequest
                throw new UnsupportedVersionException("Version " + version + " is not supported for apiKey " + apiKey);
            }
            /*
             * The client sends multiple SASL_AUTHENTICATE requests, and the client is known
             * to support the required version if any one of them indicates it supports that
             * version.
             */
            if (!reauthInfo.connectedClientSupportsReauthentication)
                reauthInfo.connectedClientSupportsReauthentication = version > 0;
            SaslAuthenticateRequest saslAuthenticateRequest = (SaslAuthenticateRequest) requestAndSize.request;

            try {
                byte[] responseToken = saslServer.evaluateResponse(
                        Utils.copyArray(saslAuthenticateRequest.data().authBytes()));
                if (reauthInfo.reauthenticating() && saslServer.isComplete())
                    reauthInfo.ensurePrincipalUnchanged(principal());
                // For versions with SASL_AUTHENTICATE header, send a response to SASL_AUTHENTICATE request even if token is empty.
                byte[] responseBytes = responseToken == null ? new byte[0] : responseToken;
                long sessionLifetimeMs = !saslServer.isComplete() ? 0L
                        : reauthInfo.calcCompletionTimesAndReturnSessionLifetimeMs();
                sendKafkaResponse(requestContext, new SaslAuthenticateResponse(
                        new SaslAuthenticateResponseData()
                        .setErrorCode(Errors.NONE.code())
                        .setAuthBytes(responseBytes)
                        .setSessionLifetimeMs(sessionLifetimeMs)));
            } catch (SaslAuthenticationException e) {
                buildResponseOnAuthenticateFailure(requestContext,
                        new SaslAuthenticateResponse(
                                new SaslAuthenticateResponseData()
                                .setErrorCode(Errors.SASL_AUTHENTICATION_FAILED.code())
                                .setErrorMessage(e.getMessage())));
                throw e;
            } catch (SaslException e) {
                KerberosError kerberosError = KerberosError.fromException(e);
                if (kerberosError != null && kerberosError.retriable()) {
                    // Handle retriable Kerberos exceptions as I/O exceptions rather than authentication exceptions
                    throw e;
                } else {
                    // DO NOT include error message from the `SaslException` in the client response since it may
                    // contain sensitive data like the existence of the user.
                    String errorMessage = "Authentication failed during "
                            + reauthInfo.authenticationOrReauthenticationText()
                            + " due to invalid credentials with SASL mechanism " + saslMechanism;
                    buildResponseOnAuthenticateFailure(requestContext, new SaslAuthenticateResponse(
                            new SaslAuthenticateResponseData()
                            .setErrorCode(Errors.SASL_AUTHENTICATION_FAILED.code())
                            .setErrorMessage(errorMessage)));
                    throw new SaslAuthenticationException(errorMessage, e);
                }
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

            LOG.debug("Handling Kafka request {} during {}", apiKey, reauthInfo.authenticationOrReauthenticationText());


            RequestContext requestContext = new RequestContext(header, connectionId, clientAddress(), Optional.of(clientPort()),
                    KafkaPrincipal.ANONYMOUS, listenerName, securityProtocol, ClientInformation.EMPTY, false);
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
        if (clientMechanism != null && (!reauthInfo.reauthenticating()
                || reauthInfo.saslMechanismUnchanged(clientMechanism))) {
            createSaslServer(clientMechanism);
            setSaslState(SaslState.AUTHENTICATE);
        }
        return isKafkaRequest;
    }

    private String handleHandshakeRequest(RequestContext context, SaslHandshakeRequest handshakeRequest) throws IOException, UnsupportedSaslMechanismException {
        String clientMechanism = handshakeRequest.data().mechanism();
        short version = context.header.apiVersion();
        if (version >= 1)
            this.enableKafkaSaslAuthenticateHeaders(true);
        if (enabledMechanisms.contains(clientMechanism)) {
            LOG.debug("Using SASL mechanism '{}' provided by client", clientMechanism);
            sendKafkaResponse(context, new SaslHandshakeResponse(
                    new SaslHandshakeResponseData().setErrorCode(Errors.NONE.code()).setMechanisms(enabledMechanisms)));
            return clientMechanism;
        } else {
            LOG.debug("SASL mechanism '{}' requested by client is not supported", clientMechanism);
            buildResponseOnAuthenticateFailure(context, new SaslHandshakeResponse(
                    new SaslHandshakeResponseData().setErrorCode(Errors.UNSUPPORTED_SASL_MECHANISM.code()).setMechanisms(enabledMechanisms)));
            throw new UnsupportedSaslMechanismException("Unsupported SASL mechanism " + clientMechanism);
        }
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
        else if (!apiVersionsRequest.isValid())
            sendKafkaResponse(context, apiVersionsRequest.getErrorResponse(0, Errors.INVALID_REQUEST.exception()));
        else {
            metadataRegistry.registerClientInformation(new ClientInformation(apiVersionsRequest.data().clientSoftwareName(),
                apiVersionsRequest.data().clientSoftwareVersion()));
            sendKafkaResponse(context, apiVersionSupplier.apply(apiVersionsRequest.version()));
            setSaslState(SaslState.HANDSHAKE_REQUEST);
        }
    }

    /**
     * Build a {@link Send} response on {@link #authenticate()} failure. The actual response is sent out when
     * {@link #sendAuthenticationFailureResponse()} is called.
     */
    private void buildResponseOnAuthenticateFailure(RequestContext context, AbstractResponse response) {
        authenticationFailureSend = context.buildResponseSend(response);
    }

    /**
     * Send any authentication failure response that may have been previously built.
     */
    private void sendAuthenticationFailureResponse() throws IOException {
        if (authenticationFailureSend == null)
            return;
        sendKafkaResponse(authenticationFailureSend);
        authenticationFailureSend = null;
    }

    private void sendKafkaResponse(RequestContext context, AbstractResponse response) throws IOException {
        sendKafkaResponse(context.buildResponseSend(response));
    }

    private void sendKafkaResponse(Send send) throws IOException {
        netOutBuffer = send;
        flushNetOutBufferAndUpdateInterestOps();
    }

    /**
     * Information related to re-authentication
     */
    private class ReauthInfo {
        public String previousSaslMechanism;
        public KafkaPrincipal previousKafkaPrincipal;
        public long reauthenticationBeginNanos;
        public Long sessionExpirationTimeNanos;
        public boolean connectedClientSupportsReauthentication;
        public long authenticationEndNanos;
        public String badMechanismErrorMessage;

        public void reauthenticating(String previousSaslMechanism, KafkaPrincipal previousKafkaPrincipal,
                long reauthenticationBeginNanos) {
            this.previousSaslMechanism = Objects.requireNonNull(previousSaslMechanism);
            this.previousKafkaPrincipal = Objects.requireNonNull(previousKafkaPrincipal);
            this.reauthenticationBeginNanos = reauthenticationBeginNanos;
        }

        public boolean reauthenticating() {
            return previousSaslMechanism != null;
        }

        public String authenticationOrReauthenticationText() {
            return reauthenticating() ? "re-authentication" : "authentication";
        }

        public void ensurePrincipalUnchanged(KafkaPrincipal reauthenticatedKafkaPrincipal) throws SaslAuthenticationException {
            if (!previousKafkaPrincipal.equals(reauthenticatedKafkaPrincipal)) {
                throw new SaslAuthenticationException(String.format(
                        "Cannot change principals during re-authentication from %s.%s: %s.%s",
                        previousKafkaPrincipal.getPrincipalType(), previousKafkaPrincipal.getName(),
                        reauthenticatedKafkaPrincipal.getPrincipalType(), reauthenticatedKafkaPrincipal.getName()));
            }
        }

        /*
         * We define the REAUTH_BAD_MECHANISM state because the failed re-authentication
         * metric does not get updated if we send back an error immediately upon the
         * start of re-authentication.
         */
        public boolean saslMechanismUnchanged(String clientMechanism) {
            if (previousSaslMechanism.equals(clientMechanism))
                return true;
            badMechanismErrorMessage = String.format(
                    "SASL mechanism '%s' requested by client is not supported for re-authentication of mechanism '%s'",
                    clientMechanism, previousSaslMechanism);
            LOG.debug(badMechanismErrorMessage);
            setSaslState(SaslState.REAUTH_BAD_MECHANISM);
            return false;
        }

        private long calcCompletionTimesAndReturnSessionLifetimeMs() {
            long retvalSessionLifetimeMs = 0L;
            long authenticationEndMs = time.milliseconds();
            authenticationEndNanos = time.nanoseconds();
            Long credentialExpirationMs = (Long) saslServer
                    .getNegotiatedProperty(SaslInternalConfigs.CREDENTIAL_LIFETIME_MS_SASL_NEGOTIATED_PROPERTY_KEY);
            Long connectionsMaxReauthMs = connectionsMaxReauthMsByMechanism.get(saslMechanism);
            boolean maxReauthSet = connectionsMaxReauthMs != null && connectionsMaxReauthMs > 0;

            if (credentialExpirationMs != null || maxReauthSet) {
                if (credentialExpirationMs == null)
                    retvalSessionLifetimeMs = zeroIfNegative(connectionsMaxReauthMs);
                else if (!maxReauthSet)
                    retvalSessionLifetimeMs = zeroIfNegative(credentialExpirationMs - authenticationEndMs);
                else
                    retvalSessionLifetimeMs = zeroIfNegative(Math.min(credentialExpirationMs - authenticationEndMs, connectionsMaxReauthMs));

                sessionExpirationTimeNanos = authenticationEndNanos + 1000 * 1000 * retvalSessionLifetimeMs;
            }

            if (credentialExpirationMs != null) {
                LOG.debug(
                        "Authentication complete; session max lifetime from broker config={} ms, credential expiration={} ({} ms); session expiration = {} ({} ms), sending {} ms to client",
                        connectionsMaxReauthMs, new Date(credentialExpirationMs),
                        credentialExpirationMs - authenticationEndMs,
                        new Date(authenticationEndMs + retvalSessionLifetimeMs), retvalSessionLifetimeMs,
                        retvalSessionLifetimeMs);
            } else {
                if (sessionExpirationTimeNanos != null)
                    LOG.debug(
                            "Authentication complete; session max lifetime from broker config={} ms, no credential expiration; session expiration = {} ({} ms), sending {} ms to client",
                            connectionsMaxReauthMs, new Date(authenticationEndMs + retvalSessionLifetimeMs),
                            retvalSessionLifetimeMs, retvalSessionLifetimeMs);
                else
                    LOG.debug(
                            "Authentication complete; session max lifetime from broker config={} ms, no credential expiration; no session expiration, sending 0 ms to client",
                            connectionsMaxReauthMs);
            }
            return retvalSessionLifetimeMs;
        }

        public Long reauthenticationLatencyMs() {
            if (!reauthenticating())
                return null;
            // record at least 1 ms if there is some latency
            long latencyNanos = authenticationEndNanos - reauthenticationBeginNanos;
            return latencyNanos == 0L ? 0L : Math.max(1L, Math.round(latencyNanos / 1000.0 / 1000.0));
        }

        private long zeroIfNegative(long value) {
            return Math.max(0L, value);
        }        
    }
}
