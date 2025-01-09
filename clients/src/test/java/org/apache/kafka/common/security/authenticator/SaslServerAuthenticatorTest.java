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

import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.network.ChannelBuilders;
import org.apache.kafka.common.network.ChannelMetadataRegistry;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.DefaultChannelMetadataRegistry;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.TransportLayer;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.SaslAuthenticateRequest;
import org.apache.kafka.common.requests.SaslAuthenticateResponse;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalBuilder;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.kerberos.KerberosShortNamer;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.security.ssl.SslPrincipalMapper;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import static org.apache.kafka.common.security.scram.internals.ScramMechanism.SCRAM_SHA_256;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SaslServerAuthenticatorTest {

    private final String clientId = "clientId";
    
    @Test
    public void testOversizeRequest() throws IOException {
        TransportLayer transportLayer = mock(TransportLayer.class);
        Map<String, ?> configs = Collections.singletonMap(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG,
                Collections.singletonList(SCRAM_SHA_256.mechanismName()));
        SaslServerAuthenticator authenticator = setupAuthenticator(configs, transportLayer,
                SCRAM_SHA_256.mechanismName(), new DefaultChannelMetadataRegistry());

        when(transportLayer.read(any(ByteBuffer.class))).then(invocation -> {
            invocation.<ByteBuffer>getArgument(0).putInt(BrokerSecurityConfigs.DEFAULT_SASL_SERVER_MAX_RECEIVE_SIZE + 1);
            return 4;
        });
        assertThrows(SaslAuthenticationException.class, authenticator::authenticate);
        verify(transportLayer).read(any(ByteBuffer.class));
    }

    @Test
    public void testUnexpectedRequestTypeWithValidRequestHeader() throws IOException {
        TransportLayer transportLayer = mock(TransportLayer.class);
        Map<String, ?> configs = Collections.singletonMap(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG,
                Collections.singletonList(SCRAM_SHA_256.mechanismName()));
        SaslServerAuthenticator authenticator = setupAuthenticator(configs, transportLayer,
                SCRAM_SHA_256.mechanismName(), new DefaultChannelMetadataRegistry());

        RequestHeader header = new RequestHeader(ApiKeys.METADATA, (short) 0, clientId, 13243);
        ByteBuffer headerBuffer = RequestTestUtils.serializeRequestHeader(header);

        when(transportLayer.read(any(ByteBuffer.class))).then(invocation -> {
            invocation.<ByteBuffer>getArgument(0).putInt(headerBuffer.remaining());
            return 4;
        }).then(invocation -> {
            // serialize only the request header. the authenticator should not parse beyond this
            invocation.<ByteBuffer>getArgument(0).put(headerBuffer.duplicate());
            return headerBuffer.remaining();
        });

        assertThrows(InvalidRequestException.class, () -> authenticator.authenticate());
        verify(transportLayer, times(2)).read(any(ByteBuffer.class));
    }

    @Test
    public void testInvalidRequestHeader() throws IOException {
        TransportLayer transportLayer = mock(TransportLayer.class);
        Map<String, ?> configs = Collections.singletonMap(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG,
                Collections.singletonList(SCRAM_SHA_256.mechanismName()));
        SaslServerAuthenticator authenticator = setupAuthenticator(configs, transportLayer,
                SCRAM_SHA_256.mechanismName(), new DefaultChannelMetadataRegistry());

        short invalidApiKeyId = (short) (Arrays.stream(ApiKeys.values()).mapToInt(k -> k.id).max().getAsInt() + 1);
        ByteBuffer headerBuffer = RequestTestUtils.serializeRequestHeader(new RequestHeader(
            new RequestHeaderData()
                .setRequestApiKey(invalidApiKeyId)
                .setRequestApiVersion((short) 0),
                (short) 2));

        when(transportLayer.read(any(ByteBuffer.class))).then(invocation -> {
            invocation.<ByteBuffer>getArgument(0).putInt(headerBuffer.remaining());
            return 4;
        }).then(invocation -> {
            // serialize only the request header. the authenticator should not parse beyond this
            invocation.<ByteBuffer>getArgument(0).put(headerBuffer.duplicate());
            return headerBuffer.remaining();
        });

        assertThrows(InvalidRequestException.class, () -> authenticator.authenticate());
        verify(transportLayer, times(2)).read(any(ByteBuffer.class));
    }

    @Test
    public void testOldestApiVersionsRequest() throws IOException {
        testApiVersionsRequest(ApiKeys.API_VERSIONS.oldestVersion(),
                ClientInformation.UNKNOWN_NAME_OR_VERSION, ClientInformation.UNKNOWN_NAME_OR_VERSION);
    }

    @Test
    public void testLatestApiVersionsRequest() throws IOException {
        testApiVersionsRequest(ApiKeys.API_VERSIONS.latestVersion(),
                "apache-kafka-java", AppInfoParser.getVersion());
    }

    @Test
    public void testSessionExpiresAtTokenExpiryDespiteNoReauthIsSet() throws IOException {
        String mechanism = OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
        Duration tokenExpirationDuration = Duration.ofSeconds(1);
        SaslServer saslServer = mock(SaslServer.class);

        MockTime time = new MockTime();
        try (
                MockedStatic<?> ignored = mockSaslServer(saslServer, mechanism, time, tokenExpirationDuration);
                MockedStatic<?> ignored2 = mockKafkaPrincipal("[principal-type]", "[principal-name");
                TransportLayer transportLayer = mockTransportLayer()
        ) {

            SaslServerAuthenticator authenticator = getSaslServerAuthenticatorForOAuth(mechanism, transportLayer, time, 0L);

            mockRequest(saslHandshakeRequest(mechanism), transportLayer);
            authenticator.authenticate();

            when(saslServer.isComplete()).thenReturn(false).thenReturn(true);
            mockRequest(saslAuthenticateRequest(), transportLayer);
            authenticator.authenticate();

            long atTokenExpiryNanos = time.nanoseconds() + tokenExpirationDuration.toNanos();
            assertEquals(atTokenExpiryNanos, authenticator.serverSessionExpirationTimeNanos());

            ByteBuffer secondResponseSent = getResponses(transportLayer).get(1);
            consumeSizeAndHeader(secondResponseSent);
            SaslAuthenticateResponse response = SaslAuthenticateResponse.parse(secondResponseSent, (short) 2);
            assertEquals(tokenExpirationDuration.toMillis(), response.sessionLifetimeMs());
        }
    }

    @Test
    public void testSessionExpiresAtMaxReauthTime() throws IOException {
        String mechanism = OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
        SaslServer saslServer = mock(SaslServer.class);
        MockTime time = new MockTime(0, 1, 1000);
        long maxReauthMs = 100L;
        Duration tokenExpiryGreaterThanMaxReauth = Duration.ofMillis(maxReauthMs).multipliedBy(10);

        try (
                MockedStatic<?> ignored = mockSaslServer(saslServer, mechanism, time, tokenExpiryGreaterThanMaxReauth);
                MockedStatic<?> ignored2 = mockKafkaPrincipal("[principal-type]", "[principal-name");
                TransportLayer transportLayer = mockTransportLayer()
        ) {

            SaslServerAuthenticator authenticator = getSaslServerAuthenticatorForOAuth(mechanism, transportLayer, time, maxReauthMs);

            mockRequest(saslHandshakeRequest(mechanism), transportLayer);
            authenticator.authenticate();

            when(saslServer.isComplete()).thenReturn(false).thenReturn(true);
            mockRequest(saslAuthenticateRequest(), transportLayer);
            authenticator.authenticate();

            long atMaxReauthNanos = time.nanoseconds() + Duration.ofMillis(maxReauthMs).toNanos();
            assertEquals(atMaxReauthNanos, authenticator.serverSessionExpirationTimeNanos());

            ByteBuffer secondResponseSent = getResponses(transportLayer).get(1);
            consumeSizeAndHeader(secondResponseSent);
            SaslAuthenticateResponse response = SaslAuthenticateResponse.parse(secondResponseSent, (short) 2);
            assertEquals(maxReauthMs, response.sessionLifetimeMs());
        }
    }

    @Test
    public void testSessionExpiresAtTokenExpiry() throws IOException {
        String mechanism = OAuthBearerLoginModule.OAUTHBEARER_MECHANISM;
        SaslServer saslServer = mock(SaslServer.class);
        MockTime time = new MockTime(0, 1, 1000);
        Duration tokenExpiryShorterThanMaxReauth = Duration.ofSeconds(2);
        long maxReauthMs = tokenExpiryShorterThanMaxReauth.multipliedBy(2).toMillis();

        try (
                MockedStatic<?> ignored = mockSaslServer(saslServer, mechanism, time, tokenExpiryShorterThanMaxReauth);
                MockedStatic<?> ignored2 = mockKafkaPrincipal("[principal-type]", "[principal-name");
                TransportLayer transportLayer = mockTransportLayer()
        ) {

            SaslServerAuthenticator authenticator = getSaslServerAuthenticatorForOAuth(mechanism, transportLayer, time, maxReauthMs);

            mockRequest(saslHandshakeRequest(mechanism), transportLayer);
            authenticator.authenticate();

            when(saslServer.isComplete()).thenReturn(false).thenReturn(true);
            mockRequest(saslAuthenticateRequest(), transportLayer);
            authenticator.authenticate();

            long atTokenExpiryNanos = time.nanoseconds() + tokenExpiryShorterThanMaxReauth.toNanos();
            assertEquals(atTokenExpiryNanos, authenticator.serverSessionExpirationTimeNanos());

            ByteBuffer secondResponseSent = getResponses(transportLayer).get(1);
            consumeSizeAndHeader(secondResponseSent);
            SaslAuthenticateResponse response = SaslAuthenticateResponse.parse(secondResponseSent, (short) 2);
            assertEquals(tokenExpiryShorterThanMaxReauth.toMillis(), response.sessionLifetimeMs());
        }
    }

    private SaslServerAuthenticator getSaslServerAuthenticatorForOAuth(String mechanism, TransportLayer transportLayer, Time time, Long maxReauth) {
        Map<String, ?> configs = Collections.singletonMap(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG,
                Collections.singletonList(mechanism));
        ChannelMetadataRegistry metadataRegistry = new DefaultChannelMetadataRegistry();

        return setupAuthenticator(configs, transportLayer, mechanism, metadataRegistry, time, maxReauth);
    }

    private MockedStatic<?> mockSaslServer(SaslServer saslServer, String mechanism, Time time, Duration tokenExpirationDuration) throws SaslException {
        when(saslServer.getMechanismName()).thenReturn(mechanism);
        when(saslServer.evaluateResponse(any())).thenReturn(new byte[]{});
        long millisToExpiration = tokenExpirationDuration.toMillis();
        when(saslServer.getNegotiatedProperty(eq(SaslInternalConfigs.CREDENTIAL_LIFETIME_MS_SASL_NEGOTIATED_PROPERTY_KEY)))
                .thenReturn(time.milliseconds() + millisToExpiration);
        return Mockito.mockStatic(Sasl.class, (Answer<SaslServer>) invocation -> saslServer);
    }

    private MockedStatic<?> mockKafkaPrincipal(String principalType, String name) {
        KafkaPrincipalBuilder kafkaPrincipalBuilder = mock(KafkaPrincipalBuilder.class);
        when(kafkaPrincipalBuilder.build(any())).thenReturn(new KafkaPrincipal(principalType, name));
        MockedStatic<ChannelBuilders> channelBuilders = Mockito.mockStatic(ChannelBuilders.class, Answers.RETURNS_MOCKS);
        channelBuilders.when(() ->
                ChannelBuilders.createPrincipalBuilder(anyMap(), any(KerberosShortNamer.class), any(SslPrincipalMapper.class))
        ).thenReturn(kafkaPrincipalBuilder);
        return channelBuilders;
    }

    private void consumeSizeAndHeader(ByteBuffer responseBuffer) {
        responseBuffer.getInt();
        ResponseHeader.parse(responseBuffer, (short) 1);
    }

    private List<ByteBuffer> getResponses(TransportLayer transportLayer) throws IOException {
        ArgumentCaptor<ByteBuffer[]> buffersCaptor = ArgumentCaptor.forClass(ByteBuffer[].class);
        verify(transportLayer, times(2)).write(buffersCaptor.capture());
        return buffersCaptor.getAllValues().stream()
                .map(this::concatBuffers)
                .collect(Collectors.toList());
    }

    private ByteBuffer concatBuffers(ByteBuffer[] buffers) {
        int combinedCapacity = 0;
        for (ByteBuffer buffer : buffers) {
            combinedCapacity += buffer.capacity();
        }
        if (combinedCapacity > 0) {
            ByteBuffer concat = ByteBuffer.allocate(combinedCapacity);
            for (ByteBuffer buffer : buffers) {
                concat.put(buffer);
            }
            return safeFlip(concat);
        } else {
            return ByteBuffer.allocate(0);
        }
    }

    private ByteBuffer safeFlip(ByteBuffer buffer) {
        return (ByteBuffer) ((Buffer) buffer).flip();
    }

    private SaslAuthenticateRequest saslAuthenticateRequest() {
        SaslAuthenticateRequestData authenticateRequestData = new SaslAuthenticateRequestData();
        return new SaslAuthenticateRequest.Builder(authenticateRequestData).build(ApiKeys.SASL_AUTHENTICATE.latestVersion());
    }

    private SaslHandshakeRequest saslHandshakeRequest(String mechanism) {
        SaslHandshakeRequestData handshakeRequestData = new SaslHandshakeRequestData();
        handshakeRequestData.setMechanism(mechanism);
        return new SaslHandshakeRequest.Builder(handshakeRequestData).build(ApiKeys.SASL_HANDSHAKE.latestVersion());
    }

    private TransportLayer mockTransportLayer() throws IOException {
        TransportLayer transportLayer = mock(TransportLayer.class, Answers.RETURNS_DEEP_STUBS);
        when(transportLayer.socketChannel().socket().getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());
        when(transportLayer.write(any(ByteBuffer[].class))).thenReturn(Long.MAX_VALUE);
        return transportLayer;
    }

    private void mockRequest(AbstractRequest request, TransportLayer transportLayer) throws IOException {
        mockRequest(new RequestHeader(request.apiKey(), request.apiKey().latestVersion(), clientId, 0), request, transportLayer);
    }

    private void mockRequest(RequestHeader header, AbstractRequest request, TransportLayer transportLayer) throws IOException {
        ByteBuffer headerBuffer = RequestTestUtils.serializeRequestHeader(header);

        ByteBuffer requestBuffer = request.serialize();
        requestBuffer.rewind();

        when(transportLayer.read(any(ByteBuffer.class))).then(invocation -> {
            invocation.<ByteBuffer>getArgument(0).putInt(headerBuffer.remaining() + requestBuffer.remaining());
            return 4;
        }).then(invocation -> {
            invocation.<ByteBuffer>getArgument(0)
                    .put(headerBuffer.duplicate())
                    .put(requestBuffer.duplicate());
            return headerBuffer.remaining() + requestBuffer.remaining();
        });
    }

    private void testApiVersionsRequest(short version, String expectedSoftwareName,
                                        String expectedSoftwareVersion) throws IOException {
        TransportLayer transportLayer = mockTransportLayer();
        Map<String, ?> configs = Collections.singletonMap(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG,
                Collections.singletonList(SCRAM_SHA_256.mechanismName()));
        ChannelMetadataRegistry metadataRegistry = new DefaultChannelMetadataRegistry();
        SaslServerAuthenticator authenticator = setupAuthenticator(configs, transportLayer, SCRAM_SHA_256.mechanismName(), metadataRegistry);

        RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, version, clientId, 0);
        ApiVersionsRequest request = new ApiVersionsRequest.Builder().build(version);
        mockRequest(header, request, transportLayer);

        authenticator.authenticate();

        assertEquals(expectedSoftwareName, metadataRegistry.clientInformation().softwareName());
        assertEquals(expectedSoftwareVersion, metadataRegistry.clientInformation().softwareVersion());

        verify(transportLayer, times(2)).read(any(ByteBuffer.class));
    }

    private SaslServerAuthenticator setupAuthenticator(Map<String, ?> configs, TransportLayer transportLayer,
                                                       String mechanism, ChannelMetadataRegistry metadataRegistry) {
        return setupAuthenticator(configs, transportLayer, mechanism, metadataRegistry, new MockTime(), null);
    }

    private SaslServerAuthenticator setupAuthenticator(Map<String, ?> configs, TransportLayer transportLayer,
                                                       String mechanism, ChannelMetadataRegistry metadataRegistry, Time time, Long maxReauth) {
        TestJaasConfig jaasConfig = new TestJaasConfig();
        jaasConfig.addEntry("jaasContext", PlainLoginModule.class.getName(), new HashMap<>());
        Map<String, Subject> subjects = Collections.singletonMap(mechanism, new Subject());
        Map<String, AuthenticateCallbackHandler> callbackHandlers = Collections.singletonMap(
                mechanism, new SaslServerCallbackHandler());
        ApiVersionsResponse apiVersionsResponse = TestUtils.defaultApiVersionsResponse(
                ApiMessageType.ListenerType.ZK_BROKER);
        Map<String, Long> connectionsMaxReauthMsByMechanism = maxReauth != null ?
                Collections.singletonMap(mechanism, maxReauth) : Collections.emptyMap();

        return new SaslServerAuthenticator(configs, callbackHandlers, "node", subjects, null,
                new ListenerName("ssl"), SecurityProtocol.SASL_SSL, transportLayer, connectionsMaxReauthMsByMechanism,
                metadataRegistry, time, version -> apiVersionsResponse);
    }

}
