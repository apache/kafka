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

import java.net.InetAddress;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.errors.IllegalSaslStateException;
import org.apache.kafka.common.network.ChannelMetadataRegistry;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.DefaultChannelMetadataRegistry;
import org.apache.kafka.common.network.InvalidReceiveException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.network.TransportLayer;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.Time;
import org.junit.Test;

import javax.security.auth.Subject;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.mockito.Answers;

import static org.apache.kafka.common.security.scram.internals.ScramMechanism.SCRAM_SHA_256;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SaslServerAuthenticatorTest {

    @Test(expected = InvalidReceiveException.class)
    public void testOversizeRequest() throws IOException {
        TransportLayer transportLayer = mock(TransportLayer.class);
        Map<String, ?> configs = Collections.singletonMap(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG,
                Collections.singletonList(SCRAM_SHA_256.mechanismName()));
        SaslServerAuthenticator authenticator = setupAuthenticator(configs, transportLayer,
            SCRAM_SHA_256.mechanismName(), new DefaultChannelMetadataRegistry());

        ByteBuffer testData =
                (ByteBuffer) ByteBuffer.allocate(4 + (SaslServerAuthenticator.MAX_RECEIVE_SIZE + 1))
                        .putInt(SaslServerAuthenticator.MAX_RECEIVE_SIZE + 1)
                        .put(new byte[SaslServerAuthenticator.MAX_RECEIVE_SIZE]).rewind();

        when(transportLayer.read(any(ByteBuffer.class))).then(invocation -> {
            ByteBuffer inputBuffer = invocation.<ByteBuffer>getArgument(0);
            int remaining = Math.min(testData.remaining(), inputBuffer.remaining());

            ByteBuffer slice = (ByteBuffer) testData.slice().limit(remaining);

            // write the test data into to the test
            inputBuffer.put(slice);

            testData.position(testData.position() + remaining);

            return remaining;
        });

        authenticator.authenticate();
        verify(transportLayer).read(any(ByteBuffer.class));
    }

    @Test
    public void testUnexpectedRequestType() throws IOException {
        TransportLayer transportLayer = mock(TransportLayer.class);
        Map<String, ?> configs = Collections.singletonMap(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG,
                Collections.singletonList(SCRAM_SHA_256.mechanismName()));
        SaslServerAuthenticator authenticator = setupAuthenticator(configs, transportLayer,
            SCRAM_SHA_256.mechanismName(), new DefaultChannelMetadataRegistry());

        final RequestHeader header = new RequestHeader(ApiKeys.METADATA, (short) 0, "clientId", 13243);
        final Struct headerStruct = header.toStruct();

        final ByteBuffer testData =
                ByteBuffer.allocate(4 + headerStruct.sizeOf()).putInt(headerStruct.sizeOf());
        headerStruct.writeTo(testData);
        testData.rewind();

        when(transportLayer.read(any(ByteBuffer.class))).then(invocation -> {
            ByteBuffer inputBuffer = invocation.<ByteBuffer>getArgument(0);
            int remaining = Math.min(testData.remaining(), inputBuffer.remaining());

            ByteBuffer slice = (ByteBuffer) testData.slice().limit(remaining);

            // write the test data into to the test
            inputBuffer.put(slice);

            testData.position(testData.position() + remaining);

            return remaining;
        });

        try {
            authenticator.authenticate();
            fail("Expected authenticate() to raise an exception");
        } catch (IllegalSaslStateException e) {
            // expected exception
        }

        assertFalse(testData.hasRemaining());
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

    public void testApiVersionsRequest(short version, String expectedSoftwareName,
                                       String expectedSoftwareVersion) throws IOException {
        TransportLayer transportLayer = mock(TransportLayer.class, Answers.RETURNS_DEEP_STUBS);
        Map<String, ?> configs = Collections.singletonMap(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG,
            Collections.singletonList(SCRAM_SHA_256.mechanismName()));
        ChannelMetadataRegistry metadataRegistry = new DefaultChannelMetadataRegistry();
        SaslServerAuthenticator authenticator = setupAuthenticator(configs, transportLayer,
            SCRAM_SHA_256.mechanismName(), metadataRegistry);

        final RequestHeader header = new RequestHeader(ApiKeys.API_VERSIONS, version, "clientId", 0);
        final Struct headerStruct = header.toStruct();

        final ApiVersionsRequest request = new ApiVersionsRequest.Builder().build(version);
        final Struct requestStruct = request.data.toStruct(version);

        int sizeOfPayload = headerStruct.sizeOf() + requestStruct.sizeOf();
        ByteBuffer testData = ByteBuffer.allocate(4 + sizeOfPayload).putInt(sizeOfPayload);
        headerStruct.writeTo(testData);
        requestStruct.writeTo(testData);
        testData.rewind();

        when(transportLayer.socketChannel().socket().getInetAddress()).thenReturn(InetAddress.getLoopbackAddress());

        when(transportLayer.read(any(ByteBuffer.class))).then(invocation -> {
            ByteBuffer inputBuffer = invocation.<ByteBuffer>getArgument(0);
            int remaining = Math.min(testData.remaining(), inputBuffer.remaining());

            ByteBuffer slice = (ByteBuffer) testData.slice().limit(remaining);

            // write the test data into to the test
            inputBuffer.put(slice);

            testData.position(testData.position() + remaining);

            return remaining;
        });

        authenticator.authenticate();

        assertEquals(expectedSoftwareName, metadataRegistry.clientInformation().softwareName());
        assertEquals(expectedSoftwareVersion, metadataRegistry.clientInformation().softwareVersion());

        assertFalse(testData.hasRemaining());
    }

    private SaslServerAuthenticator setupAuthenticator(Map<String, ?> configs, TransportLayer transportLayer,
                                                       String mechanism, ChannelMetadataRegistry metadataRegistry) throws IOException {
        TestJaasConfig jaasConfig = new TestJaasConfig();
        jaasConfig.addEntry("jaasContext", PlainLoginModule.class.getName(), new HashMap<String, Object>());
        Map<String, Subject> subjects = Collections.singletonMap(mechanism, new Subject());
        Map<String, AuthenticateCallbackHandler> callbackHandlers = Collections.singletonMap(
                mechanism, new SaslServerCallbackHandler());
        return new SaslServerAuthenticator(configs, callbackHandlers, "node", subjects, null,
                new ListenerName("ssl"), SecurityProtocol.SASL_SSL, transportLayer, Collections.emptyMap(),
                metadataRegistry, Time.SYSTEM);
    }

}
