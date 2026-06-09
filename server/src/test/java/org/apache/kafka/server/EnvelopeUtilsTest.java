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
package org.apache.kafka.server;

import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.PrincipalDeserializationException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.EnvelopeRequest;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.SecurityUtils;
import org.apache.kafka.network.Request;
import org.apache.kafka.network.metrics.RequestChannelMetrics;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class EnvelopeUtilsTest {
    private static final String CLIENT_ID = "client-id";
    private static final String CONNECTION_ID = "connection-id";
    private static final KafkaPrincipal FORWARDED_PRINCIPAL = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "forwarded");
    private static final KafkaPrincipalSerde PRINCIPAL_SERDE = new KafkaPrincipalSerde() {
        @Override
        public byte[] serialize(KafkaPrincipal principal) {
            return Utils.utf8(principal.toString());
        }

        @Override
        public KafkaPrincipal deserialize(byte[] bytes) {
            return SecurityUtils.parseKafkaPrincipal(Utils.utf8(bytes));
        }
    };

    @Test
    public void testHandleEnvelopeRequestBuildsForwardedRequest() {
        Request envelope = buildEnvelopeRequest(createForwardableRequest());
        envelope.requestDequeueTimeNanos(123L);

        AtomicReference<Request> handledRequest = new AtomicReference<>();
        EnvelopeUtils.handleEnvelopeRequest(envelope, mock(RequestChannelMetrics.class), handledRequest::set);

        Request forwardedRequest = handledRequest.get();
        assertEquals(ApiKeys.CREATE_TOPICS, forwardedRequest.header().apiKey());
        assertEquals(CLIENT_ID, forwardedRequest.header().clientId());
        assertEquals(CONNECTION_ID, forwardedRequest.context().connectionId);
        assertEquals(FORWARDED_PRINCIPAL, forwardedRequest.context().principal);
        assertEquals(InetAddress.getLoopbackAddress(), forwardedRequest.context().clientAddress);
        assertTrue(forwardedRequest.isForwarded());
        assertSame(envelope, forwardedRequest.envelope().orElseThrow());
        assertEquals(envelope.requestDequeueTimeNanos(), forwardedRequest.requestDequeueTimeNanos());
        assertInstanceOf(CreateTopicsRequest.class, forwardedRequest.body(AbstractRequest.class));
    }

    @Test
    public void testNonForwardableApiIsRejected() {
        Request envelope = buildEnvelopeRequest(createNonForwardableRequest());

        InvalidRequestException exception = assertThrows(
            InvalidRequestException.class,
            () -> EnvelopeUtils.handleEnvelopeRequest(envelope, mock(RequestChannelMetrics.class), ignored -> { })
        );
        assertEquals("API METADATA is not enabled or is not eligible for forwarding", exception.getMessage());
    }

    @Test
    public void testInvalidForwardedClientAddressIsRejected() {
        Request envelope = buildEnvelopeRequest(
            createForwardableRequest(),
            Optional.of(PRINCIPAL_SERDE),
            new byte[] {1, 2, 3}
        );

        InvalidRequestException exception = assertThrows(
            InvalidRequestException.class,
            () -> EnvelopeUtils.handleEnvelopeRequest(envelope, mock(RequestChannelMetrics.class), ignored -> { })
        );
        assertEquals("Failed to parse client address from envelope", exception.getMessage());
    }

    @Test
    public void testMissingPrincipalSerdeIsRejected() {
        Request envelope = buildEnvelopeRequest(
            createForwardableRequest(),
            Optional.empty(),
            InetAddress.getLoopbackAddress().getAddress()
        );

        PrincipalDeserializationException exception = assertThrows(
            PrincipalDeserializationException.class,
            () -> EnvelopeUtils.handleEnvelopeRequest(envelope, mock(RequestChannelMetrics.class), ignored -> { })
        );
        assertEquals(
            "Could not deserialize principal since no `KafkaPrincipalSerde` has been defined",
            exception.getMessage()
        );
    }

    @Test
    public void testPrincipalDeserializationFailureIsRejected() {
        KafkaPrincipalSerde failingSerde = new KafkaPrincipalSerde() {
            @Override
            public byte[] serialize(KafkaPrincipal principal) {
                return Utils.utf8(principal.toString());
            }

            @Override
            public KafkaPrincipal deserialize(byte[] bytes) {
                throw new IllegalArgumentException("mock error");
            }
        };
        Request envelope = buildEnvelopeRequest(
            createForwardableRequest(),
            Optional.of(failingSerde),
            InetAddress.getLoopbackAddress().getAddress()
        );

        PrincipalDeserializationException exception = assertThrows(
            PrincipalDeserializationException.class,
            () -> EnvelopeUtils.handleEnvelopeRequest(envelope, mock(RequestChannelMetrics.class), ignored -> { })
        );
        assertEquals("Failed to deserialize client principal from envelope", exception.getMessage());
    }

    private static CreateTopicsRequest createForwardableRequest() {
        CreateTopicsRequestData requestData = new CreateTopicsRequestData();
        requestData.topics().add(new CreatableTopic()
            .setName("topic")
            .setReplicationFactor((short) -1)
            .setNumPartitions(-1)
        );
        return new CreateTopicsRequest.Builder(requestData).build();
    }

    private static MetadataRequest createNonForwardableRequest() {
        return new MetadataRequest.Builder(List.of("topic"), true).build();
    }

    private static Request buildEnvelopeRequest(AbstractRequest forwardedRequest) {
        return buildEnvelopeRequest(
            forwardedRequest,
            Optional.of(PRINCIPAL_SERDE),
            InetAddress.getLoopbackAddress().getAddress()
        );
    }

    private static Request buildEnvelopeRequest(
        AbstractRequest forwardedRequest,
        Optional<KafkaPrincipalSerde> contextPrincipalSerde,
        byte[] clientAddress
    ) {
        RequestHeader forwardedRequestHeader = new RequestHeader(
            forwardedRequest.apiKey(),
            forwardedRequest.version(),
            CLIENT_ID,
            1
        );
        ByteBuffer forwardedRequestBuffer = forwardedRequest.serializeWithHeader(forwardedRequestHeader);

        RequestHeader envelopeRequestHeader = new RequestHeader(
            ApiKeys.ENVELOPE,
            ApiKeys.ENVELOPE.latestVersion(),
            CLIENT_ID,
            2
        );
        ByteBuffer envelopeRequestBuffer = new EnvelopeRequest.Builder(
            forwardedRequestBuffer,
            PRINCIPAL_SERDE.serialize(FORWARDED_PRINCIPAL),
            clientAddress
        ).build().serializeWithHeader(envelopeRequestHeader);

        // Advance the buffer past the envelope header before constructing the Request.
        RequestHeader.parse(envelopeRequestBuffer);

        RequestContext envelopeContext = new RequestContext(
            envelopeRequestHeader,
            CONNECTION_ID,
            InetAddress.getLoopbackAddress(),
            Optional.empty(),
            KafkaPrincipal.ANONYMOUS,
            ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT,
            ClientInformation.EMPTY,
            false,
            contextPrincipalSerde
        );

        return new Request(
            1,
            envelopeContext,
            0,
            MemoryPool.NONE,
            envelopeRequestBuffer,
            mock(RequestChannelMetrics.class),
            Optional.empty()
        );
    }
}
