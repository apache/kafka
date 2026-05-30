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
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.EnvelopeRequest;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipalSerde;
import org.apache.kafka.network.Request;
import org.apache.kafka.network.metrics.RequestChannelMetrics;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.function.Consumer;

public final class EnvelopeUtils {
    private EnvelopeUtils() {
    }

    public static void handleEnvelopeRequest(
        Request request,
        RequestChannelMetrics requestChannelMetrics,
        Consumer<Request> handler
    ) {
        EnvelopeRequest envelope = request.body(EnvelopeRequest.class);
        KafkaPrincipal forwardedPrincipal = parseForwardedPrincipal(request.context(), envelope.requestPrincipal());
        InetAddress forwardedClientAddress = parseForwardedClientAddress(envelope.clientAddress());

        ByteBuffer forwardedRequestBuffer = envelope.requestData().duplicate();
        RequestHeader forwardedRequestHeader = parseForwardedRequestHeader(forwardedRequestBuffer);

        ApiKeys forwardedApi = forwardedRequestHeader.apiKey();
        if (!forwardedApi.forwardable) {
            throw new InvalidRequestException("API " + forwardedApi + " is not enabled or is not eligible for forwarding");
        }

        RequestContext forwardedContext = new RequestContext(
            forwardedRequestHeader,
            request.context().connectionId,
            forwardedClientAddress,
            forwardedPrincipal,
            request.context().listenerName,
            request.context().securityProtocol,
            ClientInformation.EMPTY,
            request.context().fromPrivilegedListener
        );

        Request forwardedRequest = parseForwardedRequest(
            request,
            forwardedContext,
            forwardedRequestBuffer,
            requestChannelMetrics
        );
        handler.accept(forwardedRequest);
    }

    private static InetAddress parseForwardedClientAddress(byte[] address) {
        try {
            return InetAddress.getByAddress(address);
        } catch (UnknownHostException e) {
            throw new InvalidRequestException("Failed to parse client address from envelope", e);
        }
    }

    private static Request parseForwardedRequest(
        Request envelope,
        RequestContext forwardedContext,
        ByteBuffer buffer,
        RequestChannelMetrics requestChannelMetrics
    ) {
        try {
            Request forwardedRequest = new Request(
                envelope.processor(),
                forwardedContext,
                envelope.startTimeNanos(),
                envelope.memoryPool(),
                buffer,
                requestChannelMetrics,
                Optional.of(envelope)
            );
            // set the dequeue time of forwardedRequest as the value of envelope request
            forwardedRequest.requestDequeueTimeNanos(envelope.requestDequeueTimeNanos());
            return forwardedRequest;
        } catch (InvalidRequestException e) {
            // We use UNSUPPORTED_VERSION if the embedded request cannot be parsed.
            // The purpose is to disambiguate structural errors in the envelope request
            // itself, such as an invalid client address.
            throw new UnsupportedVersionException("Failed to parse forwarded request with header " + forwardedContext.header, e);
        }
    }

    private static RequestHeader parseForwardedRequestHeader(ByteBuffer buffer) {
        try {
            return RequestHeader.parse(buffer);
        } catch (InvalidRequestException e) {
            // We use UNSUPPORTED_VERSION if the embedded request cannot be parsed.
            // The purpose is to disambiguate structural errors in the envelope request
            // itself, such as an invalid client address.
            throw new UnsupportedVersionException("Failed to parse request header from envelope", e);
        }
    }

    private static KafkaPrincipal parseForwardedPrincipal(
        RequestContext envelopeContext,
        byte[] principalBytes
    ) {
        KafkaPrincipalSerde principalSerde = envelopeContext.principalSerde.orElseThrow(() ->
            new PrincipalDeserializationException("Could not deserialize principal since no `KafkaPrincipalSerde` has been defined"));

        try {
            return principalSerde.deserialize(principalBytes);
        } catch (Exception e) {
            throw new PrincipalDeserializationException("Failed to deserialize client principal from envelope", e);
        }
    }
}
