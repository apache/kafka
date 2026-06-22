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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.EnvelopeRequest;
import org.apache.kafka.common.requests.EnvelopeResponse;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.server.common.ControllerRequestCompletionHandler;
import org.apache.kafka.server.common.NodeToControllerChannelManager;
import org.apache.kafka.server.metrics.ForwardingManagerMetrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class ForwardingManagerImpl implements ForwardingManager, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ForwardingManagerImpl.class);

    private final NodeToControllerChannelManager channelManager;
    private final ForwardingManagerMetrics forwardingManagerMetrics;

    public ForwardingManagerImpl(NodeToControllerChannelManager channelManager, Metrics metrics) {
        this.channelManager = channelManager;
        this.forwardingManagerMetrics = new ForwardingManagerMetrics(metrics, channelManager.getTimeoutMs());
    }

    ForwardingManagerMetrics forwardingManagerMetrics() {
        return forwardingManagerMetrics;
    }

    @Override
    public void forwardRequest(
            RequestContext requestContext,
            ByteBuffer requestBufferCopy,
            long requestCreationNs,
            AbstractRequest requestBody,
            Supplier<String> requestToString,
            Consumer<Optional<AbstractResponse>> responseCallback) {
        EnvelopeRequest.Builder envelopeRequest = ForwardingManagerUtil.buildEnvelopeRequest(requestContext, requestBufferCopy);
        long requestCreationTimeMs = TimeUnit.NANOSECONDS.toMillis(requestCreationNs);

        class ForwardingResponseHandler implements ControllerRequestCompletionHandler {

            @Override
            public void onComplete(ClientResponse clientResponse) {
                forwardingManagerMetrics.decrementQueueLength();
                forwardingManagerMetrics.remoteTimeMsHist().record(clientResponse.requestLatencyMs());
                forwardingManagerMetrics.queueTimeMsHist().record(clientResponse.receivedTimeMs() - clientResponse.requestLatencyMs() - requestCreationTimeMs);

                if (clientResponse.versionMismatch() != null) {
                    LOG.debug("Returning `UNKNOWN_SERVER_ERROR` in response to {} due to unexpected version error",
                            requestToString.get(), clientResponse.versionMismatch());
                    responseCallback.accept(Optional.of(requestBody.getErrorResponse(Errors.UNKNOWN_SERVER_ERROR.exception())));
                } else if (clientResponse.authenticationException() != null) {
                    LOG.debug("Returning `UNKNOWN_SERVER_ERROR` in response to {} due to authentication error",
                            requestToString.get(), clientResponse.authenticationException());
                    responseCallback.accept(Optional.of(requestBody.getErrorResponse(Errors.UNKNOWN_SERVER_ERROR.exception())));
                } else {
                    EnvelopeResponse envelopeResponse = (EnvelopeResponse) clientResponse.responseBody();
                    Errors envelopeError = envelopeResponse.error();

                    // Unsupported version indicates an incompatibility between controller and client API versions. This
                    // could happen when the controller changed after the connection was established. The forwarding broker
                    // should close the connection with the client and let it reinitialize the connection and refresh
                    // the controller API versions.
                    if (envelopeError == Errors.UNSUPPORTED_VERSION) {
                        responseCallback.accept(Optional.empty());
                    } else {
                        AbstractResponse response;
                        if (envelopeError != Errors.NONE) {
                            // A general envelope error indicates broker misconfiguration (e.g. the principal serde
                            // might not be defined on the receiving broker). In this case, we do not return
                            // the error directly to the client since it would not be expected. Instead, we
                            // return `UNKNOWN_SERVER_ERROR` so that the user knows that there is a problem
                            // on the broker.
                            LOG.debug("Forwarded request {} failed with an error in the envelope response {}",
                                    requestToString.get(), envelopeError);
                            response = requestBody.getErrorResponse(Errors.UNKNOWN_SERVER_ERROR.exception());
                        } else {
                            response = parseResponse(envelopeResponse.responseData(), requestBody, requestContext.header);
                        }
                        responseCallback.accept(Optional.of(response));
                    }
                }
            }

            @Override
            public void onTimeout() {
                LOG.debug("Forwarding of the request {} failed due to timeout exception", requestToString.get());
                forwardingManagerMetrics.decrementQueueLength();
                forwardingManagerMetrics.queueTimeMsHist().record(channelManager.getTimeoutMs());
                AbstractResponse response = requestBody.getErrorResponse(new TimeoutException());
                responseCallback.accept(Optional.of(response));
            }
        }

        forwardingManagerMetrics.incrementQueueLength();
        channelManager.sendRequest(envelopeRequest, new ForwardingResponseHandler());
    }

    @Override
    public void close() {
        forwardingManagerMetrics.close();
    }

    @Override
    public Optional<NodeApiVersions> controllerApiVersions() {
        return channelManager.controllerApiVersions();
    }

    private AbstractResponse parseResponse(ByteBuffer buffer, AbstractRequest request, RequestHeader header) {
        try {
            return AbstractResponse.parseResponse(buffer, header);
        } catch (Exception e) {
            LOG.error("Failed to parse response from envelope for request with header {}", header, e);
            return request.getErrorResponse(Errors.UNKNOWN_SERVER_ERROR.exception());
        }
    }
}
