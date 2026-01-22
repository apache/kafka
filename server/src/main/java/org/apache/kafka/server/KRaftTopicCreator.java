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
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.EnvelopeResponse;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.server.common.ControllerRequestCompletionHandler;
import org.apache.kafka.server.common.NodeToControllerChannelManager;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * KRaft implementation of TopicCreator that forwards CreateTopics requests to the controller.
 * When creating topics with a principal, requests are wrapped in an envelope to preserve the
 * original request context for authorization.
 */
public class KRaftTopicCreator implements TopicCreator {

    private final NodeToControllerChannelManager channelManager;

    public KRaftTopicCreator(NodeToControllerChannelManager channelManager) {
        this.channelManager = channelManager;
    }

    @Override
    public CompletableFuture<CreateTopicsResponse> createTopicWithPrincipal(
        RequestContext requestContext,
        CreateTopicsRequest.Builder createTopicsRequest
    ) {
        CompletableFuture<CreateTopicsResponse> responseFuture = new CompletableFuture<>();

        short requestVersion = channelManager.controllerApiVersions()
            .map(v -> v.latestUsableVersion(ApiKeys.CREATE_TOPICS))
            .orElse(ApiKeys.CREATE_TOPICS.latestVersion());

        RequestHeader requestHeader = new RequestHeader(
            ApiKeys.CREATE_TOPICS,
            requestVersion,
            requestContext.clientId(),
            requestContext.correlationId()
        );

        AbstractRequest.Builder<? extends AbstractRequest> envelopeRequest = ForwardingManagerUtil.buildEnvelopeRequest(
            requestContext,
            createTopicsRequest.build(requestHeader.apiVersion())
                .serializeWithHeader(requestHeader)
        );

        ControllerRequestCompletionHandler handler = new ControllerRequestCompletionHandler() {
            @Override
            public void onTimeout() {
                responseFuture.completeExceptionally(
                    new TimeoutException("CreateTopicsRequest to controller timed out")
                );
            }

            @Override
            public void onComplete(ClientResponse response) {
                if (response.authenticationException() != null) {
                    responseFuture.completeExceptionally(response.authenticationException());
                } else if (response.versionMismatch() != null) {
                    responseFuture.completeExceptionally(response.versionMismatch());
                } else if (response.wasDisconnected()) {
                    responseFuture.completeExceptionally(new IOException("Disconnected before receiving CreateTopicsResponse"));
                } else if (response.hasResponse()) {
                    if (response.responseBody() instanceof EnvelopeResponse envelopeResponse) {
                        Errors envelopeError = envelopeResponse.error();
                        if (envelopeError != Errors.NONE) {
                            responseFuture.completeExceptionally(envelopeError.exception());
                        } else {
                            try {
                                CreateTopicsResponse createTopicsResponse = (CreateTopicsResponse) AbstractResponse.parseResponse(
                                    envelopeResponse.responseData(),
                                    requestHeader
                                );
                                responseFuture.complete(createTopicsResponse);
                            } catch (Exception e) {
                                responseFuture.completeExceptionally(e);
                            }
                        }
                    } else {
                        responseFuture.completeExceptionally(
                            new IllegalStateException("Expected EnvelopeResponse but got: " +
                                response.responseBody().getClass().getSimpleName())
                        );
                    }
                } else {
                    responseFuture.completeExceptionally(
                        new IllegalStateException("Got no response body for EnvelopeResponse")
                    );
                }
            }
        };

        channelManager.sendRequest(envelopeRequest, handler);
        return responseFuture;
    }

    @Override
    public CompletableFuture<CreateTopicsResponse> createTopicWithoutPrincipal(
        CreateTopicsRequest.Builder createTopicsRequest
    ) {
        CompletableFuture<CreateTopicsResponse> responseFuture = new CompletableFuture<>();

        ControllerRequestCompletionHandler handler = new ControllerRequestCompletionHandler() {
            @Override
            public void onTimeout() {
                responseFuture.completeExceptionally(
                    new TimeoutException("CreateTopicsRequest to controller timed out")
                );
            }

            @Override
            public void onComplete(ClientResponse response) {
                if (response.authenticationException() != null) {
                    responseFuture.completeExceptionally(response.authenticationException());
                } else if (response.versionMismatch() != null) {
                    responseFuture.completeExceptionally(response.versionMismatch());
                } else if (response.wasDisconnected()) {
                    responseFuture.completeExceptionally(new IOException("Disconnected before receiving CreateTopicsResponse"));
                } else if (response.hasResponse()) {
                    if (response.responseBody() instanceof CreateTopicsResponse createTopicsResponse) {
                        responseFuture.complete(createTopicsResponse);
                    } else {
                        responseFuture.completeExceptionally(
                            new IllegalStateException("Expected CreateTopicsResponse but got: " +
                                response.responseBody().getClass().getSimpleName())
                        );
                    }
                } else {
                    responseFuture.completeExceptionally(
                        new IllegalStateException("Got no response body for CreateTopicsRequest")
                    );
                }
            }
        };

        channelManager.sendRequest(createTopicsRequest, handler);
        return responseFuture;
    }

}
