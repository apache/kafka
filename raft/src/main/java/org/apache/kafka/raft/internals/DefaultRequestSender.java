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
package org.apache.kafka.raft.internals;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.raft.NetworkChannel;
import org.apache.kafka.raft.RaftMessageQueue;
import org.apache.kafka.raft.RaftRequest;
import org.apache.kafka.raft.RaftResponse;
import org.apache.kafka.raft.RequestManager;
import org.apache.kafka.raft.utils.ApiMessageUtils;

import org.slf4j.Logger;

import java.util.OptionalLong;
import java.util.function.Supplier;

public final class DefaultRequestSender  implements RequestSender {
    private final RequestManager requestManager;
    private final NetworkChannel channel;
    private final RaftMessageQueue messageQueue;
    private final Logger logger;

    public DefaultRequestSender(
        RequestManager requestManager,
        NetworkChannel channel,
        RaftMessageQueue messageQueue,
        LogContext logContext
    ) {
        this.requestManager = requestManager;
        this.channel = channel;
        this.messageQueue = messageQueue;
        this.logger = logContext.logger(DefaultRequestSender.class);
    }

    @Override
    public ListenerName listenerName() {
        return channel.listenerName();
    }

    @Override
    public OptionalLong send(
        Node destination,
        Supplier<ApiMessage> requestSupplier,
        long currentTimeMs
    ) {
        if (requestManager.isBackingOff(destination, currentTimeMs)) {
            long remainingBackoffMs = requestManager.remainingBackoffMs(destination, currentTimeMs);
            logger.debug("Connection for {} is backing off for {} ms", destination, remainingBackoffMs);
            return OptionalLong.empty();
        }

        if (!requestManager.isReady(destination, currentTimeMs)) {
            long remainingMs = requestManager.remainingRequestTimeMs(destination, currentTimeMs);
            logger.debug("Connection for {} has a pending request for {} ms", destination, remainingMs);
            return OptionalLong.empty();
        }

        int correlationId = channel.newCorrelationId();
        ApiMessage request = requestSupplier.get();

        RaftRequest.Outbound requestMessage = new RaftRequest.Outbound(
            correlationId,
            request,
            destination,
            currentTimeMs
        );

        requestMessage.completion.whenComplete((response, exception) -> {
            if (exception != null) {
                ApiKeys api = ApiKeys.forId(request.apiKey());
                Errors error = Errors.forException(exception);
                ApiMessage errorResponse = ApiMessageUtils.parseErrorResponse(api, error);

                response = new RaftResponse.Inbound(
                    correlationId,
                    errorResponse,
                    destination
                );
            }

            messageQueue.add(response);
        });

        requestManager.onRequestSent(destination, correlationId, currentTimeMs);
        channel.send(requestMessage);
        logger.trace("Sent outbound request: {}", requestMessage);

        return OptionalLong.of(requestManager.remainingRequestTimeMs(destination, currentTimeMs));
    }
}
