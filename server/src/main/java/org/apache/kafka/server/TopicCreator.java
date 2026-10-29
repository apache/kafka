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

import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.RequestContext;

import java.util.concurrent.CompletableFuture;

/**
 * Abstraction for creating topics via the controller.
 * Allows different implementations to be used interchangeably
 * by the AutoTopicCreationManager, enabling better separation of concerns and testability.
 */
public interface TopicCreator {

    /**
     * Send a create topics request with principal for user-initiated topic creation.
     * The request context is used to preserve the original client principal for auditing.
     *
     * @param requestContext      The request context containing the client principal.
     * @param createTopicsRequest The topics to be created.
     * @return A future of the create topics response. This future will be completed on the network thread.
     */
    CompletableFuture<CreateTopicsResponse> createTopicWithPrincipal(
        RequestContext requestContext,
        CreateTopicsRequest.Builder createTopicsRequest
    );

    /**
     * Send a create topics request without principal for internal topic creation (e.g., consumer offsets, transaction state).
     * No request context is required since these are system-initiated requests.
     *
     * @param createTopicsRequest The topics to be created.
     * @return A future of the create topics response. This future will be completed on the network thread.
     */
    CompletableFuture<CreateTopicsResponse> createTopicWithoutPrincipal(
        CreateTopicsRequest.Builder createTopicsRequest
    );
}
