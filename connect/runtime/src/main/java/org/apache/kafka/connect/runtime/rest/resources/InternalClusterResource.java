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
package org.apache.kafka.connect.runtime.rest.resources;

import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.distributed.Crypto;
import org.apache.kafka.connect.runtime.rest.HerderRequestHandler;
import org.apache.kafka.connect.runtime.rest.InternalRequestSignature;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.RestRequestTimeout;
import org.apache.kafka.connect.util.FutureCallback;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;

import io.swagger.v3.oas.annotations.Operation;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.UriInfo;

/**
 * Contains endpoints necessary for intra-cluster communication--that is, requests that
 * workers will issue to each other that originate from within the cluster, as opposed to
 * requests that originate from a user and are forwarded from one worker to another.
 */
@Produces(MediaType.APPLICATION_JSON)
public abstract class InternalClusterResource {

    private static final TypeReference<List<Map<String, String>>> TASK_CONFIGS_TYPE =
            new TypeReference<List<Map<String, String>>>() { };

    private final HerderRequestHandler requestHandler;

    // Visible for testing
    @Context
    UriInfo uriInfo;

    protected InternalClusterResource(RestClient restClient, RestRequestTimeout requestTimeout) {
        this.requestHandler = new HerderRequestHandler(restClient, requestTimeout);
    }

    /**
     * @return a {@link Herder} instance that can be used to satisfy the current request; may not be null
     * @throws jakarta.ws.rs.NotFoundException if no such herder can be provided
     */
    protected abstract Herder herderForRequest();

    @POST
    @Path("/{connector}/tasks")
    @Operation(hidden = true, summary = "This operation is only for inter-worker communications")
    public void putTaskConfigs(
            final @PathParam("connector") String connector,
            final @Context HttpHeaders headers,
            final @QueryParam("forward") Boolean forward,
            final byte[] requestBody) throws Throwable {
        List<Map<String, String>> taskConfigs = new ObjectMapper().readValue(requestBody, TASK_CONFIGS_TYPE);
        FutureCallback<Void> cb = new FutureCallback<>();
        herderForRequest().putTaskConfigs(connector, taskConfigs, cb, InternalRequestSignature.fromHeaders(Crypto.SYSTEM, requestBody, headers));
        requestHandler.completeOrForwardRequest(
                cb,
                uriInfo.getPath(),
                "POST",
                headers,
                taskConfigs,
                forward
        );
    }

    @PUT
    @Path("/{connector}/fence")
    @Operation(hidden = true, summary = "This operation is only for inter-worker communications")
    public void fenceZombies(
            final @PathParam("connector") String connector,
            final @Context HttpHeaders headers,
            final @QueryParam("forward") Boolean forward,
            final byte[] requestBody) throws Throwable {
        FutureCallback<Void> cb = new FutureCallback<>();
        herderForRequest().fenceZombieSourceTasks(connector, cb, InternalRequestSignature.fromHeaders(Crypto.SYSTEM, requestBody, headers));
        requestHandler.completeOrForwardRequest(
                cb,
                uriInfo.getPath(),
                "PUT",
                headers,
                requestBody,
                forward
        );
    }

}
