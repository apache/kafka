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

import com.fasterxml.jackson.core.type.TypeReference;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.core.HttpHeaders;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.RestartRequest;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.Crypto;
import org.apache.kafka.connect.runtime.distributed.RebalanceNeededException;
import org.apache.kafka.connect.runtime.distributed.RequestTargetException;
import org.apache.kafka.connect.runtime.rest.InternalRequestSignature;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.entities.ActiveTopicsInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.CreateConnectorRequest;
import org.apache.kafka.connect.runtime.rest.entities.TaskInfo;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.kafka.connect.runtime.WorkerConfig.TOPIC_TRACKING_ALLOW_RESET_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.TOPIC_TRACKING_ENABLE_CONFIG;

@Path("/connectors")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ConnectorsResource implements ConnectResource {
    private static final Logger log = LoggerFactory.getLogger(ConnectorsResource.class);
    private static final TypeReference<List<Map<String, String>>> TASK_CONFIGS_TYPE =
        new TypeReference<List<Map<String, String>>>() { };

    private final Herder herder;
    private final RestClient restClient;
    private long requestTimeoutMs;
    @javax.ws.rs.core.Context
    private ServletContext context;
    private final boolean isTopicTrackingDisabled;
    private final boolean isTopicTrackingResetDisabled;

    public ConnectorsResource(Herder herder, WorkerConfig config, RestClient restClient) {
        this.herder = herder;
        this.restClient = restClient;
        isTopicTrackingDisabled = !config.getBoolean(TOPIC_TRACKING_ENABLE_CONFIG);
        isTopicTrackingResetDisabled = !config.getBoolean(TOPIC_TRACKING_ALLOW_RESET_CONFIG);
        this.requestTimeoutMs = DEFAULT_REST_REQUEST_TIMEOUT_MS;
    }

    @Override
    public void requestTimeout(long requestTimeoutMs) {
        if (requestTimeoutMs < 1) {
            throw new IllegalArgumentException("REST request timeout must be positive");
        }
        this.requestTimeoutMs = requestTimeoutMs;
    }

    @GET
    @Path("/")
    @Operation(summary = "List all active connectors")
    public Response listConnectors(
        final @Context UriInfo uriInfo,
        final @Context HttpHeaders headers
    ) {
        if (uriInfo.getQueryParameters().containsKey("expand")) {
            Map<String, Map<String, Object>> out = new HashMap<>();
            for (String connector : herder.connectors()) {
                try {
                    Map<String, Object> connectorExpansions = new HashMap<>();
                    for (String expansion : uriInfo.getQueryParameters().get("expand")) {
                        switch (expansion) {
                            case "status":
                                connectorExpansions.put("status", herder.connectorStatus(connector));
                                break;
                            case "info":
                                connectorExpansions.put("info", herder.connectorInfo(connector));
                                break;
                            default:
                                log.info("Ignoring unknown expansion type {}", expansion);
                        }
                    }
                    out.put(connector, connectorExpansions);
                } catch (NotFoundException e) {
                    // this likely means that a connector has been removed while we look its info up
                    // we can just not include this connector in the return entity
                    log.debug("Unable to get connector info for {} on this worker", connector);
                }

            }
            return Response.ok(out).build();
        } else {
            return Response.ok(herder.connectors()).build();
        }
    }

    @POST
    @Path("/")
    @Operation(summary = "Create a new connector")
    public Response createConnector(final @Parameter(hidden = true) @QueryParam("forward") Boolean forward,
                                    final @Context HttpHeaders headers,
                                    final CreateConnectorRequest createRequest) throws Throwable {
        // Trim leading and trailing whitespaces from the connector name, replace null with empty string
        // if no name element present to keep validation within validator (NonEmptyStringWithoutControlChars
        // allows null values)
        String name = createRequest.name() == null ? "" : createRequest.name().trim();

        Map<String, String> configs = createRequest.config();
        checkAndPutConnectorConfigName(name, configs);

        FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>();
        herder.putConnectorConfig(name, configs, false, cb);
        Herder.Created<ConnectorInfo> info = completeOrForwardRequest(cb, "/connectors", "POST", headers, createRequest,
                new TypeReference<ConnectorInfo>() { }, new CreatedConnectorInfoTranslator(), forward);

        URI location = UriBuilder.fromUri("/connectors").path(name).build();
        return Response.created(location).entity(info.result()).build();
    }

    @GET
    @Path("/{connector}")
    @Operation(summary = "Get the details for the specified connector")
    public ConnectorInfo getConnector(final @PathParam("connector") String connector,
                                      final @Context HttpHeaders headers,
                                      final @Parameter(hidden = true) @QueryParam("forward") Boolean forward) throws Throwable {
        FutureCallback<ConnectorInfo> cb = new FutureCallback<>();
        herder.connectorInfo(connector, cb);
        return completeOrForwardRequest(cb, "/connectors/" + connector, "GET", headers, null, forward);
    }

    @GET
    @Path("/{connector}/config")
    @Operation(summary = "Get the configuration for the specified connector")
    public Map<String, String> getConnectorConfig(final @PathParam("connector") String connector,
                                                  final @Context HttpHeaders headers,
                                                  final @Parameter(hidden = true) @QueryParam("forward") Boolean forward) throws Throwable {
        FutureCallback<Map<String, String>> cb = new FutureCallback<>();
        herder.connectorConfig(connector, cb);
        return completeOrForwardRequest(cb, "/connectors/" + connector + "/config", "GET", headers, null, forward);
    }

    @GET
    @Path("/{connector}/tasks-config")
    @Operation(summary = "Get the configuration of all tasks for the specified connector")
    public Map<ConnectorTaskId, Map<String, String>> getTasksConfig(
            final @PathParam("connector") String connector,
            final @Context HttpHeaders headers,
            final @Parameter(hidden = true) @QueryParam("forward") Boolean forward) throws Throwable {
        FutureCallback<Map<ConnectorTaskId, Map<String, String>>> cb = new FutureCallback<>();
        herder.tasksConfig(connector, cb);
        return completeOrForwardRequest(cb, "/connectors/" + connector + "/tasks-config", "GET", headers, null, forward);
    }

    @GET
    @Path("/{connector}/status")
    @Operation(summary = "Get the status for the specified connector")
    public ConnectorStateInfo getConnectorStatus(final @PathParam("connector") String connector) {
        return herder.connectorStatus(connector);
    }

    @GET
    @Path("/{connector}/topics")
    @Operation(summary = "Get the list of topics actively used by the specified connector")
    public Response getConnectorActiveTopics(final @PathParam("connector") String connector) {
        if (isTopicTrackingDisabled) {
            throw new ConnectRestException(Response.Status.FORBIDDEN.getStatusCode(),
                    "Topic tracking is disabled.");
        }
        ActiveTopicsInfo info = herder.connectorActiveTopics(connector);
        return Response.ok(Collections.singletonMap(info.connector(), info)).build();
    }

    @PUT
    @Path("/{connector}/topics/reset")
    @Operation(summary = "Reset the list of topics actively used by the specified connector")
    public Response resetConnectorActiveTopics(final @PathParam("connector") String connector, final @Context HttpHeaders headers) {
        if (isTopicTrackingDisabled) {
            throw new ConnectRestException(Response.Status.FORBIDDEN.getStatusCode(),
                    "Topic tracking is disabled.");
        }
        if (isTopicTrackingResetDisabled) {
            throw new ConnectRestException(Response.Status.FORBIDDEN.getStatusCode(),
                    "Topic tracking reset is disabled.");
        }
        herder.resetConnectorActiveTopics(connector);
        return Response.accepted().build();
    }

    @PUT
    @Path("/{connector}/config")
    @Operation(summary = "Create or reconfigure the specified connector")
    public Response putConnectorConfig(final @PathParam("connector") String connector,
                                       final @Context HttpHeaders headers,
                                       final @Parameter(hidden = true) @QueryParam("forward") Boolean forward,
                                       final Map<String, String> connectorConfig) throws Throwable {
        FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>();
        checkAndPutConnectorConfigName(connector, connectorConfig);

        herder.putConnectorConfig(connector, connectorConfig, true, cb);
        Herder.Created<ConnectorInfo> createdInfo = completeOrForwardRequest(cb, "/connectors/" + connector + "/config",
                "PUT", headers, connectorConfig, new TypeReference<ConnectorInfo>() { }, new CreatedConnectorInfoTranslator(), forward);
        Response.ResponseBuilder response;
        if (createdInfo.created()) {
            URI location = UriBuilder.fromUri("/connectors").path(connector).build();
            response = Response.created(location);
        } else {
            response = Response.ok();
        }
        return response.entity(createdInfo.result()).build();
    }

    @POST
    @Path("/{connector}/restart")
    @Operation(summary = "Restart the specified connector")
    public Response restartConnector(final @PathParam("connector") String connector,
                                 final @Context HttpHeaders headers,
                                 final @DefaultValue("false") @QueryParam("includeTasks") @Parameter(description = "Whether to also restart tasks") Boolean includeTasks,
                                 final @DefaultValue("false") @QueryParam("onlyFailed") @Parameter(description = "Whether to only restart failed tasks/connectors")Boolean onlyFailed,
                                 final @Parameter(hidden = true) @QueryParam("forward") Boolean forward) throws Throwable {
        RestartRequest restartRequest = new RestartRequest(connector, onlyFailed, includeTasks);
        String forwardingPath = "/connectors/" + connector + "/restart";
        if (restartRequest.forceRestartConnectorOnly()) {
            // For backward compatibility, just restart the connector instance and return OK with no body
            FutureCallback<Void> cb = new FutureCallback<>();
            herder.restartConnector(connector, cb);
            completeOrForwardRequest(cb, forwardingPath, "POST", headers, null, forward);
            return Response.noContent().build();
        }

        // In all other cases, submit the async restart request and return connector state
        FutureCallback<ConnectorStateInfo> cb = new FutureCallback<>();
        herder.restartConnectorAndTasks(restartRequest, cb);
        Map<String, String> queryParameters = new HashMap<>();
        queryParameters.put("includeTasks", includeTasks.toString());
        queryParameters.put("onlyFailed", onlyFailed.toString());
        ConnectorStateInfo stateInfo = completeOrForwardRequest(cb, forwardingPath, "POST", headers, queryParameters, null, new TypeReference<ConnectorStateInfo>() {
        }, new IdentityTranslator<>(), forward);
        return Response.accepted().entity(stateInfo).build();
    }

    @PUT
    @Path("/{connector}/pause")
    @Operation(summary = "Pause the specified connector",
               description = "This operation is idempotent and has no effects if the connector is already paused")
    public Response pauseConnector(@PathParam("connector") String connector, final @Context HttpHeaders headers) {
        herder.pauseConnector(connector);
        return Response.accepted().build();
    }

    @PUT
    @Path("/{connector}/resume")
    @Operation(summary = "Resume the specified connector",
               description = "This operation is idempotent and has no effects if the connector is already running")
    public Response resumeConnector(@PathParam("connector") String connector) {
        herder.resumeConnector(connector);
        return Response.accepted().build();
    }

    @GET
    @Path("/{connector}/tasks")
    @Operation(summary = "List all tasks for the specified connector")
    public List<TaskInfo> getTaskConfigs(final @PathParam("connector") String connector,
                                         final @Context HttpHeaders headers,
                                         final @Parameter(hidden = true) @QueryParam("forward") Boolean forward) throws Throwable {
        FutureCallback<List<TaskInfo>> cb = new FutureCallback<>();
        herder.taskConfigs(connector, cb);
        return completeOrForwardRequest(cb, "/connectors/" + connector + "/tasks", "GET", headers, null, new TypeReference<List<TaskInfo>>() {
        }, forward);
    }

    @POST
    @Path("/{connector}/tasks")
    @Operation(hidden = true, summary = "This operation is only for inter-worker communications")
    public void putTaskConfigs(final @PathParam("connector") String connector,
                               final @Context HttpHeaders headers,
                               final @QueryParam("forward") Boolean forward,
                               final byte[] requestBody) throws Throwable {
        List<Map<String, String>> taskConfigs = new ObjectMapper().readValue(requestBody, TASK_CONFIGS_TYPE);
        FutureCallback<Void> cb = new FutureCallback<>();
        herder.putTaskConfigs(connector, taskConfigs, cb, InternalRequestSignature.fromHeaders(Crypto.SYSTEM, requestBody, headers));
        completeOrForwardRequest(cb, "/connectors/" + connector + "/tasks", "POST", headers, taskConfigs, forward);
    }

    @PUT
    @Path("/{connector}/fence")
    @Operation(hidden = true, summary = "This operation is only for inter-worker communications")
    public void fenceZombies(final @PathParam("connector") String connector,
                             final @Context HttpHeaders headers,
                             final @QueryParam("forward") Boolean forward,
                             final byte[] requestBody) throws Throwable {
        FutureCallback<Void> cb = new FutureCallback<>();
        herder.fenceZombieSourceTasks(connector, cb, InternalRequestSignature.fromHeaders(Crypto.SYSTEM, requestBody, headers));
        completeOrForwardRequest(cb, "/connectors/" + connector + "/fence", "PUT", headers, requestBody, forward);
    }

    @GET
    @Path("/{connector}/tasks/{task}/status")
    @Operation(summary = "Get the state of the specified task for the specified connector")
    public ConnectorStateInfo.TaskState getTaskStatus(final @PathParam("connector") String connector,
                                                      final @Context HttpHeaders headers,
                                                      final @PathParam("task") Integer task) {
        return herder.taskStatus(new ConnectorTaskId(connector, task));
    }

    @POST
    @Path("/{connector}/tasks/{task}/restart")
    @Operation(summary = "Restart the specified task for the specified connector")
    public void restartTask(final @PathParam("connector") String connector,
                            final @PathParam("task") Integer task,
                            final @Context HttpHeaders headers,
                            final @Parameter(hidden = true) @QueryParam("forward") Boolean forward) throws Throwable {
        FutureCallback<Void> cb = new FutureCallback<>();
        ConnectorTaskId taskId = new ConnectorTaskId(connector, task);
        herder.restartTask(taskId, cb);
        completeOrForwardRequest(cb, "/connectors/" + connector + "/tasks/" + task + "/restart", "POST", headers, null, forward);
    }

    @DELETE
    @Path("/{connector}")
    @Operation(summary = "Delete the specified connector")
    public void destroyConnector(final @PathParam("connector") String connector,
                                 final @Context HttpHeaders headers,
                                 final @Parameter(hidden = true) @QueryParam("forward") Boolean forward) throws Throwable {
        FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>();
        herder.deleteConnectorConfig(connector, cb);
        completeOrForwardRequest(cb, "/connectors/" + connector, "DELETE", headers, null, forward);
    }

    // Check whether the connector name from the url matches the one (if there is one) provided in the connectorConfig
    // object. Throw BadRequestException on mismatch, otherwise put connectorName in config
    private void checkAndPutConnectorConfigName(String connectorName, Map<String, String> connectorConfig) {
        String includedName = connectorConfig.get(ConnectorConfig.NAME_CONFIG);
        if (includedName != null) {
            if (!includedName.equals(connectorName))
                throw new BadRequestException("Connector name configuration (" + includedName + ") doesn't match connector name in the URL (" + connectorName + ")");
        } else {
            connectorConfig.put(ConnectorConfig.NAME_CONFIG, connectorName);
        }
    }

    // Wait for a FutureCallback to complete. If it succeeds, return the parsed response. If it fails, try to forward the
    // request to the leader.
    private <T, U> T completeOrForwardRequest(FutureCallback<T> cb,
                                              String path,
                                              String method,
                                              HttpHeaders headers,
                                              Map<String, String> queryParameters,
                                              Object body,
                                              TypeReference<U> resultType,
                                              Translator<T, U> translator,
                                              Boolean forward) throws Throwable {
        try {
            return cb.get(requestTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();

            if (cause instanceof RequestTargetException) {
                if (restClient == null) {
                    throw new ConnectRestException(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
                            "Cannot complete request as non-leader with request forwarding disabled");
                } else if (forward == null || forward) {
                    // the only time we allow recursive forwarding is when no forward flag has
                    // been set, which should only be seen by the first worker to handle a user request.
                    // this gives two total hops to resolve the request before giving up.
                    boolean recursiveForward = forward == null;
                    RequestTargetException targetException = (RequestTargetException) cause;
                    String forwardedUrl = targetException.forwardUrl();
                    if (forwardedUrl == null) {
                        // the target didn't know of the leader at this moment.
                        throw new ConnectRestException(Response.Status.CONFLICT.getStatusCode(),
                                "Cannot complete request momentarily due to no known leader URL, "
                                + "likely because a rebalance was underway.");
                    }
                    UriBuilder uriBuilder = UriBuilder.fromUri(forwardedUrl)
                            .path(path)
                            .queryParam("forward", recursiveForward);
                    if (queryParameters != null) {
                        queryParameters.forEach(uriBuilder::queryParam);
                    }
                    String forwardUrl = uriBuilder.build().toString();
                    log.debug("Forwarding request {} {} {}", forwardUrl, method, body);
                    return translator.translate(restClient.httpRequest(forwardUrl, method, headers, body, resultType));
                } else {
                    // we should find the right target for the query within two hops, so if
                    // we don't, it probably means that a rebalance has taken place.
                    throw new ConnectRestException(Response.Status.CONFLICT.getStatusCode(),
                            "Cannot complete request because of a conflicting operation (e.g. worker rebalance)");
                }
            } else if (cause instanceof RebalanceNeededException) {
                throw new ConnectRestException(Response.Status.CONFLICT.getStatusCode(),
                        "Cannot complete request momentarily due to stale configuration (typically caused by a concurrent config change)");
            }

            throw cause;
        } catch (TimeoutException e) {
            // This timeout is for the operation itself. None of the timeout error codes are relevant, so internal server
            // error is the best option
            throw new ConnectRestException(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), "Request timed out");
        } catch (InterruptedException e) {
            throw new ConnectRestException(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), "Request interrupted");
        }
    }

    private <T, U> T completeOrForwardRequest(FutureCallback<T> cb, String path, String method, HttpHeaders headers, Object body,
                                              TypeReference<U> resultType, Translator<T, U> translator, Boolean forward) throws Throwable {
        return completeOrForwardRequest(cb, path, method, headers, null, body, resultType, translator, forward);
    }

    private <T> T completeOrForwardRequest(FutureCallback<T> cb, String path, String method, HttpHeaders headers, Object body,
                                           TypeReference<T> resultType, Boolean forward) throws Throwable {
        return completeOrForwardRequest(cb, path, method, headers, body, resultType, new IdentityTranslator<>(), forward);
    }

    private <T> T completeOrForwardRequest(FutureCallback<T> cb, String path, String method, HttpHeaders headers,
                                           Object body, Boolean forward) throws Throwable {
        return completeOrForwardRequest(cb, path, method, headers, body, null, new IdentityTranslator<>(), forward);
    }

    private interface Translator<T, U> {
        T translate(RestClient.HttpResponse<U> response);
    }

    private static class IdentityTranslator<T> implements Translator<T, T> {
        @Override
        public T translate(RestClient.HttpResponse<T> response) {
            return response.body();
        }
    }

    private static class CreatedConnectorInfoTranslator implements Translator<Herder.Created<ConnectorInfo>, ConnectorInfo> {
        @Override
        public Herder.Created<ConnectorInfo> translate(RestClient.HttpResponse<ConnectorInfo> response) {
            boolean created = response.status() == 201;
            return new Herder.Created<>(created, response.body());
        }
    }
}
