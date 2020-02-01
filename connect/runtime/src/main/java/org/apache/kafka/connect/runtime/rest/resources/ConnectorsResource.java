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

import javax.ws.rs.core.HttpHeaders;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.WorkerConfig;
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
public class ConnectorsResource {
    private static final Logger log = LoggerFactory.getLogger(ConnectorsResource.class);
    private static final TypeReference<List<Map<String, String>>> TASK_CONFIGS_TYPE =
        new TypeReference<List<Map<String, String>>>() { };

    // TODO: This should not be so long. However, due to potentially long rebalances that may have to wait a full
    // session timeout to complete, during which we cannot serve some requests. Ideally we could reduce this, but
    // we need to consider all possible scenarios this could fail. It might be ok to fail with a timeout in rare cases,
    // but currently a worker simply leaving the group can take this long as well.
    public static final long REQUEST_TIMEOUT_MS = 90 * 1000;

    private final Herder herder;
    private final WorkerConfig config;
    private final URI advertisedUrl;
    @javax.ws.rs.core.Context
    private ServletContext context;

    public ConnectorsResource(Herder herder, WorkerConfig config, URI advertisedUrl) {
        this.herder = herder;
        this.config = config;
        this.advertisedUrl = advertisedUrl;
    }

    @GET
    @Path("/")
    public Response listConnectors(
        final @Context UriInfo uriInfo,
        final @Context HttpHeaders headers
    ) throws Throwable {
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
                                log.info("Ignoring unknown expanion type {}", expansion);
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
    public Response createConnector(final @QueryParam("forward") Boolean forward,
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
    public ConnectorInfo getConnector(final @PathParam("connector") String connector,
                                      final @Context HttpHeaders headers,
                                      final @QueryParam("forward") Boolean forward) throws Throwable {
        FutureCallback<ConnectorInfo> cb = new FutureCallback<>();
        herder.connectorInfo(connector, cb);
        return completeOrForwardRequest(cb, "/connectors/" + connector, "GET", headers, null, forward);
    }

    @GET
    @Path("/{connector}/config")
    public Map<String, String> getConnectorConfig(final @PathParam("connector") String connector,
                                                  final @Context HttpHeaders headers,
                                                  final @QueryParam("forward") Boolean forward) throws Throwable {
        FutureCallback<Map<String, String>> cb = new FutureCallback<>();
        herder.connectorConfig(connector, cb);
        return completeOrForwardRequest(cb, "/connectors/" + connector + "/config", "GET", headers, null, forward);
    }

    @GET
    @Path("/{connector}/status")
    public ConnectorStateInfo getConnectorStatus(final @PathParam("connector") String connector) throws Throwable {
        return herder.connectorStatus(connector);
    }

    @GET
    @Path("/{connector}/topics")
    public Response getConnectorActiveTopics(final @PathParam("connector") String connector) throws Throwable {
        if (!config.getBoolean(TOPIC_TRACKING_ENABLE_CONFIG)) {
            return Response.status(Response.Status.FORBIDDEN)
                    .entity("Topic tracking is disabled.")
                    .build();
        }
        ActiveTopicsInfo info = herder.connectorActiveTopics(connector);
        return Response.ok(Collections.singletonMap(info.connector(), info)).build();
    }

    @PUT
    @Path("/{connector}/topics/reset")
    public Response resetConnectorActiveTopics(final @PathParam("connector") String connector, final @Context HttpHeaders headers) throws Throwable {
        if (!config.getBoolean(TOPIC_TRACKING_ENABLE_CONFIG)) {
            return Response.status(Response.Status.FORBIDDEN)
                    .entity("Topic tracking is disabled.")
                    .build();
        }
        if (!config.getBoolean(TOPIC_TRACKING_ALLOW_RESET_CONFIG)) {
            return Response.status(Response.Status.FORBIDDEN)
                    .entity("Topic tracking reset is disabled.")
                    .build();
        }
        herder.resetConnectorActiveTopics(connector);
        return Response.accepted().build();
    }

    @PUT
    @Path("/{connector}/config")
    public Response putConnectorConfig(final @PathParam("connector") String connector,
                                       final @Context HttpHeaders headers,
                                       final @QueryParam("forward") Boolean forward,
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
    public void restartConnector(final @PathParam("connector") String connector,
                                 final @Context HttpHeaders headers,
                                 final @QueryParam("forward") Boolean forward) throws Throwable {
        FutureCallback<Void> cb = new FutureCallback<>();
        herder.restartConnector(connector, cb);
        completeOrForwardRequest(cb, "/connectors/" + connector + "/restart", "POST", headers, null, forward);
    }

    @PUT
    @Path("/{connector}/pause")
    public Response pauseConnector(@PathParam("connector") String connector, final @Context HttpHeaders headers) {
        herder.pauseConnector(connector);
        return Response.accepted().build();
    }

    @PUT
    @Path("/{connector}/resume")
    public Response resumeConnector(@PathParam("connector") String connector) {
        herder.resumeConnector(connector);
        return Response.accepted().build();
    }

    @GET
    @Path("/{connector}/tasks")
    public List<TaskInfo> getTaskConfigs(final @PathParam("connector") String connector,
                                         final @Context HttpHeaders headers,
                                         final @QueryParam("forward") Boolean forward) throws Throwable {
        FutureCallback<List<TaskInfo>> cb = new FutureCallback<>();
        herder.taskConfigs(connector, cb);
        return completeOrForwardRequest(cb, "/connectors/" + connector + "/tasks", "GET", headers, null, new TypeReference<List<TaskInfo>>() {
        }, forward);
    }

    @POST
    @Path("/{connector}/tasks")
    public void putTaskConfigs(final @PathParam("connector") String connector,
                               final @Context HttpHeaders headers,
                               final @QueryParam("forward") Boolean forward,
                               final byte[] requestBody) throws Throwable {
        List<Map<String, String>> taskConfigs = new ObjectMapper().readValue(requestBody, TASK_CONFIGS_TYPE);
        FutureCallback<Void> cb = new FutureCallback<>();
        herder.putTaskConfigs(connector, taskConfigs, cb, InternalRequestSignature.fromHeaders(requestBody, headers));
        completeOrForwardRequest(cb, "/connectors/" + connector + "/tasks", "POST", headers, taskConfigs, forward);
    }

    @GET
    @Path("/{connector}/tasks/{task}/status")
    public ConnectorStateInfo.TaskState getTaskStatus(final @PathParam("connector") String connector,
                                                      final @Context HttpHeaders headers,
                                                      final @PathParam("task") Integer task) throws Throwable {
        return herder.taskStatus(new ConnectorTaskId(connector, task));
    }

    @POST
    @Path("/{connector}/tasks/{task}/restart")
    public void restartTask(final @PathParam("connector") String connector,
                            final @PathParam("task") Integer task,
                            final @Context HttpHeaders headers,
                            final @QueryParam("forward") Boolean forward) throws Throwable {
        FutureCallback<Void> cb = new FutureCallback<>();
        ConnectorTaskId taskId = new ConnectorTaskId(connector, task);
        herder.restartTask(taskId, cb);
        completeOrForwardRequest(cb, "/connectors/" + connector + "/tasks/" + task + "/restart", "POST", headers, null, forward);
    }

    @DELETE
    @Path("/{connector}")
    public void destroyConnector(final @PathParam("connector") String connector,
                                 final @Context HttpHeaders headers,
                                 final @QueryParam("forward") Boolean forward) throws Throwable {
        FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>();
        herder.deleteConnectorConfig(connector, cb);
        completeOrForwardRequest(cb, "/connectors/" + connector, "DELETE", headers, null, forward);
    }

    // Check whether the connector name from the url matches the one (if there is one) provided in the connectorconfig
    // object. Throw BadRequestException on mismatch, otherwise put connectorname in config
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
                                              Object body,
                                              TypeReference<U> resultType,
                                              Translator<T, U> translator,
                                              Boolean forward) throws Throwable {
        try {
            return cb.get(REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();

            if (cause instanceof RequestTargetException) {
                if (forward == null || forward) {
                    // the only time we allow recursive forwarding is when no forward flag has
                    // been set, which should only be seen by the first worker to handle a user request.
                    // this gives two total hops to resolve the request before giving up.
                    boolean recursiveForward = forward == null;
                    RequestTargetException targetException = (RequestTargetException) cause;

                    if (this.advertisedUrl.compareTo(URI.create(targetException.forwardUrl())) == 0) {
                        log.warn("Request will be forwarded to URL {} same as this server's advertised URL (potential loop)",
                                targetException.forwardUrl());
                    }
                    String forwardUrl = UriBuilder.fromUri(targetException.forwardUrl())
                            .path(path)
                            .queryParam("forward", recursiveForward)
                            .build()
                            .toString();
                    log.debug("Forwarding request {} {} {}", forwardUrl, method, body);
                    return translator.translate(RestClient.httpRequest(forwardUrl, method, headers, body, resultType, config));
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

    private <T> T completeOrForwardRequest(FutureCallback<T> cb, String path, String method, HttpHeaders headers, Object body,
                                           TypeReference<T> resultType, Boolean forward) throws Throwable {
        return completeOrForwardRequest(cb, path, method, headers, body, resultType, new IdentityTranslator<T>(), forward);
    }

    private <T> T completeOrForwardRequest(FutureCallback<T> cb, String path, String method, HttpHeaders headers,
                                           Object body, Boolean forward) throws Throwable {
        return completeOrForwardRequest(cb, path, method, headers, body, null, new IdentityTranslator<T>(), forward);
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
