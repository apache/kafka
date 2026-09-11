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

import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.RestartRequest;
import org.apache.kafka.connect.runtime.rest.HerderRequestHandler;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.RestRequestTimeout;
import org.apache.kafka.connect.runtime.rest.RestServerConfig;
import org.apache.kafka.connect.runtime.rest.entities.ActiveTopicsInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorOffsets;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.CreateConnectorRequest;
import org.apache.kafka.connect.runtime.rest.entities.ErrorMessage;
import org.apache.kafka.connect.runtime.rest.entities.Message;
import org.apache.kafka.connect.runtime.rest.entities.TaskInfo;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.FutureCallback;

import com.fasterxml.jackson.core.type.TypeReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import jakarta.inject.Inject;
import jakarta.servlet.ServletContext;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.core.UriInfo;

import static org.apache.kafka.connect.runtime.rest.HerderRequestHandler.IdentityTranslator;
import static org.apache.kafka.connect.runtime.rest.HerderRequestHandler.Translator;

@Path("/connectors")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ConnectorsResource {
    private static final Logger log = LoggerFactory.getLogger(ConnectorsResource.class);

    private final Herder herder;
    private final HerderRequestHandler requestHandler;
    @jakarta.ws.rs.core.Context
    private ServletContext context;
    private final boolean isTopicTrackingDisabled;
    private final boolean isTopicTrackingResetDisabled;

    @Inject
    public ConnectorsResource(
            Herder herder,
            RestServerConfig config,
            RestClient restClient,
            RestRequestTimeout requestTimeout
    ) {
        this.herder = herder;
        this.requestHandler = new HerderRequestHandler(restClient, requestTimeout);
        this.isTopicTrackingDisabled = !config.topicTrackingEnabled();
        this.isTopicTrackingResetDisabled = !config.topicTrackingResetEnabled();
    }

    @GET
    @Operation(
        summary = "List all active connectors",
        parameters = @Parameter(
            name = "expand",
            in = ParameterIn.QUERY,
            description = "Optional connector details to expand. Supported values are status and info. Repeat this parameter to include both",
            array = @ArraySchema(schema = @Schema(type = "string", allowableValues = {"status", "info"}))
        )
    )
    @ApiResponse(
        responseCode = "200",
        description = "Connector names retrieved successfully. When expand is supplied, connector details are returned keyed by connector name",
        content = @Content(
            mediaType = MediaType.APPLICATION_JSON,
            schema = @Schema(oneOf = {String[].class, Object.class})
        )
    )
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
    @Operation(
        summary = "Create a new connector",
        requestBody = @RequestBody(
            required = true,
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = CreateConnectorRequest.class))
        )
    )
    @ApiResponses({
        @ApiResponse(
            responseCode = "201",
            description = "Connector created successfully",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ConnectorInfo.class))
        ),
        @ApiResponse(
            responseCode = "400",
            description = "Invalid connector creation request or configuration",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        ),
        @ApiResponse(
            responseCode = "409",
            description = "Connector already exists or request cannot be completed while a rebalance is in progress",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        ),
        @ApiResponse(
            responseCode = "500",
            description = "Request timed out or failed unexpectedly while creating the connector",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        )
    })
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
        herder.putConnectorConfig(name, configs, createRequest.initialTargetState(), false, cb);
        Herder.Created<ConnectorInfo> info = requestHandler.completeOrForwardRequest(cb, "/connectors", "POST", headers, createRequest,
                new TypeReference<>() { }, new CreatedConnectorInfoTranslator(), forward);

        URI location = UriBuilder.fromUri("/connectors").path(name).build();
        return Response.created(location).entity(info.result()).build();
    }

    @GET
    @Path("/{connector}")
    @Operation(summary = "Get the details for the specified connector")
    @ApiResponses({
        @ApiResponse(
            responseCode = "200",
            description = "Connector details retrieved successfully",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ConnectorInfo.class))
        ),
        @ApiResponse(
            responseCode = "404",
            description = "Connector not found",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        ),
        @ApiResponse(
            responseCode = "500",
            description = "Request timed out or failed unexpectedly while retrieving connector details",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        )
    })
    public ConnectorInfo getConnector(final @PathParam("connector") @Parameter(description = "The name of the connector") String connector) throws Throwable {
        FutureCallback<ConnectorInfo> cb = new FutureCallback<>();
        herder.connectorInfo(connector, cb);
        return requestHandler.completeRequest(cb);
    }

    @GET
    @Path("/{connector}/config")
    @Operation(summary = "Get the configuration for the specified connector")
    @ApiResponses({
        @ApiResponse(
            responseCode = "200",
            description = "Connector configuration retrieved successfully",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(type = "object", additionalPropertiesSchema = String.class))
        ),
        @ApiResponse(
            responseCode = "404",
            description = "Connector not found",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        ),
        @ApiResponse(
            responseCode = "500",
            description = "Request timed out or failed unexpectedly while retrieving the connector configuration",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        )
    })
    public Map<String, String> getConnectorConfig(final @PathParam("connector") @Parameter(description = "The name of the connector") String connector) throws Throwable {
        FutureCallback<Map<String, String>> cb = new FutureCallback<>();
        herder.connectorConfig(connector, cb);
        return requestHandler.completeRequest(cb);
    }

    @GET
    @Path("/{connector}/status")
    @Operation(summary = "Get the status for the specified connector")
    @ApiResponses({
        @ApiResponse(
            responseCode = "200",
            description = "Connector status retrieved successfully",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ConnectorStateInfo.class))
        ),
        @ApiResponse(
            responseCode = "404",
            description = "Connector not found",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        )
    })
    public ConnectorStateInfo getConnectorStatus(final @PathParam("connector") @Parameter(description = "The name of the connector") String connector) {
        return herder.connectorStatus(connector);
    }

    @GET
    @Path("/{connector}/topics")
    @Operation(summary = "Get the list of topics actively used by the specified connector")
    @ApiResponses({
        @ApiResponse(
            responseCode = "200",
            description = "Connector active topics retrieved successfully",
            content = @Content(
                mediaType = MediaType.APPLICATION_JSON,
                schema = @Schema(type = "object", additionalPropertiesSchema = ActiveTopicsInfo.class)
            )
        ),
        @ApiResponse(
            responseCode = "403",
            description = "Topic tracking is disabled",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        )
    })
    public Response getConnectorActiveTopics(final @PathParam("connector") @Parameter(description = "The name of the connector") String connector) {
        if (isTopicTrackingDisabled) {
            throw new ConnectRestException(Response.Status.FORBIDDEN.getStatusCode(),
                    "Topic tracking is disabled.");
        }
        ActiveTopicsInfo info = herder.connectorActiveTopics(connector);
        return Response.ok(Map.of(info.connector(), info)).build();
    }

    @PUT
    @Path("/{connector}/topics/reset")
    @Operation(summary = "Reset the list of topics actively used by the specified connector")
    @ApiResponses({
        @ApiResponse(
            responseCode = "202",
            description = "Connector active topics reset request accepted"
        ),
        @ApiResponse(
            responseCode = "403",
            description = "Topic tracking or topic tracking reset is disabled",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        )
    })
    public Response resetConnectorActiveTopics(final @PathParam("connector") @Parameter(description = "The name of the connector") String connector,
                                               final @Context HttpHeaders headers) {
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
    @Operation(
        summary = "Create or reconfigure the specified connector",
        requestBody = @RequestBody(
            description = "The full connector configuration used to create the connector or replace its existing configuration",
            required = true,
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(type = "object", additionalPropertiesSchema = String.class))
        )
    )
    @ApiResponses({
        @ApiResponse(
            responseCode = "200",
            description = "Connector reconfigured successfully",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ConnectorInfo.class))
        ),
        @ApiResponse(
            responseCode = "201",
            description = "Connector created successfully",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ConnectorInfo.class))
        ),
        @ApiResponse(
            responseCode = "400",
            description = "Invalid connector configuration",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        ),
        @ApiResponse(
            responseCode = "409",
            description = "Request cannot be completed while a rebalance is in progress",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        ),
        @ApiResponse(
            responseCode = "500",
            description = "Request timed out or failed unexpectedly while creating or updating the connector configuration",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        )
    })
    public Response putConnectorConfig(final @PathParam("connector") @Parameter(description = "The name of the connector") String connector,
                                       final @Context HttpHeaders headers,
                                       final @Parameter(hidden = true) @QueryParam("forward") Boolean forward,
                                       final Map<String, String> connectorConfig) throws Throwable {
        FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>();
        checkAndPutConnectorConfigName(connector, connectorConfig);

        herder.putConnectorConfig(connector, connectorConfig, true, cb);
        Herder.Created<ConnectorInfo> createdInfo = requestHandler.completeOrForwardRequest(cb, "/connectors/" + connector + "/config",
                "PUT", headers, connectorConfig, new TypeReference<>() { }, new CreatedConnectorInfoTranslator(), forward);
        Response.ResponseBuilder response;
        if (createdInfo.created()) {
            URI location = UriBuilder.fromUri("/connectors").path(connector).build();
            response = Response.created(location);
        } else {
            response = Response.ok();
        }
        return response.entity(createdInfo.result()).build();
    }

    @PATCH
    @Path("/{connector}/config")
    @Operation(
        summary = "Patch the configuration for the specified connector",
        requestBody = @RequestBody(
            description = "The connector configuration patch. Properties with non-null values are updated or added, and properties with null values are removed",
            required = true,
            content = @Content(
                mediaType = MediaType.APPLICATION_JSON,
                schema = @Schema(type = "object"),
                additionalPropertiesSchema = @Schema(type = "string", nullable = true)
            )
        )
    )
    @ApiResponses({
        @ApiResponse(
            responseCode = "200",
            description = "Connector configuration patched successfully",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ConnectorInfo.class))
        ),
        @ApiResponse(
            responseCode = "400",
            description = "Invalid connector configuration",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        ),
        @ApiResponse(
            responseCode = "404",
            description = "Connector not found",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        ),
        @ApiResponse(
            responseCode = "409",
            description = "Request cannot be completed while a rebalance is in progress",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        ),
        @ApiResponse(
            responseCode = "500",
            description = "Request timed out or failed unexpectedly while patching the connector configuration",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        )
    })
    public Response patchConnectorConfig(final @PathParam("connector") @Parameter(description = "The name of the connector") String connector,
                                         final @Context HttpHeaders headers,
                                         final @Parameter(hidden = true) @QueryParam("forward") Boolean forward,
                                         final Map<String, String> connectorConfigPatch) throws Throwable {
        FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>();
        herder.patchConnectorConfig(connector, connectorConfigPatch, cb);
        Herder.Created<ConnectorInfo> createdInfo = requestHandler.completeOrForwardRequest(cb, "/connectors/" + connector + "/config",
                "PATCH", headers, connectorConfigPatch, new TypeReference<>() { }, new CreatedConnectorInfoTranslator(), forward);
        return Response.ok().entity(createdInfo.result()).build();
    }

    @POST
    @Path("/{connector}/restart")
    @Operation(summary = "Restart the specified connector")
    @ApiResponses({
        @ApiResponse(
            responseCode = "202",
            description = "Restart request accepted when includeTasks or onlyFailed is true",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ConnectorStateInfo.class))
        ),
        @ApiResponse(
            responseCode = "204",
            description = "Connector restarted successfully when includeTasks and onlyFailed are false"
        ),
        @ApiResponse(
            responseCode = "404",
            description = "Connector not found",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        ),
        @ApiResponse(
            responseCode = "409",
            description = "Request cannot be completed while a rebalance is in progress",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        ),
        @ApiResponse(
            responseCode = "500",
            description = "Request timed out or failed unexpectedly while restarting the connector",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        )
    })
    public Response restartConnector(final @PathParam("connector") @Parameter(description = "The name of the connector") String connector,
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
            requestHandler.completeOrForwardRequest(cb, forwardingPath, "POST", headers, null, forward);
            return Response.noContent().build();
        }

        // In all other cases, submit the async restart request and return connector state
        FutureCallback<ConnectorStateInfo> cb = new FutureCallback<>();
        herder.restartConnectorAndTasks(restartRequest, cb);
        Map<String, String> queryParameters = new HashMap<>();
        queryParameters.put("includeTasks", includeTasks.toString());
        queryParameters.put("onlyFailed", onlyFailed.toString());
        ConnectorStateInfo stateInfo = requestHandler.completeOrForwardRequest(cb, forwardingPath, "POST", headers, queryParameters, null, new TypeReference<>() {
        }, new IdentityTranslator<>(), forward);
        return Response.accepted().entity(stateInfo).build();
    }

    @PUT
    @Path("/{connector}/stop")
    @Operation(summary = "Stop the specified connector",
               description = "This operation is idempotent and has no effects if the connector is already stopped")
    @ApiResponses({
        @ApiResponse(
            responseCode = "204",
            description = "Connector stopped successfully"
        ),
        @ApiResponse(
            responseCode = "404",
            description = "Connector not found",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        ),
        @ApiResponse(
            responseCode = "409",
            description = "Request cannot be completed while a rebalance is in progress",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        ),
        @ApiResponse(
            responseCode = "500",
            description = "Request timed out or failed unexpectedly while stopping the connector",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        )
    })
    public void stopConnector(
            @PathParam("connector") @Parameter(description = "The name of the connector") String connector,
            final @Context HttpHeaders headers,
            final @Parameter(hidden = true) @QueryParam("forward") Boolean forward) throws Throwable {
        FutureCallback<Void> cb = new FutureCallback<>();
        herder.stopConnector(connector, cb);
        requestHandler.completeOrForwardRequest(cb, "/connectors/" + connector + "/stop", "PUT", headers, null, forward);
    }

    @PUT
    @Path("/{connector}/pause")
    @Operation(summary = "Pause the specified connector",
               description = "This operation is idempotent and has no effects if the connector is already paused")
    @ApiResponses({
        @ApiResponse(
            responseCode = "202",
            description = "Connector pause request accepted"
        ),
        @ApiResponse(
            responseCode = "404",
            description = "Connector not found",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        ),
        @ApiResponse(
            responseCode = "500",
            description = "Request timed out or failed unexpectedly while pausing the connector",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        )
    })
    public Response pauseConnector(@PathParam("connector") @Parameter(description = "The name of the connector") String connector,
                                   final @Context HttpHeaders headers) {
        herder.pauseConnector(connector);
        return Response.accepted().build();
    }

    @PUT
    @Path("/{connector}/resume")
    @Operation(summary = "Resume the specified connector",
               description = "This operation is idempotent and has no effects if the connector is already running")
    @ApiResponses({
        @ApiResponse(
            responseCode = "202",
            description = "Connector resume request accepted"
        ),
        @ApiResponse(
            responseCode = "404",
            description = "Connector not found",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        ),
        @ApiResponse(
            responseCode = "500",
            description = "Request timed out or failed unexpectedly while resuming the connector",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        )
    })
    public Response resumeConnector(@PathParam("connector") @Parameter(description = "The name of the connector") String connector) {
        herder.resumeConnector(connector);
        return Response.accepted().build();
    }

    @GET
    @Path("/{connector}/tasks")
    @Operation(summary = "List all tasks and their configurations for the specified connector")
    @ApiResponses({
        @ApiResponse(
            responseCode = "200",
            description = "Connector task configurations retrieved successfully",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, array = @ArraySchema(schema = @Schema(implementation = TaskInfo.class)))
        ),
        @ApiResponse(
            responseCode = "404",
            description = "Connector not found",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        ),
        @ApiResponse(
            responseCode = "500",
            description = "Request timed out or failed unexpectedly while retrieving task configurations",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        )
    })
    public List<TaskInfo> getTaskConfigs(final @PathParam("connector") @Parameter(description = "The name of the connector") String connector) throws Throwable {
        FutureCallback<List<TaskInfo>> cb = new FutureCallback<>();
        herder.taskConfigs(connector, cb);
        return requestHandler.completeRequest(cb);
    }

    @GET
    @Path("/{connector}/tasks/{task}/status")
    @Operation(summary = "Get the state of the specified task for the specified connector")
    @ApiResponses({
        @ApiResponse(
            responseCode = "200",
            description = "Task status retrieved successfully",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ConnectorStateInfo.TaskState.class))
        ),
        @ApiResponse(
            responseCode = "404",
            description = "Connector or task not found",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        )
    })
    public ConnectorStateInfo.TaskState getTaskStatus(final @PathParam("connector") @Parameter(description = "The name of the connector") String connector,
                                                      final @Context HttpHeaders headers,
                                                      final @PathParam("task") @Parameter(description = "The task ID") Integer task) {
        return herder.taskStatus(new ConnectorTaskId(connector, task));
    }

    @POST
    @Path("/{connector}/tasks/{task}/restart")
    @Operation(summary = "Restart the specified task for the specified connector")
    @ApiResponses({
        @ApiResponse(
            responseCode = "204",
            description = "Task restarted successfully"
        ),
        @ApiResponse(
            responseCode = "404",
            description = "Connector or task not found",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        ),
        @ApiResponse(
            responseCode = "409",
            description = "Request cannot be completed while a rebalance is in progress",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        ),
        @ApiResponse(
            responseCode = "500",
            description = "Request timed out or failed unexpectedly while restarting the task",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        )
    })
    public void restartTask(final @PathParam("connector") @Parameter(description = "The name of the connector") String connector,
                            final @PathParam("task") @Parameter(description = "The task ID") Integer task,
                            final @Context HttpHeaders headers,
                            final @Parameter(hidden = true) @QueryParam("forward") Boolean forward) throws Throwable {
        FutureCallback<Void> cb = new FutureCallback<>();
        ConnectorTaskId taskId = new ConnectorTaskId(connector, task);
        herder.restartTask(taskId, cb);
        requestHandler.completeOrForwardRequest(cb, "/connectors/" + connector + "/tasks/" + task + "/restart", "POST", headers, null, new TypeReference<>() { }, forward);
    }

    @DELETE
    @Path("/{connector}")
    @Operation(summary = "Delete the specified connector")
    @ApiResponses({
        @ApiResponse(
            responseCode = "204",
            description = "Connector deleted successfully"
        ),
        @ApiResponse(
            responseCode = "404",
            description = "Connector not found",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        ),
        @ApiResponse(
            responseCode = "409",
            description = "Request cannot be completed while a rebalance is in progress",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        ),
        @ApiResponse(
            responseCode = "500",
            description = "Request timed out or failed unexpectedly while deleting the connector",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        )
    })
    public void destroyConnector(final @PathParam("connector") @Parameter(description = "The name of the connector") String connector,
                                 final @Context HttpHeaders headers,
                                 final @Parameter(hidden = true) @QueryParam("forward") Boolean forward) throws Throwable {
        FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>();
        herder.deleteConnectorConfig(connector, cb);
        requestHandler.completeOrForwardRequest(cb, "/connectors/" + connector, "DELETE", headers, null, new TypeReference<>() { }, forward);
    }

    @GET
    @Path("/{connector}/offsets")
    @Operation(summary = "Get the current offsets for the specified connector")
    @ApiResponses({
        @ApiResponse(
            responseCode = "200",
            description = "Connector offsets retrieved successfully",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ConnectorOffsets.class))
        ),
        @ApiResponse(
            responseCode = "404",
            description = "Connector not found",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        ),
        @ApiResponse(
            responseCode = "500",
            description = "Request timed out or failed unexpectedly while retrieving connector offsets",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        )
    })
    public ConnectorOffsets getOffsets(final @PathParam("connector") @Parameter(description = "The name of the connector") String connector) throws Throwable {
        FutureCallback<ConnectorOffsets> cb = new FutureCallback<>();
        herder.connectorOffsets(connector, cb);
        return requestHandler.completeRequest(cb);
    }

    @PATCH
    @Path("/{connector}/offsets")
    @Operation(
        summary = "Alter the offsets for the specified connector",
        requestBody = @RequestBody(
            description = "The connector offsets to write. A null offset removes the offset for that partition",
            required = true,
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ConnectorOffsets.class))
        )
    )
    @ApiResponses({
        @ApiResponse(
            responseCode = "200",
            description = "Connector offsets altered successfully",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = Message.class))
        ),
        @ApiResponse(
            responseCode = "400",
            description = "Invalid connector offsets or the connector is not in the STOPPED state",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        ),
        @ApiResponse(
            responseCode = "404",
            description = "Connector not found",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        ),
        @ApiResponse(
            responseCode = "409",
            description = "Request cannot be completed while a rebalance is in progress",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        ),
        @ApiResponse(
            responseCode = "500",
            description = "Request timed out or failed unexpectedly while altering connector offsets",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        )
    })
    public Response alterConnectorOffsets(final @Parameter(hidden = true) @QueryParam("forward") Boolean forward,
                                          final @Context HttpHeaders headers,
                                          final @PathParam("connector") @Parameter(description = "The name of the connector") String connector,
                                          final ConnectorOffsets offsets) throws Throwable {
        if (offsets.offsets() == null || offsets.offsets().isEmpty()) {
            throw new BadRequestException("Partitions / offsets need to be provided for an alter offsets request");
        }

        FutureCallback<Message> cb = new FutureCallback<>();
        herder.alterConnectorOffsets(connector, offsets.toMap(), cb);
        Message msg = requestHandler.completeOrForwardRequest(cb, "/connectors/" + connector + "/offsets", "PATCH", headers, offsets,
                new TypeReference<>() { }, new IdentityTranslator<>(), forward);
        return Response.ok().entity(msg).build();
    }

    @DELETE
    @Path("/{connector}/offsets")
    @Operation(summary = "Reset the offsets for the specified connector")
    @ApiResponses({
        @ApiResponse(
            responseCode = "200",
            description = "Connector offsets reset successfully",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = Message.class))
        ),
        @ApiResponse(
            responseCode = "404",
            description = "Connector not found",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        ),
        @ApiResponse(
            responseCode = "400",
            description = "Connector is not in the STOPPED state",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        ),
        @ApiResponse(
            responseCode = "409",
            description = "Request cannot be completed while a rebalance is in progress",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        ),
        @ApiResponse(
            responseCode = "500",
            description = "Request timed out or failed unexpectedly while resetting connector offsets",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        )
    })
    public Response resetConnectorOffsets(final @Parameter(hidden = true) @QueryParam("forward") Boolean forward,
                                          final @Context HttpHeaders headers,
                                          final @PathParam("connector") @Parameter(description = "The name of the connector") String connector) throws Throwable {
        FutureCallback<Message> cb = new FutureCallback<>();
        herder.resetConnectorOffsets(connector, cb);
        Message msg = requestHandler.completeOrForwardRequest(cb, "/connectors/" + connector + "/offsets", "DELETE", headers, null,
                new TypeReference<>() { }, new IdentityTranslator<>(), forward);
        return Response.ok().entity(msg).build();
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

    private static class CreatedConnectorInfoTranslator implements Translator<Herder.Created<ConnectorInfo>, ConnectorInfo> {
        @Override
        public Herder.Created<ConnectorInfo> translate(RestClient.HttpResponse<ConnectorInfo> response) {
            boolean created = response.status() == 201;
            return new Herder.Created<>(created, response.body());
        }
    }
}
