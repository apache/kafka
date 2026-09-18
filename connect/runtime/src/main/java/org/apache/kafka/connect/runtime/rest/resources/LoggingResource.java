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
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.rest.entities.ErrorMessage;
import org.apache.kafka.connect.runtime.rest.entities.LoggerLevel;
import org.apache.kafka.connect.runtime.rest.errors.BadRequestException;

import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.media.SchemaProperty;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

/**
 * A set of endpoints to adjust the log levels of runtime loggers.
 */
@Path("/admin/loggers")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class LoggingResource {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(LoggingResource.class);

    private static final String WORKER_SCOPE = "worker";
    private static final String CLUSTER_SCOPE = "cluster";

    private final Herder herder;

    @Inject
    public LoggingResource(Herder herder) {
        this.herder = herder;
    }

    /**
     * List the current loggers that have their levels explicitly set and their log levels.
     *
     * @return a list of current loggers and their levels.
     */
    @GET
    @Operation(summary = "List the current loggers that have their levels explicitly set and their log levels")
    @ApiResponse(
        responseCode = "200",
        description = "Logger levels retrieved successfully",
        content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(type = "object", additionalPropertiesSchema = LoggerLevel.class))
    )
    public Response listLoggers() {
        return Response.ok(herder.allLoggerLevels()).build();
    }

    /**
     * Get the log level of a named logger.
     *
     * @param namedLogger name of a logger
     * @return level of the logger, effective level if the level was not explicitly set.
     */
    @GET
    @Path("/{logger}")
    @Operation(summary = "Get the log level for the specified logger")
    @ApiResponses({
        @ApiResponse(
            responseCode = "200",
            description = "Logger level retrieved successfully",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = LoggerLevel.class))
        ),
        @ApiResponse(
            responseCode = "404",
            description = "Logger not found",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        )
    })
    public Response getLogger(final @PathParam("logger") @Parameter(description = "The name of the logger") String namedLogger) {
        Objects.requireNonNull(namedLogger, "require non-null name");

        LoggerLevel loggerLevel = herder.loggerLevel(namedLogger);
        if (loggerLevel == null)
            throw new NotFoundException("Logger " + namedLogger + " not found.");

        return Response.ok(loggerLevel).build();
    }

    /**
     * Adjust level of a named logger. If the name corresponds to an ancestor, then the log level is applied to all child loggers.
     *
     * @param namespace name of the logger
     * @param levelMap a map that is expected to contain one key 'level', and a value that is one of the log4j levels:
     *                 DEBUG, ERROR, FATAL, INFO, TRACE, WARN
     * @return names of loggers whose levels were modified
     */
    @PUT
    @Path("/{logger}")
    @Operation(
        summary = "Set the log level for the specified logger",
        requestBody = @RequestBody(
            description = "The log level configuration",
            required = true,
            content = @Content(
                mediaType = MediaType.APPLICATION_JSON,
                schema = @Schema(type = "object", requiredProperties = "level"),
                schemaProperties = @SchemaProperty(name = "level", schema = @Schema(type = "string", description = "The log level to set"))
            )
        )
    )
    @ApiResponses({
        @ApiResponse(
            responseCode = "200",
            description = "Worker logger level set successfully",
            content = @Content(
                mediaType = MediaType.APPLICATION_JSON,
                array = @ArraySchema(schema = @Schema(type = "string", description = "The names of loggers whose levels were changed")))
        ),
        @ApiResponse(
            responseCode = "204",
            description = "Cluster logger level set successfully"
        ),
        @ApiResponse(
            responseCode = "400",
            description = "Log level was not specified",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        ),
        @ApiResponse(
            responseCode = "404",
            description = "Invalid log level",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        ),
        @ApiResponse(
            responseCode = "500",
            description = "Request timed out or failed unexpectedly while setting the cluster logger level",
            content = @Content(mediaType = MediaType.APPLICATION_JSON, schema = @Schema(implementation = ErrorMessage.class))
        )
    })
    @SuppressWarnings("fallthrough")
    public Response setLevel(final @PathParam("logger") @Parameter(description = "The name of the logger") String namespace,
                             final Map<String, String> levelMap,
                             @DefaultValue("worker") @QueryParam("scope") @Parameter(description = "The scope for the logging modification (single-worker, cluster-wide, etc.)") String scope) {
        if (scope == null) {
            log.warn("Received null scope in request to adjust logging level; will default to {}", WORKER_SCOPE);
            scope = WORKER_SCOPE;
        }

        String levelString = levelMap.get("level");
        if (levelString == null) {
            throw new BadRequestException("Desired 'level' parameter was not specified in request.");
        }

        // Make sure that this is a valid level
        if (org.apache.logging.log4j.Level.getLevel(levelString) == null) {
            throw new NotFoundException("invalid log level '" + levelString + "'.");
        }

        switch (scope.toLowerCase(Locale.ROOT)) {
            default:
                log.warn("Received invalid scope '{}' in request to adjust logging level; will default to {}", scope, WORKER_SCOPE);
            case WORKER_SCOPE:
                List<String> affectedLoggers = herder.setWorkerLoggerLevel(namespace, levelString);
                return Response.ok(affectedLoggers).build();
            case CLUSTER_SCOPE:
                herder.setClusterLoggerLevel(namespace, levelString);
                return Response.noContent().build();
        }
    }

}
