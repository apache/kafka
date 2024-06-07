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

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.rest.entities.LoggerLevel;
import org.apache.kafka.connect.runtime.rest.errors.BadRequestException;
import org.apache.log4j.Level;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

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
    public Response getLogger(final @PathParam("logger") String namedLogger) {
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
    @Operation(summary = "Set the log level for the specified logger")
    @SuppressWarnings("fallthrough")
    public Response setLevel(final @PathParam("logger") String namespace,
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
        Level level = Level.toLevel(levelString.toUpperCase(Locale.ROOT), null);
        if (level == null) {
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
