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
import org.apache.kafka.connect.runtime.rest.errors.BadRequestException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * A set of endpoints to adjust the log levels of runtime loggers.
 */
@Path("/admin/loggers")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class LoggingResource {

    /**
     * Note: In log4j, the root logger's name was "root" and Kafka also followed that name for dynamic logging control feature.
     *
     * The root logger's name is changed in log4j2 to empty string (see: [[LogManager.ROOT_LOGGER_NAME]]) but for backward-
     * compatibility. Kafka keeps its original root logger name. It is why here is a dedicated definition for the root logger name.
     */
    private static final String ROOT_LOGGER_NAME = "root";

    /**
     * List the current loggers that have their levels explicitly set and their log levels.
     *
     * @return a list of current loggers and their levels.
     */
    @GET
    @Path("/")
    public Response listLoggers() {
        // current loggers
        final Map<String, Map<String, String>> loggers = currentLoggers()
            .stream()
            .filter(logger -> logger.getLevel() != Level.OFF)
            .collect(Collectors.toMap(logger -> logger.getName(), logger -> levelToMap(logger)));

        // Replace "" logger to "root" logger
        Logger root = rootLogger();
        if (root.getLevel() != Level.OFF) {
            loggers.put(ROOT_LOGGER_NAME, levelToMap(root));
        }

        return Response.ok(new TreeMap<>(loggers)).build();
    }

    /**
     * Get the log level of a named logger.
     *
     * @param loggerName name of a logger
     * @return level of the logger, effective level if the level was not explicitly set.
     */
    @GET
    @Path("/{logger}")
    public Response getLogger(final @PathParam("logger") String loggerName) {
        Objects.requireNonNull(loggerName, "require non-null name");

        final Logger logger;
        if (ROOT_LOGGER_NAME.equalsIgnoreCase(loggerName)) {
            logger = rootLogger();
        } else {
            List<Logger> en = currentLoggers();
            Optional<Logger> found = en.stream().filter(existingLogger -> loggerName.equals(existingLogger.getName())).findAny();

            logger = found.orElse(null);
        }

        if (logger == null) {
            throw new NotFoundException("Logger " + loggerName + " not found.");
        } else {
            return Response.ok(effectiveLevelToMap(logger)).build();
        }
    }

    /**
     * Adjust level of a named logger. if name corresponds to an ancestor, then the log level is applied to all child loggers.
     *
     * @param loggerName  name of the logger
     * @param levelMap    a map that is expected to contain one key 'level', and a value that is one of the log4j levels:
     *                    DEBUG, ERROR, FATAL, INFO, TRACE, WARN, OFF
     * @return names of loggers whose levels were modified
     */
    @PUT
    @Path("/{logger}")
    public Response setLevel(final @PathParam("logger") String loggerName,
                             final Map<String, String> levelMap) {
        String desiredLevelStr = levelMap.get("level");
        if (desiredLevelStr == null) {
            throw new BadRequestException("Desired 'level' parameter was not specified in request.");
        }

        Level level = Level.toLevel(desiredLevelStr.toUpperCase(Locale.ROOT), null);
        if (level == null) {
            throw new NotFoundException("invalid log level '" + desiredLevelStr + "'.");
        }

        List<Logger> childLoggers;
        if (ROOT_LOGGER_NAME.equalsIgnoreCase(loggerName)) {
            childLoggers = new ArrayList<>(currentLoggers());
            childLoggers.add(rootLogger());
        } else {
            childLoggers = new ArrayList<>();
            Logger ancestorLogger = lookupLogger(loggerName);
            boolean present = false;
            for (Logger logger : currentLoggers()) {
                if (logger.getName().startsWith(loggerName)) {
                    childLoggers.add(logger);
                }
                if (loggerName.equals(logger.getName())) {
                    present = true;
                }
            }
            if (!present) {
                childLoggers.add(ancestorLogger);
            }
        }

        List<String> modifiedLoggerNames = new ArrayList<>();
        for (Logger logger: childLoggers) {
            logger.setLevel(level);
            if (LogManager.ROOT_LOGGER_NAME.equals(logger.getName())) {
                modifiedLoggerNames.add(ROOT_LOGGER_NAME);
            } else {
                modifiedLoggerNames.add(logger.getName());
            }
        }
        Collections.sort(modifiedLoggerNames);

        return Response.ok(modifiedLoggerNames).build();
    }

    protected Logger lookupLogger(String loggerName) {
        final LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);

        return loggerContext.getLogger(loggerName);
    }

    @SuppressWarnings("unchecked")
    protected List<Logger> currentLoggers() {
        final LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);

        return loggerContext.getLoggers()
            .stream()
            .filter(logger -> !logger.getName().equals(LogManager.ROOT_LOGGER_NAME))
            .collect(Collectors.toList());
    }

    protected Logger rootLogger() {
        final LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);

        return loggerContext.getRootLogger();
    }

    /**
     * Map representation of a logger's effective log level.
     *
     * @param logger a non-null log4j logger
     * @return a singleton map whose key is level and the value is the string representation of the logger's effective log level.
     */
    private static Map<String, String> effectiveLevelToMap(Logger logger) {
        return Collections.singletonMap("level", logger.getLevel() != null ? logger.getLevel().toString() : null);
    }

    /**
     * Map representation of a logger's log level.
     *
     * @param logger a non-null log4j logger
     * @return a singleton map whose key is level and the value is the string representation of the logger's log level.
     */
    private static Map<String, String> levelToMap(Logger logger) {
        return Collections.singletonMap("level", String.valueOf(logger.getLevel()));
    }
}
