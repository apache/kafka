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
package org.apache.kafka.connect.runtime.rest.admin;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

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
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

@Path("/admin/loggers")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class LoggingResource {

    private static final String ROOT_LOGGER_NAME = "@root";

    @GET
    @Path("/")
    public Response listLoggers() {
        Enumeration en = LogManager.getCurrentLoggers();
        Map<String, LogLevel> loggers = new TreeMap<>();
        while (en.hasMoreElements()) {
            Logger current = (Logger) en.nextElement();
            // exclude loggers that do not have a specific level set
            if (current.getLevel() != null) {
                loggers.put(current.getName(), new LogLevel(current.getLevel()));
            }
        }

        Logger root = LogManager.getRootLogger();
        if (root.getLevel() != null) {
            loggers.put(ROOT_LOGGER_NAME, new LogLevel(root.getLevel()));
        }

        return Response.ok(loggers).build();
    }

    @GET
    @Path("/{logger}")
    public Response getLogger(final @PathParam("logger") String namedLogger) {
        Objects.requireNonNull(namedLogger, "require non-null name");

        Enumeration en = LogManager.getCurrentLoggers();
        Logger logger = null;
        while (en.hasMoreElements()) {
            Logger l = (Logger) en.nextElement();
            if (namedLogger.equals(l.getName())) {
                logger = l;
                break;
            }
        }
        if (logger == null) {
            throw new NotFoundException("logger " + namedLogger + " not found.");
        } else {
            Map<String, String> levelMap = new HashMap<>();

            String level;
            if (logger.getLevel() == null) {
                level = String.valueOf(logger.getEffectiveLevel());
            } else {
                level = String.valueOf(logger.getLevel());
            }

            levelMap.put("level", level);
            return Response.ok(levelMap).build();
        }
    }

    @PUT
    @Path("/")
    public Response setLevel() {
        return Response.ok(ROOT_LOGGER_NAME).build();
    }

    @PUT
    @Path("/{logger}")
    public Response setLevel(final @PathParam("logger") String namedLogger,
                             final Map<String, String> levelMap) {
        String desiredLevelStr = levelMap.get("level");
        Objects.requireNonNull(desiredLevelStr, "desired level was not specified in PUT request.");

        Level level = Level.toLevel(desiredLevelStr.toUpperCase(Locale.ROOT), null);
        if (level == null) {
            throw new NotFoundException("invalid log level '" + desiredLevelStr + "'.");
        }

        List<Logger> childLoggers;
        if (ROOT_LOGGER_NAME.equals(namedLogger)) {
            childLoggers = Collections.list((Enumeration<Logger>) LogManager.getCurrentLoggers());
            childLoggers.add(LogManager.getRootLogger());
        } else {
            childLoggers = new ArrayList<>();
            Logger ancestorLogger = LogManager.getLogger(namedLogger);
            Enumeration en = LogManager.getCurrentLoggers();
            boolean present = false;
            while (en.hasMoreElements()) {
                Logger current = (Logger) en.nextElement();
                if (current.getName().startsWith(namedLogger)) {
                    childLoggers.add(current);
                }
                if (namedLogger.equals(current.getName())) {
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
            modifiedLoggerNames.add(logger.getName());
        }
        Collections.sort(modifiedLoggerNames);

        return Response.ok(modifiedLoggerNames).build();
    }

    private static class LogLevel {
        private final String level;

        LogLevel(Level level) {
            this(String.valueOf(level));
        }

        LogLevel(String level) {
            this.level = level;
        }

        @JsonProperty("level")
        public String level() {
            return this.level;
        }
    }
}
