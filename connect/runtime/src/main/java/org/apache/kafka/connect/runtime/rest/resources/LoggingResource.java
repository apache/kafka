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

import com.fasterxml.jackson.annotation.JsonProperty;
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
import java.util.Enumeration;
import java.util.Map;
import java.util.TreeMap;

@Path("/loggers")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class LoggingResource {

    @GET
    @Path("/")
    public Response listLoggers() {
        Enumeration en = LogManager.getCurrentLoggers();
        Map<String, LogLevel> loggers = new TreeMap<>();
        while (en.hasMoreElements()) {
            Logger l = (Logger) en.nextElement();
            loggers.put(l.getName(), new LogLevel(l.getLevel()));
        }
        return Response.ok(loggers).build();
    }

    @GET
    @Path("/{connector}")
    public Response getConnector(final @PathParam("connector") String connector) {
        Logger l = LogManager.getLogger(connector);
        if (l == null) {
            return Response.ok("Not Found").build();
        } else {
            if (l.getLevel() == null) {
                return Response.ok("Eff-" + l.getEffectiveLevel()).build();
            } else {
                return Response.ok(String.valueOf(l.getLevel())).build();
            }
        }
    }

    @PUT
    @Path("/")
    public Response setLevel() {
        return Response.ok("@root").build();
    }

    @PUT
    @Path("/{logger}")
    public Response setLevel(final @PathParam("logger") String logger) {
        return Response.ok(logger).build();
    }

    private static class LogLevel {
        private final String level;

        LogLevel(Level level) {
            this(String.valueOf(level));
        }

        LogLevel(String level) {
            this.level = level;
        }

        @JsonProperty
        public String logLevel() {
            return this.level;
        }
    }
}
