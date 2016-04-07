/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.runtime.rest.resources;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.distributed.NotAssignedException;
import org.apache.kafka.connect.runtime.distributed.NotLeaderException;
import org.apache.kafka.connect.runtime.rest.RestServer;
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
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Path("/connectors")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ConnectorsResource {
    private static final Logger log = LoggerFactory.getLogger(ConnectorsResource.class);

    // TODO: This should not be so long. However, due to potentially long rebalances that may have to wait a full
    // session timeout to complete, during which we cannot serve some requests. Ideally we could reduce this, but
    // we need to consider all possible scenarios this could fail. It might be ok to fail with a timeout in rare cases,
    // but currently a worker simply leaving the group can take this long as well.
    private static final long REQUEST_TIMEOUT_MS = 90 * 1000;

    private final Herder herder;
    @javax.ws.rs.core.Context
    private ServletContext context;

    public ConnectorsResource(Herder herder) {
        this.herder = herder;
    }

    @GET
    @Path("/")
    public Collection<String> listConnectors() throws Throwable {
        FutureCallback<Collection<String>> cb = new FutureCallback<>();
        herder.connectors(cb);
        return completeOrForwardRequest(cb, "/connectors", "GET", null, new TypeReference<Collection<String>>() {
        });
    }

    @POST
    @Path("/")
    public Response createConnector(final CreateConnectorRequest createRequest) throws Throwable {
        String name = createRequest.name();
        Map<String, String> configs = createRequest.config();
        if (!configs.containsKey(ConnectorConfig.NAME_CONFIG))
            configs.put(ConnectorConfig.NAME_CONFIG, name);

        FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>();
        herder.putConnectorConfig(name, configs, false, cb);
        Herder.Created<ConnectorInfo> info = completeOrForwardRequest(cb, "/connectors", "POST", createRequest,
                new TypeReference<ConnectorInfo>() { }, new CreatedConnectorInfoTranslator());
        return Response.created(URI.create("/connectors/" + name)).entity(info.result()).build();
    }

    @GET
    @Path("/{connector}")
    public ConnectorInfo getConnector(final @PathParam("connector") String connector) throws Throwable {
        FutureCallback<ConnectorInfo> cb = new FutureCallback<>();
        herder.connectorInfo(connector, cb);
        return completeOrForwardRequest(cb, "/connectors/" + connector, "GET", null);
    }

    @GET
    @Path("/{connector}/config")
    public Map<String, String> getConnectorConfig(final @PathParam("connector") String connector) throws Throwable {
        FutureCallback<Map<String, String>> cb = new FutureCallback<>();
        herder.connectorConfig(connector, cb);
        return completeOrForwardRequest(cb, "/connectors/" + connector + "/config", "GET", null);
    }

    @GET
    @Path("/{connector}/status")
    public ConnectorStateInfo getConnectorStatus(final @PathParam("connector") String connector) throws Throwable {
        return herder.connectorStatus(connector);
    }

    @PUT
    @Path("/{connector}/config")
    public Response putConnectorConfig(final @PathParam("connector") String connector,
                                       final Map<String, String> connectorConfig) throws Throwable {
        FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>();
        herder.putConnectorConfig(connector, connectorConfig, true, cb);
        Herder.Created<ConnectorInfo> createdInfo = completeOrForwardRequest(cb, "/connectors/" + connector + "/config",
                "PUT", connectorConfig, new TypeReference<ConnectorInfo>() { }, new CreatedConnectorInfoTranslator());
        Response.ResponseBuilder response;
        if (createdInfo.created())
            response = Response.created(URI.create("/connectors/" + connector));
        else
            response = Response.ok();
        return response.entity(createdInfo.result()).build();
    }

    @POST
    @Path("/{connector}/restart")
    public void restartConnector(final @PathParam("connector") String connector) throws Throwable {
        FutureCallback<Void> cb = new FutureCallback<>();
        herder.restartConnector(connector, cb);
        completeOrForwardRequest(cb, "/connectors/" + connector + "/restart", "POST", null);
    }

    @GET
    @Path("/{connector}/tasks")
    public List<TaskInfo> getTaskConfigs(final @PathParam("connector") String connector) throws Throwable {
        FutureCallback<List<TaskInfo>> cb = new FutureCallback<>();
        herder.taskConfigs(connector, cb);
        return completeOrForwardRequest(cb, "/connectors/" + connector + "/tasks", "GET", null, new TypeReference<List<TaskInfo>>() {
        });
    }

    @POST
    @Path("/{connector}/tasks")
    public void putTaskConfigs(final @PathParam("connector") String connector,
                               final List<Map<String, String>> taskConfigs) throws Throwable {
        FutureCallback<Void> cb = new FutureCallback<>();
        herder.putTaskConfigs(connector, taskConfigs, cb);
        completeOrForwardRequest(cb, "/connectors/" + connector + "/tasks", "POST", taskConfigs);
    }

    @GET
    @Path("/{connector}/tasks/{task}/status")
    public ConnectorStateInfo.TaskState getTaskStatus(@PathParam("connector") String connector,
                                                      @PathParam("task") Integer task) throws Throwable {
        return herder.taskStatus(new ConnectorTaskId(connector, task));
    }

    @POST
    @Path("/{connector}/tasks/{task}/restart")
    public void restartTask(@PathParam("connector") String connector,
                            @PathParam("task") Integer task) throws Throwable {
        FutureCallback<Void> cb = new FutureCallback<>();
        ConnectorTaskId taskId = new ConnectorTaskId(connector, task);
        herder.restartTask(taskId, cb);
        completeOrForwardRequest(cb, "/connectors/" + connector + "/tasks/" + task + "/restart", "POST", null);
    }

    @DELETE
    @Path("/{connector}")
    public void destroyConnector(final @PathParam("connector") String connector) throws Throwable {
        FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>();
        herder.putConnectorConfig(connector, null, true, cb);
        completeOrForwardRequest(cb, "/connectors/" + connector, "DELETE", null);
    }

    // Wait for a FutureCallback to complete. If it succeeds, return the parsed response. If it fails, try to forward the
    // request to the leader.
    private <T, U> T completeOrForwardRequest(
            FutureCallback<T> cb, String path, String method, Object body, TypeReference<U> resultType,
            Translator<T, U> translator) throws Throwable {
        try {
            return cb.get(REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            String forwardUrl = null;

            if (cause instanceof NotLeaderException) {
                NotLeaderException notLeaderError = (NotLeaderException) cause;
                forwardUrl = RestServer.urlJoin(notLeaderError.leaderUrl(), path);
                log.debug("Forwarding request to leader: {} {} {}", forwardUrl, method, body);
            } else if (cause instanceof NotAssignedException) {
                NotAssignedException notTaskOwnerError = (NotAssignedException) cause;
                forwardUrl = RestServer.urlJoin(notTaskOwnerError.ownerUrl(), path);
                log.debug("Forwarding request to task owner: {} {} {}", forwardUrl, method, body);
            }

            if (forwardUrl != null)
                return translator.translate(RestServer.httpRequest(forwardUrl, method, body, resultType));

            throw cause;
        } catch (TimeoutException e) {
            // This timeout is for the operation itself. None of the timeout error codes are relevant, so internal server
            // error is the best option
            throw new ConnectRestException(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), "Request timed out");
        } catch (InterruptedException e) {
            throw new ConnectRestException(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), "Request interrupted");
        }
    }

    private <T> T completeOrForwardRequest(FutureCallback<T> cb, String path, String method, Object body, TypeReference<T> resultType) throws Throwable {
        return completeOrForwardRequest(cb, path, method, body, resultType, new IdentityTranslator<T>());
    }

    private <T> T completeOrForwardRequest(FutureCallback<T> cb, String path, String method, Object body) throws Throwable {
        return completeOrForwardRequest(cb, path, method, body, null, new IdentityTranslator<T>());
    }

    private interface Translator<T, U> {
        T translate(RestServer.HttpResponse<U> response);
    }

    private class IdentityTranslator<T> implements Translator<T, T> {
        @Override
        public T translate(RestServer.HttpResponse<T> response) {
            return response.body();
        }
    }

    private class CreatedConnectorInfoTranslator implements Translator<Herder.Created<ConnectorInfo>, ConnectorInfo> {
        @Override
        public Herder.Created<ConnectorInfo> translate(RestServer.HttpResponse<ConnectorInfo> response) {
            boolean created = response.status() == 201;
            return new Herder.Created<>(created, response.body());
        }
    }
}
