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
package org.apache.kafka.trogdor.coordinator;

import org.apache.kafka.trogdor.rest.CoordinatorShutdownRequest;
import org.apache.kafka.trogdor.rest.CoordinatorStatusResponse;
import org.apache.kafka.trogdor.rest.CreateTaskRequest;
import org.apache.kafka.trogdor.rest.DestroyTaskRequest;
import org.apache.kafka.trogdor.rest.Empty;
import org.apache.kafka.trogdor.rest.StopTaskRequest;
import org.apache.kafka.trogdor.rest.TaskRequest;
import org.apache.kafka.trogdor.rest.TaskState;
import org.apache.kafka.trogdor.rest.TaskStateType;
import org.apache.kafka.trogdor.rest.TasksRequest;
import org.apache.kafka.trogdor.rest.TasksResponse;
import org.apache.kafka.trogdor.rest.UptimeResponse;

import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The REST resource for the Coordinator. This describes the RPCs which the coordinator
 * can accept.
 *
 * RPCs should be idempotent.  This is important because if the server's response is
 * lost, the client will simply retransmit the same request. The server's response must
 * be the same the second time around.
 *
 * We return the empty JSON object {} rather than void for RPCs that have no results.
 * This ensures that if we want to add more return results later, we can do so in a
 * compatible way.
 */
@Path("/coordinator")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class CoordinatorRestResource {
    private final AtomicReference<Coordinator> coordinator = new AtomicReference<Coordinator>();

    @javax.ws.rs.core.Context
    private ServletContext context;

    public void setCoordinator(Coordinator myCoordinator) {
        coordinator.set(myCoordinator);
    }

    @GET
    @Path("/status")
    public CoordinatorStatusResponse status() throws Throwable {
        return coordinator().status();
    }

    @GET
    @Path("/uptime")
    public UptimeResponse uptime() {
        return coordinator().uptime();
    }

    @POST
    @Path("/task/create")
    public Empty createTask(CreateTaskRequest request) throws Throwable {
        coordinator().createTask(request);
        return Empty.INSTANCE;
    }

    @PUT
    @Path("/task/stop")
    public Empty stopTask(StopTaskRequest request) throws Throwable {
        coordinator().stopTask(request);
        return Empty.INSTANCE;
    }

    @DELETE
    @Path("/tasks")
    public Empty destroyTask(@DefaultValue("") @QueryParam("taskId") String taskId) throws Throwable {
        coordinator().destroyTask(new DestroyTaskRequest(taskId));
        return Empty.INSTANCE;
    }

    @GET
    @Path("/tasks/")
    public Response tasks(@QueryParam("taskId") List<String> taskId,
            @DefaultValue("0") @QueryParam("firstStartMs") long firstStartMs,
            @DefaultValue("0") @QueryParam("lastStartMs") long lastStartMs,
            @DefaultValue("0") @QueryParam("firstEndMs") long firstEndMs,
            @DefaultValue("0") @QueryParam("lastEndMs") long lastEndMs,
            @DefaultValue("") @QueryParam("state") String state) throws Throwable {
        boolean isEmptyState = state.equals("");
        if (!isEmptyState && !TaskStateType.Constants.VALUES.contains(state)) {
            return Response.status(400).entity(
                String.format("State %s is invalid. Must be one of %s",
                    state, TaskStateType.Constants.VALUES)
            ).build();
        }

        Optional<TaskStateType> givenState = Optional.ofNullable(isEmptyState ? null : TaskStateType.valueOf(state));
        TasksResponse resp = coordinator().tasks(new TasksRequest(taskId, firstStartMs, lastStartMs, firstEndMs, lastEndMs, givenState));

        return Response.status(200).entity(resp).build();
    }

    @GET
    @Path("/tasks/{taskId}")
    public TaskState tasks(@PathParam("taskId") String taskId) throws Throwable {
        TaskState response = coordinator().task(new TaskRequest(taskId));
        if (response == null)
            throw new NotFoundException(String.format("No task with ID \"%s\" exists.", taskId));

        return response;
    }

    @PUT
    @Path("/shutdown")
    public Empty beginShutdown(CoordinatorShutdownRequest request) throws Throwable {
        coordinator().beginShutdown(request.stopAgents());
        return Empty.INSTANCE;
    }

    private Coordinator coordinator() {
        Coordinator myCoordinator = coordinator.get();
        if (myCoordinator == null) {
            throw new RuntimeException("CoordinatorRestResource has not been initialized yet.");
        }
        return myCoordinator;
    }
}
