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
package org.apache.kafka.trogdor.agent;

import org.apache.kafka.trogdor.rest.AgentStatusResponse;
import org.apache.kafka.trogdor.rest.CreateWorkerRequest;
import org.apache.kafka.trogdor.rest.DestroyWorkerRequest;
import org.apache.kafka.trogdor.rest.Empty;
import org.apache.kafka.trogdor.rest.StopWorkerRequest;
import org.apache.kafka.trogdor.rest.UptimeResponse;

import java.util.concurrent.atomic.AtomicReference;

import jakarta.servlet.ServletContext;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

/**
 * The REST resource for the Agent. This describes the RPCs which the agent can accept.
 *
 * RPCs should be idempotent.  This is important because if the server's response is
 * lost, the client will simply retransmit the same request. The server's response must
 * be the same the second time around.
 *
 * We return the empty JSON object {} rather than void for RPCs that have no results.
 * This ensures that if we want to add more return results later, we can do so in a
 * compatible way.
 */
@Path("/agent")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class AgentRestResource {
    private final AtomicReference<Agent> agent = new AtomicReference<>(null);

    @jakarta.ws.rs.core.Context
    private ServletContext context;

    public void setAgent(Agent myAgent) {
        agent.set(myAgent);
    }

    @GET
    @Path("/status")
    public AgentStatusResponse getStatus() throws Throwable {
        return agent().status();
    }

    @GET
    @Path("/uptime")
    public UptimeResponse uptime() {
        return agent().uptime();
    }

    @POST
    @Path("/worker/create")
    public Empty createWorker(CreateWorkerRequest req) throws Throwable {
        agent().createWorker(req);
        return Empty.INSTANCE;
    }

    @PUT
    @Path("/worker/stop")
    public Empty stopWorker(StopWorkerRequest req) throws Throwable {
        agent().stopWorker(req);
        return Empty.INSTANCE;
    }

    @DELETE
    @Path("/worker")
    public Empty destroyWorker(@DefaultValue("0") @QueryParam("workerId") long workerId) throws Throwable {
        agent().destroyWorker(new DestroyWorkerRequest(workerId));
        return Empty.INSTANCE;
    }

    @PUT
    @Path("/shutdown")
    public Empty shutdown() throws Throwable {
        agent().beginShutdown();
        return Empty.INSTANCE;
    }

    private Agent agent() {
        Agent myAgent = agent.get();
        if (myAgent == null) {
            throw new RuntimeException("AgentRestResource has not been initialized yet.");
        }
        return myAgent;
    }
}
