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

import org.apache.kafka.trogdor.rest.AgentFaultsResponse;
import org.apache.kafka.trogdor.rest.AgentStatusResponse;
import org.apache.kafka.trogdor.rest.CreateAgentFaultRequest;
import org.apache.kafka.trogdor.rest.Empty;

import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.concurrent.atomic.AtomicReference;


@Path("/agent")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class AgentRestResource {
    private final AtomicReference<Agent> agent = new AtomicReference<>(null);

    @javax.ws.rs.core.Context
    private ServletContext context;

    public void setAgent(Agent myAgent) {
        agent.set(myAgent);
    }

    @GET
    @Path("/status")
    public AgentStatusResponse getStatus() throws Throwable {
        return new AgentStatusResponse(agent().startTimeMs());
    }

    @GET
    @Path("/faults")
    public AgentFaultsResponse getAgentFaults() throws Throwable {
        return agent().faults();
    }

    @PUT
    @Path("/fault")
    public Empty putAgentFault(CreateAgentFaultRequest request) throws Throwable {
        agent().createFault(request);
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
