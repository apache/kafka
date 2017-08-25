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

import org.apache.kafka.trogdor.rest.CoordinatorFaultsResponse;
import org.apache.kafka.trogdor.rest.CoordinatorStatusResponse;
import org.apache.kafka.trogdor.rest.CreateCoordinatorFaultRequest;
import org.apache.kafka.trogdor.rest.Empty;

import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.concurrent.atomic.AtomicReference;


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
    public CoordinatorStatusResponse getStatus() throws Throwable {
        return new CoordinatorStatusResponse(coordinator().startTimeMs());
    }

    @GET
    @Path("/faults")
    public CoordinatorFaultsResponse getCoordinatorFaults() throws Throwable {
        return coordinator().getFaults();
    }

    @PUT
    @Path("/fault")
    public Empty putCoordinatorFault(CreateCoordinatorFaultRequest request) throws Throwable {
        coordinator().createFault(request);
        return Empty.INSTANCE;
    }

    @PUT
    @Path("/shutdown")
    public Empty shutdown() throws Throwable {
        coordinator().beginShutdown();
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
