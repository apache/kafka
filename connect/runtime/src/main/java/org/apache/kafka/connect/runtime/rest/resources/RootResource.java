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

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.rest.RestRequestTimeout;
import org.apache.kafka.connect.runtime.rest.entities.ServerInfo;
import org.apache.kafka.connect.runtime.rest.entities.WorkerStatus;
import org.apache.kafka.connect.util.FutureCallback;
import org.apache.kafka.connect.util.StagedTimeoutException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.swagger.v3.oas.annotations.Operation;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Path("/")
@Produces(MediaType.APPLICATION_JSON)
public class RootResource {

    private final Herder herder;
    private final RestRequestTimeout requestTimeout;
    private final Time time;

    @Inject
    public RootResource(Herder herder, RestRequestTimeout requestTimeout) {
        this(herder, requestTimeout, Time.SYSTEM);
    }

    // For testing only
    RootResource(Herder herder, RestRequestTimeout requestTimeout, Time time) {
        this.herder = herder;
        this.requestTimeout = requestTimeout;
        this.time = time;
    }

    @GET
    @Operation(summary = "Get details about this Connect worker and the ID of the Kafka cluster it is connected to")
    public ServerInfo serverInfo() {
        return new ServerInfo(herder.kafkaClusterId());
    }

    @GET
    @Path("/health")
    @Operation(summary = "Health check endpoint to verify worker readiness and liveness")
    public Response healthCheck() throws Throwable {
        WorkerStatus workerStatus;
        int statusCode;
        try {
            FutureCallback<Void> cb = new FutureCallback<>();
            herder.healthCheck(cb);

            long timeoutNs = TimeUnit.MILLISECONDS.toNanos(requestTimeout.healthCheckTimeoutMs());
            long deadlineNs = timeoutNs + time.nanoseconds();
            time.waitForFuture(cb, deadlineNs);

            statusCode = Response.Status.OK.getStatusCode();
            workerStatus = WorkerStatus.healthy();
        } catch (TimeoutException e) {
            String statusDetails = e instanceof StagedTimeoutException
                    ? ((StagedTimeoutException) e).stage().summarize()
                    : null;
            if (!herder.isReady()) {
                statusCode = Response.Status.SERVICE_UNAVAILABLE.getStatusCode();
                workerStatus = WorkerStatus.starting(statusDetails);
            } else {
                statusCode = Response.Status.INTERNAL_SERVER_ERROR.getStatusCode();
                workerStatus = WorkerStatus.unhealthy(statusDetails);
            }
        } catch (ExecutionException e) {
            throw e.getCause();
        }

        return Response.status(statusCode).entity(workerStatus).build();
    }

}
