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

import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.rest.RestRequestTimeout;
import org.apache.kafka.connect.runtime.rest.entities.ServerInfo;
import org.apache.kafka.connect.runtime.rest.entities.WorkerStatus;
import org.apache.kafka.connect.util.Stage;
import org.apache.kafka.connect.util.StagedTimeoutException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.mockito.stubbing.Stubber;

import java.util.concurrent.TimeoutException;

import jakarta.ws.rs.core.Response;

import static org.apache.kafka.connect.runtime.rest.RestServer.DEFAULT_HEALTH_CHECK_TIMEOUT_MS;
import static org.apache.kafka.connect.runtime.rest.RestServer.DEFAULT_REST_REQUEST_TIMEOUT_MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
public class RootResourceTest {

    @Mock private Herder herder;
    @Mock private Time time;
    private RootResource rootResource;

    private static final RestRequestTimeout REQUEST_TIMEOUT = RestRequestTimeout.constant(
            DEFAULT_REST_REQUEST_TIMEOUT_MS,
            DEFAULT_HEALTH_CHECK_TIMEOUT_MS
    );

    @BeforeEach
    public void setUp() {
        rootResource = new RootResource(herder, REQUEST_TIMEOUT, time);
    }

    @Test
    public void testRootGet() {
        when(herder.kafkaClusterId()).thenReturn(MockAdminClient.DEFAULT_CLUSTER_ID);

        ServerInfo info = rootResource.serverInfo();
        assertEquals(AppInfoParser.getVersion(), info.version());
        assertEquals(AppInfoParser.getCommitId(), info.commit());
        assertEquals(MockAdminClient.DEFAULT_CLUSTER_ID, info.clusterId());

        verify(herder).kafkaClusterId();
    }

    @Test
    public void testHealthCheckRunning() throws Throwable {
        expectHealthCheck(null);

        Response response = rootResource.healthCheck();
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

        WorkerStatus expectedStatus = WorkerStatus.healthy();
        WorkerStatus actualStatus = workerStatus(response);
        assertEquals(expectedStatus, actualStatus);
    }

    @Test
    public void testHealthCheckStarting() throws Throwable {
        expectHealthCheck(new TimeoutException());
        when(herder.isReady()).thenReturn(false);

        Response response = rootResource.healthCheck();
        assertEquals(Response.Status.SERVICE_UNAVAILABLE.getStatusCode(), response.getStatus());

        WorkerStatus expectedStatus = WorkerStatus.starting(null);
        WorkerStatus actualStatus = workerStatus(response);
        assertEquals(expectedStatus, actualStatus);
    }

    @Test
    public void testHealthCheckStartingWithStage() throws Throwable {
        String stageDescription = "experiencing a simulated failure for testing purposes";
        Stage stage = new Stage(stageDescription, 0);
        StagedTimeoutException exception = new StagedTimeoutException(stage);
        expectHealthCheck(exception);
        when(herder.isReady()).thenReturn(false);

        Response response = rootResource.healthCheck();
        assertEquals(Response.Status.SERVICE_UNAVAILABLE.getStatusCode(), response.getStatus());

        WorkerStatus expectedStatus = WorkerStatus.starting(stage.summarize());
        WorkerStatus actualStatus = workerStatus(response);
        assertEquals(expectedStatus, actualStatus);
        assertTrue(
                actualStatus.message().contains(stageDescription),
                "Status message '" + actualStatus.message() + "' did not contain stage description '" + stageDescription + "'"
        );
    }

    @Test
    public void testHealthCheckUnhealthy() throws Throwable {
        expectHealthCheck(new TimeoutException());
        when(herder.isReady()).thenReturn(true);

        Response response = rootResource.healthCheck();
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());

        WorkerStatus expectedStatus = WorkerStatus.unhealthy(null);
        WorkerStatus actualStatus = workerStatus(response);
        assertEquals(expectedStatus, actualStatus);
    }

    @Test
    public void testHealthCheckUnhealthyWithStage() throws Throwable {
        String stageDescription = "experiencing a simulated failure for testing purposes";
        Stage stage = new Stage(stageDescription, 0);
        StagedTimeoutException exception = new StagedTimeoutException(stage);
        expectHealthCheck(exception);
        when(herder.isReady()).thenReturn(true);

        Response response = rootResource.healthCheck();
        assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());

        WorkerStatus expectedStatus = WorkerStatus.unhealthy(stage.summarize());
        WorkerStatus actualStatus = workerStatus(response);
        assertEquals(expectedStatus, actualStatus);
        assertTrue(
                actualStatus.message().contains(stageDescription),
                "Status message '" + actualStatus.message() + "' did not contain stage description '" + stageDescription + "'"
        );
    }

    private WorkerStatus workerStatus(Response response) {
        return (WorkerStatus) (response.getEntity());
    }

    private void expectHealthCheck(Throwable error) throws Throwable {
        Stubber stubber = error != null
                ? doThrow(error)
                : doReturn(null);
        stubber.when(time).waitForFuture(any(), anyLong());
    }

}
