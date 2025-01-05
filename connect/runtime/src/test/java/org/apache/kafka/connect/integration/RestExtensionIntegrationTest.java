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
package org.apache.kafka.connect.integration;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.health.ConnectClusterState;
import org.apache.kafka.connect.health.ConnectorHealth;
import org.apache.kafka.connect.health.ConnectorState;
import org.apache.kafka.connect.health.ConnectorType;
import org.apache.kafka.connect.health.TaskState;
import org.apache.kafka.connect.rest.ConnectRestExtension;
import org.apache.kafka.connect.rest.ConnectRestExtensionContext;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.connect.util.clusters.WorkerHandle;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.core.Response;

import static jakarta.ws.rs.core.Response.Status.BAD_REQUEST;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.NAME_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.apache.kafka.connect.runtime.rest.RestServerConfig.REST_EXTENSION_CLASSES_CONFIG;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A simple integration test to ensure that REST extensions are registered correctly.
 */
@Tag("integration")
public class RestExtensionIntegrationTest {

    private static final long REST_EXTENSION_REGISTRATION_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(1);
    private static final long CONNECTOR_HEALTH_AND_CONFIG_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(1);
    private static final int NUM_WORKERS = 1;

    private EmbeddedConnectCluster connect;

    @Test
    public void testRestExtensionApi() throws InterruptedException {
        // setup Connect worker properties
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put(REST_EXTENSION_CLASSES_CONFIG, IntegrationTestRestExtension.class.getName());

        // build a Connect cluster backed by a Kafka KRaft cluster
        connect = new EmbeddedConnectCluster.Builder()
            .name("connect-cluster")
            .numWorkers(NUM_WORKERS)
            .numBrokers(1)
            .workerProps(workerProps)
            .build();

        // start the clusters
        connect.start();

        WorkerHandle worker = connect.workers().stream()
            .findFirst()
            .orElseThrow(() -> new AssertionError("At least one worker handle should be available"));

        waitForCondition(
            this::extensionIsRegistered,
            REST_EXTENSION_REGISTRATION_TIMEOUT_MS,
            "REST extension was never registered"
        );

        ConnectorHandle connectorHandle = RuntimeHandles.get().connectorHandle("test-conn");
        try {
            // setup up props for the connector
            Map<String, String> connectorProps = new HashMap<>();
            connectorProps.put(CONNECTOR_CLASS_CONFIG, MonitorableSinkConnector.class.getSimpleName());
            connectorProps.put(TASKS_MAX_CONFIG, String.valueOf(1));
            connectorProps.put(TOPICS_CONFIG, "test-topic");

            // start a connector
            connectorHandle.taskHandle(connectorHandle.name() + "-0");
            StartAndStopLatch connectorStartLatch = connectorHandle.expectedStarts(1);
            connect.configureConnector(connectorHandle.name(), connectorProps);
            connect.assertions().assertConnectorAndAtLeastNumTasksAreRunning(connectorHandle.name(), 1,
                    "Connector tasks did not start in time.");
            connectorStartLatch.await(CONNECTOR_HEALTH_AND_CONFIG_TIMEOUT_MS, TimeUnit.MILLISECONDS);

            String workerId = String.format("%s:%d", worker.url().getHost(), worker.url().getPort());
            ConnectorHealth expectedHealth = new ConnectorHealth(
                connectorHandle.name(),
                new ConnectorState(
                    "RUNNING",
                    workerId,
                    null
                ),
                Collections.singletonMap(
                    0,
                    new TaskState(0, "RUNNING", workerId, null)
                ),
                ConnectorType.SINK
            );

            connectorProps.put(NAME_CONFIG, connectorHandle.name());

            // Test the REST extension API; specifically, that the connector's health and configuration
            // are available to the REST extension we registered and that they contain expected values
            waitForCondition(
                () -> verifyConnectorHealthAndConfig(connectorHandle.name(), expectedHealth, connectorProps),
                CONNECTOR_HEALTH_AND_CONFIG_TIMEOUT_MS,
                "Connector health and/or config was never accessible by the REST extension"
            );
        } finally {
            RuntimeHandles.get().deleteConnector(connectorHandle.name());
        }
    }

    @AfterEach
    public void close() {
        // stop the Connect cluster and its backing Kafka cluster.
        connect.stop();
        IntegrationTestRestExtension.instance = null;
    }

    private boolean extensionIsRegistered() {
        try {
            String extensionUrl = connect.endpointForResource("integration-test-rest-extension/registered");
            Response response = connect.requestGet(extensionUrl);
            return response.getStatus() < BAD_REQUEST.getStatusCode();
        } catch (ConnectException e) {
            return false;
        }
    }

    private boolean verifyConnectorHealthAndConfig(
        String connectorName,
        ConnectorHealth expectedHealth,
        Map<String, String> expectedConfig
    ) {
        ConnectClusterState clusterState =
            IntegrationTestRestExtension.instance.restPluginContext.clusterState();
        
        ConnectorHealth actualHealth = clusterState.connectorHealth(connectorName);
        if (actualHealth.tasksState().isEmpty()) {
            // Happens if the task has been started but its status has not yet been picked up from
            // the status topic by the worker.
            return false;
        }
        Map<String, String> actualConfig = clusterState.connectorConfig(connectorName);

        assertEquals(expectedConfig, actualConfig);
        assertEquals(expectedHealth, actualHealth);

        return true;
    }

    public static class IntegrationTestRestExtension implements ConnectRestExtension {
        private static IntegrationTestRestExtension instance;

        public ConnectRestExtensionContext restPluginContext;

        @Override
        public void register(ConnectRestExtensionContext restPluginContext) {
            instance = this;
            this.restPluginContext = restPluginContext;
            // Immediately request a list of connectors to confirm that the context and its fields
            // has been fully initialized and there is no risk of deadlock
            restPluginContext.clusterState().connectors();
            // Install a new REST resource that can be used to confirm that the extension has been
            // successfully registered
            restPluginContext.configurable().register(new IntegrationTestRestExtensionResource());
        }
    
        @Override
        public void close() {
        }
    
        @Override
        public void configure(Map<String, ?> configs) {
        }
    
        @Override
        public String version() {
            return "test";
        }

        @Path("integration-test-rest-extension")
        public static class IntegrationTestRestExtensionResource {

            @GET
            @Path("/registered")
            public boolean isRegistered() {
                return true;
            }
        }
    }
}
