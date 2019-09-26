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

import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.health.ConnectClusterState;
import org.apache.kafka.connect.rest.ConnectRestExtension;
import org.apache.kafka.connect.rest.ConnectRestExtensionContext;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.REST_EXTENSION_CLASSES_CONFIG;
import static org.apache.kafka.test.TestUtils.waitForCondition;

/**
 * A simple integration test to ensure that REST extensions are registered correctly.
 */
@Category(IntegrationTest.class)
public class RestExtensionIntegrationTest {

    private static final int NUM_WORKERS = 3;
    private static final long REST_EXTENSION_REGISTRATION_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(1);
    private static final long CONNECTOR_HEALTH_AND_CONFIG_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(1);

    private EmbeddedConnectCluster connect;

    @Test
    public void testImmediateRequestForListOfConnectors() throws IOException, InterruptedException {
        // setup Connect worker properties
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put(REST_EXTENSION_CLASSES_CONFIG, IntegrationTestRestExtension.class.getName());

        // build a Connect cluster backed by Kafka and Zk
        connect = new EmbeddedConnectCluster.Builder()
            .name("connect-cluster")
            .numWorkers(NUM_WORKERS)
            .numBrokers(1)
            .workerProps(workerProps)
            .build();

        // start the clusters
        connect.start();

        waitForCondition(
            this::extensionIsRegistered,
            REST_EXTENSION_REGISTRATION_TIMEOUT_MS,
            "REST extension was never registered"
        );
    }

    @Test
    public void testConnectorHealthAndConfig() throws IOException, InterruptedException {
        // setup Connect worker properties
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put(REST_EXTENSION_CLASSES_CONFIG, IntegrationTestRestExtension.class.getName());

        // build a Connect cluster backed by Kafka and Zk
        connect = new EmbeddedConnectCluster.Builder()
            .name("connect-cluster")
            .numWorkers(NUM_WORKERS)
            .numBrokers(1)
            .workerProps(workerProps)
            .build();

        // start the clusters
        connect.start();

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
            connect.configureConnector(connectorHandle.name(), connectorProps);

            // Test the REST extension API; specifically, that the connector's health and configuration
            // is available to the REST extension we registered
            waitForCondition(
                () -> connectorHealthAndConfigIsAvailable(connectorHandle.name()),
                CONNECTOR_HEALTH_AND_CONFIG_TIMEOUT_MS,
                "Connector health and/or config was never accessible by the REST extension"
            );
        } finally {
            RuntimeHandles.get().deleteConnector(connectorHandle.name());
        }
    }

    @After
    public void close() {
        // stop all Connect, Kafka and Zk threads.
        connect.stop();
        IntegrationTestRestExtension.instance = null;
    }

    private boolean extensionIsRegistered() {
        try {
            String extensionUrl = connect.endpointForResource("integration-test-rest-extension/registered");
            return "true".equals(connect.executeGet(extensionUrl));
        } catch (ConnectRestException | IOException e) {
            return false;
        }
    }

    private boolean connectorHealthAndConfigIsAvailable(String connectorName) {
        try {
            ConnectClusterState clusterState =
                IntegrationTestRestExtension.instance.restPluginContext.clusterState();
            return clusterState.connectorHealth(connectorName) != null
                && clusterState.connectorConfig(connectorName) != null;
        } catch (NotFoundException e) {
            // Connector start not yet propagated to the worker that hosts the REST extension we're
            // testing against
            return false;
        }
    }

    public static class IntegrationTestRestExtension implements ConnectRestExtension {
        private static IntegrationTestRestExtension instance;

        public ConnectRestExtensionContext restPluginContext;

        @Override
        public void register(ConnectRestExtensionContext restPluginContext) {
            instance = this;
            this.restPluginContext = restPluginContext;
            restPluginContext.clusterState().connectors();
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
