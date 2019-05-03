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

import static org.apache.kafka.connect.runtime.WorkerConfig.REST_EXTENSION_CLASSES_CONFIG;
import static org.apache.kafka.test.TestUtils.waitForCondition;

/**
 * A simple integration test to ensure that REST extensions are registered correctly.
 */
@Category(IntegrationTest.class)
public class RestExtensionIntegrationTest {

    private static final int NUM_WORKERS = 3;
    private static final long REST_EXTENSION_REGISTRATION_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(1);

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

    @After
    public void close() {
        // stop all Connect, Kafka and Zk threads.
        connect.stop();
    }

    private boolean extensionIsRegistered() {
        try {
            String extensionUrl = connect.endpointForResource("integration-test-rest-extension/registered");
            return "true".equals(connect.executeGet(extensionUrl));
        } catch (ConnectRestException | IOException e) {
            return false;
        }
    }

    public static class IntegrationTestRestExtension implements ConnectRestExtension {

        @Override
        public void register(ConnectRestExtensionContext restPluginContext) {
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
