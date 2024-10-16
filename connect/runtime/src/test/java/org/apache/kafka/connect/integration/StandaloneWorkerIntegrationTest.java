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

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.rest.entities.CreateConnectorRequest;
import org.apache.kafka.connect.runtime.rest.entities.LoggerLevel;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectStandalone;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import jakarta.ws.rs.core.Response;

import static org.apache.kafka.connect.integration.BlockingConnectorTest.Block.BLOCK_CONFIG;
import static org.apache.kafka.connect.integration.BlockingConnectorTest.CONNECTOR_START;
import static org.apache.kafka.connect.integration.BlockingConnectorTest.CONNECTOR_TASK_CONFIGS;
import static org.apache.kafka.connect.integration.MonitorableSourceConnector.TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.NAME_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.DEFAULT_TOPIC_CREATION_PREFIX;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.PARTITIONS_CONFIG;
import static org.apache.kafka.connect.runtime.TopicCreationConfig.REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
public class StandaloneWorkerIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(StandaloneWorkerIntegrationTest.class);

    private static final String CONNECTOR_NAME = "test-connector";
    private static final int NUM_TASKS = 4;
    private static final String TOPIC_NAME = "test-topic";

    private EmbeddedConnectStandalone.Builder connectBuilder;
    private EmbeddedConnectStandalone connect;

    @BeforeEach
    public void setup() {
        connectBuilder = new EmbeddedConnectStandalone.Builder();
    }

    @AfterEach
    public void cleanup() {
        // Unblock everything so that we don't leak threads even if a test run fails
        BlockingConnectorTest.Block.reset();

        if (connect != null)
            connect.stop();

        BlockingConnectorTest.Block.join();
    }

    @Test
    public void testDynamicLogging() {
        connect = connectBuilder.build();
        connect.start();

        Map<String, LoggerLevel> initialLevels = connect.allLogLevels();
        assertFalse(initialLevels.isEmpty(), "Connect REST API did not list any known loggers");
        Map<String, LoggerLevel> invalidModifiedLoggers = Utils.filterMap(
                initialLevels,
                StandaloneWorkerIntegrationTest::isModified
        );
        assertEquals(
                Collections.emptyMap(),
                invalidModifiedLoggers,
                "No loggers should have a non-null last-modified timestamp"
        );

        // Tests with no scope
        // The current level may match the first level we set the namespace to,
        // so we issue a preliminary request with a different level to guarantee that a
        // change takes place and that the last modified timestamp should be non-null
        final String namespace1 = "org.apache.kafka.connect";
        final String level1 = "DEBUG";
        connect.setLogLevel(namespace1, "ERROR", null);
        Map<String, LoggerLevel> currentLevels = testSetLoggingLevel(namespace1, level1, null, initialLevels);

        // Tests with scope=worker
        final String namespace2 = "org.apache.kafka.clients";
        final String level2 = "INFO";
        connect.setLogLevel(namespace2, "WARN", "worker");
        currentLevels = testSetLoggingLevel(namespace2, level2, "worker", currentLevels);

        LoggerLevel priorLoggerLevel = connect.getLogLevel(namespace2);
        connect.setLogLevel(namespace2, level2, "worker");
        LoggerLevel currentLoggerLevel = connect.getLogLevel(namespace2);
        assertEquals(
                priorLoggerLevel,
                currentLoggerLevel,
                "Log level and last-modified timestamp should not be affected by consecutive identical requests"
        );

        // Tests with scope=cluster
        final String namespace3 = "org.apache.kafka.streams";
        final String level3 = "TRACE";
        connect.setLogLevel(namespace3, "DEBUG", "cluster");
        testSetLoggingLevel(namespace3, level3, "cluster", currentLevels);
    }

    private Map<String, LoggerLevel> testSetLoggingLevel(
            String namespace,
            String level,
            String scope,
            Map<String, LoggerLevel> initialLevels
    ) {
        long requestTime = System.currentTimeMillis();
        List<String> affectedLoggers = connect.setLogLevel(namespace, level, scope);
        if ("cluster".equals(scope)) {
            assertNull(
                    affectedLoggers,
                    "Modifying log levels with scope=cluster should result in an empty response"
            );
        } else {
            assertTrue(affectedLoggers.contains(namespace));
            List<String> invalidAffectedLoggers = affectedLoggers.stream()
                    .filter(l -> !l.startsWith(namespace))
                    .collect(Collectors.toList());
            assertEquals(
                    Collections.emptyList(),
                    invalidAffectedLoggers,
                    "No loggers outside the namespace '" + namespace
                            + "' should have been included in the response for a request to modify that namespace"
            );
        }

        // Verify the information for this single logger

        LoggerLevel loggerLevel = connect.getLogLevel(namespace);
        assertNotNull(loggerLevel);
        assertEquals(level, loggerLevel.level());
        assertNotNull(loggerLevel.lastModified());
        assertTrue(
                loggerLevel.lastModified() >= requestTime,
                "Last-modified timestamp for logger level is " + loggerLevel.lastModified()
                        + ", which is before " + requestTime + ", the most-recent time the level was adjusted"
        );

        // Verify information for all listed loggers

        Map<String, LoggerLevel> newLevels = connect.allLogLevels();

        Map<String, LoggerLevel> invalidAffectedLoggerLevels = Utils.filterMap(
                newLevels,
                e -> hasNamespace(e, namespace)
                        && (!level(e).equals(level)
                            || !isModified(e)
                            || lastModified(e) < requestTime
                        )
        );
        assertEquals(
                Collections.emptyMap(),
                invalidAffectedLoggerLevels,
                "At least one logger in the affected namespace '" + namespace
                        + "' does not have the expected level of '" + level
                        + "', has a null last-modified timestamp, or has a last-modified timestamp "
                        + "that is less recent than " + requestTime
                        + ", which is when the namespace was last adjusted"
        );

        Set<String> droppedLoggers = Utils.diff(HashSet::new, initialLevels.keySet(), newLevels.keySet());
        assertEquals(
                Collections.emptySet(),
                droppedLoggers,
                "At least one logger was present in the listing of all loggers "
                        + "before the logging level for namespace '" + namespace
                        + "' was set to '" + level
                        + "' that is no longer present"
        );

        Map<String, LoggerLevel> invalidUnaffectedLoggerLevels = Utils.filterMap(
                newLevels,
                e -> !hasNamespace(e, namespace) && !e.getValue().equals(initialLevels.get(e.getKey()))
        );
        assertEquals(
                Collections.emptyMap(),
                invalidUnaffectedLoggerLevels,
                "At least one logger outside of the affected namespace '" + namespace
                        + "' has a different logging level or last-modified timestamp than it did "
                        + "before the namespace was set to level '" + level
                        + "'; none of these loggers should have been affected"
        );

        return newLevels;
    }

    private static boolean hasNamespace(Map.Entry<String, ?> entry, String namespace) {
        return entry.getKey().startsWith(namespace);
    }

    private static boolean isModified(Map.Entry<?, LoggerLevel> entry) {
        return lastModified(entry) != null;
    }

    private static Long lastModified(Map.Entry<?, LoggerLevel> entry) {
        return entry.getValue().lastModified();
    }

    private static String level(Map.Entry<?, LoggerLevel> entry) {
        return entry.getValue().level();
    }

    @Test
    public void testCreateConnectorWithStoppedInitialState() throws Exception {
        connect = connectBuilder.build();
        connect.start();

        CreateConnectorRequest createConnectorRequest = new CreateConnectorRequest(
            CONNECTOR_NAME,
            defaultSourceConnectorProps(TOPIC_NAME),
            CreateConnectorRequest.InitialState.STOPPED
        );
        connect.configureConnector(createConnectorRequest);

        // Verify that the connector's status is STOPPED and also that no tasks were spawned for the connector
        connect.assertions().assertConnectorIsStopped(
            CONNECTOR_NAME,
            "Connector was not created in a stopped state"
        );
        assertEquals(Collections.emptyList(), connect.connectorInfo(CONNECTOR_NAME).tasks());
        assertEquals(Collections.emptyList(), connect.taskConfigs(CONNECTOR_NAME));

        // Verify that a connector created in the STOPPED state can be resumed successfully
        connect.resumeConnector(CONNECTOR_NAME);
        connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
            CONNECTOR_NAME,
            NUM_TASKS,
            "Connector or tasks did not start running healthily in time"
        );
    }

    @Test
    public void testHealthCheck() throws Exception {
        int numTasks = 1; // The blocking connector only generates a single task anyways

        Map<String, String> blockedConnectorConfig = new HashMap<>();
        blockedConnectorConfig.put(CONNECTOR_CLASS_CONFIG, BlockingConnectorTest.BlockingConnector.class.getName());
        blockedConnectorConfig.put(NAME_CONFIG, CONNECTOR_NAME);
        blockedConnectorConfig.put(TASKS_MAX_CONFIG, Integer.toString(numTasks));
        blockedConnectorConfig.put(BLOCK_CONFIG, CONNECTOR_START);

        connect = connectBuilder.withCommandLineConnector(blockedConnectorConfig).build();
        Thread workerThread = new Thread(connect::start);
        workerThread.setName("integration-test-standalone-connect-worker");
        try {
            workerThread.start();

            AtomicReference<Response> healthCheckResponse = new AtomicReference<>();
            connect.requestTimeout(1_000);
            waitForCondition(
                    () -> {
                        Response response = connect.healthCheck();
                        healthCheckResponse.set(response);
                        return true;
                    },
                    60_000,
                    "Health check endpoint for standalone worker was not available in time"
            );
            connect.resetRequestTimeout();

            // Worker hasn't completed startup; should be serving 503 responses from the health check endpoint
            try (Response response = healthCheckResponse.get()) {
                assertEquals(Response.Status.SERVICE_UNAVAILABLE.getStatusCode(), response.getStatus());
                assertNotNull(response.getEntity());
                String body = response.getEntity().toString();
                assertTrue(
                        body.contains("Worker is still starting up"),
                        "Body did not contain expected message: " + body
                );
            }

            BlockingConnectorTest.Block.reset();

            // Worker has completed startup by this point; should serve 200 responses
            try (Response response = connect.healthCheck()) {
                assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
                assertNotNull(response.getEntity());
                String body = response.getEntity().toString();
                assertTrue(
                        body.contains("Worker has completed startup and is ready to handle requests."),
                        "Body did not contain expected message: " + body
                );
            }

            // And, if the worker claims that it's healthy, it should have also been able to generate tasks
            // for the now-unblocked connector
            connect.assertions().assertConnectorAndExactlyNumTasksAreRunning(
                    CONNECTOR_NAME,
                    numTasks,
                    "Connector or tasks did not start running healthily in time"
            );

            // Hack: if a connector blocks in its taskConfigs method, then the worker gets blocked
            // If that bug is ever fixed, we can remove this section from the test (it's not worth keeping
            // the bug just for the coverage it provides)
            blockedConnectorConfig.put(BLOCK_CONFIG, CONNECTOR_TASK_CONFIGS);
            connect.requestTimeout(1_000);
            assertThrows(
                    ConnectRestException.class,
                    () -> connect.configureConnector(CONNECTOR_NAME, blockedConnectorConfig)
            );

            // Worker has completed startup but is now blocked; should serve 500 responses
            try (Response response = connect.healthCheck()) {
                assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
                assertNotNull(response.getEntity());
                String body = response.getEntity().toString();
                assertTrue(
                        body.contains("Worker was unable to handle this request and may be unable to handle other requests."),
                        "Body did not contain expected message: " + body
                );
            }
            connect.resetRequestTimeout();

            BlockingConnectorTest.Block.reset();

            connect.deleteConnector(CONNECTOR_NAME);
        } finally {
            if (workerThread.isAlive()) {
                log.debug("Standalone worker startup not completed yet; interrupting and waiting for startup to finish");
                workerThread.interrupt();
                workerThread.join(TimeUnit.MINUTES.toMillis(1));
                if (workerThread.isAlive()) {
                    log.warn("Standalone worker startup never completed; abandoning thread");
                }
            }
        }
    }

    private Map<String, String> defaultSourceConnectorProps(String topic) {
        // setup props for the source connector
        Map<String, String> props = new HashMap<>();
        props.put(NAME_CONFIG, CONNECTOR_NAME);
        props.put(CONNECTOR_CLASS_CONFIG, MonitorableSourceConnector.class.getSimpleName());
        props.put(TASKS_MAX_CONFIG, String.valueOf(NUM_TASKS));
        props.put(TOPIC_CONFIG, topic);
        props.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + REPLICATION_FACTOR_CONFIG, String.valueOf(1));
        props.put(DEFAULT_TOPIC_CREATION_PREFIX + PARTITIONS_CONFIG, String.valueOf(1));
        return props;
    }
}
