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

import org.apache.kafka.connect.runtime.distributed.ConnectProtocolCompatibility;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static jakarta.ws.rs.core.Response.Status.BAD_REQUEST;
import static jakarta.ws.rs.core.Response.Status.FORBIDDEN;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONNECTOR_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TASKS_MAX_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.CONNECT_PROTOCOL_CONFIG;
import static org.apache.kafka.connect.runtime.rest.InternalRequestSignature.SIGNATURE_ALGORITHM_HEADER;
import static org.apache.kafka.connect.runtime.rest.InternalRequestSignature.SIGNATURE_HEADER;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A simple integration test to ensure that internal request validation becomes enabled with the
 * "sessioned" protocol.
 */
@Tag("integration")
public class SessionedProtocolIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(SessionedProtocolIntegrationTest.class);

    private static final String CONNECTOR_NAME = "connector";
    private static final long CONNECTOR_SETUP_DURATION_MS = 60000;

    private EmbeddedConnectCluster connect;
    private ConnectorHandle connectorHandle;

    @BeforeEach
    public void setup() {
        // setup Connect worker properties
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put(CONNECT_PROTOCOL_CONFIG, ConnectProtocolCompatibility.SESSIONED.protocol());

        // build a Connect cluster backed by a Kafka KRaft cluster
        connect = new EmbeddedConnectCluster.Builder()
            .name("connect-cluster")
            .numWorkers(2)
            .numBrokers(1)
            .workerProps(workerProps)
            .build();

        // start the clusters
        connect.start();

        // get a handle to the connector
        connectorHandle = RuntimeHandles.get().connectorHandle(CONNECTOR_NAME);
    }

    @AfterEach
    public void close() {
        // stop the Connect cluster and its backing Kafka cluster.
        connect.stop();
    }

    @Test
    public void ensureInternalEndpointIsSecured() throws Throwable {
        final String connectorTasksEndpoint = connect.endpointForResource(String.format(
            "connectors/%s/tasks",
            CONNECTOR_NAME
        ));
        final Map<String, String> emptyHeaders = new HashMap<>();
        final Map<String, String> invalidSignatureHeaders = new HashMap<>();
        invalidSignatureHeaders.put(SIGNATURE_HEADER, "S2Fma2Flc3F1ZQ==");
        invalidSignatureHeaders.put(SIGNATURE_ALGORITHM_HEADER, "HmacSHA256");

        // We haven't created the connector yet, but this should still return a 400 instead of a 404
        // if the endpoint is secured
        log.info(
            "Making a POST request to the {} endpoint with no connector started and no signature header; " 
                + "expecting 400 error response",
            connectorTasksEndpoint
        );
        assertEquals(
            BAD_REQUEST.getStatusCode(),
            connect.requestPost(connectorTasksEndpoint, "[]", emptyHeaders).getStatus()
        );

        // Try again, but with an invalid signature
        log.info(
            "Making a POST request to the {} endpoint with no connector started and an invalid signature header; "
                + "expecting 403 error response",
            connectorTasksEndpoint
        );
        assertEquals(
                FORBIDDEN.getStatusCode(),
                connect.requestPost(connectorTasksEndpoint, "[]", invalidSignatureHeaders).getStatus()
        );

        // Create the connector now
        // setup up props for the sink connector
        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put(CONNECTOR_CLASS_CONFIG, MonitorableSinkConnector.class.getSimpleName());
        connectorProps.put(TASKS_MAX_CONFIG, String.valueOf(1));
        connectorProps.put(TOPICS_CONFIG, "test-topic");
        connectorProps.put(KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        connectorProps.put(VALUE_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());

        // start a sink connector
        log.info("Starting the {} connector", CONNECTOR_NAME);
        StartAndStopLatch startLatch = connectorHandle.expectedStarts(1);
        connect.configureConnector(CONNECTOR_NAME, connectorProps);
        startLatch.await(CONNECTOR_SETUP_DURATION_MS, TimeUnit.MILLISECONDS);


        // Verify the exact same behavior, after starting the connector

        // We haven't created the connector yet, but this should still return a 400 instead of a 404
        // if the endpoint is secured
        log.info(
            "Making a POST request to the {} endpoint with the connector started and no signature header; "
                + "expecting 400 error response",
            connectorTasksEndpoint
        );
        assertEquals(
            BAD_REQUEST.getStatusCode(),
            connect.requestPost(connectorTasksEndpoint, "[]", emptyHeaders).getStatus()
        );

        // Try again, but with an invalid signature
        log.info(
            "Making a POST request to the {} endpoint with the connector started and an invalid signature header; "
                + "expecting 403 error response",
            connectorTasksEndpoint
        );
        assertEquals(
            FORBIDDEN.getStatusCode(),
            connect.requestPost(connectorTasksEndpoint, "[]", invalidSignatureHeaders).getStatus()
        );
    }
}
