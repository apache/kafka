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
package org.apache.kafka.common.network;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ConnectionRegistryTest {

    private Metrics metrics;
    private ConnectionRegistry registry;

    @Before
    public void setUp() {
        metrics = new Metrics();
        registry = new ConnectionRegistry(metrics);
    }

    @After
    public void tearDown() {
        registry.close();
        registry = null;
        metrics.close();
        metrics = null;
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testBasicRegistry() {
        KafkaMetric connections = findMetric("Connections", "ClientMetrics");
        KafkaMetric connectedClients = findMetric("ConnectedClients", "ClientMetrics");

        assertNotNull(connections);
        assertNotNull(connectedClients);

        assertTrue(((List<Map<String, String>>) connections.metricValue()).isEmpty());
        assertEquals(0, connectedClients.metricValue());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAddingRemovingConnections() {
        KafkaMetric connections = findMetric("Connections", "ClientMetrics");
        KafkaMetric connectedClients = findMetric("ConnectedClients", "ClientMetrics");

        assertNotNull(connections);
        assertNotNull(connectedClients);

        ConnectionMetadata con1 = registry.register(
            "con1",
            "client1",
            "",
            "",
            ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT,
            InetAddress.getLoopbackAddress(),
            KafkaPrincipal.ANONYMOUS
        );

        assertEquals(con1, registry.get("con1"));

        assertEquals(1, ((List<Map<String, String>>) connections.metricValue()).size());
        assertTrue(((List<Map<String, String>>) connections.metricValue()).get(0).equals(con1.asMap()));
        assertEquals(1, connectedClients.metricValue());

        KafkaMetric clientNameVersion = findMetric("ConnectedClients", "ClientMetrics",
            tags(ConnectionMetadata.UNKNOWN_NAME_OR_VERSION, ConnectionMetadata.UNKNOWN_NAME_OR_VERSION));

        assertNotNull(clientNameVersion);
        assertEquals(1, clientNameVersion.metricValue());

        registry.remove("con1");
        assertEquals(0, clientNameVersion.metricValue());

        // It should have been removed
        clientNameVersion = findMetric("ConnectedClients", "ClientMetrics",
            tags(ConnectionMetadata.UNKNOWN_NAME_OR_VERSION, ConnectionMetadata.UNKNOWN_NAME_OR_VERSION));
        assertNull(clientNameVersion);

        assertTrue(((List<Map<String, String>>) connections.metricValue()).isEmpty());
        assertEquals(0, connectedClients.metricValue());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testUpdateClientSoftwareNameAndVersion() {
        KafkaMetric connections = findMetric("Connections", "ClientMetrics");
        KafkaMetric connectedClients = findMetric("ConnectedClients", "ClientMetrics");

        assertNotNull(connections);
        assertNotNull(connectedClients);

        ConnectionMetadata con1 = registry.register(
            "con1",
            "client1",
            "",
            "",
            ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT,
            InetAddress.getLoopbackAddress(),
            KafkaPrincipal.ANONYMOUS
        );

        assertEquals(con1, registry.get("con1"));

        assertEquals(1, ((List<Map<String, String>>) connections.metricValue()).size());
        assertEquals(1, connectedClients.metricValue());

        KafkaMetric clientNameVersion = findMetric("ConnectedClients", "ClientMetrics",
            tags(ConnectionMetadata.UNKNOWN_NAME_OR_VERSION, ConnectionMetadata.UNKNOWN_NAME_OR_VERSION));

        assertNotNull(clientNameVersion);
        assertEquals(1, clientNameVersion.metricValue());

        registry.updateClientSoftwareNameAndVersion("con1", "name", "version");

        assertEquals(0, clientNameVersion.metricValue());

        // It should have been removed
        clientNameVersion = findMetric("ConnectedClients", "ClientMetrics",
            tags(ConnectionMetadata.UNKNOWN_NAME_OR_VERSION, ConnectionMetadata.UNKNOWN_NAME_OR_VERSION));
        assertNull(clientNameVersion);

        // New one should be there
        clientNameVersion = findMetric("ConnectedClients", "ClientMetrics",
            tags("name", "version"));
        assertNotNull(clientNameVersion);
        assertEquals(1, clientNameVersion.metricValue());

        assertEquals(1, ((List<Map<String, String>>) connections.metricValue()).size());
        assertEquals(1, connectedClients.metricValue());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testClose() {
        KafkaMetric connections = findMetric("Connections", "ClientMetrics");
        KafkaMetric connectedClients = findMetric("ConnectedClients", "ClientMetrics");

        assertNotNull(connections);
        assertNotNull(connectedClients);

        ConnectionMetadata con1 = registry.register(
            "con1",
            "client1",
            "",
            "",
            ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT,
            InetAddress.getLoopbackAddress(),
            KafkaPrincipal.ANONYMOUS
        );

        assertEquals(con1, registry.get("con1"));

        assertEquals(1, ((List<Map<String, String>>) connections.metricValue()).size());
        assertEquals(1, connectedClients.metricValue());

        KafkaMetric clientNameVersion = findMetric("ConnectedClients", "ClientMetrics",
            tags(ConnectionMetadata.UNKNOWN_NAME_OR_VERSION, ConnectionMetadata.UNKNOWN_NAME_OR_VERSION));

        assertNotNull(clientNameVersion);
        assertEquals(1, clientNameVersion.metricValue());

        registry.close();

        // Metrics should be gone
        clientNameVersion = findMetric("ConnectedClients", "ClientMetrics",
            tags(ConnectionMetadata.UNKNOWN_NAME_OR_VERSION, ConnectionMetadata.UNKNOWN_NAME_OR_VERSION));
        connections = findMetric("Connections", "ClientMetrics");
        connectedClients = findMetric("ConnectedClients", "ClientMetrics");
        assertNull(clientNameVersion);
        assertNull(connections);
        assertNull(connectedClients);
    }

    private KafkaMetric findMetric(String name, String group) {
        return findMetric(name, group, emptyTags());
    }

    private KafkaMetric findMetric(String name, String group, Map<String, String> tags) {
        MetricName metricName = new MetricName(name, group, "", tags);
        for (Entry<MetricName, KafkaMetric> entry : metrics.metrics().entrySet()) {
            if (entry.getKey().equals(metricName))
                return entry.getValue();
        }

        return null;
    }

    private Map<String, String> emptyTags() {
        return Collections.emptyMap();
    }

    private Map<String, String> tags(String clientSoftwareName, String clientSoftwareVersion) {
        Map<String, String> tags = new HashMap<>(2);
        tags.put("softwarename", clientSoftwareName);
        tags.put("softwareversion", clientSoftwareVersion);
        return tags;
    }

}
