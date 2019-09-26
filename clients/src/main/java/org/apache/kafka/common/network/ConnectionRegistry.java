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

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;

public class ConnectionRegistry {
    private static final String METRICS_GROUP_NAME = "ClientMetrics";

    private static class CounterMetric implements Gauge<Integer> {
        private MetricName name;
        private int count;

        CounterMetric(MetricName name) {
            this.name = name;
        }

        MetricName name() {
            return name;
        }

        int inc() {
            count += 1;
            return count;
        }

        int dec() {
            count -= 1;
            return count;
        }

        @Override
        public Integer value(final MetricConfig config, final long now) {
            return count;
        }

    }

    private final Metrics metrics;
    private final String metricsGroup;
    private final MetricName connectionsMetricName;
    private final MetricName connectedClientsMetricName;

    private final Map<String, CounterMetric> counters;
    private final Map<String, ConnectionMetadata> connections;

    public ConnectionRegistry(Metrics metrics, String metricsGroup) {
        this.metrics = metrics;
        this.metricsGroup = metricsGroup;
        this.counters = new HashMap<>();
        this.connections = new HashMap<>();

        this.connectionsMetricName =
            new MetricName("Connections", metricsGroup,
                "List of connected clients with detailed metadata.",
                Collections.emptyMap());
        this.connectedClientsMetricName =
            new MetricName("ConnectedClients", metricsGroup,
                "Number of connected clients.",
                Collections.emptyMap());

        metrics.addMetric(connectionsMetricName,
            new Gauge<List<Map<String, String>>>() {
                @Override
                public List<Map<String, String>> value(final MetricConfig config, final long now) {
                    List<Map<String, String>> list = new LinkedList<>();
                    for (ConnectionMetadata client : connections.values()) {
                        list.add(client.asMap());
                    }
                    return list;
                }
            }
        );

        metrics.addMetric(connectedClientsMetricName,
            new Gauge<Integer>() {
                @Override
                public Integer value(final MetricConfig config, final long now) {
                    return connections.size();
                }
            }
        );
    }

    public ConnectionRegistry(Metrics metrics) {
        this(metrics, METRICS_GROUP_NAME);
    }

    public synchronized ConnectionMetadata register(
            String connectionId,
            String clientId,
            String clientSoftwareName,
            String clientSoftwareVersion,
            ListenerName listenerName,
            SecurityProtocol securityProtocol,
            InetAddress clientAddress,
            KafkaPrincipal principal) {

        ConnectionMetadata connection = new ConnectionMetadata(
            clientId,
            clientSoftwareName,
            clientSoftwareVersion,
            listenerName,
            securityProtocol,
            clientAddress,
            principal
        );

        connections.put(connectionId, connection);

        incCounterFor(connection.clientSoftwareName(), connection.clientSoftwareVersion());

        return connection;
    }

    public synchronized ConnectionMetadata updateClientSoftwareNameAndVersion(
            String connectionId,
            String clientSoftwareName,
            String clientSoftwareVersion) {

        ConnectionMetadata connection = connections.get(connectionId);

        if (connection != null) {
            decCounterFor(connection.clientSoftwareName(), connection.clientSoftwareVersion());
            connection.clientSoftwareName = clientSoftwareName;
            connection.clientSoftwareVersion = clientSoftwareVersion;
            incCounterFor(connection.clientSoftwareName(), connection.clientSoftwareVersion());
        }

        return connection;
    }

    public synchronized void remove(String connectionId) {
        ConnectionMetadata connection = connections.remove(connectionId);

        if (connection != null) {
            decCounterFor(connection.clientSoftwareName(), connection.clientSoftwareVersion());
        }
    }

    public synchronized void close() {
        for (CounterMetric counter : counters.values()) {
            metrics.removeMetric(counter.name());
        }
        metrics.removeMetric(connectedClientsMetricName);
        metrics.removeMetric(connectionsMetricName);
        counters.clear();
        connections.clear();
    }

    public ConnectionMetadata get(String connectionId) {
        return connections.get(connectionId);
    }

    private void incCounterFor(String clientSoftwareName, String clientSoftwareVersion) {
        String counterId = counterId(clientSoftwareName, clientSoftwareVersion);

        CounterMetric counter = counters.get(counterId);
        if (counter == null) {
            MetricName name = metricNameFor(clientSoftwareName, clientSoftwareVersion);
            counter = new CounterMetric(name);
            counters.put(counterId, counter);
            metrics.addMetric(name, counter);
        }

        counter.inc();
    }

    private void decCounterFor(String clientSoftwareName, String clientSoftwareVersion) {
        String counterId = counterId(clientSoftwareName, clientSoftwareVersion);

        CounterMetric counter = counters.get(counterId);
        if (counter != null) {
            if (counter.dec() == 0) {
                metrics.removeMetric(counter.name());
                counters.remove(counterId);
            }
        }
    }

    private String counterId(String clientSoftwareName, String clientSoftwareVersion) {
        return clientSoftwareName + ":" + clientSoftwareVersion;
    }

    private MetricName metricNameFor(String clientSoftwareName, String clientSoftwareVersion) {
        Map<String, String> tags = new HashMap<>(2);
        tags.put("softwarename", clientSoftwareName);
        tags.put("softwareversion", clientSoftwareVersion);
        return new MetricName("ConnectedClients", metricsGroup,
            String.format(
                "Number of connected clients with ClientSoftwareName={} and ClientSoftwareVersion={}",
                clientSoftwareName, clientSoftwareVersion),
            tags);
    }
}
