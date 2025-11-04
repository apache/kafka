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
package org.apache.kafka.server.metrics;

import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.telemetry.ClientTelemetry;
import org.apache.kafka.server.telemetry.ClientTelemetryContext;
import org.apache.kafka.server.telemetry.ClientTelemetryExporter;
import org.apache.kafka.server.telemetry.ClientTelemetryExporterProvider;
import org.apache.kafka.server.telemetry.ClientTelemetryPayload;
import org.apache.kafka.server.telemetry.ClientTelemetryReceiver;
import org.apache.kafka.test.TestUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

public class ClientMetricsTestUtils {

    public static final String METRICS_TEST_DEFAULT =
        "org.apache.kafka.client.producer.partition.queue.,org.apache.kafka.client.producer.partition.latency";
    public static final int INTERVAL_MS_TEST_DEFAULT = 30 * 1000; // 30 seconds
    public static final List<String> MATCH_TEST_DEFAULT = List.of(
        ClientMetricsConfigs.CLIENT_SOFTWARE_NAME + "=apache-kafka-java",
        ClientMetricsConfigs.CLIENT_SOFTWARE_VERSION + "=3.5.*"
    );
    public static final int CLIENT_PORT = 56078;

    public static Properties defaultTestProperties() {
        Properties props = new Properties();
        props.put(ClientMetricsConfigs.METRICS_CONFIG, METRICS_TEST_DEFAULT);
        props.put(ClientMetricsConfigs.INTERVAL_MS_CONFIG, Integer.toString(INTERVAL_MS_TEST_DEFAULT));
        props.put(ClientMetricsConfigs.MATCH_CONFIG, String.join(",", MATCH_TEST_DEFAULT));
        return props;
    }

    public static RequestContext requestContext() throws UnknownHostException {
        return new RequestContext(
            new RequestHeader(ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS, (short) 0, "producer-1", 0),
            TestUtils.randomString(5),
            InetAddress.getLocalHost(),
            Optional.of(CLIENT_PORT),
            KafkaPrincipal.ANONYMOUS,
            ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT,
            new ClientInformation("apache-kafka-java", "3.5.2"),
            false);
    }

    public static RequestContext requestContextWithNullClientInfo() throws UnknownHostException {
        return new RequestContext(
            new RequestHeader(ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS, (short) 0, "producer-1", 0),
             "1",
            InetAddress.getLocalHost(),
            Optional.of(CLIENT_PORT),
            KafkaPrincipal.ANONYMOUS,
            ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT,
            null,
            false);
    }

    public static RequestContext requestContextWithConnectionId(String connectionId) throws UnknownHostException {
        return new RequestContext(
            new RequestHeader(ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS, (short) 0, "producer-1", 0),
            connectionId,
            InetAddress.getLocalHost(),
            Optional.of(CLIENT_PORT),
            KafkaPrincipal.ANONYMOUS,
            ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT,
            new ClientInformation("apache-kafka-java", "3.5.2"),
            false);
    }

    @SuppressWarnings("deprecation")
    public static class TestClientMetricsReceiver implements ClientTelemetryReceiver {
        public int exportMetricsInvokedCount = 0;
        public List<ByteBuffer> metricsData = new ArrayList<>();

        public void exportMetrics(AuthorizableRequestContext context, ClientTelemetryPayload payload) {
            exportMetricsInvokedCount += 1;
            metricsData.add(payload.data());
        }
    }

    public static class TestClientTelemetryExporter implements ClientTelemetryExporter {
        public int exportMetricsInvokedCount = 0;
        public List<ByteBuffer> metricsData = new ArrayList<>();
        public List<Integer> pushIntervals = new ArrayList<>();

        @Override
        public void exportMetrics(ClientTelemetryContext context, ClientTelemetryPayload payload) {
            exportMetricsInvokedCount += 1;
            metricsData.add(payload.data());
            pushIntervals.add(context.pushIntervalMs());
        }
    }

    /**
     * Test implementation that supports both deprecated and new interfaces.
     * When both are implemented, only the new interface should be used.
     */
    @SuppressWarnings("deprecation")
    public static class TestDualImplementation implements ClientTelemetry, ClientTelemetryExporterProvider {
        private final TestClientMetricsReceiver receiver;
        private final TestClientTelemetryExporter exporter;

        public TestDualImplementation() {
            this.receiver = new TestClientMetricsReceiver();
            this.exporter = new TestClientTelemetryExporter();
        }

        @Override
        public ClientTelemetryReceiver clientReceiver() {
            return receiver;
        }

        @Override
        public ClientTelemetryExporter clientTelemetryExporter() {
            return exporter;
        }

        public TestClientMetricsReceiver getReceiver() {
            return receiver;
        }

        public TestClientTelemetryExporter getExporter() {
            return exporter;
        }
    }
}
